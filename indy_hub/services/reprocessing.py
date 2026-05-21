"""Helpers for Reprocessing Services workflow."""

from __future__ import annotations

# Standard Library
from decimal import Decimal, ROUND_FLOOR
from functools import lru_cache
import math
import re
from typing import Iterable

# Django
from django.db import connection
from django.utils import timezone

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

# Local
from indy_hub.services.esi_client import shared_client
from indy_hub.services.fuzzwork import FuzzworkError, fetch_fuzzwork_prices
from indy_hub.utils.eve import get_type_name

logger = get_extension_logger(__name__)

REPROCESSING_SKILLS_SCOPE = "esi-skills.read_skills.v1"
REPROCESSING_CLONES_SCOPE = "esi-clones.read_clones.v1"

# Core reprocessing skills
REPROCESSING_SKILL_TYPE_IDS = {
    "reprocessing": 3385,
    "reprocessing_efficiency": 3389,
    "scrapmetal_processing": 12196,
}

# Athanor / Tatara
ATHANOR_TYPE_IDS = {35835}
TATARA_TYPE_IDS = {35836}
SUPPORTED_STRUCTURE_TYPE_IDS = ATHANOR_TYPE_IDS | TATARA_TYPE_IDS

STRUCTURE_BONUS_BY_TYPE_ID: dict[int, Decimal] = {
    35835: Decimal("0.020"),  # Athanor structure modifier (Sm)
    35836: Decimal("0.055"),  # Tatara structure modifier (Sm)
}

STRUCTURE_LABEL_BY_TYPE_ID: dict[int, str] = {
    35835: "Athanor",
    35836: "Tatara",
}

REPROCESSING_RIG_PROFILES: list[dict[str, str | Decimal]] = [
    {
        "key": "none",
        "label": "No Reprocessing Rig",
        "bonus_percent": Decimal("0.000"),  # Rm
        "effect_label": "No rig modifier",
    },
    {
        "key": "ore_t1",
        "label": "Standup M-Set Ore Grading Processor I",
        "bonus_percent": Decimal("1.000"),  # Rm
        "effect_label": "Rm +1 (ore)",
    },
    {
        "key": "ore_t2",
        "label": "Standup M-Set Ore Grading Processor II",
        "bonus_percent": Decimal("3.000"),  # Rm
        "effect_label": "Rm +3 (ore)",
    },
    {
        "key": "moon_t1",
        "label": "Standup M-Set Moon Ore Grading Processor I",
        "bonus_percent": Decimal("1.000"),  # Rm
        "effect_label": "Rm +1 (moon ore)",
    },
    {
        "key": "moon_t2",
        "label": "Standup M-Set Moon Ore Grading Processor II",
        "bonus_percent": Decimal("3.000"),  # Rm
        "effect_label": "Rm +3 (moon ore)",
    },
]

_BEANCOUNTER_PATTERN_BONUSES: list[tuple[re.Pattern[str], Decimal]] = [
    (re.compile(r"rx-801", re.IGNORECASE), Decimal("1.000")),
    (re.compile(r"rx-802", re.IGNORECASE), Decimal("2.000")),
    (re.compile(r"rx-804", re.IGNORECASE), Decimal("4.000")),
    (re.compile(r"beancounter.*(?:\+|\s)(1(?:\\.0+)?)%"), Decimal("1.000")),
    (re.compile(r"beancounter.*(?:\+|\s)(2(?:\\.0+)?)%"), Decimal("2.000")),
    (re.compile(r"beancounter.*(?:\+|\s)(4(?:\\.0+)?)%"), Decimal("4.000")),
]


def _coerce_mapping(payload: object) -> dict:
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return payload
    for attr in ("model_dump", "dict", "to_dict"):
        converter = getattr(payload, attr, None)
        if callable(converter):
            try:
                result = converter()
            except Exception:
                result = None
            if isinstance(result, dict):
                return result
    try:
        return dict(payload)
    except Exception:
        return {}


def _get_operation(resource_name: str, snake_name: str, camel_name: str):
    resource = getattr(shared_client.client, resource_name, None)
    if resource is None:
        return None
    operation = getattr(resource, snake_name, None) or getattr(resource, camel_name, None)
    return operation if callable(operation) else None


def _to_decimal(value) -> Decimal:
    try:
        return Decimal(str(value or 0))
    except Exception:
        return Decimal("0")


def _normalize_structure_modifier(raw_value: Decimal) -> Decimal:
    # Backwards compatibility for earlier snapshots that stored 2/4.
    if raw_value == Decimal("2"):
        return Decimal("0.020")
    if raw_value == Decimal("4"):
        return Decimal("0.055")
    if raw_value >= Decimal("1"):
        return raw_value / Decimal("100")
    return raw_value


def _normalize_rig_modifier(raw_value: Decimal) -> Decimal:
    # Backwards compatibility for earlier snapshots that stored 2/4.
    if raw_value == Decimal("2"):
        return Decimal("1.000")
    if raw_value == Decimal("4"):
        return Decimal("3.000")
    return raw_value


def infer_security_modifier(security_status: Decimal) -> Decimal:
    """Convert a system security status to Sec modifier from the Upwell formula."""
    value = _to_decimal(security_status)
    if value <= Decimal("0"):
        return Decimal("0.120")
    if value < Decimal("0.5"):
        return Decimal("0.060")
    return Decimal("0.000")


def _is_beancounter_implant(name: str) -> bool:
    normalized = str(name or "").lower()
    return "beancounter" in normalized or "rx-80" in normalized


def infer_beancounter_bonus_percent(implant_names: Iterable[str]) -> Decimal:
    best = Decimal("0.000")
    for implant_name in implant_names or []:
        text = str(implant_name or "")
        if not text:
            continue
        for pattern, bonus in _BEANCOUNTER_PATTERN_BONUSES:
            if pattern.search(text):
                if bonus > best:
                    best = bonus
                break
    return best


def fetch_character_skill_levels(
    character_id: int,
    *,
    force_refresh: bool = False,
) -> dict[int, dict[str, int]]:
    """Return character skills as {skill_id: {'active': int, 'trained': int}}."""
    _ = force_refresh
    return _fetch_corptools_skill_levels(int(character_id))


def _resolve_location_names(location_ids: list[int]) -> dict[int, str]:
    location_ids = [int(x) for x in location_ids if int(x) > 0]
    if not location_ids:
        return {}
    try:
        return shared_client.resolve_ids_to_names(location_ids)
    except Exception:
        return {}


def _get_corptools_character_audit(character_id: int):
    try:
        # AA Example App
        from corptools.models.audits import CharacterAudit
        # Django
        from django.db.models import Q
    except Exception:
        return None
    try:
        return (
            CharacterAudit.objects.select_related("character")
            .filter(
                Q(character__character_id=int(character_id))
                | Q(character_id=int(character_id))
            )
            .first()
        )
    except Exception:
        return None


def _fetch_corptools_skill_levels(character_id: int) -> dict[int, dict[str, int]]:
    audit = _get_corptools_character_audit(int(character_id))
    if audit is None:
        return {}
    try:
        # AA Example App
        from corptools.models.skills import Skill
    except Exception:
        return {}
    try:
        rows = Skill.objects.filter(character=audit).values_list(
            "skill_id",
            "active_skill_level",
            "trained_skill_level",
        )
    except Exception:
        return {}

    levels: dict[int, dict[str, int]] = {}
    for skill_id, active_skill_level, trained_skill_level in rows:
        try:
            sid = int(skill_id)
        except (TypeError, ValueError):
            continue
        if sid <= 0:
            continue
        levels[sid] = {
            "active": int(active_skill_level or 0),
            "trained": int(trained_skill_level or 0),
        }
    return levels


def _fetch_corptools_clone_options(character_id: int) -> list[dict[str, object]]:
    audit = _get_corptools_character_audit(int(character_id))
    if audit is None:
        return []
    try:
        # AA Example App
        from corptools.models.clones import Implant, JumpClone
    except Exception:
        return []

    try:
        clones = list(
            JumpClone.objects.filter(character=audit)
            .select_related("location_name")
            .order_by("id")
        )
    except Exception:
        return []
    if not clones:
        return []

    implants_by_clone: dict[int, list[tuple[int, str]]] = {}
    try:
        implants = list(
            Implant.objects.filter(clone__in=clones).select_related("type_name")
        )
    except Exception:
        implants = []
    for implant in implants:
        try:
            clone_id_int = int(getattr(implant, "clone_id", 0) or 0)
            type_id_int = int(getattr(implant, "type_name_id", 0) or 0)
        except (TypeError, ValueError):
            continue
        if clone_id_int <= 0 or type_id_int <= 0:
            continue
        type_name_obj = getattr(implant, "type_name", None)
        type_name = ""
        for attr in ("name", "name_en", "name_en_us", "type_name"):
            type_name = str(getattr(type_name_obj, attr, "") or "").strip()
            if type_name:
                break
        if not type_name:
            type_name = str(get_type_name(type_id_int) or "")
        implants_by_clone.setdefault(clone_id_int, []).append((type_id_int, type_name))

    clone_rows: list[dict[str, object]] = []
    for idx, clone in enumerate(clones, start=1):
        try:
            row_clone_id = int(getattr(clone, "jump_clone_id", None) or 0)
        except (TypeError, ValueError):
            row_clone_id = 0
        try:
            location_id = int(getattr(clone, "location_id", None) or 0)
        except (TypeError, ValueError):
            location_id = 0

        if row_clone_id <= 0:
            row_clone_id = location_id if location_id > 0 else int(getattr(clone, "id", 0) or 0)

        location_name = ""
        location_obj = getattr(clone, "location_name", None)
        if location_obj is not None:
            location_name = str(getattr(location_obj, "location_name", "") or "").strip()

        implant_pairs = implants_by_clone.get(int(getattr(clone, "id", 0) or 0), [])
        implant_type_ids = [type_id for type_id, _ in implant_pairs if type_id > 0]
        implant_names = [name for _type_id, name in implant_pairs if name]
        beancounter_names = [n for n in implant_names if _is_beancounter_implant(n)]
        beancounter_bonus = infer_beancounter_bonus_percent(beancounter_names)

        clone_name = str(getattr(clone, "name", "") or "").strip()
        label = clone_name or f"Clone {idx}"
        if location_name:
            label = f"{label} - {location_name}"

        clone_rows.append(
            {
                "clone_id": int(row_clone_id or 0),
                "location_id": location_id if location_id > 0 else None,
                "implant_type_ids": implant_type_ids,
                "implant_names": implant_names,
                "beancounter_implants": beancounter_names,
                "beancounter_bonus_percent": beancounter_bonus,
                "clone_label": label,
                "location_name": location_name,
            }
        )

    return clone_rows


def fetch_character_clone_options(
    character_id: int,
    *,
    force_refresh: bool = False,
) -> list[dict[str, object]]:
    """Return clone options with implant names and inferred beancounter bonuses."""
    _ = force_refresh
    clone_rows = _fetch_corptools_clone_options(int(character_id))
    if not clone_rows:
        # Keep one selectable fallback option to avoid blocking signup UX.
        clone_rows = [
            {
                "clone_id": 0,
                "clone_label": "Active Clone",
                "location_id": None,
                "location_name": "",
                "implant_type_ids": [],
                "implant_names": [],
                "beancounter_implants": [],
                "beancounter_bonus_percent": Decimal("0.000"),
            }
        ]
    return clone_rows


def build_reprocessing_skill_snapshot(
    skill_levels: dict[int, dict[str, int]],
) -> dict[str, int]:
    """Extract core reprocessing-relevant skill levels from an ESI skill payload."""

    def _active(skill_id: int) -> int:
        entry = skill_levels.get(int(skill_id), {})
        if isinstance(entry, dict):
            return int(entry.get("active") or 0)
        return int(entry or 0)

    processing_level = 0
    for raw_skill_id, raw_level in (skill_levels or {}).items():
        try:
            skill_id = int(raw_skill_id)
        except (TypeError, ValueError):
            continue
        level = int((raw_level or {}).get("active") or 0) if isinstance(raw_level, dict) else int(raw_level or 0)
        if level <= 0:
            continue
        skill_name = str(get_type_name(skill_id) or "").lower()
        if (
            "processing" in skill_name
            and "reprocessing" not in skill_name
            and "efficiency" not in skill_name
        ):
            processing_level = max(processing_level, level)

    return {
        "reprocessing": _active(REPROCESSING_SKILL_TYPE_IDS["reprocessing"]),
        "reprocessing_efficiency": _active(
            REPROCESSING_SKILL_TYPE_IDS["reprocessing_efficiency"]
        ),
        "scrapmetal_processing": _active(
            REPROCESSING_SKILL_TYPE_IDS["scrapmetal_processing"]
        ),
        "processing": processing_level,
    }


def compute_estimated_yield_percent(
    *,
    skill_snapshot: dict[str, int],
    implant_bonus_percent: Decimal,
    structure_bonus_percent: Decimal,
    rig_bonus_percent: Decimal,
    security_bonus_percent: Decimal = Decimal("0.000"),
) -> Decimal:
    """Estimate net reprocessing yield percentage (taxes/fees excluded)."""
    # Upwell formula:
    # (50 + Rm) * (1 + Sec) * (1 + Sm) * (1 + R*0.03) * (1 + Re*0.02) * (1 + Op*0.02) * (1 + Im)
    rig_modifier = _normalize_rig_modifier(_to_decimal(rig_bonus_percent))
    structure_modifier = _normalize_structure_modifier(_to_decimal(structure_bonus_percent))

    security_modifier = _to_decimal(security_bonus_percent)
    if security_modifier >= Decimal("1"):
        security_modifier = security_modifier / Decimal("100")
    if rig_modifier <= Decimal("0"):
        security_modifier = Decimal("0.000")

    implant_modifier = _to_decimal(implant_bonus_percent) / Decimal("100")
    reprocessing_level = Decimal(str(int(skill_snapshot.get("reprocessing", 0))))
    efficiency_level = Decimal(str(int(skill_snapshot.get("reprocessing_efficiency", 0))))
    processing_level = Decimal(str(int(skill_snapshot.get("processing", 0))))

    total = (
        (Decimal("50.000") + rig_modifier)
        * (Decimal("1.000") + security_modifier)
        * (Decimal("1.000") + structure_modifier)
        * (Decimal("1.000") + (reprocessing_level * Decimal("0.03")))
        * (Decimal("1.000") + (efficiency_level * Decimal("0.02")))
        * (Decimal("1.000") + (processing_level * Decimal("0.02")))
        * (Decimal("1.000") + implant_modifier)
    )
    if total < Decimal("0"):
        total = Decimal("0")
    if total > Decimal("100"):
        total = Decimal("100")
    return total.quantize(Decimal("0.001"))


def _query_type_material_rows(type_id: int) -> list[tuple[int, int]]:
    """Return [(material_type_id, quantity)] using django-eveonline-sde models."""
    try:
        # Alliance Auth (External Libs)
        import eve_sde.models as sde_models
    except Exception:
        return []

    candidates = [
        ("TypeMaterial", ("eve_type_id", "material_eve_type_id", "quantity")),
        ("ItemTypeMaterial", ("eve_type_id", "material_eve_type_id", "quantity")),
        ("TypeMaterials", ("eve_type_id", "material_eve_type_id", "quantity")),
        ("TypeMaterial", ("type_id", "material_type_id", "quantity")),
        ("ItemTypeMaterial", ("type_id", "material_type_id", "quantity")),
        ("ItemTypeMaterials", ("item_type_id", "material_item_type_id", "quantity")),
        ("ItemTypeMaterials", ("item_type", "material_item_type", "quantity")),
    ]

    for model_name, (source_field, output_field, qty_field) in candidates:
        model = getattr(sde_models, model_name, None)
        if model is None:
            continue
        field_names = {field.name for field in model._meta.get_fields()}
        if source_field not in field_names or output_field not in field_names:
            continue
        if qty_field not in field_names:
            continue
        filters = {source_field: int(type_id)}
        rows = list(model.objects.filter(**filters).values_list(output_field, qty_field))
        normalized: list[tuple[int, int]] = []
        for material_type_id, qty in rows:
            try:
                mid = int(material_type_id)
                quantity = int(qty or 0)
            except (TypeError, ValueError):
                continue
            if mid > 0 and quantity > 0:
                normalized.append((mid, quantity))
        if normalized:
            return normalized
    return []


def get_reprocessing_outputs_for_type(type_id: int) -> dict[int, int]:
    """Return reprocessing output materials for a source item type."""
    rows = _query_type_material_rows(int(type_id))
    outputs: dict[int, int] = {}
    for material_type_id, quantity in rows:
        outputs[int(material_type_id)] = outputs.get(int(material_type_id), 0) + int(quantity)
    return outputs


def get_reprocessing_portion_size(type_id: int) -> int:
    """Return reprocessing batch size (portion size) for a source item type."""
    try:
        # Alliance Auth (External Libs)
        import eve_sde.models as sde_models
    except Exception:
        return 1

    item_type_model = getattr(sde_models, "ItemType", None)
    if item_type_model is None:
        return 1
    try:
        raw_value = item_type_model.objects.filter(id=int(type_id)).values_list("portion_size", flat=True).first()
        value = int(raw_value or 1)
    except Exception:
        value = 1
    return value if value > 0 else 1


def _get_reprocessing_skill_attribute_ids() -> list[int]:
    try:
        # Alliance Auth (External Libs)
        import eve_sde.models as sde_models
    except Exception:
        return []

    attribute_model = getattr(sde_models, "DogmaAttribute", None)
    if attribute_model is None:
        return []

    try:
        rows = list(
            attribute_model.objects.filter(
                name__in=[
                    "reprocessingSkillTypeID",
                    "reprocessingSkillTypeId",
                ]
            ).values_list("id", flat=True)
        )
        if rows:
            return [int(x) for x in rows if int(x) > 0]
    except Exception:
        pass

    try:
        rows = list(
            attribute_model.objects.filter(name__icontains="reprocessing")
            .filter(name__icontains="skill")
            .filter(name__icontains="type")
            .values_list("id", flat=True)
        )
    except Exception:
        rows = []
    return [int(x) for x in rows if int(x) > 0]


@lru_cache(maxsize=4096)
def _get_reprocessing_item_context(type_id: int) -> dict[str, str]:
    context = {
        "type_name": str(get_type_name(int(type_id)) or "").strip(),
        "group_name": "",
        "category_name": "",
        "market_group_name": "",
    }

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COALESCE(t.name_en, t.name, ''),
                    COALESCE(g.name_en, g.name, ''),
                    COALESCE(c.name_en, c.name, ''),
                    COALESCE(mg.name_en, mg.name, '')
                FROM eve_sde_itemtype t
                LEFT JOIN eve_sde_itemgroup g
                    ON g.id = t.group_id
                LEFT JOIN eve_sde_itemcategory c
                    ON c.id = g.category_id
                LEFT JOIN eve_sde_itemmarketgroup mg
                    ON mg.id = t.market_group_id
                WHERE t.id = %s
                """,
                [int(type_id)],
            )
            row = cursor.fetchone()
    except Exception:
        row = None

    if row:
        type_name, group_name, category_name, market_group_name = row
        if str(type_name or "").strip():
            context["type_name"] = str(type_name).strip()
        context["group_name"] = str(group_name or "").strip()
        context["category_name"] = str(category_name or "").strip()
        context["market_group_name"] = str(market_group_name or "").strip()

    return context


@lru_cache(maxsize=1)
def _get_processing_skill_candidates_from_sde() -> tuple[tuple[int, str], ...]:
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    t.id,
                    COALESCE(t.name_en, t.name, '')
                FROM eve_sde_itemtype t
                LEFT JOIN eve_sde_itemgroup g
                    ON g.id = t.group_id
                LEFT JOIN eve_sde_itemcategory c
                    ON c.id = g.category_id
                WHERE LOWER(COALESCE(t.name_en, t.name, '')) LIKE %s
                  AND LOWER(COALESCE(c.name_en, c.name, '')) LIKE %s
                ORDER BY t.id
                """,
                ["% processing", "%skill%"],
            )
            rows = cursor.fetchall()
    except Exception:
        rows = []

    normalized: list[tuple[int, str]] = []
    seen: set[int] = set()
    for raw_skill_id, raw_name in rows:
        try:
            skill_id = int(raw_skill_id)
        except (TypeError, ValueError):
            continue
        skill_name = str(raw_name or "").strip()
        if skill_id <= 0 or not skill_name or skill_id in seen:
            continue
        seen.add(skill_id)
        normalized.append((skill_id, skill_name))
    return tuple(normalized)


def _normalize_processing_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())).strip()


def _extract_processing_subject(skill_name: str) -> str:
    normalized = re.sub(r"\s+processing$", "", str(skill_name or "").strip(), flags=re.IGNORECASE)
    return normalized.strip()


def _iter_processing_skill_candidates(
    skill_levels_by_id: dict[int, int] | None,
) -> list[tuple[int, str]]:
    candidates = list(_get_processing_skill_candidates_from_sde())
    seen = {skill_id for skill_id, _skill_name in candidates}

    for raw_skill_id in (skill_levels_by_id or {}):
        try:
            skill_id = int(raw_skill_id)
        except (TypeError, ValueError):
            continue
        if skill_id <= 0 or skill_id in seen:
            continue
        skill_name = str(get_type_name(skill_id) or "").strip()
        if not skill_name:
            continue
        seen.add(skill_id)
        candidates.append((skill_id, skill_name))

    return candidates


def _infer_processing_skill_type_id_for_item(
    *,
    type_id: int,
    skill_levels_by_id: dict[int, int] | None,
) -> int | None:
    context = _get_reprocessing_item_context(int(type_id))
    context_texts = [
        _normalize_processing_text(context.get("type_name", "")),
        _normalize_processing_text(context.get("group_name", "")),
        _normalize_processing_text(context.get("category_name", "")),
        _normalize_processing_text(context.get("market_group_name", "")),
    ]
    context_texts = [text for text in context_texts if text]

    if not context_texts:
        return None

    best_skill_id: int | None = None
    best_score = -1
    for skill_id, skill_name in _iter_processing_skill_candidates(skill_levels_by_id):
        normalized_skill_name = _normalize_processing_text(skill_name)
        if (
            "processing" not in normalized_skill_name
            or "reprocessing" in normalized_skill_name
            or "efficiency" in normalized_skill_name
        ):
            continue

        subject = _normalize_processing_text(_extract_processing_subject(skill_name))
        if not subject:
            continue

        if not any(
            text == subject
            or text.endswith(f" {subject}")
            or f" {subject} " in f" {text} "
            for text in context_texts
        ):
            continue

        score = len(subject.split())
        if score > best_score:
            best_skill_id = int(skill_id)
            best_score = score

    return best_skill_id


@lru_cache(maxsize=4096)
def resolve_processing_skill_type_id_for_item(type_id: int) -> int | None:
    """Best-effort lookup of the specific processing skill type ID for an item type."""
    try:
        # Alliance Auth (External Libs)
        import eve_sde.models as sde_models
    except Exception:
        return None

    dogma_model = getattr(sde_models, "TypeDogma", None)
    if dogma_model is None:
        return None

    attribute_ids = _get_reprocessing_skill_attribute_ids()
    if not attribute_ids:
        return None
    try:
        value = (
            dogma_model.objects.filter(
                item_type_id=int(type_id),
                dogma_attribute_id__in=attribute_ids,
            )
            .values_list("value", flat=True)
            .first()
        )
    except Exception:
        return None
    try:
        skill_type_id = int(float(value))
    except (TypeError, ValueError):
        return None
    return skill_type_id if skill_type_id > 0 else None


def resolve_processing_skill_level_for_item(
    *,
    type_id: int,
    skill_levels_by_id: dict[int, int] | None,
    fallback_level: int = 0,
) -> int:
    normalized_skill_levels = skill_levels_by_id or {}
    required_skill_type_id = resolve_processing_skill_type_id_for_item(int(type_id))
    if not required_skill_type_id:
        required_skill_type_id = _infer_processing_skill_type_id_for_item(
            type_id=int(type_id),
            skill_levels_by_id=normalized_skill_levels,
        )
    if required_skill_type_id:
        if not normalized_skill_levels:
            return int(fallback_level or 0)
        return int(normalized_skill_levels.get(int(required_skill_type_id), 0) or 0)
    return int(fallback_level or 0)


def build_reprocessing_estimate(
    *,
    input_items: list[dict[str, int]],
    yield_percent: Decimal,
    margin_percent: Decimal,
    yield_percent_by_type: dict[int, Decimal] | None = None,
) -> dict[str, object]:
    """Compute expected outputs, value, and reward from submitted source items."""
    expected_outputs: dict[int, int] = {}
    unsupported_inputs: list[dict[str, int]] = []

    for row in input_items:
        try:
            source_type_id = int(row.get("type_id") or 0)
            source_qty = int(row.get("quantity") or 0)
        except (TypeError, ValueError):
            continue
        if source_type_id <= 0 or source_qty <= 0:
            continue
        output_map = get_reprocessing_outputs_for_type(source_type_id)
        if not output_map:
            unsupported_inputs.append({"type_id": source_type_id, "quantity": source_qty})
            continue
        type_yield_percent = _to_decimal(
            (yield_percent_by_type or {}).get(int(source_type_id), yield_percent)
        )
        type_yield_ratio = type_yield_percent / Decimal("100")
        if type_yield_ratio < Decimal("0"):
            type_yield_ratio = Decimal("0")
        portion_size = max(int(get_reprocessing_portion_size(source_type_id)), 1)
        processable_portions = source_qty // portion_size
        if processable_portions <= 0:
            continue
        for output_type_id, output_qty_per_unit in output_map.items():
            refined_quantity = (
                _to_decimal(processable_portions)
                * _to_decimal(output_qty_per_unit)
                * type_yield_ratio
            ).quantize(Decimal("1"), rounding=ROUND_FLOOR)
            refined_int = max(int(refined_quantity), 0)
            expected_outputs[int(output_type_id)] = (
                expected_outputs.get(int(output_type_id), 0) + refined_int
            )

    output_type_ids = sorted(expected_outputs.keys())
    try:
        price_map = fetch_fuzzwork_prices(output_type_ids, timeout=15)
    except FuzzworkError:
        price_map = {}

    output_rows: list[dict[str, object]] = []
    total_value = Decimal("0.00")
    for output_type_id in output_type_ids:
        quantity = int(expected_outputs.get(output_type_id, 0))
        if quantity <= 0:
            continue
        type_prices = price_map.get(int(output_type_id), {})
        unit_price = _to_decimal(type_prices.get("sell", 0)).quantize(Decimal("0.01"))
        total_row_value = (_to_decimal(quantity) * unit_price).quantize(Decimal("0.01"))
        total_value += total_row_value
        output_rows.append(
            {
                "type_id": int(output_type_id),
                "type_name": get_type_name(int(output_type_id)),
                "expected_quantity": int(quantity),
                "unit_price": unit_price,
                "total_value": total_row_value,
            }
        )

    # EVE contracts effectively operate in whole ISK; floor to avoid false mismatches.
    reward_isk = (
        total_value * (_to_decimal(margin_percent) / Decimal("100"))
    ).quantize(Decimal("1"), rounding=ROUND_FLOOR)

    return {
        "outputs": output_rows,
        "unsupported_inputs": unsupported_inputs,
        "total_output_value": total_value.quantize(Decimal("0.01")),
        "reward_isk": reward_isk,
    }


def aggregate_contract_items_by_type(
    contract_items: Iterable[object],
) -> dict[int, int]:
    aggregated: dict[int, int] = {}
    for contract_item in contract_items:
        is_included = bool(getattr(contract_item, "is_included", False))
        if not is_included:
            continue
        try:
            type_id = int(getattr(contract_item, "type_id", 0) or 0)
            quantity = int(getattr(contract_item, "quantity", 0) or 0)
        except (TypeError, ValueError):
            continue
        if type_id <= 0 or quantity <= 0:
            continue
        aggregated[type_id] = aggregated.get(type_id, 0) + quantity
    return aggregated


def contract_items_match_exact(
    *,
    contract_items: Iterable[object],
    expected_by_type: dict[int, int],
) -> bool:
    expected = {int(k): int(v) for k, v in (expected_by_type or {}).items() if int(v) > 0}
    actual = aggregate_contract_items_by_type(contract_items)
    return actual == expected


def contract_items_match_with_tolerance(
    *,
    contract_items: Iterable[object],
    expected_by_type: dict[int, int],
    tolerance_percent: Decimal = Decimal("1.00"),
) -> tuple[bool, list[str]]:
    """Validate contract items with tolerance and no substitutions."""
    expected = {int(k): int(v) for k, v in (expected_by_type or {}).items() if int(v) > 0}
    actual = aggregate_contract_items_by_type(contract_items)
    errors: list[str] = []

    expected_types = set(expected.keys())
    actual_types = set(actual.keys())
    if expected_types != actual_types:
        missing = sorted(expected_types - actual_types)
        extras = sorted(actual_types - expected_types)
        if missing:
            errors.append(
                "Missing types: " + ", ".join(get_type_name(type_id) for type_id in missing)
            )
        if extras:
            errors.append(
                "Unexpected types: " + ", ".join(get_type_name(type_id) for type_id in extras)
            )
        return False, errors

    tolerance_ratio = _to_decimal(tolerance_percent) / Decimal("100")
    for type_id in sorted(expected_types):
        expected_qty = int(expected.get(type_id, 0))
        actual_qty = int(actual.get(type_id, 0))
        max_delta = max(1, int(math.ceil(float(_to_decimal(expected_qty) * tolerance_ratio))))
        delta = abs(actual_qty - expected_qty)
        if delta > max_delta:
            errors.append(
                (
                    f"{get_type_name(type_id)} quantity mismatch: expected {expected_qty:,}, "
                    f"actual {actual_qty:,} (allowed +/- {max_delta:,})"
                )
            )

    return (len(errors) == 0), errors


def _populate_compressed_ore_cache() -> tuple[bool, str]:
    """
    Populate the CompressedOreCache with all compressed ore reprocessing data.

    Returns:
        Tuple of (success, status_message)
    """
    from indy_hub.models import CompressedOreCache

    try:
        import eve_sde.models as sde_models
    except Exception as e:
        return False, f"EVE SDE not available: {e}"

    item_type_model = getattr(sde_models, "ItemType", None)
    if not item_type_model:
        return False, "ItemType model not found"

    # Find all compressed ores
    try:
        compressed_ore_types = list(
            item_type_model.objects.filter(
                name__icontains="Compressed"
            ).exclude(
                name__icontains="Blueprint"
            ).exclude(
                name__icontains="SKIN"
            ).values_list("id", "name")
        )
    except Exception as e:
        return False, f"Failed to query compressed ores: {e}"

    if not compressed_ore_types:
        return False, "No compressed ores found"

    logger.info(f"Populating cache with {len(compressed_ore_types)} compressed ore types")

    # Get reprocessing data for all compressed ores
    for ore_type_id, ore_name in compressed_ore_types:
        outputs = get_reprocessing_outputs_for_type(ore_type_id)
        if outputs:
            CompressedOreCache.objects.update_or_create(
                ore_type_id=ore_type_id,
                defaults={
                    "ore_name": ore_name,
                    "reprocessing_outputs": outputs,
                }
            )

    return True, f"Populated cache with {len(compressed_ore_types)} compressed ores"


def _update_compressed_ore_prices() -> tuple[bool, str]:
    """
    Update prices for all compressed ores in the cache.

    Returns:
        Tuple of (success, status_message)
    """
    from indy_hub.models import CompressedOreCache

    ore_type_ids = list(CompressedOreCache.objects.values_list("ore_type_id", flat=True))

    if not ore_type_ids:
        return False, "No ores in cache to update prices for"

    try:
        price_map = fetch_fuzzwork_prices(ore_type_ids, timeout=10)

        # Update prices in bulk
        for ore_type_id, prices in price_map.items():
            CompressedOreCache.objects.filter(ore_type_id=ore_type_id).update(
                buy_price=_to_decimal(prices.get("buy", 0)),
                sell_price=_to_decimal(prices.get("sell", 0)),
                pricing_data_updated=timezone.now(),
            )

        return True, f"Updated prices for {len(price_map)} compressed ores"
    except Exception as e:
        logger.warning(f"Failed to update prices: {e}")
        return False, f"Failed to update prices: {e}"


def calculate_compressed_ore_for_minerals(
    *,
    mineral_requirements: dict[int, int],
    refine_rate_percent: Decimal,
    progress_callback=None,
) -> dict[str, object]:
    """
    Calculate the optimal compressed ore types needed to satisfy mineral requirements.

    Args:
        mineral_requirements: Dict of {mineral_type_id: quantity_needed}
        refine_rate_percent: Refining efficiency percentage (e.g., 84.2 for 84.2%)
        progress_callback: Optional callback function(status_message) for progress updates

    Returns:
        Dict containing:
            - compressed_ores: List of {type_id, type_name, quantity, cost, mineral_yields}
            - total_cost: Total ISK cost
            - excess_minerals: Dict of excess minerals produced
            - prices_estimated: Boolean indicating if prices are estimated (not from Fuzzwork)
    """
    from indy_hub.models import CompressedOreCache

    def update_progress(message: str):
        """Helper to call progress callback if provided."""
        if progress_callback:
            progress_callback(message)
        logger.info(message)
    if not mineral_requirements:
        return {
            "compressed_ores": [],
            "total_cost": Decimal("0.00"),
            "excess_minerals": {},
            "prices_estimated": False,
            "error": None,
        }

    # Check if cache needs initial setup
    if CompressedOreCache.needs_initial_setup():
        update_progress("Running first-time setup...")
        update_progress("Populating database with compressed ore data...")
        success, message = _populate_compressed_ore_cache()
        if not success:
            return {
                "compressed_ores": [],
                "total_cost": Decimal("0.00"),
                "excess_minerals": {},
                "prices_estimated": False,
                "error": message,
            }
        update_progress(message)

    # Check if prices need updating
    if CompressedOreCache.needs_price_update():
        update_progress("Updating prices from market data...")
        _update_compressed_ore_prices()  # Don't fail if this doesn't work

    # Load ore data from cache
    update_progress("Running calculation...")
    cached_ores = CompressedOreCache.objects.all().values(
        "ore_type_id", "ore_name", "reprocessing_outputs", "sell_price"
    )

    # Build ore_mineral_yields from cache, filtering to only ores that produce requested minerals
    ore_mineral_yields: dict[int, dict[int, int]] = {}
    price_map: dict[int, dict[str, Decimal]] = {}
    prices_estimated = False

    for ore in cached_ores:
        # Convert JSON field back to dict[int, int]
        outputs = {int(k): int(v) for k, v in ore["reprocessing_outputs"].items()}

        # Only include ores that produce at least one requested mineral
        if any(mineral_id in mineral_requirements for mineral_id in outputs.keys()):
            ore_type_id = ore["ore_type_id"]
            ore_mineral_yields[ore_type_id] = outputs

            # Get price
            sell_price = ore.get("sell_price")
            if sell_price and sell_price > 0:
                price_map[ore_type_id] = {"sell": sell_price}
            else:
                # Use fallback pricing
                price_map[ore_type_id] = {"sell": Decimal("1.0")}
                prices_estimated = True

    if not ore_mineral_yields:
        return {
            "compressed_ores": [],
            "total_cost": Decimal("0.00"),
            "excess_minerals": {},
            "prices_estimated": False,
            "error": "No compressed ores found that produce the required minerals",
        }

    # Calculate refine rate ratio
    refine_ratio = _to_decimal(refine_rate_percent) / Decimal("100")
    if refine_ratio <= Decimal("0") or refine_ratio > Decimal("1"):
        refine_ratio = Decimal("0.842")  # Default to 84.2%

    # Greedy algorithm: repeatedly pick the most cost-effective ore for remaining minerals
    remaining_minerals = {int(k): int(v) for k, v in mineral_requirements.items()}
    selected_ores: dict[int, int] = {}  # {ore_type_id: quantity}

    max_iterations = 10000  # Safety limit
    iteration = 0

    while any(qty > 0 for qty in remaining_minerals.values()) and iteration < max_iterations:
        iteration += 1
        best_ore_id = None
        best_cost_per_need = None

        for ore_type_id, mineral_outputs in ore_mineral_yields.items():
            # Calculate how much of each needed mineral this ore provides per unit
            ore_prices = price_map.get(ore_type_id, {})
            ore_unit_price = _to_decimal(ore_prices.get("sell", 0))

            # If no price available, use fallback
            if ore_unit_price <= 0:
                ore_unit_price = Decimal("1.0")

            # Get portion size for this ore
            portion_size = max(get_reprocessing_portion_size(ore_type_id), 1)

            # Calculate minerals produced per portion at given refine rate
            minerals_per_portion: dict[int, int] = {}
            for mineral_id, base_qty in mineral_outputs.items():
                refined_qty = int((Decimal(base_qty) * refine_ratio).quantize(Decimal("1"), rounding=ROUND_FLOOR))
                if refined_qty > 0:
                    minerals_per_portion[mineral_id] = refined_qty

            # Calculate how much this ore helps with remaining needs
            need_coverage = Decimal("0")
            for mineral_id, produced_qty in minerals_per_portion.items():
                if mineral_id in remaining_minerals and remaining_minerals[mineral_id] > 0:
                    # This ore produces something we need
                    need_coverage += Decimal(produced_qty)

            if need_coverage <= 0:
                continue

            # Cost per portion
            cost_per_portion = ore_unit_price * Decimal(portion_size)

            # Cost efficiency: cost per unit of "need coverage"
            cost_efficiency = cost_per_portion / need_coverage

            if best_cost_per_need is None or cost_efficiency < best_cost_per_need:
                best_cost_per_need = cost_efficiency
                best_ore_id = ore_type_id

        if best_ore_id is None:
            # No ore can satisfy remaining needs (possibly due to missing prices)
            break

        # Add one portion of the best ore
        portion_size = max(get_reprocessing_portion_size(best_ore_id), 1)
        selected_ores[best_ore_id] = selected_ores.get(best_ore_id, 0) + portion_size

        # Update remaining minerals
        for mineral_id, base_qty in ore_mineral_yields[best_ore_id].items():
            refined_qty = int((Decimal(base_qty) * refine_ratio).quantize(Decimal("1"), rounding=ROUND_FLOOR))
            if mineral_id in remaining_minerals:
                remaining_minerals[mineral_id] -= refined_qty
                if remaining_minerals[mineral_id] < 0:
                    remaining_minerals[mineral_id] = 0

    # Calculate total minerals produced and excess
    total_minerals_produced: dict[int, int] = {}
    for ore_type_id, ore_qty in selected_ores.items():
        portion_size = max(get_reprocessing_portion_size(ore_type_id), 1)
        num_portions = ore_qty // portion_size

        for mineral_id, base_qty in ore_mineral_yields[ore_type_id].items():
            refined_qty = int((Decimal(base_qty) * refine_ratio * Decimal(num_portions)).quantize(Decimal("1"), rounding=ROUND_FLOOR))
            total_minerals_produced[mineral_id] = total_minerals_produced.get(mineral_id, 0) + refined_qty

    excess_minerals: dict[int, int] = {}
    for mineral_id, produced in total_minerals_produced.items():
        required = mineral_requirements.get(mineral_id, 0)
        if produced > required:
            excess_minerals[mineral_id] = produced - required

    # Build result with ore details and costs
    compressed_ore_list = []
    total_cost = Decimal("0.00")

    for ore_type_id, ore_qty in selected_ores.items():
        ore_prices = price_map.get(ore_type_id, {})
        ore_unit_price = _to_decimal(ore_prices.get("sell", 0)).quantize(Decimal("0.01"))
        ore_total_cost = (_to_decimal(ore_qty) * ore_unit_price).quantize(Decimal("0.01"))
        total_cost += ore_total_cost

        compressed_ore_list.append({
            "type_id": ore_type_id,
            "type_name": get_type_name(ore_type_id),
            "quantity": ore_qty,
            "unit_price": ore_unit_price,
            "total_cost": ore_total_cost,
            "mineral_yields": ore_mineral_yields[ore_type_id],
        })

    # Sort by type name for consistent ordering
    compressed_ore_list.sort(key=lambda x: x["type_name"])

    return {
        "compressed_ores": compressed_ore_list,
        "total_cost": total_cost.quantize(Decimal("0.01")),
        "excess_minerals": excess_minerals,
        "prices_estimated": prices_estimated,
        "error": None,
    }
