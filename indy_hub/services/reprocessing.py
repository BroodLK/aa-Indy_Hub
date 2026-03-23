"""Helpers for Reprocessing Services workflow."""

from __future__ import annotations

# Standard Library
from decimal import Decimal, ROUND_FLOOR
from functools import lru_cache
import math
import re
from typing import Iterable

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger
from esi.models import Token

try:  # pragma: no cover - older django-esi versions
    # Alliance Auth
    from esi.exceptions import HTTPNotModified
except Exception:  # pragma: no cover - fallback
    HTTPNotModified = None

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
    fallback_levels = _fetch_corptools_skill_levels(int(character_id))
    try:
        token_obj = Token.get_token(character_id, REPROCESSING_SKILLS_SCOPE)
    except Exception:
        token_obj = None
    operation = _get_operation(
        "Skills",
        "get_characters_character_id_skills",
        "GetCharactersCharacterIdSkills",
    ) or _get_operation(
        "Character",
        "get_characters_character_id_skills",
        "GetCharactersCharacterIdSkills",
    )
    if not operation:
        return fallback_levels

    request_kwargs = {"If-None-Match": ""} if force_refresh else {}
    try:
        result_obj = operation(
            character_id=int(character_id),
            token=token_obj,
            **request_kwargs,
        )
        payload = result_obj.results()
    except HTTPNotModified:
        result_obj = operation(
            character_id=int(character_id),
            token=token_obj,
            **request_kwargs,
        )
        payload = result_obj.results(use_cache=True)
    except Exception as exc:
        if "is not of type 'string'" in str(exc) and token_obj is not None:
            try:
                access_token = token_obj.valid_access_token()
                result_obj = operation(
                    character_id=int(character_id),
                    token=access_token,
                    **request_kwargs,
                )
                payload = result_obj.results()
            except Exception as nested_exc:
                logger.debug(
                    "Using fallback skills for %s after token conversion error: %s",
                    character_id,
                    nested_exc,
                )
                return fallback_levels
        else:
            logger.debug(
                "Using fallback skills for %s after ESI error: %s",
                character_id,
                exc,
            )
            return fallback_levels

    payload_map = _coerce_mapping(payload)
    skill_rows = payload_map.get("skills", []) if payload_map else []
    levels: dict[int, dict[str, int]] = {}
    for row in skill_rows:
        row_map = _coerce_mapping(row)
        skill_id = row_map.get("skill_id")
        if not skill_id:
            continue
        try:
            sid = int(skill_id)
        except (TypeError, ValueError):
            continue
        levels[sid] = {
            "active": int(row_map.get("active_skill_level") or 0),
            "trained": int(row_map.get("trained_skill_level") or 0),
        }
    if levels:
        return levels
    return fallback_levels


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
    fallback_clones = _fetch_corptools_clone_options(int(character_id))
    try:
        token_obj = Token.get_token(character_id, REPROCESSING_CLONES_SCOPE)
    except Exception:
        token_obj = None
    operation = _get_operation(
        "Clones",
        "get_characters_character_id_clones",
        "GetCharactersCharacterIdClones",
    ) or _get_operation(
        "Character",
        "get_characters_character_id_clones",
        "GetCharactersCharacterIdClones",
    )
    if not operation:
        if fallback_clones:
            return fallback_clones
        return [
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

    request_kwargs = {"If-None-Match": ""} if force_refresh else {}
    try:
        result_obj = operation(
            character_id=int(character_id),
            token=token_obj,
            **request_kwargs,
        )
        payload = result_obj.results()
    except HTTPNotModified:
        result_obj = operation(
            character_id=int(character_id),
            token=token_obj,
            **request_kwargs,
        )
        payload = result_obj.results(use_cache=True)
    except Exception as exc:
        if "is not of type 'string'" in str(exc) and token_obj is not None:
            try:
                access_token = token_obj.valid_access_token()
                result_obj = operation(
                    character_id=int(character_id),
                    token=access_token,
                    **request_kwargs,
                )
                payload = result_obj.results()
            except Exception as nested_exc:
                logger.debug(
                    "Using fallback clones for %s after token conversion error: %s",
                    character_id,
                    nested_exc,
                )
                clone_rows = fallback_clones
                if not clone_rows:
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
        else:
            logger.debug(
                "Using fallback clones for %s after ESI error: %s",
                character_id,
                exc,
            )
            clone_rows = fallback_clones
            if not clone_rows:
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

    payload_map = _coerce_mapping(payload)
    jump_clones = payload_map.get("jump_clones", []) if payload_map else []
    if not isinstance(jump_clones, list):
        jump_clones = []

    location_ids: list[int] = []
    clone_rows: list[dict[str, object]] = []
    for row in jump_clones:
        row_map = _coerce_mapping(row)
        clone_id = row_map.get("jump_clone_id")
        location_id = row_map.get("location_id")
        try:
            clone_id_int = int(clone_id) if clone_id is not None else None
        except (TypeError, ValueError):
            clone_id_int = None
        try:
            location_id_int = int(location_id) if location_id is not None else None
        except (TypeError, ValueError):
            location_id_int = None
        if location_id_int and location_id_int > 0:
            location_ids.append(location_id_int)

        implant_type_ids: list[int] = []
        for raw_tid in row_map.get("implants") or []:
            try:
                implant_type_ids.append(int(raw_tid))
            except (TypeError, ValueError):
                continue

        implant_names = [get_type_name(int(tid)) for tid in implant_type_ids]
        beancounter_names = [n for n in implant_names if _is_beancounter_implant(n)]
        beancounter_bonus = infer_beancounter_bonus_percent(beancounter_names)

        clone_rows.append(
            {
                "clone_id": clone_id_int or (location_id_int or 0),
                "location_id": location_id_int,
                "implant_type_ids": implant_type_ids,
                "implant_names": implant_names,
                "beancounter_implants": beancounter_names,
                "beancounter_bonus_percent": beancounter_bonus,
            }
        )

    location_name_map = _resolve_location_names(sorted(set(location_ids)))
    for idx, row in enumerate(clone_rows, start=1):
        location_name = location_name_map.get(int(row.get("location_id") or 0), "")
        label = f"Clone {idx}"
        if location_name:
            label = f"{label} - {location_name}"
        row["clone_label"] = label
        row["location_name"] = location_name

    if not clone_rows:
        clone_rows = fallback_clones
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
    required_skill_type_id = resolve_processing_skill_type_id_for_item(int(type_id))
    if required_skill_type_id:
        return int((skill_levels_by_id or {}).get(int(required_skill_type_id), 0) or 0)
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

    reward_isk = (
        total_value * (_to_decimal(margin_percent) / Decimal("100"))
    ).quantize(Decimal("0.01"))

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
