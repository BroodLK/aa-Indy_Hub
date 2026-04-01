"""Shared helpers for craft build-environment discovery."""

from __future__ import annotations

# Standard Library
import re

# Django
from django.core.cache import cache

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership
from allianceauth.eveonline.models import EveCorporationInfo
from allianceauth.services.hooks import get_extension_logger

# Local
from indy_hub.services.asset_cache import get_corp_assets_cached
from indy_hub.services.cache_utils import get_or_set_cache_with_lock
from indy_hub.services.esi_client import shared_client
from indy_hub.utils.eve import get_corporation_name, get_type_name

logger = get_extension_logger(__name__)

ENGINEERING_COMPLEX_TYPE_INFO: dict[int, dict[str, object]] = {
    35825: {
        "key": "raitaru",
        "label": "Raitaru",
        "material_bonus": 0.01,
    },
    35826: {
        "key": "azbel",
        "label": "Azbel",
        "material_bonus": 0.01,
    },
    35827: {
        "key": "sotiyo",
        "label": "Sotiyo",
        "material_bonus": 0.01,
    },
}

CRAFT_RIG_BONUS_BY_KEY: dict[str, float] = {
    "equipment_t1": 0.02,
    "equipment_t2": 0.024,
    "component_t1": 0.02,
    "component_t2": 0.024,
    "capital_component_t1": 0.02,
    "capital_component_t2": 0.024,
    "ship_t1": 0.02,
    "ship_t2": 0.024,
    "capital_ship_t1": 0.02,
    "capital_ship_t2": 0.024,
    "structure_t1": 0.02,
    "structure_t2": 0.024,
}

_RIG_LOCATION_FLAG_HINTS = (
    "rigslot",
    "serviceslot",
    "service",
    "fitting",
)

_RIG_NAME_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(
            r"capital\s+ship.*manufacturing.*material\s+efficiency.*\bii\b",
            re.IGNORECASE,
        ),
        "capital_ship_t2",
    ),
    (
        re.compile(
            r"capital\s+ship.*manufacturing.*material\s+efficiency.*\bi\b",
            re.IGNORECASE,
        ),
        "capital_ship_t1",
    ),
    (
        re.compile(
            r"capital\s+component.*manufacturing.*material\s+efficiency.*\bii\b",
            re.IGNORECASE,
        ),
        "capital_component_t2",
    ),
    (
        re.compile(
            r"capital\s+component.*manufacturing.*material\s+efficiency.*\bi\b",
            re.IGNORECASE,
        ),
        "capital_component_t1",
    ),
    (
        re.compile(
            r"component.*manufacturing.*material\s+efficiency.*\bii\b",
            re.IGNORECASE,
        ),
        "component_t2",
    ),
    (
        re.compile(
            r"component.*manufacturing.*material\s+efficiency.*\bi\b",
            re.IGNORECASE,
        ),
        "component_t1",
    ),
    (
        re.compile(
            r"structure.*manufacturing.*material\s+efficiency.*\bii\b",
            re.IGNORECASE,
        ),
        "structure_t2",
    ),
    (
        re.compile(
            r"structure.*manufacturing.*material\s+efficiency.*\bi\b",
            re.IGNORECASE,
        ),
        "structure_t1",
    ),
    (
        re.compile(
            r"(?<!capital\s)ship.*manufacturing.*material\s+efficiency.*\bii\b",
            re.IGNORECASE,
        ),
        "ship_t2",
    ),
    (
        re.compile(
            r"(?<!capital\s)ship.*manufacturing.*material\s+efficiency.*\bi\b",
            re.IGNORECASE,
        ),
        "ship_t1",
    ),
    (
        re.compile(
            r"equipment.*manufacturing.*material\s+efficiency.*\bii\b",
            re.IGNORECASE,
        ),
        "equipment_t2",
    ),
    (
        re.compile(
            r"equipment.*manufacturing.*material\s+efficiency.*\bi\b",
            re.IGNORECASE,
        ),
        "equipment_t1",
    ),
]

INDUSTRY_SYSTEMS_CACHE_TTL_SECONDS = 3600
INDUSTRY_FACILITIES_CACHE_TTL_SECONDS = 3600


def _security_class_from_status(security_status: float | None) -> str:
    value = float(security_status or 0.0)
    if value >= 0.5:
        return "HIGH_SEC"
    if value > 0.0:
        return "LOW_SEC"
    return "NULL_SEC"


def _resolve_operation(resource: str, snake_name: str):
    client = shared_client.client
    resource_obj = getattr(client, resource, None)
    if resource_obj is None:
        return None
    operation = getattr(resource_obj, snake_name, None)
    if callable(operation):
        return operation
    camel_name = "".join(part.capitalize() for part in snake_name.split("_"))
    operation = getattr(resource_obj, camel_name, None)
    return operation if callable(operation) else None


def _fetch_esi_industry_system_rows() -> list[dict]:
    cache_key = "indy_hub:craft_env:esi_industry_systems:v2"

    def _loader() -> list[dict]:
        payload = None
        operation = _resolve_operation("Industry", "get_industry_systems")
        if operation is not None:
            try:
                result = operation()
                payload = result.results() if hasattr(result, "results") else result
            except Exception as exc:
                logger.debug("Industry.get_industry_systems failed: %s", exc)

        if payload is None:
            logger.debug(
                "Unable to resolve Industry.get_industry_systems via OpenAPI; using empty payload"
            )
            payload = []

        return payload if isinstance(payload, list) else []

    rows = get_or_set_cache_with_lock(
        cache_key=cache_key,
        ttl_seconds=INDUSTRY_SYSTEMS_CACHE_TTL_SECONDS,
        loader=_loader,
        lock_ttl_seconds=25,
        wait_timeout_seconds=10.0,
        poll_interval_seconds=0.2,
    )
    return rows if isinstance(rows, list) else []


def _fetch_esi_industry_facility_rows() -> list[dict]:
    cache_key = "indy_hub:craft_env:esi_industry_facilities:v1"

    def _loader() -> list[dict]:
        payload = None
        operation = _resolve_operation("Industry", "get_industry_facilities")
        if operation is not None:
            try:
                result = operation()
                payload = result.results() if hasattr(result, "results") else result
            except Exception as exc:
                logger.debug("Industry.get_industry_facilities failed: %s", exc)

        if payload is None:
            logger.debug(
                "Unable to resolve Industry.get_industry_facilities via OpenAPI; using empty payload"
            )
            payload = []

        return payload if isinstance(payload, list) else []

    rows = get_or_set_cache_with_lock(
        cache_key=cache_key,
        ttl_seconds=INDUSTRY_FACILITIES_CACHE_TTL_SECONDS,
        loader=_loader,
        lock_ttl_seconds=25,
        wait_timeout_seconds=10.0,
        poll_interval_seconds=0.2,
    )
    return rows if isinstance(rows, list) else []


def _get_manufacturing_index_map() -> dict[int, float]:
    cache_key = "indy_hub:craft_env:manufacturing_index_map:v1"
    cached = cache.get(cache_key)
    if isinstance(cached, dict):
        try:
            return {
                int(system_id): float(index)
                for system_id, index in cached.items()
                if int(system_id) > 0
            }
        except Exception:
            pass

    mapping: dict[int, float] = {}
    for row in _fetch_esi_industry_system_rows():
        if not isinstance(row, dict):
            continue
        try:
            system_id = int(
                row.get("solar_system_id")
                or row.get("system_id")
                or 0
            )
        except (TypeError, ValueError):
            continue
        if system_id <= 0:
            continue
        cost_indices = row.get("cost_indices")
        if not isinstance(cost_indices, list):
            continue
        manufacturing_index = None
        for activity_row in cost_indices:
            if not isinstance(activity_row, dict):
                continue
            if str(activity_row.get("activity") or "").strip().lower() != "manufacturing":
                continue
            try:
                manufacturing_index = float(activity_row.get("cost_index") or 0.0)
            except (TypeError, ValueError):
                manufacturing_index = None
            break
        if manufacturing_index is None:
            continue
        mapping[system_id] = max(0.0, float(manufacturing_index))

    cache.set(cache_key, mapping, INDUSTRY_SYSTEMS_CACHE_TTL_SECONDS)
    return mapping


def _get_industry_facility_tax_map() -> dict[int, float]:
    cache_key = "indy_hub:craft_env:facility_tax_map:v1"
    cached = cache.get(cache_key)
    if isinstance(cached, dict):
        try:
            return {
                int(facility_id): float(tax_value)
                for facility_id, tax_value in cached.items()
                if int(facility_id) > 0
            }
        except Exception:
            pass

    mapping: dict[int, float] = {}
    for row in _fetch_esi_industry_facility_rows():
        if not isinstance(row, dict):
            continue
        try:
            facility_id = int(row.get("facility_id") or 0)
        except (TypeError, ValueError):
            continue
        if facility_id <= 0:
            continue
        try:
            tax_value = float(row.get("tax") or 0.0)
        except (TypeError, ValueError):
            tax_value = 0.0
        mapping[facility_id] = max(0.0, tax_value)

    cache.set(cache_key, mapping, INDUSTRY_FACILITIES_CACHE_TTL_SECONDS)
    return mapping


def _get_solar_system_model():
    try:
        # Alliance Auth (External Libs)
        import eve_sde.models as sde_models

        return getattr(sde_models, "SolarSystem", None)
    except Exception:
        return None


def _extract_solar_system_row(system_row) -> dict[str, object] | None:
    if system_row is None:
        return None
    try:
        system_id = int(getattr(system_row, "id", 0) or 0)
    except (TypeError, ValueError):
        return None
    if system_id <= 0:
        return None

    system_name = ""
    for attr in ("name", "name_en", "name_en_us"):
        system_name = str(getattr(system_row, attr, "") or "").strip()
        if system_name:
            break
    if not system_name:
        system_name = f"System {system_id}"

    try:
        security_status = float(getattr(system_row, "security_status", 0.0) or 0.0)
    except (TypeError, ValueError):
        security_status = 0.0

    return {
        "system_id": system_id,
        "system_name": system_name,
        "security_status": security_status,
        "security_class": _security_class_from_status(security_status),
    }


def resolve_solar_system(
    *,
    system_text: str = "",
    system_id: int | None = None,
) -> dict[str, object] | None:
    model = _get_solar_system_model()
    if model is None:
        return None

    resolved_row = None

    try:
        numeric_id = int(system_id or 0)
    except (TypeError, ValueError):
        numeric_id = 0
    if numeric_id > 0:
        try:
            resolved_row = model.objects.filter(id=numeric_id).first()
        except Exception:
            resolved_row = None

    query_text = str(system_text or "").strip()
    if resolved_row is None and query_text:
        if query_text.isdigit():
            try:
                resolved_row = model.objects.filter(id=int(query_text)).first()
            except Exception:
                resolved_row = None
        if resolved_row is None:
            for lookup in ("iexact", "istartswith", "icontains"):
                for field in ("name", "name_en", "name_en_us"):
                    try:
                        resolved_row = (
                            model.objects.filter(
                                **{f"{field}__{lookup}": query_text}
                            )
                            .order_by("name")
                            .first()
                        )
                    except Exception:
                        resolved_row = None
                    if resolved_row is not None:
                        break
                if resolved_row is not None:
                    break

    return _extract_solar_system_row(resolved_row)


def _get_user_alliance_corporation_ids(user) -> list[int]:
    corp_ids: set[int] = set()
    alliance_ids: set[int] = set()
    rows = CharacterOwnership.objects.filter(user=user).values_list(
        "character__corporation_id",
        "character__alliance_id",
    )
    for corp_id, alliance_id in rows:
        try:
            corp_id_int = int(corp_id or 0)
        except (TypeError, ValueError):
            corp_id_int = 0
        if corp_id_int > 0:
            corp_ids.add(corp_id_int)

        try:
            alliance_id_int = int(alliance_id or 0)
        except (TypeError, ValueError):
            alliance_id_int = 0
        if alliance_id_int > 0:
            alliance_ids.add(alliance_id_int)

    if alliance_ids:
        try:
            alliance_corp_rows = EveCorporationInfo.objects.filter(
                alliance_id__in=sorted(alliance_ids)
            ).values_list("corporation_id", flat=True)
            for corp_id in alliance_corp_rows:
                try:
                    corp_id_int = int(corp_id or 0)
                except (TypeError, ValueError):
                    corp_id_int = 0
                if corp_id_int > 0:
                    corp_ids.add(corp_id_int)
        except Exception:
            pass

    return sorted(corp_ids)


def _load_corptools_engineering_structures(
    corporation_ids: list[int],
) -> list[dict[str, object]]:
    try:
        # AA Example App
        from corptools.models.audits import CorporationAudit
        from corptools.models.structures import Structure
    except Exception:
        return []

    corp_ids = [int(corp_id) for corp_id in corporation_ids if int(corp_id) > 0]
    if not corp_ids:
        return []

    try:
        corp_audits = CorporationAudit.objects.filter(
            corporation__corporation_id__in=corp_ids
        ).select_related("corporation")
        rows = (
            Structure.objects.filter(
                corporation__in=corp_audits,
                type_id__in=list(ENGINEERING_COMPLEX_TYPE_INFO.keys()),
            )
            .select_related("corporation__corporation", "system_name")
            .order_by("structure_id")
        )
    except Exception:
        return []

    structures: list[dict[str, object]] = []
    for row in rows:
        try:
            structure_id = int(getattr(row, "structure_id", 0) or 0)
            structure_type_id = int(getattr(row, "type_id", 0) or 0)
        except (TypeError, ValueError):
            continue
        if structure_id <= 0 or structure_type_id not in ENGINEERING_COMPLEX_TYPE_INFO:
            continue

        type_info = ENGINEERING_COMPLEX_TYPE_INFO[structure_type_id]

        try:
            location_id = int(getattr(row, "system_id", 0) or 0)
        except (TypeError, ValueError):
            location_id = 0

        location_name = ""
        system_name_obj = getattr(row, "system_name", None)
        if system_name_obj is not None:
            location_name = str(getattr(system_name_obj, "name", "") or "").strip()

        owner_corporation_id = None
        owner_corporation_name = ""
        owner_corporation = getattr(row, "corporation", None)
        owner_corporation_eve = (
            getattr(owner_corporation, "corporation", None)
            if owner_corporation is not None
            else None
        )
        try:
            owner_corporation_id = int(
                getattr(owner_corporation_eve, "corporation_id", 0) or 0
            ) or None
        except (TypeError, ValueError):
            owner_corporation_id = None

        if owner_corporation_id:
            owner_corporation_name = str(
                getattr(owner_corporation_eve, "corporation_name", "") or ""
            ).strip()
            if not owner_corporation_name:
                owner_corporation_name = str(
                    get_corporation_name(owner_corporation_id) or ""
                )

        structures.append(
            {
                "structure_id": structure_id,
                "structure_name": str(getattr(row, "name", "") or "").strip()
                or f"Structure {structure_id}",
                "structure_type_id": structure_type_id,
                "structure_type_key": str(type_info["key"]),
                "structure_type_name": str(type_info["label"]),
                "material_bonus": float(type_info["material_bonus"]),
                "location_id": location_id if location_id > 0 else None,
                "location_name": location_name,
                "owner_corporation_id": owner_corporation_id,
                "owner_corporation_name": owner_corporation_name,
                "rig_keys": [],
                "rig_type_ids": [],
                "facility_tax": None,
            }
        )
    return structures


def _infer_rig_key_from_type_name(type_name: str) -> str | None:
    text = str(type_name or "").strip()
    if not text:
        return None
    for pattern, rig_key in _RIG_NAME_PATTERNS:
        if pattern.search(text):
            return rig_key
    return None


def _rank_rig_keys(rig_keys: set[str]) -> list[str]:
    return sorted(
        [str(key) for key in rig_keys if str(key)],
        key=lambda key: (-float(CRAFT_RIG_BONUS_BY_KEY.get(str(key), 0.0)), str(key)),
    )


def _infer_engineering_rigs_from_corptools_assets(
    corporation_ids: list[int],
    structure_ids: list[int],
) -> tuple[dict[int, list[str]], dict[int, list[int]]]:
    try:
        # AA Example App
        from corptools.models.assets import CorpAsset
        from corptools.models.audits import CorporationAudit
    except Exception:
        return {}, {}

    corp_ids = [int(corp_id) for corp_id in corporation_ids if int(corp_id) > 0]
    structure_id_set = {int(structure_id) for structure_id in structure_ids if int(structure_id) > 0}
    if not corp_ids or not structure_id_set:
        return {}, {}

    try:
        corp_audits = CorporationAudit.objects.filter(corporation__corporation_id__in=corp_ids)
        assets_qs = CorpAsset.objects.filter(corporation__in=corp_audits)
    except Exception:
        return {}, {}

    rig_keys_by_structure: dict[int, set[str]] = {
        structure_id: set() for structure_id in structure_id_set
    }
    rig_type_ids_by_structure: dict[int, set[int]] = {
        structure_id: set() for structure_id in structure_id_set
    }
    type_name_by_id: dict[int, str] = {}

    frontier_root_by_location: dict[int, int] = {
        int(structure_id): int(structure_id) for structure_id in structure_id_set
    }
    visited_location_ids: set[int] = set()

    for _depth in range(6):
        query_location_ids = [
            int(location_id)
            for location_id in frontier_root_by_location
            if int(location_id) not in visited_location_ids
        ]
        if not query_location_ids:
            break
        visited_location_ids.update(query_location_ids)

        try:
            rows = assets_qs.filter(location_id__in=query_location_ids).values_list(
                "item_id",
                "location_id",
                "location_flag",
                "type_id",
                "type_name__name",
            )
        except Exception:
            break

        next_frontier: dict[int, int] = {}
        for item_id, location_id, location_flag, type_id, type_name in rows:
            try:
                parent_location_id = int(location_id)
                type_id_int = int(type_id or 0)
            except (TypeError, ValueError):
                continue
            structure_id = int(frontier_root_by_location.get(parent_location_id) or 0)
            if structure_id <= 0 or structure_id not in structure_id_set or type_id_int <= 0:
                continue

            flag_text = str(location_flag or "").strip().lower()
            if not flag_text or any(hint in flag_text for hint in _RIG_LOCATION_FLAG_HINTS):
                type_name_text = str(type_name or "").strip()
                if not type_name_text:
                    cached_type_name = type_name_by_id.get(type_id_int)
                    if cached_type_name is None:
                        cached_type_name = str(get_type_name(type_id_int) or "")
                        type_name_by_id[type_id_int] = cached_type_name
                    type_name_text = cached_type_name
                candidate_key = _infer_rig_key_from_type_name(type_name_text)
                if candidate_key:
                    rig_keys_by_structure.setdefault(structure_id, set()).add(candidate_key)
                    rig_type_ids_by_structure.setdefault(structure_id, set()).add(type_id_int)

            try:
                item_id_int = int(item_id or 0)
            except (TypeError, ValueError):
                item_id_int = 0
            if (
                item_id_int > 0
                and item_id_int not in visited_location_ids
                and item_id_int not in next_frontier
            ):
                next_frontier[item_id_int] = structure_id

        frontier_root_by_location = next_frontier

    return (
        {
            structure_id: _rank_rig_keys(rig_keys)
            for structure_id, rig_keys in rig_keys_by_structure.items()
            if rig_keys
        },
        {
            structure_id: sorted(int(type_id) for type_id in type_ids if int(type_id) > 0)
            for structure_id, type_ids in rig_type_ids_by_structure.items()
            if type_ids
        },
    )


def _infer_engineering_rigs_from_cached_assets(
    corporation_ids: list[int],
    structure_ids: list[int],
) -> tuple[dict[int, list[str]], dict[int, list[int]]]:
    structure_id_set = {int(structure_id) for structure_id in structure_ids if int(structure_id) > 0}
    if not structure_id_set:
        return {}, {}

    rig_keys_by_structure: dict[int, set[str]] = {
        structure_id: set() for structure_id in structure_id_set
    }
    rig_type_ids_by_structure: dict[int, set[int]] = {
        structure_id: set() for structure_id in structure_id_set
    }

    type_name_by_id: dict[int, str] = {}
    for corp_id in corporation_ids:
        try:
            assets_qs, _ = get_corp_assets_cached(
                int(corp_id),
                allow_refresh=False,
                as_queryset=True,
                values_fields=["item_id", "location_id", "location_flag", "type_id"],
            )
        except Exception:
            continue

        frontier_root_by_location: dict[int, int] = {
            int(structure_id): int(structure_id) for structure_id in structure_id_set
        }
        visited_location_ids: set[int] = set()
        for _depth in range(6):
            query_location_ids = [
                int(location_id)
                for location_id in frontier_root_by_location
                if int(location_id) not in visited_location_ids
            ]
            if not query_location_ids:
                break
            visited_location_ids.update(query_location_ids)

            try:
                asset_rows = assets_qs.filter(location_id__in=query_location_ids)
            except Exception:
                break

            next_frontier: dict[int, int] = {}
            for asset in asset_rows:
                try:
                    parent_location_id = int(asset.get("location_id") or 0)
                    type_id = int(asset.get("type_id") or 0)
                except (TypeError, ValueError, AttributeError):
                    continue
                structure_id = int(frontier_root_by_location.get(parent_location_id) or 0)
                if structure_id <= 0 or structure_id not in structure_id_set or type_id <= 0:
                    continue

                flag_text = str(asset.get("location_flag") or "").strip().lower()
                if not flag_text or any(hint in flag_text for hint in _RIG_LOCATION_FLAG_HINTS):
                    type_name = type_name_by_id.get(type_id)
                    if type_name is None:
                        type_name = str(get_type_name(type_id) or "")
                        type_name_by_id[type_id] = type_name
                    candidate_key = _infer_rig_key_from_type_name(type_name)
                    if candidate_key:
                        rig_keys_by_structure.setdefault(structure_id, set()).add(candidate_key)
                        rig_type_ids_by_structure.setdefault(structure_id, set()).add(type_id)

                try:
                    item_id = int(asset.get("item_id") or 0)
                except (TypeError, ValueError, AttributeError):
                    item_id = 0
                if (
                    item_id > 0
                    and item_id not in visited_location_ids
                    and item_id not in next_frontier
                ):
                    next_frontier[item_id] = structure_id

            frontier_root_by_location = next_frontier

    return (
        {
            structure_id: _rank_rig_keys(rig_keys)
            for structure_id, rig_keys in rig_keys_by_structure.items()
            if rig_keys
        },
        {
            structure_id: sorted(int(type_id) for type_id in type_ids if int(type_id) > 0)
            for structure_id, type_ids in rig_type_ids_by_structure.items()
            if type_ids
        },
    )


def _merge_rig_data(
    *rig_data: tuple[dict[int, list[str]], dict[int, list[int]]],
) -> tuple[dict[int, list[str]], dict[int, list[int]]]:
    rig_keys_by_structure: dict[int, set[str]] = {}
    rig_type_ids_by_structure: dict[int, set[int]] = {}

    for rig_keys_map, rig_ids_map in rig_data:
        for structure_id, rig_keys in (rig_keys_map or {}).items():
            sid = int(structure_id or 0)
            if sid <= 0:
                continue
            rig_keys_by_structure.setdefault(sid, set()).update(
                str(rig_key) for rig_key in (rig_keys or []) if str(rig_key)
            )
        for structure_id, rig_type_ids in (rig_ids_map or {}).items():
            sid = int(structure_id or 0)
            if sid <= 0:
                continue
            rig_type_ids_by_structure.setdefault(sid, set()).update(
                int(rig_type_id)
                for rig_type_id in (rig_type_ids or [])
                if int(rig_type_id) > 0
            )

    return (
        {
            structure_id: _rank_rig_keys(rig_keys)
            for structure_id, rig_keys in rig_keys_by_structure.items()
            if rig_keys
        },
        {
            structure_id: sorted(rig_type_ids)
            for structure_id, rig_type_ids in rig_type_ids_by_structure.items()
            if rig_type_ids
        },
    )


def fetch_engineering_structures_for_user(
    user,
    *,
    system_id: int | None = None,
) -> list[dict[str, object]]:
    corp_ids = _get_user_alliance_corporation_ids(user)
    if not corp_ids:
        return []

    cache_key = f"indy_hub:craft_env:engineering_structures:v1:user:{int(user.id)}"
    cached = cache.get(cache_key)
    structures: list[dict[str, object]]
    if isinstance(cached, list):
        structures = [dict(row) for row in cached]
    else:
        structures = _load_corptools_engineering_structures(corp_ids)
        structure_ids = [
            int(row.get("structure_id") or 0)
            for row in structures
            if int(row.get("structure_id") or 0) > 0
        ]
        rig_data_corptools = _infer_engineering_rigs_from_corptools_assets(
            corp_ids,
            structure_ids,
        )
        rig_data_cached_assets = _infer_engineering_rigs_from_cached_assets(
            corp_ids,
            structure_ids,
        )
        rig_keys_map, rig_type_ids_map = _merge_rig_data(
            rig_data_corptools,
            rig_data_cached_assets,
        )
        for row in structures:
            structure_id = int(row.get("structure_id") or 0)
            row["rig_keys"] = rig_keys_map.get(structure_id, [])
            row["rig_type_ids"] = rig_type_ids_map.get(structure_id, [])
        cache.set(cache_key, structures, 300)

    facility_tax_by_structure = _get_industry_facility_tax_map()
    for row in structures:
        structure_id = int(row.get("structure_id") or 0)
        if structure_id <= 0:
            row["facility_tax"] = None
            continue
        tax_value = facility_tax_by_structure.get(structure_id)
        row["facility_tax"] = (
            float(tax_value)
            if tax_value is not None
            else None
        )

    if system_id and int(system_id) > 0:
        sid = int(system_id)
        structures = [
            row
            for row in structures
            if int(row.get("location_id") or 0) == sid
        ]

    structures.sort(
        key=lambda row: (
            str(row.get("structure_name") or "").lower(),
            int(row.get("structure_id") or 0),
        )
    )
    return structures


def resolve_craft_system_context(
    *,
    user,
    system_text: str = "",
    system_id: int | None = None,
    include_structures: bool = True,
) -> dict[str, object]:
    resolved_system = resolve_solar_system(system_text=system_text, system_id=system_id)
    if not resolved_system:
        return {
            "system": None,
            "structures": [],
        }

    resolved_system_id = int(resolved_system.get("system_id") or 0)
    manufacturing_index = _get_manufacturing_index_map().get(resolved_system_id)
    if manufacturing_index is None:
        manufacturing_index = 0.0

    system_payload = {
        **resolved_system,
        "manufacturing_cost_index": float(manufacturing_index),
        "manufacturing_cost_percent": float(manufacturing_index) * 100.0,
    }

    structures: list[dict[str, object]] = []
    if include_structures:
        structures = fetch_engineering_structures_for_user(
            user,
            system_id=resolved_system_id,
        )

    return {
        "system": system_payload,
        "structures": structures,
    }
