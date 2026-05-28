"""Reusable capital ship option helpers."""

from __future__ import annotations

# Django
from django.core.cache import cache

# Alliance Auth (External Libs)
from eve_sde.models import ItemType

# AA Example App
from indy_hub.models import MaterialExchangeConfig

_CAPITAL_SHIP_OPTIONS_CACHE_KEY = "indy_hub:capital_ship_orders:options:v2"
_SHIP_CLASS_ORDER = {
    "dread": 0,
    "carrier": 1,
    "fax": 2,
    "super": 3,
    "titan": 4,
    "freighter": 5,
    "jump_freighter": 6,
    "capital_indy": 7,
}
_SHIP_CLASS_LABEL = {
    "dread": "Dreadnought",
    "carrier": "Carrier",
    "fax": "FAX",
    "super": "Supercarrier",
    "titan": "Titan",
    "freighter": "Freighter",
    "jump_freighter": "Jump Freighter",
    "capital_indy": "Capital Industrial",
}
_SDE_GROUP_NAME_TO_SHIP_CLASS = {
    "Dreadnought": "dread",
    "Lancer Dreadnought": "dread",
    "Carrier": "carrier",
    "Force Auxiliary": "fax",
    "Supercarrier": "super",
    "Titan": "titan",
    "Freighter": "freighter",
    "Jump Freighter": "jump_freighter",
    "Capital Industrial Ship": "capital_indy",
}


def normalize_ship_class_key(raw_value: str) -> str:
    normalized = str(raw_value or "").strip().lower()
    if not normalized:
        return ""
    normalized = normalized.replace("-", "_").replace(" ", "_")
    normalized = "".join(char if (char.isalnum() or char == "_") else "_" for char in normalized).strip("_")
    return normalized


def default_ship_class_label(ship_class: str) -> str:
    key = normalize_ship_class_key(ship_class)
    if key in _SHIP_CLASS_LABEL:
        return _SHIP_CLASS_LABEL[key]
    if not key:
        return "Capital"
    return key.replace("_", " ").title()


def resolve_ship_class_for_group_name(group_name: str) -> str | None:
    return _SDE_GROUP_NAME_TO_SHIP_CLASS.get(str(group_name or "").strip())


def _load_base_capital_ship_options() -> list[dict[str, object]]:
    cached = cache.get(_CAPITAL_SHIP_OPTIONS_CACHE_KEY)
    if isinstance(cached, list):
        normalized_cached: list[dict[str, object]] = []
        for entry in cached:
            if not isinstance(entry, dict):
                continue
            try:
                type_id = int(entry.get("type_id"))
            except (TypeError, ValueError):
                continue
            type_name = str(entry.get("type_name") or "").strip()
            ship_class = normalize_ship_class_key(entry.get("ship_class"))
            if type_id <= 0 or not type_name or ship_class not in _SHIP_CLASS_ORDER:
                continue
            normalized_cached.append(
                {
                    "type_id": type_id,
                    "type_name": type_name,
                    "ship_class": ship_class,
                    "ship_class_label": _SHIP_CLASS_LABEL[ship_class],
                }
            )
        if normalized_cached:
            return normalized_cached

    options_by_type_id: dict[int, dict[str, object]] = {}
    type_rows = ItemType.objects.filter(
        group__category_id=6,
        group__name__in=list(_SDE_GROUP_NAME_TO_SHIP_CLASS.keys()),
    ).values_list("id", "name", "group__name")
    for type_id, type_name, group_name in type_rows:
        ship_class = resolve_ship_class_for_group_name(str(group_name or ""))
        if not ship_class:
            continue
        type_id_int = int(type_id)
        clean_name = str(type_name or "").strip()
        if type_id_int <= 0 or not clean_name:
            continue
        options_by_type_id[type_id_int] = {
            "type_id": type_id_int,
            "type_name": clean_name,
            "ship_class": ship_class,
            "ship_class_label": _SHIP_CLASS_LABEL[ship_class],
        }

    options = sorted(
        options_by_type_id.values(),
        key=lambda row: (
            _SHIP_CLASS_ORDER.get(str(row["ship_class"]), 99),
            str(row["type_name"]).lower(),
        ),
    )
    cache.set(_CAPITAL_SHIP_OPTIONS_CACHE_KEY, options, 3600)
    return options


def _sort_capital_ship_options(options: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(
        options,
        key=lambda row: (
            _SHIP_CLASS_ORDER.get(normalize_ship_class_key(row.get("ship_class")), 99),
            str(row.get("ship_class_label") or default_ship_class_label(str(row.get("ship_class") or ""))).lower(),
            str(row.get("type_name") or "").lower(),
        ),
    )


def load_capital_ship_options(*, config: MaterialExchangeConfig | None = None) -> list[dict[str, object]]:
    """Return enabled capital hull options for the current config."""
    base_options = _load_base_capital_ship_options()
    if not config:
        return _sort_capital_ship_options([dict(row) for row in base_options])

    options_by_type_id: dict[int, dict[str, object]] = {
        int(row["type_id"]): {
            "type_id": int(row["type_id"]),
            "type_name": str(row["type_name"]),
            "ship_class": normalize_ship_class_key(row.get("ship_class")),
            "ship_class_label": str(
                row.get("ship_class_label") or default_ship_class_label(str(row.get("ship_class") or ""))
            ),
        }
        for row in base_options
    }

    disabled_type_ids: set[int] = set()
    disabled_groups: set[str] = set()
    try:
        disabled_type_ids = set(config.get_capital_disabled_ship_type_ids())
    except Exception:
        disabled_type_ids = set()
    try:
        disabled_groups = {
            normalize_ship_class_key(group_value) for group_value in config.get_capital_disabled_ship_groups()
        }
        disabled_groups.discard("")
    except Exception:
        disabled_groups = set()

    for type_id in disabled_type_ids:
        options_by_type_id.pop(int(type_id), None)

    if disabled_groups:
        options_by_type_id = {
            type_id: row
            for type_id, row in options_by_type_id.items()
            if normalize_ship_class_key(row.get("ship_class")) not in disabled_groups
        }

    return _sort_capital_ship_options(list(options_by_type_id.values()))


def load_capital_ship_options_for_editor(
    *,
    config: MaterialExchangeConfig | None = None,
) -> list[dict[str, object]]:
    """Return editor-friendly capital hull options."""
    del config

    options_by_type_id: dict[int, dict[str, object]] = {
        int(row["type_id"]): {
            "type_id": int(row["type_id"]),
            "type_name": str(row["type_name"]),
            "ship_class": normalize_ship_class_key(row.get("ship_class")),
            "ship_class_label": str(
                row.get("ship_class_label") or default_ship_class_label(str(row.get("ship_class") or ""))
            ),
            "enabled": True,
        }
        for row in _load_base_capital_ship_options()
    }
    return _sort_capital_ship_options(list(options_by_type_id.values()))
