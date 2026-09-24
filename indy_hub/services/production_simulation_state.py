"""Validation and compatibility helpers for production simulation snapshots."""

# Future
from __future__ import annotations

# Standard Library
import json
from copy import deepcopy
from typing import Any

STATE_SCHEMA_VERSION = 3

# A simulation snapshot is a whole workspace, so it is legitimately large, but
# nothing bounded it before: Django's 2.5 MB body cap was the only limit and a
# blob that size would persist and be re-served on every load.
MAX_UI_STATE_BYTES = 512 * 1024

# Preferences are a handful of scalars plus two small dicts.
MAX_PREFERENCE_BYTES = 32 * 1024
MAX_PREFERENCE_STRING_LENGTH = 128

# Tab names come from data-tab-name in Craft_BP_v2.html. showCraftMainTab()
# interpolates this straight into a querySelector, so an unconstrained value is
# both a broken selector and a bad thing to persist.
CRAFT_MAIN_TABS = frozenset(
    {
        "plan",
        "buy",
        "build",
        "configure",
        "buy_bpcs",
        "how_to",
    }
)

MATERIALS_SOURCE_MODES = frozenset({"manual", "designated_bay"})


class StateTooLargeError(ValueError):
    """Raised when a submitted snapshot exceeds its byte budget."""

    def __init__(self, *, size: int, limit: int) -> None:
        self.size = size
        self.limit = limit
        super().__init__(f"state is {size} bytes, limit is {limit}")


def _encoded_size(value: Any) -> int:
    """Byte size of the value as it would be stored in the JSONField."""
    try:
        return len(
            json.dumps(value, separators=(",", ":"), default=str).encode("utf-8")
        )
    except (TypeError, ValueError):
        return 0


def migrate_ui_state(
    value: Any, *, max_bytes: int = MAX_UI_STATE_BYTES
) -> dict[str, Any]:
    """Return a safe, versioned snapshot while tolerating older payloads.

    Applied on write *and* on read, so an old row is normalized before it
    reaches the browser rather than only as a side effect of the next save.

    Raises StateTooLargeError when the payload exceeds max_bytes.
    """
    if not isinstance(value, dict):
        value = {}

    size = _encoded_size(value)
    if size > max_bytes:
        raise StateTooLargeError(size=size, limit=max_bytes)

    state = deepcopy(value)
    version = state.get("schemaVersion", state.get("schema_version", 1))
    try:
        version = int(version)
    except (TypeError, ValueError):
        version = 1

    # Version 1 snapshots used camelCase and had no explicit version. Keep all
    # unknown keys so older clients do not lose controls during a save.
    if version < 2:
        state.setdefault("displayPreferences", {})
    # Version 3 retired the Run optimized tab. Its saved results are dropped,
    # and a snapshot that was left on that tab (or on any unknown tab) opens on
    # Plan instead of pointing showCraftMainTab() at a pane that is gone.
    if version < 3:
        state.pop("runOptimized", None)
    craft_main_tab = state.get("craftMainTab")
    if craft_main_tab is not None and craft_main_tab not in CRAFT_MAIN_TABS:
        state["craftMainTab"] = "plan"
    state["schemaVersion"] = STATE_SCHEMA_VERSION
    state.pop("schema_version", None)
    return state


def _clean_bool(value: Any):
    return bool(value) if isinstance(value, bool) else None


def _clean_number(value: Any, *, minimum: float, maximum: float):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if value != value or value in (float("inf"), float("-inf")):  # NaN / inf
        return None
    if value < minimum or value > maximum:
        return None
    return value


def _clean_string(value: Any, *, choices: frozenset[str] | None = None):
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned or len(cleaned) > MAX_PREFERENCE_STRING_LENGTH:
        return None
    if choices is not None and cleaned not in choices:
        return None
    return cleaned


def _clean_location_id(value: Any):
    if isinstance(value, bool):
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return ""
        if not cleaned.isdigit() or len(cleaned) > 20:
            return None
        return cleaned
    if isinstance(value, int) and value > 0:
        return str(value)
    return None


def _clean_mapping(value: Any):
    if not isinstance(value, dict):
        return None
    if _encoded_size(value) > MAX_PREFERENCE_BYTES:
        return None
    return deepcopy(value)


# Each durable preference declares how its value is validated. Allowlisting the
# key alone let a 5000-char activeTab or an arbitrarily large blob through.
_PREFERENCE_VALIDATORS = {
    "taxesEnabled": _clean_bool,
    "taxRate": lambda v: _clean_number(v, minimum=0, maximum=100),
    "shippingPreference": _clean_string,
    "ownedMaterialsSource": _clean_string,
    "bpcSource": _clean_string,
    "slotPreferences": _clean_mapping,
    "displayPreferences": _clean_mapping,
    "activeTab": lambda v: _clean_string(v, choices=CRAFT_MAIN_TABS),
    "scheduleTrackingOptIn": _clean_bool,
    "materialsSourceMode": lambda v: _clean_string(v, choices=MATERIALS_SOURCE_MODES),
    "materialsSourceLocationId": _clean_location_id,
    # Private defaults for Configure; never part of a share link. These are
    # only hints: the client selects the structure only when the server lists
    # it for this user and system.
    "lastSystemId": _clean_location_id,
    "lastStructureId": _clean_location_id,
}


def normalize_preference_state(value: Any) -> dict[str, Any]:
    """Keep only durable, non-sensitive simulator defaults with valid values.

    Unknown keys and values that fail validation are dropped rather than
    rejected, so one bad field cannot discard an otherwise good payload.
    """
    if not isinstance(value, dict):
        return {}

    cleaned: dict[str, Any] = {}
    for key, validator in _PREFERENCE_VALIDATORS.items():
        if key not in value:
            continue
        result = validator(value[key])
        if result is not None:
            cleaned[key] = result

    # Belt and braces: the per-field caps should make this unreachable.
    if _encoded_size(cleaned) > MAX_PREFERENCE_BYTES:
        return {}
    return cleaned


# --- Share links -----------------------------------------------------------
#
# Server-side mirror of the share schema in craft_bp.js (collectCraftShareState
# / decodeCraftShareState). The browser builds and applies links; this is the
# reference for what a link may contain, so the allowlist is reviewable and
# tested in Python. Keep the two in step.

SHARE_SCHEMA_VERSION = 1
MAX_SHARE_ENCODED_LENGTH = 12000
MAX_SHARE_RUNS = 1_000_000
SHARE_INPUT_ALLOWLIST = {
    "industryFeeEnabledInput": "checkbox",
    "industryFeeFacilityTaxInput": "number",
    "industryFeeSecurityInput": "text",
    "industryFeeStructureTypeIdInput": "text",
    "industryFeeSystemCostBonusInput": "number",
    "buildScheduleMode": "text",
    "buildScheduleTargetDays": "number",
}
_SHARE_TEXT_ALLOWED = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
)


class ShareStateError(ValueError):
    """A share link that cannot be used; callers fall back to user defaults."""


def _positive_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return number


def _share_inputs(entries: Any) -> list[dict[str, Any]]:
    safe: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, dict):
            continue
        input_id = str(entry.get("id") or "")
        kind = SHARE_INPUT_ALLOWLIST.get(input_id)
        if kind is None or input_id in seen:
            continue
        if kind == "checkbox":
            safe.append(
                {"id": input_id, "type": kind, "checked": bool(entry.get("checked"))}
            )
            seen.add(input_id)
            continue
        raw = "" if entry.get("value") is None else str(entry.get("value"))[:32]
        if kind == "number":
            number = _finite_number(raw) if raw else 0.0
            if number is None or number < 0 or number > 1_000_000:
                continue
        elif not set(raw) <= _SHARE_TEXT_ALLOWED:
            continue
        safe.append({"id": input_id, "type": kind, "value": raw})
        seen.add(input_id)
    return safe


def normalize_share_state(value: Any) -> dict[str, Any]:
    """Validate a decoded share payload against the explicit allowlist.

    Unknown keys are dropped (so newer links degrade gracefully), values are
    bounded and type-normalized, and nothing private can survive: there is no
    field for characters, owned blueprints or their use flag, assets, the
    materials source, or the build system/structure.
    """
    if not isinstance(value, dict):
        raise ShareStateError("malformed")
    if value.get("v") != SHARE_SCHEMA_VERSION:
        raise ShareStateError("unsupported_version")
    blueprint_type_id = _positive_int(value.get("blueprint_type_id"))
    if not blueprint_type_id:
        raise ShareStateError("malformed")

    runs = _finite_number(value.get("runs")) or 1
    tab = value.get("tab")
    buy = value.get("buy") if isinstance(value.get("buy"), list) else []

    me_te_raw = value.get("me_te") if isinstance(value.get("me_te"), dict) else {}
    configs_raw = me_te_raw.get("blueprintConfigs")
    configs: dict[str, dict[str, int]] = {}
    for raw_type_id, entry in list(
        (configs_raw if isinstance(configs_raw, dict) else {}).items()
    )[:200]:
        type_id = _positive_int(raw_type_id)
        if not type_id or not isinstance(entry, dict):
            continue
        safe_entry: dict[str, int] = {}
        if "me" in entry:
            safe_entry["me"] = max(0, min(10, int(_finite_number(entry["me"]) or 0)))
        if "te" in entry:
            safe_entry["te"] = max(0, min(20, int(_finite_number(entry["te"]) or 0)))
        if safe_entry:
            configs[str(type_id)] = safe_entry

    prices = []
    for price in value.get("prices") if isinstance(value.get("prices"), list) else []:
        if not isinstance(price, dict):
            continue
        type_id = _positive_int(price.get("item_type_id"))
        unit_price = _finite_number(price.get("unit_price"))
        if not type_id or unit_price is None or unit_price < 0:
            continue
        prices.append(
            {
                "item_type_id": type_id,
                "unit_price": unit_price,
                "is_sale_price": bool(price.get("is_sale_price")),
            }
        )

    shipping = value.get("shipping") if isinstance(value.get("shipping"), dict) else {}
    route = _positive_int(shipping.get("route"))
    display = value.get("display") if isinstance(value.get("display"), dict) else {}
    tree_open = (
        display.get("tree_open") if isinstance(display.get("tree_open"), list) else []
    )
    configure_open = (
        display.get("configure_open")
        if isinstance(display.get("configure_open"), list)
        else []
    )

    return {
        "v": SHARE_SCHEMA_VERSION,
        "blueprint_type_id": blueprint_type_id,
        "runs": max(1, min(MAX_SHARE_RUNS, int(runs))),
        "tab": tab if tab in CRAFT_MAIN_TABS else "plan",
        "buy": [
            type_id for type_id in (_positive_int(item) for item in buy) if type_id
        ][:5000],
        "me_te": {
            "mainME": max(
                0, min(10, int(_finite_number(me_te_raw.get("mainME")) or 0))
            ),
            "mainTE": max(
                0, min(20, int(_finite_number(me_te_raw.get("mainTE")) or 0))
            ),
            "blueprintConfigs": configs,
        },
        "prices": prices[:500],
        "inputs": _share_inputs(value.get("inputs")),
        "shipping": {"route": route} if route else None,
        "display": {
            "tree_open": [bool(item) for item in tree_open][:200],
            "configure_open": [
                item
                for item in configure_open
                if isinstance(item, str)
                and 0 < len(item) <= 64
                and item.replace("-", "").replace("_", "").isalnum()
            ][:100],
        },
    }


def decode_share_param(encoded: Any) -> dict[str, Any]:
    """Decode and validate a ``share`` query value (URL-safe base64 JSON).

    Raises ShareStateError with a short reason code: ``missing``,
    ``oversized``, ``malformed`` or ``unsupported_version``.
    """
    # Standard Library
    import base64
    import binascii

    text = str(encoded or "").strip()
    if not text:
        raise ShareStateError("missing")
    if len(text) > MAX_SHARE_ENCODED_LENGTH:
        raise ShareStateError("oversized")
    try:
        padded = text + "=" * (-len(text) % 4)
        payload = json.loads(
            base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        )
    except (binascii.Error, UnicodeError, ValueError):
        raise ShareStateError("malformed") from None
    return normalize_share_state(payload)
