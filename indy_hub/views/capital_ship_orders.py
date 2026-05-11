"""Capital ship order views."""

from __future__ import annotations

# Standard Library
import json
from decimal import Decimal, InvalidOperation

# Django
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.db.models import Q
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from django.views.decorators.http import require_GET, require_POST

# Alliance Auth
from allianceauth.authentication.models import State, UserProfile
from allianceauth.services.hooks import get_extension_logger
from eve_sde.models import ItemType

# AA Example App
from indy_hub.decorators import indy_hub_permission_required
from indy_hub.models import (
    CapitalShipOrder,
    CapitalShipOrderChat,
    CapitalShipOrderEvent,
    CapitalShipOrderMessage,
    IndustryJob,
    MaterialExchangeConfig,
    MaterialExchangeSettings,
)
from indy_hub.notifications import notify_multi, notify_user
from indy_hub.utils.analytics import emit_view_analytics_event

# Local
from .navigation import build_nav_context

logger = get_extension_logger(__name__)

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
_CAPITAL_TERMINAL_STATUSES = {
    CapitalShipOrder.Status.COMPLETED,
    CapitalShipOrder.Status.REJECTED,
    CapitalShipOrder.Status.CANCELLED,
}
_CAPITAL_ACTIVE_STATUSES = {
    CapitalShipOrder.Status.WAITING,
    CapitalShipOrder.Status.GATHERING_MATERIALS,
    CapitalShipOrder.Status.IN_PRODUCTION,
    CapitalShipOrder.Status.CONTRACT_CREATED,
    CapitalShipOrder.Status.ANOMALY,
}
_ROLE_TO_VIEWER = {
    CapitalShipOrderChat.SenderRole.REQUESTER: "buyer",
    CapitalShipOrderChat.SenderRole.ADMIN: "seller",
}
_VIEWER_TO_ROLE = {
    "buyer": CapitalShipOrderChat.SenderRole.REQUESTER,
    "seller": CapitalShipOrderChat.SenderRole.ADMIN,
}
_CAPITAL_MANAGER_PERMISSION = "indy_hub.can_manage_capital_orders"
_CAPITAL_BUILDER_PERMISSION = "indy_hub.can_build_capital_orders"
_PRE_PRODUCTION_STATUSES = {
    CapitalShipOrder.Status.WAITING,
}


def _normalize_ship_class_key(raw_value: str) -> str:
    normalized = str(raw_value or "").strip().lower()
    if not normalized:
        return ""
    normalized = normalized.replace("-", "_").replace(" ", "_")
    normalized = "".join(
        char if (char.isalnum() or char == "_") else "_"
        for char in normalized
    ).strip("_")
    return normalized


def _default_ship_class_label(ship_class: str) -> str:
    key = _normalize_ship_class_key(ship_class)
    if key in _SHIP_CLASS_LABEL:
        return _SHIP_CLASS_LABEL[key]
    if not key:
        return "Capital"
    return key.replace("_", " ").title()


def _is_material_exchange_enabled() -> bool:
    try:
        return bool(MaterialExchangeSettings.get_solo().is_enabled)
    except Exception:
        return True


def _get_material_exchange_config() -> MaterialExchangeConfig | None:
    return MaterialExchangeConfig.objects.first()


def _resolve_ship_class_for_group_name(group_name: str) -> str | None:
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
            ship_class = _normalize_ship_class_key(entry.get("ship_class"))
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
        ship_class = _resolve_ship_class_for_group_name(str(group_name or ""))
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
            _SHIP_CLASS_ORDER.get(_normalize_ship_class_key(row.get("ship_class")), 99),
            str(
                row.get("ship_class_label")
                or _default_ship_class_label(str(row.get("ship_class") or ""))
            ).lower(),
            str(row.get("type_name") or "").lower(),
        ),
    )


def _load_capital_ship_options(
    *, config: MaterialExchangeConfig | None = None
) -> list[dict[str, object]]:
    base_options = _load_base_capital_ship_options()
    if not config:
        return _sort_capital_ship_options([dict(row) for row in base_options])

    options_by_type_id: dict[int, dict[str, object]] = {
        int(row["type_id"]): {
            "type_id": int(row["type_id"]),
            "type_name": str(row["type_name"]),
            "ship_class": _normalize_ship_class_key(row.get("ship_class")),
            "ship_class_label": str(
                row.get("ship_class_label")
                or _default_ship_class_label(str(row.get("ship_class") or ""))
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
            _normalize_ship_class_key(group_value)
            for group_value in config.get_capital_disabled_ship_groups()
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
            if _normalize_ship_class_key(row.get("ship_class")) not in disabled_groups
        }

    return _sort_capital_ship_options(list(options_by_type_id.values()))


def _load_capital_ship_options_for_editor(
    *, config: MaterialExchangeConfig | None = None
) -> list[dict[str, object]]:
    options_by_type_id: dict[int, dict[str, object]] = {
        int(row["type_id"]): {
            "type_id": int(row["type_id"]),
            "type_name": str(row["type_name"]),
            "ship_class": _normalize_ship_class_key(row.get("ship_class")),
            "ship_class_label": str(
                row.get("ship_class_label")
                or _default_ship_class_label(str(row.get("ship_class") or ""))
            ),
            "enabled": True,
        }
        for row in _load_base_capital_ship_options()
    }
    return _sort_capital_ship_options(list(options_by_type_id.values()))


def _resolve_main_character_name(user) -> str:
    if not user:
        return ""
    try:
        profile = UserProfile.objects.select_related("main_character").get(user=user)
        main_character = getattr(profile, "main_character", None)
        if main_character and getattr(main_character, "character_name", None):
            return str(main_character.character_name)
    except UserProfile.DoesNotExist:
        pass
    except Exception:
        pass
    return str(getattr(user, "username", ""))


def _quantize_isk(value: Decimal | str | int | float | None) -> Decimal | None:
    if value is None:
        return None
    normalized = str(value).strip().replace(",", "")
    if not normalized:
        return None
    try:
        parsed = Decimal(normalized)
    except (InvalidOperation, TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed.quantize(Decimal("0.01"))


def _parse_positive_int(raw_value, *, minimum: int = 0) -> int | None:
    try:
        parsed = int(str(raw_value).strip())
    except (TypeError, ValueError, AttributeError):
        return None
    if parsed < minimum:
        return None
    return parsed


def _parse_positive_int_or_raise(
    raw_value,
    *,
    label: str,
    minimum: int = 0,
    fallback: int | None = None,
) -> int:
    normalized = str(raw_value or "").strip()
    if not normalized and fallback is not None:
        return int(fallback)
    try:
        parsed = int(normalized)
    except (TypeError, ValueError):
        raise ValueError(f"{label} must be a whole number.")
    if parsed < minimum:
        raise ValueError(f"{label} must be at least {minimum}.")
    return int(parsed)


def _normalize_ship_group_list(raw_values: list[str]) -> list[str]:
    normalized: list[str] = []
    for raw_value in raw_values:
        key = _normalize_ship_class_key(raw_value)
        if not key:
            continue
        if key not in normalized:
            normalized.append(key)
    return normalized


def _parse_positive_type_ids(raw_values: list[str]) -> list[int]:
    parsed: list[int] = []
    for raw_value in raw_values:
        try:
            type_id = int(str(raw_value).strip())
        except (TypeError, ValueError, AttributeError):
            continue
        if type_id <= 0:
            continue
        if type_id not in parsed:
            parsed.append(type_id)
    return parsed


def _parse_state_name_list(raw_value: str | list[str] | tuple[str, ...] | set[str]) -> list[str]:
    if isinstance(raw_value, (list, tuple, set)):
        tokens = [str(token or "").strip() for token in raw_value]
    else:
        tokens = [
            str(token or "").strip()
            for token in str(raw_value or "").replace("\n", ",").split(",")
        ]

    normalized: list[str] = []
    for token in tokens:
        state_name = str(token or "").strip()
        if not state_name:
            continue
        if state_name not in normalized:
            normalized.append(state_name)
    return normalized


def _load_allianceauth_state_name_choices(
    *, selected_names: list[str] | None = None
) -> list[str]:
    selected_names = list(selected_names or [])
    state_names: list[str] = []
    try:
        state_names = list(
            State.objects.order_by("-priority", "name").values_list("name", flat=True)
        )
    except Exception:
        state_names = []

    combined_names: list[str] = []
    for raw_name in [*state_names, *selected_names]:
        name = str(raw_name or "").strip()
        if not name:
            continue
        if name not in combined_names:
            combined_names.append(name)
    return combined_names


def _decimal_to_json_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _median_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    size = len(ordered)
    mid = size // 2
    if size % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / Decimal("2")


def _get_class_default_price(config: MaterialExchangeConfig, ship_class: str) -> Decimal | None:
    return None


def _get_ship_default_price(
    config: MaterialExchangeConfig, *, ship_type_id: int | str, ship_class: str
) -> tuple[Decimal | None, str]:
    try:
        type_id = int(ship_type_id)
    except (TypeError, ValueError):
        type_id = 0

    if type_id > 0:
        try:
            override_map = config.get_capital_ship_estimated_price_map()
        except Exception:
            override_map = {}
        price_override = _quantize_isk(override_map.get(type_id))
        if price_override is not None:
            return price_override, "ship_config_override"

        try:
            auto_map = config.get_capital_ship_auto_estimated_price_map()
        except Exception:
            auto_map = {}
        auto_price = _quantize_isk(auto_map.get(type_id))
        if auto_price is not None:
            return auto_price, "craft_buy_cost_plus_10"

    class_price = _get_class_default_price(config, ship_class)
    if class_price is not None:
        return class_price, "class_config_default"
    return None, ""


def _get_class_default_eta_window(config: MaterialExchangeConfig, ship_class: str) -> tuple[int | None, int | None]:
    return None, None


def _estimate_guideline_price(order: CapitalShipOrder) -> tuple[Decimal | None, str]:
    historical_prices = list(
        CapitalShipOrder.objects.filter(
            config=order.config,
            ship_type_id=int(order.ship_type_id),
            agreed_price_isk__isnull=False,
        )
        .exclude(id=order.id)
        .order_by("-agreement_locked_at", "-updated_at")
        .values_list("agreed_price_isk", flat=True)[:30]
    )
    historical_decimals = [
        _quantize_isk(value)
        for value in historical_prices
        if _quantize_isk(value) is not None
    ]
    if historical_decimals:
        return _quantize_isk(_median_decimal(historical_decimals)), "historical_orders"

    default_price, price_source = _get_ship_default_price(
        order.config,
        ship_type_id=order.ship_type_id,
        ship_class=order.ship_class,
    )
    if default_price is not None:
        return default_price, price_source or "config_default"
    return None, ""


def _estimate_guideline_eta(order: CapitalShipOrder) -> tuple[int | None, int | None, str]:
    durations_seconds = list(
        IndustryJob.objects.filter(
            product_type_id=int(order.ship_type_id),
            duration__gt=0,
        )
        .order_by("-end_date")
        .values_list("duration", flat=True)[:120]
    )
    lead_time_days = (
        _parse_positive_int(order.lead_time_days, minimum=0)
        if order.lead_time_days is not None
        else None
    )
    if lead_time_days is None:
        lead_time_days = _parse_positive_int(
            getattr(order.config, "capital_default_lead_time_days", 0), minimum=0
        )
    lead_time_days = int(lead_time_days or 0)

    if durations_seconds:
        day_samples: list[int] = []
        for raw_seconds in durations_seconds:
            try:
                seconds_value = int(raw_seconds)
            except (TypeError, ValueError):
                continue
            if seconds_value <= 0:
                continue
            days_value = max(1, (seconds_value + 86399) // 86400)
            day_samples.append(int(days_value))
        if day_samples:
            sorted_days = sorted(day_samples)
            idx_low = int(round((len(sorted_days) - 1) * 0.30))
            idx_high = int(round((len(sorted_days) - 1) * 0.70))
            eta_min = int(sorted_days[idx_low]) + lead_time_days
            eta_max = int(sorted_days[idx_high]) + lead_time_days
            if eta_max < eta_min:
                eta_max = eta_min
            return eta_min, eta_max, "industry_jobs"

    default_min, default_max = _get_class_default_eta_window(order.config, order.ship_class)
    if default_min is None or default_max is None:
        return None, None, ""
    eta_min = int(default_min) + lead_time_days
    eta_max = int(default_max) + lead_time_days
    if eta_max < eta_min:
        eta_max = eta_min
    return eta_min, eta_max, "config_default"


def _refresh_guideline(order: CapitalShipOrder) -> None:
    guideline_price, guideline_price_source = _estimate_guideline_price(order)
    eta_min_days, eta_max_days, eta_source = _estimate_guideline_eta(order)
    source = guideline_price_source or eta_source or ""

    order.guideline_price_isk = guideline_price
    order.guideline_price_source = source
    order.guideline_eta_min_days = eta_min_days
    order.guideline_eta_max_days = eta_max_days
    order.guideline_generated_at = timezone.now()
    order.save(
        update_fields=[
            "guideline_price_isk",
            "guideline_price_source",
            "guideline_eta_min_days",
            "guideline_eta_max_days",
            "guideline_generated_at",
            "updated_at",
        ]
    )


def _can_manage_capital_orders(user: User | None) -> bool:
    return bool(user and user.has_perm(_CAPITAL_MANAGER_PERMISSION))


def _can_build_capital_orders(user: User | None) -> bool:
    return bool(user and user.has_perm(_CAPITAL_BUILDER_PERMISSION))


def _can_work_capital_orders(user: User | None) -> bool:
    return bool(_can_manage_capital_orders(user) or _can_build_capital_orders(user))


def _capital_staff_actor_role(user: User | None) -> str | None:
    if _can_manage_capital_orders(user):
        return "manager"
    if _can_build_capital_orders(user):
        return "worker"
    return None


def _is_capital_order_requester(order: CapitalShipOrder, user: User | None) -> bool:
    return bool(
        user
        and int(getattr(user, "id", 0) or 0) == int(getattr(order, "requester_id", 0) or 0)
    )


def _redirect_after_capital_order_action(request, *, default_route: str | None = None):
    next_url = str(request.POST.get("next") or request.GET.get("next") or "").strip()
    if next_url.startswith("/"):
        return redirect(next_url)

    if default_route:
        return redirect(default_route)

    if _can_work_capital_orders(getattr(request, "user", None)):
        return redirect("indy_hub:capital_ship_orders_admin")
    return redirect("indy_hub:capital_ship_orders")


def _resolve_locked_capital_manager_id(order: CapitalShipOrder) -> int:
    """Resolve the manager currently claiming this order."""
    try:
        in_production_by_id = int(getattr(order, "in_production_by_id", 0) or 0)
    except (TypeError, ValueError):
        in_production_by_id = 0
    if in_production_by_id > 0:
        return int(in_production_by_id)

    try:
        gathering_materials_by_id = int(
            getattr(order, "gathering_materials_by_id", 0) or 0
        )
    except (TypeError, ValueError):
        gathering_materials_by_id = 0
    if gathering_materials_by_id <= 0:
        return 0

    status = str(getattr(order, "status", "") or "")
    if status == CapitalShipOrder.Status.GATHERING_MATERIALS:
        return int(gathering_materials_by_id)
    if status not in _PRE_PRODUCTION_STATUSES:
        return int(gathering_materials_by_id)
    return 0


def _resolve_locked_capital_manager(order: CapitalShipOrder) -> User | None:
    manager_id = _resolve_locked_capital_manager_id(order)
    if manager_id <= 0:
        return None
    in_production_manager = getattr(order, "in_production_by", None)
    if (
        in_production_manager is not None
        and int(getattr(in_production_manager, "id", 0) or 0) == manager_id
    ):
        return in_production_manager
    gathering_manager = getattr(order, "gathering_materials_by", None)
    if (
        gathering_manager is not None
        and int(getattr(gathering_manager, "id", 0) or 0) == manager_id
    ):
        return gathering_manager
    return User.objects.filter(id=manager_id).first()


def _is_chat_locked_to_in_producer(order: CapitalShipOrder) -> bool:
    manager_id = _resolve_locked_capital_manager_id(order)
    if manager_id <= 0:
        return False
    return str(getattr(order, "status", "") or "") not in _PRE_PRODUCTION_STATUSES


def _is_capital_order_claimed_by_user(order: CapitalShipOrder, user: User | None) -> bool:
    if not user:
        return False
    try:
        return _resolve_locked_capital_manager_id(order) == int(
            getattr(user, "id", 0) or 0
        )
    except (TypeError, ValueError):
        return False


def _can_act_as_capital_manager_for_order(order: CapitalShipOrder, user: User) -> bool:
    if not _can_manage_capital_orders(user):
        return False
    if not _is_chat_locked_to_in_producer(order):
        return True
    try:
        return _resolve_locked_capital_manager_id(order) == int(
            getattr(user, "id", 0) or 0
        )
    except (TypeError, ValueError):
        return False


def _notify_capital_managers(
    *,
    title: str,
    body: str,
    order: CapitalShipOrder | None = None,
    level: str = "info",
    link: str = "/indy_hub/material-exchange/capital-orders/admin/",
) -> None:
    if order and _is_chat_locked_to_in_producer(order):
        manager = _resolve_locked_capital_manager(order)
        if manager and bool(getattr(manager, "is_active", False)):
            notify_user(manager, title, body, level=level, link=link)
            return

    managers = User.objects.filter(is_active=True).filter(
        Q(
            user_permissions__codename="can_manage_capital_orders",
            user_permissions__content_type__app_label="indy_hub",
        )
        | Q(
            groups__permissions__codename="can_manage_capital_orders",
            groups__permissions__content_type__app_label="indy_hub",
        )
    ).distinct()
    notify_multi(managers, title, body, level=level, link=link)


def _get_capital_manager_users() -> list[User]:
    return list(
        User.objects.filter(is_active=True)
        .filter(
            Q(
                user_permissions__codename="can_manage_capital_orders",
                user_permissions__content_type__app_label="indy_hub",
            )
            | Q(
                groups__permissions__codename="can_manage_capital_orders",
                groups__permissions__content_type__app_label="indy_hub",
            )
        )
        .distinct()
    )


def _get_capital_worker_users() -> list[User]:
    return list(
        User.objects.filter(is_active=True)
        .filter(
            Q(
                user_permissions__codename="can_manage_capital_orders",
                user_permissions__content_type__app_label="indy_hub",
            )
            | Q(
                groups__permissions__codename="can_manage_capital_orders",
                groups__permissions__content_type__app_label="indy_hub",
            )
            | Q(
                user_permissions__codename="can_build_capital_orders",
                user_permissions__content_type__app_label="indy_hub",
            )
            | Q(
                groups__permissions__codename="can_build_capital_orders",
                groups__permissions__content_type__app_label="indy_hub",
            )
        )
        .distinct()
    )


def _build_capital_manager_transfer_options() -> list[dict[str, object]]:
    options: list[dict[str, object]] = []
    for manager in _get_capital_worker_users():
        main_character_name = _resolve_main_character_name(manager).strip()
        username = str(getattr(manager, "username", "") or "").strip()
        if main_character_name and username and main_character_name != username:
            label = f"{main_character_name} ({username})"
        else:
            label = main_character_name or username
        if not label:
            continue
        options.append(
            {
                "user_id": int(manager.id),
                "label": label,
                "username": username,
            }
        )
    return sorted(options, key=lambda row: str(row.get("label") or "").lower())


def _record_capital_event(
    *,
    order: CapitalShipOrder,
    event_type: str,
    actor: User | None = None,
    payload: dict | None = None,
) -> None:
    try:
        CapitalShipOrderEvent.objects.create(
            order=order,
            event_type=event_type,
            actor=actor,
            payload=payload or {},
        )
    except Exception as exc:
        logger.warning(
            "Failed to record capital order event %s for order %s: %s",
            event_type,
            order.id,
            exc,
        )


def _append_order_note(order: CapitalShipOrder, note: str) -> None:
    previous_notes = str(order.notes or "").strip()
    note_text = str(note or "").strip()
    if not note_text:
        return
    order.notes = f"{previous_notes}\n{note_text}".strip()


def _can_access_chat(order: CapitalShipOrder, user: User) -> bool:
    if int(getattr(user, "id", 0) or 0) == int(order.requester_id):
        return True
    if _can_manage_capital_orders(user):
        return _can_act_as_capital_manager_for_order(order, user)
    if _can_build_capital_orders(user):
        return _is_capital_order_claimed_by_user(order, user)
    return False


def _can_update_claimed_capital_order(order: CapitalShipOrder, user: User | None) -> bool:
    if _can_manage_capital_orders(user):
        return _can_act_as_capital_manager_for_order(order, user)
    if _can_build_capital_orders(user):
        return _is_capital_order_claimed_by_user(order, user)
    return False


def _require_capital_queue_access(request) -> bool:
    if _can_work_capital_orders(getattr(request, "user", None)):
        return True
    messages.error(request, "You do not have the required Indy Hub permission.")
    return False


def _require_order_update_access_as_worker(
    request,
    order: CapitalShipOrder,
    *,
    allow_waiting_for_builders: bool = False,
) -> bool:
    if _can_manage_capital_orders(request.user):
        if _can_act_as_capital_manager_for_order(order, request.user):
            return True
    elif _can_build_capital_orders(request.user):
        if allow_waiting_for_builders and str(getattr(order, "status", "") or "").strip() == (
            CapitalShipOrder.Status.WAITING
        ):
            return True
        if _is_capital_order_claimed_by_user(order, request.user):
            return True
        if str(getattr(order, "status", "") or "").strip() == CapitalShipOrder.Status.WAITING:
            messages.warning(
                request,
                f"Claim order {order.order_reference} before updating it.",
            )
            return False
    else:
        messages.warning(request, "You cannot update this capital order.")
        return False

    locked_manager = _resolve_locked_capital_manager(order)
    locked_manager_name = str(getattr(locked_manager, "username", "") or "").strip()
    if locked_manager_name:
        messages.warning(
            request,
            f"Order {order.order_reference} is currently claimed by {locked_manager_name}.",
        )
    else:
        messages.warning(
            request,
            f"Order {order.order_reference} is currently claimed by another worker.",
        )
    return False


def _resolve_chat_internal_role(
    chat: CapitalShipOrderChat,
    user: User,
    *,
    base_role: str | None,
    override: str | None = None,
) -> str | None:
    if not base_role:
        return None
    viewer_role = str(base_role)
    candidate = str(override or "").strip().lower()
    if candidate not in _VIEWER_TO_ROLE:
        return viewer_role

    mapped_candidate = _VIEWER_TO_ROLE[candidate]
    if mapped_candidate == viewer_role:
        return viewer_role
    if (
        int(chat.requester_id) == int(getattr(user, "id", 0) or 0)
        and _can_act_as_capital_manager_for_order(chat.order, user)
    ):
        return mapped_candidate
    return viewer_role


def _to_public_message_role(sender_role: str) -> str:
    if sender_role == CapitalShipOrderChat.SenderRole.REQUESTER:
        return "buyer"
    if sender_role == CapitalShipOrderChat.SenderRole.ADMIN:
        return "seller"
    return "system"


def _create_chat_system_message(order: CapitalShipOrder, content: str) -> None:
    text = str(content or "").strip()
    if not text:
        return
    chat = order.ensure_chat()
    message = CapitalShipOrderMessage(
        chat=chat,
        sender=None,
        sender_role=CapitalShipOrderChat.SenderRole.SYSTEM,
        content=text,
    )
    try:
        message.full_clean()
        message.save()
        chat.register_message(sender_role=CapitalShipOrderChat.SenderRole.SYSTEM)
    except Exception as exc:
        logger.warning(
            "Failed to create system chat message for capital order %s: %s",
            order.id,
            exc,
        )


def _build_decision_payload(order: CapitalShipOrder, *, viewer_role_public: str) -> dict | None:
    if viewer_role_public != "buyer":
        if order.has_pending_offer_confirmation:
            return {
                "url": reverse("indy_hub:capital_ship_order_chat_decide", args=[order.id]),
                "accepted_by_buyer": False,
                "accepted_by_seller": True,
                "viewer_can_accept": False,
                "viewer_can_reject": False,
                "accept_label": _("Confirm"),
                "reject_label": _("Decline"),
                "status_label": _("Waiting for requester confirmation."),
                "status_tone": "warning",
                "state": "waiting_on_requester",
                "pending_label": _("Updating decision..."),
            }
        return None

    if order.has_pending_offer_confirmation:
        return {
            "url": reverse("indy_hub:capital_ship_order_chat_decide", args=[order.id]),
            "accepted_by_buyer": False,
            "accepted_by_seller": True,
            "viewer_can_accept": True,
            "viewer_can_reject": True,
            "accept_label": _("Confirm agreement"),
            "reject_label": _("Decline offer"),
            "status_label": _(
                "A new offer is ready. Confirm to lock Agreed upon price and likely delivery window."
            ),
            "status_tone": "info",
            "state": "pending",
            "pending_label": _("Updating decision..."),
        }

    if order.user_offer_confirmed_at:
        return {
            "url": reverse("indy_hub:capital_ship_order_chat_decide", args=[order.id]),
            "accepted_by_buyer": True,
            "accepted_by_seller": True,
            "viewer_can_accept": False,
            "viewer_can_reject": False,
            "accept_label": _("Confirmed"),
            "reject_label": _("Decline"),
            "status_label": _("You already confirmed the current agreement."),
            "status_tone": "success",
            "state": "accepted",
            "pending_label": _("Updating decision..."),
        }
    return None


def _build_chat_payload(
    *,
    order: CapitalShipOrder,
    chat: CapitalShipOrderChat,
    viewer_role_internal: str,
) -> dict:
    viewer_public = _ROLE_TO_VIEWER.get(viewer_role_internal, "buyer")
    other_public = "seller" if viewer_public == "buyer" else "buyer"
    decision_payload = _build_decision_payload(
        order, viewer_role_public=viewer_public
    )
    requester_main_character = _resolve_main_character_name(order.requester)
    return {
        "chat": {
            "id": chat.id,
            "is_open": chat.is_open,
            "closed_reason": chat.closed_reason,
            "viewer_role": viewer_public,
            "other_role": other_public,
            "labels": {
                "buyer": _("Requester"),
                "seller": _("Builder"),
                "system": _("System"),
            },
            "type_id": order.ship_type_id,
            "type_name": order.ship_type_name,
            "material_efficiency": None,
            "time_efficiency": None,
            "runs_requested": None,
            "copies_requested": None,
            "requester_name": requester_main_character,
            "requester_username": str(order.requester.username),
            "can_send": bool(chat.is_open),
            "decision": decision_payload,
        },
        "messages": [
            {
                "id": msg.id,
                "role": _to_public_message_role(msg.sender_role),
                "content": msg.content,
                "created_at": timezone.localtime(msg.created_at).isoformat(),
                "created_display": timezone.localtime(msg.created_at).strftime(
                    "%Y-%m-%d %H:%M"
                ),
            }
            for msg in chat.messages.all()
        ],
    }


def _build_order_chat_trigger(order: CapitalShipOrder, *, viewer_role_public: str) -> dict:
    chat = order.ensure_chat()
    role_internal = _VIEWER_TO_ROLE.get(viewer_role_public, CapitalShipOrderChat.SenderRole.REQUESTER)
    has_unread = chat.has_unread_for(role_internal)
    return {
        "id": int(chat.id),
        "fetch_url": reverse("indy_hub:capital_ship_order_chat_history", args=[order.id]),
        "send_url": reverse("indy_hub:capital_ship_order_chat_send", args=[order.id]),
        "has_unread": bool(has_unread),
    }


def _remaining_eta_days_from_anchor(
    days_value: int | None,
    *,
    anchor_at,
    clamp_min_zero: bool = True,
) -> int | None:
    parsed_days = _parse_positive_int(days_value, minimum=0)
    if parsed_days is None:
        return None
    if anchor_at is None:
        return int(parsed_days)
    try:
        today = timezone.localdate(timezone.now())
        anchored_day = timezone.localdate(anchor_at)
        elapsed_days = max(0, int((today - anchored_day).days))
    except Exception:
        elapsed_days = 0
    remaining_days = int(parsed_days) - elapsed_days
    if clamp_min_zero:
        return max(0, remaining_days)
    return remaining_days


def _attach_user_display_fields(order: CapitalShipOrder) -> None:
    order.display_price_isk = order.agreed_price_isk or order.offer_price_isk
    order.display_eta_min_days = None
    order.display_eta_max_days = None
    order.display_eta_label = ""
    order.display_eta_is_countdown = False
    order.display_eta_is_overdue = False
    if order.status == CapitalShipOrder.Status.COMPLETED:
        return
    if order.definitive_eta_min_days is not None and order.definitive_eta_max_days is not None:
        remaining_min_days_raw = _remaining_eta_days_from_anchor(
            order.definitive_eta_min_days,
            anchor_at=order.definitive_eta_updated_at,
            clamp_min_zero=False,
        )
        remaining_max_days_raw = _remaining_eta_days_from_anchor(
            order.definitive_eta_max_days,
            anchor_at=order.definitive_eta_updated_at,
            clamp_min_zero=False,
        )
        order.display_eta_min_days = (
            max(0, int(remaining_min_days_raw))
            if remaining_min_days_raw is not None
            else None
        )
        order.display_eta_max_days = (
            max(0, int(remaining_max_days_raw))
            if remaining_max_days_raw is not None
            else None
        )
        if (
            remaining_min_days_raw is not None
            and remaining_max_days_raw is not None
            and int(remaining_min_days_raw) < 0
            and int(remaining_max_days_raw) < 0
        ):
            order.display_eta_is_overdue = True
            order.display_eta_label = _("Definitive ETA (overdue)")
            return
        if order.definitive_eta_updated_at is not None:
            order.display_eta_is_countdown = True
            order.display_eta_label = _("Definitive ETA (remaining)")
        else:
            order.display_eta_label = _("Definitive ETA")
    elif order.likely_eta_min_days is not None and order.likely_eta_max_days is not None:
        order.display_eta_min_days = order.likely_eta_min_days
        order.display_eta_max_days = order.likely_eta_max_days
        order.display_eta_label = _("Likely ETA")
    elif order.offer_eta_min_days is not None and order.offer_eta_max_days is not None:
        order.display_eta_min_days = order.offer_eta_min_days
        order.display_eta_max_days = order.offer_eta_max_days
        order.display_eta_label = _("Proposed ETA")
    else:
        order.display_eta_min_days = order.guideline_eta_min_days
        order.display_eta_max_days = order.guideline_eta_max_days
        order.display_eta_label = _("Estimated ETA")


def _iter_recent_capital_cancellation_events(order: CapitalShipOrder):
    try:
        recent_events = order.events.filter(
            event_type__in=[
                CapitalShipOrderEvent.EventType.STATUS_CHANGED,
                CapitalShipOrderEvent.EventType.AUTO_CANCELLED_STATE_MISMATCH,
            ]
        ).order_by("-created_at", "-id")
    except Exception:
        return []
    return recent_events[:30]


def _get_latest_capital_cancellation_context(order: CapitalShipOrder) -> dict[str, object]:
    valid_statuses = {
        str(choice[0]).strip().lower() for choice in CapitalShipOrder.Status.choices
    }
    fallback_status = CapitalShipOrder.Status.WAITING

    for event in _iter_recent_capital_cancellation_events(order):
        payload = event.payload if isinstance(event.payload, dict) else {}
        event_type = str(getattr(event, "event_type", "") or "").strip()

        if event_type == CapitalShipOrderEvent.EventType.AUTO_CANCELLED_STATE_MISMATCH:
            previous_status = str(payload.get("previous_status") or "").strip().lower()
            if previous_status not in valid_statuses:
                previous_status = fallback_status
            if previous_status in _CAPITAL_TERMINAL_STATUSES:
                previous_status = fallback_status
            return {
                "event_type": event_type,
                "previous_status": previous_status,
                "cancelled_by_role": "system",
                "cancelled_by_user_id": 0,
            }

        new_status = str(payload.get("new_status") or "").strip().lower()
        if new_status != CapitalShipOrder.Status.CANCELLED:
            continue

        previous_status = str(payload.get("previous_status") or "").strip().lower()
        if previous_status not in valid_statuses or previous_status in _CAPITAL_TERMINAL_STATUSES:
            previous_status = fallback_status

        cancelled_by_user_id = int(
            payload.get("cancelled_by_user_id")
            or payload.get("changed_by_user_id")
            or getattr(event, "actor_id", 0)
            or 0
        )
        cancelled_by_role = str(
            payload.get("cancelled_by_role") or payload.get("changed_by_role") or ""
        ).strip().lower()
        if not cancelled_by_role:
            if cancelled_by_user_id > 0 and cancelled_by_user_id == int(order.requester_id):
                cancelled_by_role = "requester"
            elif cancelled_by_user_id > 0:
                cancelled_by_role = "manager"
            else:
                cancelled_by_role = "unknown"

        return {
            "event_type": event_type,
            "previous_status": previous_status,
            "cancelled_by_role": cancelled_by_role,
            "cancelled_by_user_id": cancelled_by_user_id,
        }

    return {
        "event_type": "",
        "previous_status": fallback_status,
        "cancelled_by_role": "unknown",
        "cancelled_by_user_id": 0,
    }


def _can_requester_reopen_cancelled_order(order: CapitalShipOrder, user: User | None) -> bool:
    if not _is_capital_order_requester(order, user):
        return False
    if str(getattr(order, "status", "") or "").strip().lower() != CapitalShipOrder.Status.CANCELLED:
        return False
    cancel_context = _get_latest_capital_cancellation_context(order)
    return (
        str(cancel_context.get("cancelled_by_role") or "").strip().lower() == "requester"
        and int(cancel_context.get("cancelled_by_user_id") or 0)
        == int(getattr(user, "id", 0) or 0)
    )


def _load_latest_declined_offer_by_order(
    order_ids: list[int],
) -> dict[int, dict[str, object]]:
    if not order_ids:
        return {}

    latest_by_order: dict[int, dict[str, object]] = {}
    rejected_events = (
        CapitalShipOrderEvent.objects.filter(
            order_id__in=order_ids,
            event_type=CapitalShipOrderEvent.EventType.OFFER_REJECTED_BY_USER,
        )
        .order_by("order_id", "-created_at")
        .only("order_id", "payload", "created_at")
    )
    for event in rejected_events:
        order_id = int(getattr(event, "order_id", 0) or 0)
        if order_id <= 0 or order_id in latest_by_order:
            continue
        payload = event.payload if isinstance(event.payload, dict) else {}
        price_isk = _quantize_isk(payload.get("offer_price_isk"))
        eta_min_days = _parse_positive_int(payload.get("offer_eta_min_days"), minimum=1)
        eta_max_days = _parse_positive_int(payload.get("offer_eta_max_days"), minimum=1)
        notes = str(payload.get("offer_notes") or "").strip()
        latest_by_order[order_id] = {
            "price_isk": price_isk,
            "eta_min_days": eta_min_days,
            "eta_max_days": eta_max_days,
            "notes": notes,
            "declined_at": getattr(event, "created_at", None),
        }
    return latest_by_order


@login_required
@indy_hub_permission_required("can_access_indy_hub")
def capital_ship_orders(request):
    emit_view_analytics_event(view_name="capital_ship_orders.index", request=request)

    if not _is_material_exchange_enabled():
        messages.warning(request, "Buyback is disabled.")
        return redirect("indy_hub:material_exchange_index")

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, "Buyback is not configured.")
        return redirect("indy_hub:material_exchange_index")

    ship_options = _load_capital_ship_options(config=config)
    ship_options_by_id = {int(row["type_id"]): row for row in ship_options}

    if request.method == "POST":
        ship_type_id_raw = (request.POST.get("ship_type_id") or "").strip()
        reason = (request.POST.get("reason") or "").strip()
        selected_ship = None
        try:
            selected_ship_type_id = int(ship_type_id_raw)
        except (TypeError, ValueError):
            selected_ship_type_id = 0
        if selected_ship_type_id > 0:
            selected_ship = ship_options_by_id.get(selected_ship_type_id)

        if not selected_ship:
            messages.error(request, "Select one capital hull to continue.")
            return redirect("indy_hub:capital_ship_orders")
        if reason not in set(CapitalShipOrder.Reason.values):
            messages.error(request, "Select one required reason for this order.")
            return redirect("indy_hub:capital_ship_orders")

        order = CapitalShipOrder.objects.create(
            config=config,
            requester=request.user,
            ship_type_id=int(selected_ship["type_id"]),
            ship_type_name=str(selected_ship["type_name"]),
            ship_class=str(selected_ship["ship_class"]),
            reason=reason,
            lead_time_days=int(getattr(config, "capital_default_lead_time_days", 0) or 0),
        )
        _refresh_guideline(order)
        order.ensure_chat()
        messages.success(
            request,
            f"Capital order {order.order_reference} created for {order.ship_type_name}.",
        )
        return redirect("indy_hub:capital_ship_orders")

    my_orders = list(
        CapitalShipOrder.objects.filter(requester=request.user, config=config)
        .select_related(
            "in_production_by",
            "gathering_materials_by",
            "offer_updated_by",
            "agreement_locked_by",
        )
        .order_by("-created_at")
    )
    for order in my_orders:
        order.chat_trigger = _build_order_chat_trigger(order, viewer_role_public="buyer")
        _attach_user_display_fields(order)
        order.can_requester_cancel = not order.is_terminal
        order.can_requester_reopen = _can_requester_reopen_cancelled_order(
            order,
            request.user,
        )

    ship_options_by_class: dict[str, list[dict[str, object]]] = {}
    ship_class_labels: dict[str, str] = {}
    for option in ship_options:
        ship_class = _normalize_ship_class_key(option.get("ship_class"))
        if not ship_class:
            continue
        default_price, _price_source = _get_ship_default_price(
            config,
            ship_type_id=option.get("type_id"),
            ship_class=ship_class,
        )
        option["guideline_price"] = default_price
        ship_options_by_class.setdefault(ship_class, []).append(option)
        label = str(option.get("ship_class_label") or "").strip()
        ship_class_labels[ship_class] = label or _default_ship_class_label(ship_class)

    ordered_ship_classes = sorted(
        ship_options_by_class.keys(),
        key=lambda key: (
            _SHIP_CLASS_ORDER.get(key, 99),
            str(ship_class_labels.get(key) or _default_ship_class_label(key)).lower(),
        ),
    )
    ship_option_sections: list[dict[str, object]] = []
    for ship_class in ordered_ship_classes:
        eta_min_days, eta_max_days = _get_class_default_eta_window(config, ship_class)
        ship_option_sections.append(
            {
                "ship_class": ship_class,
                "label": str(
                    ship_class_labels.get(ship_class)
                    or _default_ship_class_label(ship_class)
                ),
                "options": ship_options_by_class.get(ship_class, []),
                "guideline": {
                    "price": _get_class_default_price(config, ship_class),
                    "eta_min_days": eta_min_days,
                    "eta_max_days": eta_max_days,
                },
            }
        )

    auto_open_chat_id: str | None = None
    requested_chat = request.GET.get("open_chat")
    if requested_chat:
        try:
            requested_chat_id = int(requested_chat)
        except (TypeError, ValueError):
            requested_chat_id = None
        if requested_chat_id:
            exists = CapitalShipOrderChat.objects.filter(
                id=requested_chat_id,
                requester=request.user,
            ).exists()
            if exists:
                auto_open_chat_id = str(requested_chat_id)

    context = {
        "ship_option_sections": ship_option_sections,
        "reason_choices": CapitalShipOrder.Reason.choices,
        "my_orders": my_orders,
        "can_work_capital_orders": _can_work_capital_orders(request.user),
        "can_manage_capital_orders": request.user.has_perm(
            "indy_hub.can_manage_capital_orders"
        ),
    }
    if auto_open_chat_id:
        context["auto_open_chat_id"] = auto_open_chat_id
    context.update(build_nav_context(request.user, active_tab="capital_orders"))
    return render(request, "indy_hub/material_exchange/capital_orders.html", context)


@login_required
@indy_hub_permission_required("can_access_indy_hub")
def capital_ship_orders_admin(request):
    emit_view_analytics_event(view_name="capital_ship_orders.admin", request=request)

    if not _require_capital_queue_access(request):
        return redirect("indy_hub:index")

    include_completed = str(request.GET.get("include_completed") or "").strip() == "1"

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, "Buyback is not configured.")
        return redirect("indy_hub:material_exchange_index")

    orders_qs = CapitalShipOrder.objects.filter(config=config).select_related(
        "requester",
        "in_production_by",
        "gathering_materials_by",
        "offer_updated_by",
        "agreement_locked_by",
    )
    if not include_completed:
        orders_qs = orders_qs.exclude(status__in=list(_CAPITAL_TERMINAL_STATUSES))
    orders = list(orders_qs.order_by("-created_at"))
    last_declined_by_order = _load_latest_declined_offer_by_order(
        [int(order.id) for order in orders]
    )
    for order in orders:
        order_status = str(getattr(order, "status", "") or "").strip().lower()
        can_manage_order = _can_manage_capital_orders(
            request.user
        ) and _can_act_as_capital_manager_for_order(order, request.user)
        is_claimed_by_user = _is_capital_order_claimed_by_user(order, request.user)
        can_claim_waiting = order_status == CapitalShipOrder.Status.WAITING and (
            can_manage_order or _can_build_capital_orders(request.user)
        )
        can_adjust_claimed = can_manage_order or is_claimed_by_user

        order.requester_main_character = _resolve_main_character_name(order.requester)
        order.chat_trigger = _build_order_chat_trigger(order, viewer_role_public="seller")
        order.can_access_chat_as_admin = _can_access_chat(order, request.user)
        order.can_refresh_guideline = (not order.is_terminal) and (
            can_manage_order or is_claimed_by_user
        )
        order.can_set_gathering_materials = can_claim_waiting
        order.can_set_in_production = (
            can_claim_waiting
            or (
                order_status == CapitalShipOrder.Status.GATHERING_MATERIALS
                and can_adjust_claimed
            )
        )
        order.can_update_offer = (not order.is_terminal) and can_adjust_claimed
        order.can_transfer_claim = (
            order_status
            in {
                CapitalShipOrder.Status.GATHERING_MATERIALS,
                CapitalShipOrder.Status.IN_PRODUCTION,
                CapitalShipOrder.Status.CONTRACT_CREATED,
                CapitalShipOrder.Status.ANOMALY,
            }
            and can_adjust_claimed
        )
        order.can_release_claim = order.can_transfer_claim
        order.can_set_definitive_eta = (
            order_status
            in {
                CapitalShipOrder.Status.GATHERING_MATERIALS,
                CapitalShipOrder.Status.IN_PRODUCTION,
                CapitalShipOrder.Status.CONTRACT_CREATED,
            }
            and can_adjust_claimed
        )
        order.can_cancel_as_admin = (can_manage_order or is_claimed_by_user) and (
            not order.is_terminal
        )
        order.can_uncancel_as_admin = (
            order_status == CapitalShipOrder.Status.CANCELLED and can_manage_order
        )
        order.can_update_queue_order = any(
            [
                order.can_refresh_guideline,
                order.can_set_gathering_materials,
                order.can_set_in_production,
                order.can_update_offer,
                order.can_transfer_claim,
                order.can_release_claim,
                order.can_set_definitive_eta,
                order.can_cancel_as_admin,
                order.can_uncancel_as_admin,
            ]
        )
        _attach_user_display_fields(order)
        declined_offer = last_declined_by_order.get(int(order.id))
        order.last_declined_offer = declined_offer
        order.last_declined_offer_details = ""
        order.last_declined_offer_notes = ""
        order.last_declined_offer_price_display = ""
        order.last_declined_offer_eta_display = ""
        order.last_declined_offer_declined_at_display = ""
        order.revision_required = False
        if declined_offer:
            declined_price = declined_offer.get("price_isk")
            declined_eta_min = declined_offer.get("eta_min_days")
            declined_eta_max = declined_offer.get("eta_max_days")
            declined_at = declined_offer.get("declined_at")
            declined_price_display = (
                f"{declined_price:,.2f} ISK"
                if isinstance(declined_price, Decimal)
                else "-"
            )
            declined_eta_display = (
                f"{int(declined_eta_min)}-{int(declined_eta_max)} days"
                if declined_eta_min is not None and declined_eta_max is not None
                else "-"
            )
            declined_at_display = (
                timezone.localtime(declined_at).strftime("%Y-%m-%d %H:%M")
                if declined_at
                else ""
            )
            detail_parts = [
                _("Last declined offer"),
                f"{declined_price_display}",
                f"{declined_eta_display}",
            ]
            if declined_at_display:
                detail_parts.append(_("declined at %(when)s") % {"when": declined_at_display})
            order.last_declined_offer_details = " | ".join(
                [str(part).strip() for part in detail_parts if str(part).strip()]
            )
            order.last_declined_offer_price_display = declined_price_display
            order.last_declined_offer_eta_display = declined_eta_display
            order.last_declined_offer_declined_at_display = declined_at_display
            order.last_declined_offer_notes = str(declined_offer.get("notes") or "").strip()
            order.revision_required = bool(
                not order.is_terminal
                and order.offer_price_isk is None
                and order.offer_eta_min_days is None
                and order.offer_eta_max_days is None
            )

    auto_open_chat_id: str | None = None
    requested_chat = request.GET.get("open_chat")
    if requested_chat:
        try:
            requested_chat_id = int(requested_chat)
        except (TypeError, ValueError):
            requested_chat_id = None
        if requested_chat_id:
            exists = CapitalShipOrderChat.objects.filter(id=requested_chat_id).exists()
            if exists:
                auto_open_chat_id = str(requested_chat_id)

    context = {
        "orders": orders,
        "include_completed": include_completed,
        "can_manage_capital_orders": _can_manage_capital_orders(request.user),
        "capital_manager_transfer_options": _build_capital_manager_transfer_options(),
    }
    if auto_open_chat_id:
        context["auto_open_chat_id"] = auto_open_chat_id
    context.update(build_nav_context(request.user, active_tab="capital_orders"))
    return render(
        request,
        "indy_hub/material_exchange/capital_orders_admin.html",
        context,
    )


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@indy_hub_permission_required("can_manage_capital_orders")
def capital_ship_orders_config(request):
    emit_view_analytics_event(view_name="capital_ship_orders.config", request=request)

    config = _get_material_exchange_config()
    if not config:
        messages.warning(
            request,
            "Buyback must be configured before capital order settings can be edited.",
        )
        return redirect("indy_hub:material_exchange_config")

    editor_options = _load_capital_ship_options_for_editor(config=config)

    if request.method == "POST":
        try:
            capital_default_lead_time_days = _parse_positive_int_or_raise(
                request.POST.get("capital_default_lead_time_days"),
                label="Default lead time days",
                minimum=0,
                fallback=0,
            )

            capital_auto_cancel_on_state_change = (
                request.POST.get("capital_auto_cancel_on_state_change") == "on"
            )
            valid_capital_statuses = set(CapitalShipOrder.Status.values)
            capital_auto_cancel_eligible_statuses: list[str] = []
            for raw_status in request.POST.getlist("capital_auto_cancel_eligible_statuses"):
                status_value = str(raw_status or "").strip().lower()
                if status_value not in valid_capital_statuses:
                    continue
                if status_value in {
                    CapitalShipOrder.Status.COMPLETED,
                    CapitalShipOrder.Status.REJECTED,
                    CapitalShipOrder.Status.CANCELLED,
                }:
                    continue
                if status_value not in capital_auto_cancel_eligible_statuses:
                    capital_auto_cancel_eligible_statuses.append(status_value)
            if (
                capital_auto_cancel_on_state_change
                and not capital_auto_cancel_eligible_statuses
            ):
                capital_auto_cancel_eligible_statuses = [
                    CapitalShipOrder.Status.WAITING,
                    CapitalShipOrder.Status.GATHERING_MATERIALS,
                    CapitalShipOrder.Status.IN_PRODUCTION,
                    CapitalShipOrder.Status.CONTRACT_CREATED,
                    CapitalShipOrder.Status.ANOMALY,
                ]

            capital_auto_cancel_preapproved_state_names = _parse_state_name_list(
                request.POST.getlist("capital_auto_cancel_preapproved_state_names")
            )
            if not capital_auto_cancel_preapproved_state_names:
                capital_auto_cancel_preapproved_state_names = ["Pre-Approved", "Preapproved"]
            capital_auto_cancel_delay_value = _parse_positive_int_or_raise(
                request.POST.get("capital_auto_cancel_delay_value"),
                label="Auto-cancel delay value",
                minimum=0,
                fallback=0,
            )
            valid_delay_units = {
                MaterialExchangeConfig.CAPITAL_AUTO_CANCEL_DELAY_HOURS,
                MaterialExchangeConfig.CAPITAL_AUTO_CANCEL_DELAY_DAYS,
            }
            capital_auto_cancel_delay_unit = str(
                request.POST.get("capital_auto_cancel_delay_unit")
                or MaterialExchangeConfig.CAPITAL_AUTO_CANCEL_DELAY_HOURS
            ).strip().lower()
            if capital_auto_cancel_delay_unit not in valid_delay_units:
                capital_auto_cancel_delay_unit = (
                    MaterialExchangeConfig.CAPITAL_AUTO_CANCEL_DELAY_HOURS
                )

            capital_disabled_ship_groups = _normalize_ship_group_list(
                request.POST.getlist("capital_disabled_ship_groups")
            )
            capital_disabled_ship_type_ids = _parse_positive_type_ids(
                request.POST.getlist("capital_disabled_ship_type_ids")
            )

            estimated_price_overrides_by_type: dict[int, str] = {}
            for form_key, form_value in request.POST.items():
                if not str(form_key).startswith("estimated_price_"):
                    continue
                type_suffix = str(form_key).replace("estimated_price_", "", 1).strip()
                try:
                    type_id = int(type_suffix)
                except (TypeError, ValueError):
                    continue
                if type_id <= 0:
                    continue
                parsed_price = _quantize_isk(form_value)
                if parsed_price is None:
                    continue
                estimated_price_overrides_by_type[type_id] = _decimal_to_json_string(
                    parsed_price
                )

            capital_ship_estimated_price_overrides = [
                {"type_id": type_id, "price_isk": price_isk}
                for type_id, price_isk in sorted(
                    estimated_price_overrides_by_type.items(),
                    key=lambda row: int(row[0]),
                )
                if int(type_id) > 0 and price_isk is not None
            ]

        except (ValueError, InvalidOperation) as exc:
            messages.error(request, str(exc))
            return redirect("indy_hub:capital_ship_orders_config")

        config.capital_default_lead_time_days = capital_default_lead_time_days
        config.capital_auto_cancel_on_state_change = (
            capital_auto_cancel_on_state_change
        )
        config.capital_auto_cancel_preapproved_state_names = (
            capital_auto_cancel_preapproved_state_names
        )
        config.capital_auto_cancel_eligible_statuses = (
            capital_auto_cancel_eligible_statuses
        )
        config.capital_auto_cancel_delay_value = capital_auto_cancel_delay_value
        config.capital_auto_cancel_delay_unit = capital_auto_cancel_delay_unit
        config.capital_disabled_ship_groups = capital_disabled_ship_groups
        config.capital_disabled_ship_type_ids = capital_disabled_ship_type_ids
        config.capital_custom_ship_options = []
        config.capital_ship_estimated_price_overrides = (
            capital_ship_estimated_price_overrides
        )
        config.save(
            update_fields=[
                "capital_default_lead_time_days",
                "capital_auto_cancel_on_state_change",
                "capital_auto_cancel_preapproved_state_names",
                "capital_auto_cancel_eligible_statuses",
                "capital_auto_cancel_delay_value",
                "capital_auto_cancel_delay_unit",
                "capital_disabled_ship_groups",
                "capital_disabled_ship_type_ids",
                "capital_custom_ship_options",
                "capital_ship_estimated_price_overrides",
            ]
        )
        messages.success(request, "Capital order settings updated.")
        return redirect("indy_hub:capital_ship_orders_config")

    disabled_groups = set(config.get_capital_disabled_ship_groups())
    disabled_ship_type_ids = set(config.get_capital_disabled_ship_type_ids())
    manual_estimated_price_map = config.get_capital_ship_estimated_price_map()
    auto_estimate_row_map = config.get_capital_ship_auto_estimate_row_map()

    group_labels: dict[str, str] = {}
    for option in editor_options:
        ship_class = _normalize_ship_class_key(option.get("ship_class"))
        if not ship_class:
            continue
        label = str(option.get("ship_class_label") or "").strip()
        group_labels[ship_class] = label or _default_ship_class_label(ship_class)
    for group_key in disabled_groups:
        if group_key not in group_labels:
            group_labels[group_key] = _default_ship_class_label(group_key)

    group_choices = sorted(
        [
            {"key": group_key, "label": group_labels[group_key]}
            for group_key in group_labels.keys()
        ],
        key=lambda row: (
            _SHIP_CLASS_ORDER.get(_normalize_ship_class_key(row.get("key")), 99),
            str(row.get("label") or "").lower(),
        ),
    )

    ship_rows: list[dict[str, object]] = []
    for option in editor_options:
        type_id = int(option.get("type_id") or 0)
        if type_id <= 0:
            continue
        ship_class = _normalize_ship_class_key(option.get("ship_class"))
        auto_row = auto_estimate_row_map.get(type_id, {})
        ship_rows.append(
            {
                "type_id": type_id,
                "type_name": str(option.get("type_name") or "").strip(),
                "ship_class": ship_class,
                "ship_class_label": str(
                    option.get("ship_class_label")
                    or _default_ship_class_label(ship_class)
                ),
                "is_disabled_type": type_id in disabled_ship_type_ids,
                "is_disabled_group": ship_class in disabled_groups,
                "manual_estimated_price": manual_estimated_price_map.get(type_id),
                "auto_estimated_price": auto_row.get("price_isk"),
                "auto_estimated_contract_count": int(
                    auto_row.get("contract_count") or 0
                ),
            }
        )

    valid_statuses = [
        status
        for status in CapitalShipOrder.Status.choices
        if status[0]
        not in {
            CapitalShipOrder.Status.COMPLETED,
            CapitalShipOrder.Status.REJECTED,
            CapitalShipOrder.Status.CANCELLED,
        }
    ]
    selected_preapproved_state_names = config.get_capital_preapproved_state_names()

    context = {
        "config": config,
        "group_choices": group_choices,
        "disabled_group_keys": disabled_groups,
        "ship_rows": ship_rows,
        "capital_default_lead_time_days": int(
            getattr(config, "capital_default_lead_time_days", 0) or 0
        ),
        "capital_auto_cancel_on_state_change": bool(
            getattr(config, "capital_auto_cancel_on_state_change", False)
        ),
        "capital_auto_cancel_preapproved_state_names": selected_preapproved_state_names,
        "capital_auto_cancel_state_name_choices": _load_allianceauth_state_name_choices(
            selected_names=selected_preapproved_state_names
        ),
        "capital_auto_cancel_eligible_statuses": set(
            config.get_capital_auto_cancel_eligible_statuses()
        ),
        "capital_auto_cancel_delay_value": int(
            getattr(config, "capital_auto_cancel_delay_value", 0) or 0
        ),
        "capital_auto_cancel_delay_unit": str(
            getattr(
                config,
                "capital_auto_cancel_delay_unit",
                MaterialExchangeConfig.CAPITAL_AUTO_CANCEL_DELAY_HOURS,
            )
            or MaterialExchangeConfig.CAPITAL_AUTO_CANCEL_DELAY_HOURS
        ),
        "capital_auto_cancel_delay_unit_choices": MaterialExchangeConfig.CAPITAL_AUTO_CANCEL_DELAY_UNIT_CHOICES,
        "capital_auto_cancel_status_choices": valid_statuses,
    }
    context.update(build_nav_context(request.user, active_tab="capital_orders"))
    return render(
        request,
        "indy_hub/material_exchange/capital_orders_config.html",
        context,
    )


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_POST
def capital_ship_order_refresh_guideline(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.refresh_guideline",
        request=request,
    )
    if not _require_capital_queue_access(request):
        return redirect("indy_hub:index")
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    if not _require_order_update_access_as_worker(request, order):
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )
    if order.is_terminal:
        messages.warning(
            request,
            f"Order {order.order_reference} is already closed.",
        )
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )
    _refresh_guideline(order)
    messages.success(
        request,
        f"Estimate refreshed for order {order.order_reference}.",
    )
    return _redirect_after_capital_order_action(
        request, default_route="indy_hub:capital_ship_orders_admin"
    )


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_POST
def capital_ship_order_set_gathering_materials(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.set_gathering_materials",
        request=request,
    )
    if not _require_capital_queue_access(request):
        return redirect("indy_hub:index")
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    if not _require_order_update_access_as_worker(
        request,
        order,
        allow_waiting_for_builders=True,
    ):
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )
    if order.status != CapitalShipOrder.Status.WAITING:
        messages.warning(
            request,
            f"Order {order.order_reference} is not in waiting status.",
        )
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    order.status = CapitalShipOrder.Status.GATHERING_MATERIALS
    order.gathering_materials_by = request.user
    order.gathering_materials_at = timezone.now()
    _append_order_note(
        order,
        (
            f"Gathering materials by {request.user.username} at "
            f"{timezone.now().strftime('%Y-%m-%d %H:%M:%S %Z')}"
        ),
    )
    order.save(
        update_fields=[
            "status",
            "gathering_materials_by",
            "gathering_materials_at",
            "notes",
            "updated_at",
        ]
    )
    _record_capital_event(
        order=order,
        event_type=CapitalShipOrderEvent.EventType.STATUS_CHANGED,
        actor=request.user,
        payload={
            "new_status": CapitalShipOrder.Status.GATHERING_MATERIALS,
            "previous_status": CapitalShipOrder.Status.WAITING,
        },
    )

    notify_user(
        order.requester,
        _("Capital Order Update"),
        _(
            "Order %(ref)s (%(hull)s) moved to Gathering Materials."
        )
        % {"ref": order.order_reference, "hull": order.ship_type_name},
        level="info",
        link="/indy_hub/material-exchange/capital-orders/",
    )
    messages.success(
        request,
        f"Order {order.order_reference} moved to Gathering Materials.",
    )
    return _redirect_after_capital_order_action(
        request, default_route="indy_hub:capital_ship_orders_admin"
    )


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_POST
def capital_ship_order_set_in_production(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.set_in_production",
        request=request,
    )
    if not _require_capital_queue_access(request):
        return redirect("indy_hub:index")
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    if not _require_order_update_access_as_worker(
        request,
        order,
        allow_waiting_for_builders=True,
    ):
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    if order.status not in {
        CapitalShipOrder.Status.WAITING,
        CapitalShipOrder.Status.GATHERING_MATERIALS,
    }:
        messages.warning(
            request,
            f"Order {order.order_reference} is not in waiting or gathering materials status.",
        )
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    previous_status = order.status
    order.status = CapitalShipOrder.Status.IN_PRODUCTION
    order.in_production_by = request.user
    order.in_production_at = timezone.now()
    _append_order_note(
        order,
        (
            f"In production by {request.user.username} at "
            f"{timezone.now().strftime('%Y-%m-%d %H:%M:%S %Z')}"
        ),
    )
    order.save(
        update_fields=[
            "status",
            "in_production_by",
            "in_production_at",
            "notes",
            "updated_at",
        ]
    )
    _record_capital_event(
        order=order,
        event_type=CapitalShipOrderEvent.EventType.STATUS_CHANGED,
        actor=request.user,
        payload={
            "new_status": CapitalShipOrder.Status.IN_PRODUCTION,
            "previous_status": previous_status,
        },
    )

    try:
        from indy_hub.tasks.material_exchange_contracts import (
            handle_capital_ship_order_marked_in_production,
        )

        handle_capital_ship_order_marked_in_production.apply_async(
            args=(int(order.id),), countdown=1, expires=300
        )
    except Exception as exc:
        logger.warning(
            "Failed to queue in-production notification for capital order %s: %s",
            order.id,
            exc,
        )

    messages.success(
        request,
        f"Order {order.order_reference} moved to In Production.",
    )
    return _redirect_after_capital_order_action(
        request, default_route="indy_hub:capital_ship_orders_admin"
    )


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_POST
def capital_ship_order_transfer_manager(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.transfer_manager",
        request=request,
    )
    if not _require_capital_queue_access(request):
        return redirect("indy_hub:index")
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    if not _require_order_update_access_as_worker(request, order):
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )
    if order.is_terminal:
        messages.warning(
            request,
            f"Order {order.order_reference} is already closed.",
        )
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    current_manager_id = _resolve_locked_capital_manager_id(order)
    if current_manager_id <= 0:
        messages.warning(
            request,
            f"Order {order.order_reference} is not currently claimed by a worker.",
        )
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    target_manager_id = _parse_positive_int(
        request.POST.get("transfer_manager_user_id"),
        minimum=1,
    )
    if target_manager_id is None:
        messages.error(request, "Select a capital worker to transfer this order.")
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    manager_by_id = {
        int(user.id): user
        for user in _get_capital_worker_users()
    }
    target_manager = manager_by_id.get(int(target_manager_id))
    if target_manager is None:
        messages.error(request, "Selected user cannot work capital orders.")
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    if int(target_manager.id) == int(current_manager_id):
        messages.info(
            request,
            f"Order {order.order_reference} is already assigned to {target_manager.username}.",
        )
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    previous_manager = _resolve_locked_capital_manager(order)
    previous_manager_name = str(getattr(previous_manager, "username", "") or "").strip()
    target_manager_name = str(getattr(target_manager, "username", "") or "").strip()
    status = str(getattr(order, "status", "") or "")
    update_fields = ["notes", "updated_at"]

    if status == CapitalShipOrder.Status.GATHERING_MATERIALS:
        order.gathering_materials_by = target_manager
        update_fields.append("gathering_materials_by")
    elif status in {
        CapitalShipOrder.Status.IN_PRODUCTION,
        CapitalShipOrder.Status.CONTRACT_CREATED,
        CapitalShipOrder.Status.ANOMALY,
    }:
        order.in_production_by = target_manager
        update_fields.append("in_production_by")
    elif int(getattr(order, "in_production_by_id", 0) or 0) > 0:
        order.in_production_by = target_manager
        update_fields.append("in_production_by")
    else:
        order.gathering_materials_by = target_manager
        update_fields.append("gathering_materials_by")

    now_text = timezone.now().strftime("%Y-%m-%d %H:%M:%S %Z")
    from_text = previous_manager_name or "unknown"
    to_text = target_manager_name or "unknown"
    _append_order_note(
        order,
        (
            f"Order worker transferred from {from_text} to {to_text} by "
            f"{request.user.username} at {now_text}"
        ),
    )
    order.save(update_fields=list(dict.fromkeys(update_fields)))
    _create_chat_system_message(
        order,
        _(
            "Order worker transferred from %(from_manager)s to %(to_manager)s by %(actor)s."
        )
        % {
            "from_manager": from_text,
            "to_manager": to_text,
            "actor": request.user.username,
        },
    )
    notify_user(
        order.requester,
        _("Capital Order Update"),
        _(
            "Order %(ref)s was transferred to another capital worker."
        )
        % {"ref": order.order_reference},
        level="info",
        link="/indy_hub/material-exchange/capital-orders/",
    )
    _notify_capital_managers(
        title=_("Capital Order Transferred"),
        body=_(
            "%(actor)s transferred capital order %(ref)s from %(from_manager)s to %(to_manager)s."
        )
        % {
            "actor": request.user.username,
            "ref": order.order_reference,
            "from_manager": from_text,
            "to_manager": to_text,
        },
        order=order,
        level="info",
        link="/indy_hub/material-exchange/capital-orders/admin/",
    )
    messages.success(
        request,
        f"Order {order.order_reference} transferred to {target_manager_name}.",
    )
    return _redirect_after_capital_order_action(
        request, default_route="indy_hub:capital_ship_orders_admin"
    )


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_POST
def capital_ship_order_update_offer(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.update_offer",
        request=request,
    )
    if not _require_capital_queue_access(request):
        return redirect("indy_hub:index")
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    if not _require_order_update_access_as_worker(request, order):
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )
    if order.is_terminal:
        messages.warning(
            request,
            f"Order {order.order_reference} is already closed.",
        )
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    offer_price = _quantize_isk((request.POST.get("offer_price_isk") or "").strip())
    offer_eta_min_days = _parse_positive_int(
        request.POST.get("offer_eta_min_days"), minimum=1
    )
    offer_eta_max_days = _parse_positive_int(
        request.POST.get("offer_eta_max_days"), minimum=1
    )
    lead_time_days = _parse_positive_int(request.POST.get("lead_time_days"), minimum=0)
    offer_notes = (request.POST.get("offer_notes") or "").strip()

    if offer_price is None:
        messages.error(request, "Offer price must be a positive ISK value.")
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )
    if offer_eta_min_days is None or offer_eta_max_days is None:
        messages.error(request, "Offer ETA min and max days are required.")
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )
    if offer_eta_max_days < offer_eta_min_days:
        messages.error(request, "Offer ETA max days must be greater than or equal to min days.")
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    now = timezone.now()
    update_fields = [
        "offer_price_isk",
        "offer_eta_min_days",
        "offer_eta_max_days",
        "offer_notes",
        "offer_updated_by",
        "offer_updated_at",
        "user_offer_confirmed_at",
        "user_offer_confirmed_by",
        "agreed_price_isk",
        "likely_eta_min_days",
        "likely_eta_max_days",
        "agreement_locked_at",
        "agreement_locked_by",
        "updated_at",
    ]
    order.offer_price_isk = offer_price
    order.offer_eta_min_days = int(offer_eta_min_days)
    order.offer_eta_max_days = int(offer_eta_max_days)
    order.offer_notes = offer_notes
    order.offer_updated_by = request.user
    order.offer_updated_at = now
    order.user_offer_confirmed_at = None
    order.user_offer_confirmed_by = None
    order.agreed_price_isk = None
    order.likely_eta_min_days = None
    order.likely_eta_max_days = None
    order.agreement_locked_at = None
    order.agreement_locked_by = None

    if lead_time_days is not None:
        order.lead_time_days = int(lead_time_days)
        update_fields.append("lead_time_days")

    order.save(update_fields=update_fields)
    _refresh_guideline(order)

    _record_capital_event(
        order=order,
        event_type=CapitalShipOrderEvent.EventType.OFFER_UPDATED,
        actor=request.user,
        payload={
            "offer_price_isk": str(order.offer_price_isk),
            "offer_eta_min_days": order.offer_eta_min_days,
            "offer_eta_max_days": order.offer_eta_max_days,
            "lead_time_days": order.lead_time_days,
        },
    )
    _create_chat_system_message(
        order,
        _(
            "Admin updated offer: %(price)s ISK, likely delivery %(eta_min)s-%(eta_max)s days."
        )
        % {
            "price": f"{order.offer_price_isk:,.2f}",
            "eta_min": int(order.offer_eta_min_days or 0),
            "eta_max": int(order.offer_eta_max_days or 0),
        },
    )

    notify_user(
        order.requester,
        _("Capital Offer Updated"),
        _(
            "A new offer is available for order %(ref)s.\n"
            "Proposed price: %(price)s ISK\n"
            "Likely delivery: %(eta_min)s-%(eta_max)s days\n\n"
            "**You must click the link below and accept or decline the offer to confirm delivery.**\n"
            "If you decline the offer, please inform us via the chat *(also in the link)* of why."
        )
        % {
            "ref": order.order_reference,
            "price": f"{order.offer_price_isk:,.2f}",
            "eta_min": int(order.offer_eta_min_days or 0),
            "eta_max": int(order.offer_eta_max_days or 0),
        },
        level="info",
        link=f"/indy_hub/material-exchange/capital-orders/?open_chat={order.chat.id}",
    )
    messages.success(
        request,
        f"Offer updated for order {order.order_reference}.",
    )
    return _redirect_after_capital_order_action(
        request, default_route="indy_hub:capital_ship_orders_admin"
    )


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_POST
def capital_ship_order_set_definitive_eta(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.set_definitive_eta",
        request=request,
    )
    if not _require_capital_queue_access(request):
        return redirect("indy_hub:index")
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    if not _require_order_update_access_as_worker(request, order):
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )
    if order.status not in {
        CapitalShipOrder.Status.GATHERING_MATERIALS,
        CapitalShipOrder.Status.IN_PRODUCTION,
        CapitalShipOrder.Status.CONTRACT_CREATED,
    }:
        messages.warning(
            request,
            "Definitive ETA can only be set once work has started.",
        )
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    eta_min_days = _parse_positive_int(request.POST.get("definitive_eta_min_days"), minimum=1)
    eta_max_days = _parse_positive_int(request.POST.get("definitive_eta_max_days"), minimum=1)
    if eta_min_days is None or eta_max_days is None:
        messages.error(request, "Definitive ETA min and max days are required.")
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )
    if eta_max_days < eta_min_days:
        messages.error(request, "Definitive ETA max days must be greater than or equal to min days.")
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    order.definitive_eta_min_days = int(eta_min_days)
    order.definitive_eta_max_days = int(eta_max_days)
    order.definitive_eta_updated_by = request.user
    order.definitive_eta_updated_at = timezone.now()
    order.save(
        update_fields=[
            "definitive_eta_min_days",
            "definitive_eta_max_days",
            "definitive_eta_updated_by",
            "definitive_eta_updated_at",
            "updated_at",
        ]
    )
    _create_chat_system_message(
        order,
        _(
            "Definitive ETA updated to %(eta_min)s-%(eta_max)s days."
        )
        % {"eta_min": eta_min_days, "eta_max": eta_max_days},
    )
    notify_user(
        order.requester,
        _("Capital ETA Updated"),
        _(
            "Order %(ref)s definitive ETA updated to %(eta_min)s-%(eta_max)s days."
        )
        % {
            "ref": order.order_reference,
            "eta_min": eta_min_days,
            "eta_max": eta_max_days,
        },
        level="info",
        link="/indy_hub/material-exchange/capital-orders/",
    )
    messages.success(request, f"Definitive ETA updated for order {order.order_reference}.")
    return _redirect_after_capital_order_action(
        request, default_route="indy_hub:capital_ship_orders_admin"
    )


def _close_capital_order(
    request,
    *,
    order_id: int,
    target_status: str,
    action_label: str,
    task_name: str,
    actor_role: str = "manager",
):
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    is_requester = _is_capital_order_requester(order, request.user)
    if actor_role in {"manager", "worker"}:
        if not _require_order_update_access_as_worker(request, order):
            return _redirect_after_capital_order_action(
                request, default_route="indy_hub:capital_ship_orders_admin"
            )
    elif actor_role != "requester" or not is_requester:
        messages.warning(request, "You cannot update this capital order.")
        return _redirect_after_capital_order_action(request)
    current_status = str(order.status or "")

    if current_status == str(target_status):
        messages.info(
            request,
            f"Order {order.order_reference} is already {action_label.lower()}.",
        )
        return _redirect_after_capital_order_action(request)

    if current_status in _CAPITAL_TERMINAL_STATUSES:
        messages.warning(
            request,
            f"Order {order.order_reference} is already closed ({order.get_status_display()}).",
        )
        return _redirect_after_capital_order_action(request)

    actor_name = str(getattr(request.user, "username", "") or "User").strip()
    actor_label = {
        "requester": "requester",
        "manager": "manager",
        "worker": "worker",
    }.get(actor_role, "user")
    status_note = (
        f"{action_label} by {actor_label} {actor_name} at "
        f"{timezone.now().strftime('%Y-%m-%d %H:%M:%S %Z')}"
    )
    order.status = target_status
    order.anomaly_reason = ""
    _append_order_note(order, status_note)
    update_fields = ["status", "anomaly_reason", "notes", "updated_at"]
    if target_status == CapitalShipOrder.Status.CANCELLED:
        order.requester_preapproved_mismatch_since = None
        update_fields.append("requester_preapproved_mismatch_since")
    order.save(update_fields=update_fields)
    event_payload = {
        "new_status": target_status,
        "previous_status": current_status,
        "changed_by_role": actor_role,
        "changed_by_user_id": int(getattr(request.user, "id", 0) or 0),
    }
    if target_status == CapitalShipOrder.Status.CANCELLED:
        event_payload["cancelled_by_role"] = actor_role
        event_payload["cancelled_by_user_id"] = int(getattr(request.user, "id", 0) or 0)
    _record_capital_event(
        order=order,
        event_type=CapitalShipOrderEvent.EventType.STATUS_CHANGED,
        actor=request.user,
        payload=event_payload,
    )

    if actor_role == "manager":
        _queue_capital_order_closed_by_manager_notification(
            order=order,
            target_status=target_status,
            task_name=task_name,
            manager_user_id=int(getattr(request.user, "id", 0) or 0),
        )
    elif actor_role == "worker":
        notify_user(
            order.requester,
            _("Capital Order %(action)s") % {"action": str(action_label or "").strip()},
            _("Order %(ref)s (%(hull)s) was %(action)s by %(worker)s.")
            % {
                "ref": order.order_reference,
                "hull": order.ship_type_name,
                "action": str(action_label or "").strip().lower(),
                "worker": actor_name,
            },
            level="info" if target_status == CapitalShipOrder.Status.CANCELLED else "warning",
            link="/indy_hub/material-exchange/capital-orders/",
        )
    else:
        _create_chat_system_message(
            order,
            _(
                "Requester cancelled the order. Status changed from %(previous_status)s to Cancelled."
            )
            % {
                "previous_status": CapitalShipOrder.Status(current_status).label
                if current_status in CapitalShipOrder.Status.values
                else current_status.title(),
            },
        )
        _notify_capital_managers(
            title=_("Capital Order Cancelled"),
            body=_(
                "%(requester)s cancelled capital order %(ref)s.\n"
                "Hull: %(hull)s\n"
                "Previous status: %(status)s"
            )
            % {
                "requester": order.requester.username,
                "ref": order.order_reference,
                "hull": order.ship_type_name,
                "status": current_status,
            },
            order=order,
            level="info",
            link="/indy_hub/material-exchange/capital-orders/admin/",
        )

    messages.success(
        request,
        f"Order {order.order_reference} marked as {action_label}.",
    )
    return _redirect_after_capital_order_action(request)


def _queue_capital_order_closed_by_manager_notification(
    *,
    order: CapitalShipOrder,
    target_status: str,
    task_name: str,
    manager_user_id: int,
) -> None:
    try:
        from indy_hub.tasks.material_exchange_contracts import (
            handle_capital_ship_order_closed_by_manager,
        )

        handle_capital_ship_order_closed_by_manager.apply_async(
            args=(int(order.id), str(target_status), int(manager_user_id)),
            countdown=1,
            expires=300,
        )
    except Exception as exc:
        logger.warning(
            "Failed to queue %s notification for capital order %s: %s",
            task_name,
            order.id,
            exc,
        )


def _resolve_restore_status_for_cancelled_order(order: CapitalShipOrder) -> str:
    return str(
        _get_latest_capital_cancellation_context(order).get("previous_status")
        or CapitalShipOrder.Status.WAITING
    )


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@indy_hub_permission_required("can_manage_capital_orders")
@require_POST
def capital_ship_order_reject(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.reject",
        request=request,
    )
    return _close_capital_order(
        request,
        order_id=order_id,
        target_status=CapitalShipOrder.Status.REJECTED,
        action_label="Rejected",
        task_name="reject",
    )


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_POST
def capital_ship_order_cancel(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.cancel",
        request=request,
    )
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    staff_actor_role = _capital_staff_actor_role(request.user)
    is_worker = bool(staff_actor_role)
    is_requester = _is_capital_order_requester(order, request.user)
    if is_worker and not is_requester:
        if not _require_order_update_access_as_worker(request, order):
            return _redirect_after_capital_order_action(
                request, default_route="indy_hub:capital_ship_orders_admin"
            )
    elif not is_requester:
        messages.warning(request, "You cannot update this capital order.")
        return _redirect_after_capital_order_action(request)
    return _close_capital_order(
        request,
        order_id=order_id,
        target_status=CapitalShipOrder.Status.CANCELLED,
        action_label="Cancelled",
        task_name="cancel",
        actor_role=staff_actor_role if is_worker and not is_requester else "requester",
    )


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_POST
def capital_ship_order_uncancel(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.uncancel",
        request=request,
    )
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    is_manager = _can_manage_capital_orders(request.user)
    is_requester = _is_capital_order_requester(order, request.user)
    if is_manager:
        if not _require_order_update_access_as_worker(request, order):
            return _redirect_after_capital_order_action(
                request, default_route="indy_hub:capital_ship_orders_admin"
            )
    elif not _can_requester_reopen_cancelled_order(order, request.user):
        messages.warning(request, "You cannot reopen this capital order.")
        return _redirect_after_capital_order_action(request)
    current_status = str(order.status or "").strip().lower()
    if current_status != CapitalShipOrder.Status.CANCELLED:
        messages.warning(
            request,
            f"Order {order.order_reference} is not cancelled.",
        )
        return _redirect_after_capital_order_action(request)

    actor_role = "requester" if is_requester and not is_manager else "manager"
    restored_status = _resolve_restore_status_for_cancelled_order(order)
    actor_name = str(getattr(request.user, "username", "") or "User").strip()
    status_note = (
        f"Reopened from cancelled by {actor_role} {actor_name} at "
        f"{timezone.now().strftime('%Y-%m-%d %H:%M:%S %Z')}; "
        f"restored to {restored_status}"
    )
    order.status = restored_status
    order.anomaly_reason = ""
    order.requester_preapproved_mismatch_since = None
    _append_order_note(order, status_note)
    order.save(
        update_fields=[
            "status",
            "anomaly_reason",
            "requester_preapproved_mismatch_since",
            "notes",
            "updated_at",
        ]
    )
    _record_capital_event(
        order=order,
        event_type=CapitalShipOrderEvent.EventType.STATUS_CHANGED,
        actor=request.user,
        payload={
            "new_status": restored_status,
            "previous_status": CapitalShipOrder.Status.CANCELLED,
            "restored_from_cancelled": True,
            "changed_by_role": actor_role,
            "changed_by_user_id": int(getattr(request.user, "id", 0) or 0),
        },
    )
    _create_chat_system_message(
        order,
        (
            _(
                "Order reopened by requester. Status restored from Cancelled to %(status)s."
            )
            if actor_role == "requester"
            else _(
                "Order reopened by admin. Status restored from Cancelled to %(status)s."
            )
        )
        % {"status": order.get_status_display()},
    )

    if actor_role == "manager":
        notify_user(
            order.requester,
            _("Capital Order Reopened"),
            _(
                "Order %(ref)s (%(hull)s) was reopened by %(manager)s and is now %(status)s."
            )
            % {
                "ref": order.order_reference,
                "hull": order.ship_type_name,
                "manager": actor_name,
                "status": order.get_status_display(),
            },
            level="info",
            link="/indy_hub/material-exchange/capital-orders/",
        )
        _notify_capital_managers(
            title=_("Capital Order Reopened"),
            body=_(
                "%(manager)s reopened capital order %(ref)s.\n"
                "User: %(user)s\n"
                "Hull: %(hull)s\n"
                "Status: %(status)s"
            )
            % {
                "manager": actor_name,
                "ref": order.order_reference,
                "user": order.requester.username,
                "hull": order.ship_type_name,
                "status": order.get_status_display(),
            },
            order=order,
            level="info",
            link="/indy_hub/material-exchange/capital-orders/admin/",
        )
    else:
        _notify_capital_managers(
            title=_("Capital Order Reopened"),
            body=_(
                "%(requester)s reopened capital order %(ref)s.\n"
                "Hull: %(hull)s\n"
                "Status: %(status)s"
            )
            % {
                "requester": order.requester.username,
                "ref": order.order_reference,
                "hull": order.ship_type_name,
                "status": order.get_status_display(),
            },
            order=order,
            level="info",
            link="/indy_hub/material-exchange/capital-orders/admin/",
        )

    messages.success(
        request,
        f"Order {order.order_reference} reopened to {order.get_status_display()}.",
    )
    return _redirect_after_capital_order_action(request)


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_POST
def capital_ship_order_release_claim(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.release_claim",
        request=request,
    )
    if not _require_capital_queue_access(request):
        return redirect("indy_hub:index")
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    if not _require_order_update_access_as_worker(request, order):
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )
    if order.is_terminal:
        messages.warning(
            request,
            f"Order {order.order_reference} is already closed.",
        )
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    previous_status = str(getattr(order, "status", "") or "").strip().lower()
    if previous_status not in {
        CapitalShipOrder.Status.GATHERING_MATERIALS,
        CapitalShipOrder.Status.IN_PRODUCTION,
        CapitalShipOrder.Status.CONTRACT_CREATED,
        CapitalShipOrder.Status.ANOMALY,
    }:
        messages.warning(
            request,
            f"Order {order.order_reference} is not currently claimed by a worker.",
        )
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    current_manager_id = _resolve_locked_capital_manager_id(order)
    if current_manager_id <= 0:
        messages.warning(
            request,
            f"Order {order.order_reference} is not currently claimed by a worker.",
        )
        return _redirect_after_capital_order_action(
            request, default_route="indy_hub:capital_ship_orders_admin"
        )

    actor_role = _capital_staff_actor_role(request.user) or "manager"
    actor_name = str(
        getattr(
            request.user,
            "username",
            "Manager" if actor_role == "manager" else "Worker",
        )
        or ("Manager" if actor_role == "manager" else "Worker")
    ).strip()
    order.status = CapitalShipOrder.Status.WAITING
    order.gathering_materials_by = None
    order.gathering_materials_at = None
    order.in_production_by = None
    order.in_production_at = None
    order.contract_created_at = None
    order.esi_contract_id = None
    order.definitive_eta_min_days = None
    order.definitive_eta_max_days = None
    order.definitive_eta_updated_at = None
    order.definitive_eta_updated_by = None
    order.anomaly_reason = ""
    _append_order_note(
        order,
        (
            f"Claim released by {actor_role} {actor_name} at "
            f"{timezone.now().strftime('%Y-%m-%d %H:%M:%S %Z')}; "
            f"status reset from {previous_status} to waiting"
        ),
    )
    order.save(
        update_fields=[
            "status",
            "gathering_materials_by",
            "gathering_materials_at",
            "in_production_by",
            "in_production_at",
            "contract_created_at",
            "esi_contract_id",
            "definitive_eta_min_days",
            "definitive_eta_max_days",
            "definitive_eta_updated_at",
            "definitive_eta_updated_by",
            "anomaly_reason",
            "notes",
            "updated_at",
        ]
    )
    _record_capital_event(
        order=order,
        event_type=CapitalShipOrderEvent.EventType.STATUS_CHANGED,
        actor=request.user,
        payload={
            "new_status": CapitalShipOrder.Status.WAITING,
            "previous_status": previous_status,
            "released_claim": True,
            "released_by_user_id": int(getattr(request.user, "id", 0) or 0),
            "changed_by_role": actor_role,
            "changed_by_user_id": int(getattr(request.user, "id", 0) or 0),
        },
    )
    _create_chat_system_message(
        order,
        _("Order claim released by %(actor)s. Status reset to Waiting.")
        % {"actor": actor_name},
    )
    notify_user(
        order.requester,
        _("Capital Order Update"),
        _(
            "Order %(ref)s (%(hull)s) was released back to the waiting queue by %(actor)s."
        )
        % {
            "ref": order.order_reference,
            "hull": order.ship_type_name,
            "actor": actor_name,
        },
        level="info",
        link="/indy_hub/material-exchange/capital-orders/",
    )
    if actor_role == "manager":
        _notify_capital_managers(
            title=_("Capital Order Claim Released"),
            body=_(
                "%(manager)s released the claim on capital order %(ref)s.\n"
                "User: %(user)s\n"
                "Hull: %(hull)s"
            )
            % {
                "manager": actor_name,
                "ref": order.order_reference,
                "user": order.requester.username,
                "hull": order.ship_type_name,
            },
            order=None,
            level="info",
            link="/indy_hub/material-exchange/capital-orders/admin/",
        )
    messages.success(
        request,
        f"Order {order.order_reference} released back to Waiting.",
    )
    return _redirect_after_capital_order_action(
        request, default_route="indy_hub:capital_ship_orders_admin"
    )


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_GET
def capital_ship_order_chat_history(request, order_id: int):
    order = get_object_or_404(
        CapitalShipOrder.objects.select_related("requester"),
        id=order_id,
    )
    if not _can_access_chat(order, request.user):
        return JsonResponse({"error": _("Unauthorized")}, status=403)

    chat = order.ensure_chat()
    base_role = chat.role_for(request.user)
    requested_role = request.GET.get("viewer_role")
    viewer_role_internal = _resolve_chat_internal_role(
        chat,
        request.user,
        base_role=base_role,
        override=requested_role,
    )
    if viewer_role_internal not in {
        CapitalShipOrderChat.SenderRole.REQUESTER,
        CapitalShipOrderChat.SenderRole.ADMIN,
    }:
        return JsonResponse({"error": _("Unauthorized")}, status=403)

    payload = _build_chat_payload(
        order=order,
        chat=chat,
        viewer_role_internal=viewer_role_internal,
    )
    chat.mark_seen(viewer_role_internal, force=True)
    return JsonResponse(payload)


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_POST
def capital_ship_order_chat_send(request, order_id: int):
    order = get_object_or_404(CapitalShipOrder.objects.select_related("requester"), id=order_id)
    if not _can_access_chat(order, request.user):
        return JsonResponse({"error": _("Unauthorized")}, status=403)

    chat = order.ensure_chat()
    base_role = chat.role_for(request.user)
    if base_role not in {
        CapitalShipOrderChat.SenderRole.REQUESTER,
        CapitalShipOrderChat.SenderRole.ADMIN,
    }:
        return JsonResponse({"error": _("Unauthorized")}, status=403)
    if not chat.is_open:
        return JsonResponse(
            {"error": _("This chat is closed."), "closed": True}, status=409
        )

    payload = {}
    if request.content_type == "application/json":
        try:
            payload = json.loads(request.body.decode("utf-8"))
        except json.JSONDecodeError:
            payload = {}
    if not payload:
        payload = request.POST

    requested_role = payload.get("viewer_role") or payload.get("role")
    viewer_role_internal = _resolve_chat_internal_role(
        chat,
        request.user,
        base_role=base_role,
        override=requested_role,
    )
    if viewer_role_internal not in {
        CapitalShipOrderChat.SenderRole.REQUESTER,
        CapitalShipOrderChat.SenderRole.ADMIN,
    }:
        return JsonResponse({"error": _("Unauthorized")}, status=403)

    message_content = (payload.get("message") or payload.get("content") or "").strip()
    if not message_content:
        return JsonResponse({"error": _("Message cannot be empty.")}, status=400)

    msg = CapitalShipOrderMessage(
        chat=chat,
        sender=request.user,
        sender_role=viewer_role_internal,
        content=message_content,
    )
    try:
        msg.full_clean()
        msg.save()
    except ValidationError as exc:
        detail = ""
        if hasattr(exc, "messages") and exc.messages:
            detail = exc.messages[0]
        else:
            detail = str(exc)
        return JsonResponse(
            {"error": _("Invalid message."), "details": detail}, status=400
        )
    chat.register_message(sender_role=viewer_role_internal)

    if viewer_role_internal == CapitalShipOrderChat.SenderRole.REQUESTER:
        _notify_capital_managers(
            title=_("Capital Order Chat Message"),
            body=_(
                "%(user)s sent a new message for %(ref)s (%(hull)s)."
            )
            % {
                "user": order.requester.username,
                "ref": order.order_reference,
                "hull": order.ship_type_name,
            },
            order=order,
            level="info",
            link=f"/indy_hub/material-exchange/capital-orders/admin/?open_chat={chat.id}",
        )
    else:
        notify_user(
            order.requester,
            _("Capital Order Chat Message"),
            _(
                "An admin sent a new message for order %(ref)s."
            )
            % {"ref": order.order_reference},
            level="info",
            link=f"/indy_hub/material-exchange/capital-orders/?open_chat={chat.id}",
        )

    created_local = timezone.localtime(msg.created_at)
    return JsonResponse(
        {
            "message": {
                "id": msg.id,
                "role": _to_public_message_role(msg.sender_role),
                "content": msg.content,
                "created_at": created_local.isoformat(),
                "created_display": created_local.strftime("%Y-%m-%d %H:%M"),
            }
        },
        status=201,
    )


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_POST
def capital_ship_order_chat_decide(request, order_id: int):
    order = get_object_or_404(
        CapitalShipOrder.objects.select_related("requester", "offer_updated_by"),
        id=order_id,
    )
    if int(request.user.id) != int(order.requester_id):
        return JsonResponse({"error": _("Unauthorized")}, status=403)

    chat = order.ensure_chat()
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        payload = {}
    decision = (payload.get("decision") or "").strip().lower()
    if decision not in {"accept", "reject"}:
        return JsonResponse({"error": _("Unsupported decision.")}, status=400)

    if decision == "accept":
        if not order.has_pending_offer_confirmation:
            return JsonResponse(
                {"error": _("No pending offer is available for confirmation.")},
                status=409,
            )
        now = timezone.now()
        order.user_offer_confirmed_at = now
        order.user_offer_confirmed_by = request.user
        order.agreed_price_isk = order.offer_price_isk
        order.likely_eta_min_days = order.offer_eta_min_days
        order.likely_eta_max_days = order.offer_eta_max_days
        order.agreement_locked_at = now
        order.agreement_locked_by = order.offer_updated_by
        order.save(
            update_fields=[
                "user_offer_confirmed_at",
                "user_offer_confirmed_by",
                "agreed_price_isk",
                "likely_eta_min_days",
                "likely_eta_max_days",
                "agreement_locked_at",
                "agreement_locked_by",
                "updated_at",
            ]
        )
        _record_capital_event(
            order=order,
            event_type=CapitalShipOrderEvent.EventType.OFFER_CONFIRMED_BY_USER,
            actor=request.user,
            payload={
                "agreed_price_isk": str(order.agreed_price_isk or ""),
                "likely_eta_min_days": order.likely_eta_min_days,
                "likely_eta_max_days": order.likely_eta_max_days,
            },
        )
        _create_chat_system_message(
            order,
            _(
                "Requester confirmed agreement: Agreed upon price %(price)s ISK, likely delivery %(eta_min)s-%(eta_max)s days."
            )
            % {
                "price": f"{order.agreed_price_isk:,.2f}",
                "eta_min": int(order.likely_eta_min_days or 0),
                "eta_max": int(order.likely_eta_max_days or 0),
            },
        )
        _notify_capital_managers(
            title=_("Capital Offer Confirmed"),
            body=_(
                "%(user)s confirmed offer for %(ref)s.\n"
                "Agreed price: %(price)s ISK\n"
                "Likely delivery: %(eta_min)s-%(eta_max)s days"
            )
            % {
                "user": order.requester.username,
                "ref": order.order_reference,
                "price": f"{order.agreed_price_isk:,.2f}",
                "eta_min": int(order.likely_eta_min_days or 0),
                "eta_max": int(order.likely_eta_max_days or 0),
            },
            order=order,
            level="success",
            link=f"/indy_hub/material-exchange/capital-orders/admin/?open_chat={chat.id}",
        )
        return JsonResponse({"status": "accepted"})

    declined_offer_payload = {
        "offer_price_isk": (
            str(order.offer_price_isk) if order.offer_price_isk is not None else ""
        ),
        "offer_eta_min_days": order.offer_eta_min_days,
        "offer_eta_max_days": order.offer_eta_max_days,
        "lead_time_days": order.lead_time_days,
        "offer_notes": str(order.offer_notes or "").strip(),
        "offer_updated_by_id": int(order.offer_updated_by_id or 0) or None,
        "offer_updated_by_username": (
            str(order.offer_updated_by.username)
            if getattr(order, "offer_updated_by", None)
            else ""
        ),
        "offer_updated_at": (
            order.offer_updated_at.isoformat() if order.offer_updated_at else ""
        ),
    }
    order.user_offer_confirmed_at = None
    order.user_offer_confirmed_by = None
    order.agreed_price_isk = None
    order.likely_eta_min_days = None
    order.likely_eta_max_days = None
    order.agreement_locked_at = None
    order.agreement_locked_by = None
    order.offer_price_isk = None
    order.offer_eta_min_days = None
    order.offer_eta_max_days = None
    order.offer_notes = ""
    order.offer_updated_by = None
    order.offer_updated_at = None
    order.save(
        update_fields=[
            "user_offer_confirmed_at",
            "user_offer_confirmed_by",
            "agreed_price_isk",
            "likely_eta_min_days",
            "likely_eta_max_days",
            "agreement_locked_at",
            "agreement_locked_by",
            "offer_price_isk",
            "offer_eta_min_days",
            "offer_eta_max_days",
            "offer_notes",
            "offer_updated_by",
            "offer_updated_at",
            "updated_at",
        ]
    )
    _record_capital_event(
        order=order,
        event_type=CapitalShipOrderEvent.EventType.OFFER_REJECTED_BY_USER,
        actor=request.user,
        payload=declined_offer_payload,
    )
    _create_chat_system_message(
        order,
        _("Requester declined the current offer and asked for revisions."),
    )
    declined_price_text = (
        f"{Decimal(str(declined_offer_payload.get('offer_price_isk') or 0)):,.2f}"
        if str(declined_offer_payload.get("offer_price_isk") or "").strip()
        else "-"
    )
    declined_eta_min = declined_offer_payload.get("offer_eta_min_days")
    declined_eta_max = declined_offer_payload.get("offer_eta_max_days")
    declined_eta_text = (
        f"{int(declined_eta_min)}-{int(declined_eta_max)}"
        if declined_eta_min is not None and declined_eta_max is not None
        else "-"
    )
    _notify_capital_managers(
        title=_("Capital Offer Declined"),
        body=_(
            "%(user)s declined the offer for %(ref)s.\n"
            "Declined proposal: %(price)s ISK, %(eta)s days.\n\n"
            "**You must review chat and update the proposal or cancel the order**."
        )
        % {
            "user": order.requester.username,
            "ref": order.order_reference,
            "price": declined_price_text,
            "eta": declined_eta_text,
        },
        order=order,
        level="warning",
        link=f"/indy_hub/material-exchange/capital-orders/admin/?open_chat={chat.id}",
    )
    return JsonResponse({"status": "rejected"})
