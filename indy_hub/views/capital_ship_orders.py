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
from allianceauth.authentication.models import UserProfile
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

_CAPITAL_SHIP_OPTIONS_CACHE_KEY = "indy_hub:capital_ship_orders:options:v1"
_SHIP_CLASS_ORDER = {"dread": 0, "carrier": 1, "fax": 2}
_SHIP_CLASS_LABEL = {"dread": "Dreadnought", "carrier": "Carrier", "fax": "FAX"}
_ROOT_MARKET_GROUP_CLASSIFIERS = {
    "dread": ("dreadnought",),
    "carrier": ("carrier",),
    "fax": ("force auxili", "force auxiliary"),
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


def _expand_market_group_ids(
    root_ids: set[int], children_map: dict[int | None, set[int]]
) -> set[int]:
    if not root_ids:
        return set()
    expanded = {int(group_id) for group_id in root_ids}
    stack = list(expanded)
    while stack:
        current = int(stack.pop())
        for child_id in children_map.get(current, set()):
            child_int = int(child_id)
            if child_int in expanded:
                continue
            expanded.add(child_int)
            stack.append(child_int)
    return expanded


def _resolve_ship_class_for_group_name(group_name: str) -> str | None:
    lowered = str(group_name or "").strip().lower()
    if not lowered:
        return None
    if "force auxili" in lowered:
        return "fax"
    if "dreadnought" in lowered:
        return "dread"
    if lowered in {"carrier", "carriers"}:
        return "carrier"
    return None


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

    try:
        from indy_hub.models import SdeMarketGroup

        group_rows = list(SdeMarketGroup.objects.values_list("id", "name", "parent_id"))
    except Exception as exc:
        logger.warning("Failed to load market groups for capital orders: %s", exc)
        group_rows = []

    children_map: dict[int | None, set[int]] = {}
    root_group_ids_by_class: dict[str, set[int]] = {
        "dread": set(),
        "carrier": set(),
        "fax": set(),
    }
    for group_id, group_name, parent_id in group_rows:
        group_id_int = int(group_id)
        parent_key = int(parent_id) if parent_id is not None else None
        children_map.setdefault(parent_key, set()).add(group_id_int)

        lowered_name = str(group_name or "").strip().lower()
        for ship_class, markers in _ROOT_MARKET_GROUP_CLASSIFIERS.items():
            if any(marker in lowered_name for marker in markers):
                root_group_ids_by_class[ship_class].add(group_id_int)

    options_by_type_id: dict[int, dict[str, object]] = {}
    for ship_class, root_ids in root_group_ids_by_class.items():
        expanded_ids = _expand_market_group_ids(root_ids, children_map)
        if not expanded_ids:
            continue
        type_rows = ItemType.objects.filter(
            market_group_id__in=list(expanded_ids),
            group__category_id=6,
            group__name__in=["Dreadnought", "Carrier", "Force Auxiliary"],
        ).values_list("id", "name")
        for type_id, type_name in type_rows:
            type_id_int = int(type_id)
            if type_id_int in options_by_type_id:
                continue
            clean_name = str(type_name or "").strip()
            if not clean_name:
                continue
            options_by_type_id[type_id_int] = {
                "type_id": type_id_int,
                "type_name": clean_name,
                "ship_class": ship_class,
                "ship_class_label": _SHIP_CLASS_LABEL[ship_class],
            }

    if not options_by_type_id:
        fallback_rows = ItemType.objects.filter(
            group__name__in=["Dreadnought", "Carrier", "Force Auxiliary"],
            group__category_id=6,
        ).values_list("id", "name", "group__name")
        for type_id, type_name, group_name in fallback_rows:
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
    custom_options: list[dict[str, object]] = []
    try:
        disabled_type_ids = set(config.get_capital_disabled_ship_type_ids())
    except Exception:
        disabled_type_ids = set()
    try:
        custom_options = config.get_capital_custom_ship_options()
    except Exception:
        custom_options = []

    for type_id in disabled_type_ids:
        options_by_type_id.pop(int(type_id), None)

    for custom_entry in custom_options:
        try:
            type_id = int(custom_entry.get("type_id") or 0)
        except (TypeError, ValueError):
            continue
        if type_id <= 0 or type_id in disabled_type_ids:
            continue
        if not bool(custom_entry.get("enabled", True)):
            options_by_type_id.pop(type_id, None)
            continue

        type_name = str(custom_entry.get("type_name") or "").strip()
        ship_class = _normalize_ship_class_key(custom_entry.get("ship_class"))
        if not type_name or not ship_class:
            continue
        ship_class_label = str(custom_entry.get("ship_class_label") or "").strip()
        if not ship_class_label:
            ship_class_label = _default_ship_class_label(ship_class)

        options_by_type_id[type_id] = {
            "type_id": type_id,
            "type_name": type_name,
            "ship_class": ship_class,
            "ship_class_label": ship_class_label,
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
    try:
        parsed = Decimal(str(value))
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
    ship_class = str(ship_class or "").strip().lower()
    if ship_class == CapitalShipOrder.ShipClass.DREAD:
        return _quantize_isk(getattr(config, "capital_default_price_dread", None))
    if ship_class == CapitalShipOrder.ShipClass.CARRIER:
        return _quantize_isk(getattr(config, "capital_default_price_carrier", None))
    if ship_class == CapitalShipOrder.ShipClass.FAX:
        return _quantize_isk(getattr(config, "capital_default_price_fax", None))
    return None


def _get_class_default_eta_window(config: MaterialExchangeConfig, ship_class: str) -> tuple[int | None, int | None]:
    ship_class = str(ship_class or "").strip().lower()
    if ship_class == CapitalShipOrder.ShipClass.DREAD:
        return (
            int(getattr(config, "capital_default_eta_min_days_dread", 14) or 14),
            int(getattr(config, "capital_default_eta_max_days_dread", 28) or 28),
        )
    if ship_class == CapitalShipOrder.ShipClass.CARRIER:
        return (
            int(getattr(config, "capital_default_eta_min_days_carrier", 14) or 14),
            int(getattr(config, "capital_default_eta_max_days_carrier", 28) or 28),
        )
    if ship_class == CapitalShipOrder.ShipClass.FAX:
        return (
            int(getattr(config, "capital_default_eta_min_days_fax", 14) or 14),
            int(getattr(config, "capital_default_eta_max_days_fax", 28) or 28),
        )
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

    default_price = _get_class_default_price(order.config, order.ship_class)
    if default_price is not None:
        return default_price, "config_default"
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


def _notify_material_exchange_admins(
    *,
    title: str,
    body: str,
    level: str = "info",
    link: str = "/indy_hub/material-exchange/capital-orders/admin/",
) -> None:
    admins = User.objects.filter(is_active=True).filter(
        Q(
            user_permissions__codename="can_manage_material_hub",
            user_permissions__content_type__app_label="indy_hub",
        )
        | Q(
            groups__permissions__codename="can_manage_material_hub",
            groups__permissions__content_type__app_label="indy_hub",
        )
    )
    notify_multi(admins, title, body, level=level, link=link)


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
    return bool(user.has_perm("indy_hub.can_manage_material_hub"))


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
        and user.has_perm("indy_hub.can_manage_material_hub")
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


def _attach_user_display_fields(order: CapitalShipOrder) -> None:
    order.display_price_isk = order.agreed_price_isk or order.offer_price_isk
    if order.definitive_eta_min_days is not None and order.definitive_eta_max_days is not None:
        order.display_eta_min_days = order.definitive_eta_min_days
        order.display_eta_max_days = order.definitive_eta_max_days
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
        order.display_eta_label = _("Guideline ETA")


@login_required
@indy_hub_permission_required("can_access_indy_hub")
def capital_ship_orders(request):
    emit_view_analytics_event(view_name="capital_ship_orders.index", request=request)

    if not _is_material_exchange_enabled():
        messages.warning(request, "Material Exchange is disabled.")
        return redirect("indy_hub:material_exchange_index")

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, "Material Exchange is not configured.")
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

    ship_options_by_class: dict[str, list[dict[str, object]]] = {}
    ship_class_labels: dict[str, str] = {}
    for option in ship_options:
        ship_class = _normalize_ship_class_key(option.get("ship_class"))
        if not ship_class:
            continue
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
        "can_manage_material_hub": request.user.has_perm(
            "indy_hub.can_manage_material_hub"
        ),
    }
    if auto_open_chat_id:
        context["auto_open_chat_id"] = auto_open_chat_id
    context.update(build_nav_context(request.user, active_tab="capital_orders"))
    return render(request, "indy_hub/material_exchange/capital_orders.html", context)


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@indy_hub_permission_required("can_manage_material_hub")
def capital_ship_orders_admin(request):
    emit_view_analytics_event(view_name="capital_ship_orders.admin", request=request)

    include_completed = str(request.GET.get("include_completed") or "").strip() == "1"

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, "Material Exchange is not configured.")
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
    for order in orders:
        order.requester_main_character = _resolve_main_character_name(order.requester)
        order.chat_trigger = _build_order_chat_trigger(order, viewer_role_public="seller")
        _attach_user_display_fields(order)

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
@indy_hub_permission_required("can_manage_material_hub")
@require_POST
def capital_ship_order_refresh_guideline(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.refresh_guideline",
        request=request,
    )
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    if order.is_terminal:
        messages.warning(
            request,
            f"Order {order.order_reference} is already closed.",
        )
        return redirect("indy_hub:capital_ship_orders_admin")
    _refresh_guideline(order)
    messages.success(
        request,
        f"Guideline refreshed for order {order.order_reference}.",
    )
    return redirect("indy_hub:capital_ship_orders_admin")


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@indy_hub_permission_required("can_manage_material_hub")
@require_POST
def capital_ship_order_set_gathering_materials(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.set_gathering_materials",
        request=request,
    )
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    if order.status != CapitalShipOrder.Status.WAITING:
        messages.warning(
            request,
            f"Order {order.order_reference} is not in waiting status.",
        )
        return redirect("indy_hub:capital_ship_orders_admin")

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
    return redirect("indy_hub:capital_ship_orders_admin")


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@indy_hub_permission_required("can_manage_material_hub")
@require_POST
def capital_ship_order_set_in_production(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.set_in_production",
        request=request,
    )
    order = get_object_or_404(CapitalShipOrder, id=order_id)

    if order.status not in {
        CapitalShipOrder.Status.WAITING,
        CapitalShipOrder.Status.GATHERING_MATERIALS,
    }:
        messages.warning(
            request,
            f"Order {order.order_reference} is not in waiting or gathering materials status.",
        )
        return redirect("indy_hub:capital_ship_orders_admin")

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
    return redirect("indy_hub:capital_ship_orders_admin")


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@indy_hub_permission_required("can_manage_material_hub")
@require_POST
def capital_ship_order_update_offer(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.update_offer",
        request=request,
    )
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    if order.is_terminal:
        messages.warning(
            request,
            f"Order {order.order_reference} is already closed.",
        )
        return redirect("indy_hub:capital_ship_orders_admin")

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
        return redirect("indy_hub:capital_ship_orders_admin")
    if offer_eta_min_days is None or offer_eta_max_days is None:
        messages.error(request, "Offer ETA min and max days are required.")
        return redirect("indy_hub:capital_ship_orders_admin")
    if offer_eta_max_days < offer_eta_min_days:
        messages.error(request, "Offer ETA max days must be greater than or equal to min days.")
        return redirect("indy_hub:capital_ship_orders_admin")

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
            "Likely delivery: %(eta_min)s-%(eta_max)s days"
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
    return redirect("indy_hub:capital_ship_orders_admin")


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@indy_hub_permission_required("can_manage_material_hub")
@require_POST
def capital_ship_order_set_definitive_eta(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.set_definitive_eta",
        request=request,
    )
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    if order.status not in {
        CapitalShipOrder.Status.GATHERING_MATERIALS,
        CapitalShipOrder.Status.IN_PRODUCTION,
        CapitalShipOrder.Status.CONTRACT_CREATED,
    }:
        messages.warning(
            request,
            "Definitive ETA can only be set once work has started.",
        )
        return redirect("indy_hub:capital_ship_orders_admin")

    eta_min_days = _parse_positive_int(request.POST.get("definitive_eta_min_days"), minimum=1)
    eta_max_days = _parse_positive_int(request.POST.get("definitive_eta_max_days"), minimum=1)
    if eta_min_days is None or eta_max_days is None:
        messages.error(request, "Definitive ETA min and max days are required.")
        return redirect("indy_hub:capital_ship_orders_admin")
    if eta_max_days < eta_min_days:
        messages.error(request, "Definitive ETA max days must be greater than or equal to min days.")
        return redirect("indy_hub:capital_ship_orders_admin")

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
    return redirect("indy_hub:capital_ship_orders_admin")


def _close_capital_order(
    request,
    *,
    order_id: int,
    target_status: str,
    action_label: str,
    task_name: str,
):
    order = get_object_or_404(CapitalShipOrder, id=order_id)
    current_status = str(order.status or "")

    if current_status == str(target_status):
        messages.info(
            request,
            f"Order {order.order_reference} is already {action_label.lower()}.",
        )
        return redirect("indy_hub:capital_ship_orders_admin")

    if current_status in _CAPITAL_TERMINAL_STATUSES:
        messages.warning(
            request,
            f"Order {order.order_reference} is already closed ({order.get_status_display()}).",
        )
        return redirect("indy_hub:capital_ship_orders_admin")

    manager_name = str(getattr(request.user, "username", "") or "Manager").strip()
    status_note = (
        f"{action_label} by manager {manager_name} at "
        f"{timezone.now().strftime('%Y-%m-%d %H:%M:%S %Z')}"
    )
    order.status = target_status
    order.anomaly_reason = ""
    _append_order_note(order, status_note)
    order.save(update_fields=["status", "anomaly_reason", "notes", "updated_at"])
    _record_capital_event(
        order=order,
        event_type=CapitalShipOrderEvent.EventType.STATUS_CHANGED,
        actor=request.user,
        payload={"new_status": target_status, "previous_status": current_status},
    )

    try:
        from indy_hub.tasks.material_exchange_contracts import (
            handle_capital_ship_order_closed_by_manager,
        )

        handle_capital_ship_order_closed_by_manager.apply_async(
            args=(int(order.id), str(target_status), int(request.user.id)),
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

    messages.success(
        request,
        f"Order {order.order_reference} marked as {action_label}.",
    )
    return redirect("indy_hub:capital_ship_orders_admin")


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@indy_hub_permission_required("can_manage_material_hub")
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
@indy_hub_permission_required("can_manage_material_hub")
@require_POST
def capital_ship_order_cancel(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.cancel",
        request=request,
    )
    return _close_capital_order(
        request,
        order_id=order_id,
        target_status=CapitalShipOrder.Status.CANCELLED,
        action_label="Cancelled",
        task_name="cancel",
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
        _notify_material_exchange_admins(
            title=_("Capital Order Chat Message"),
            body=_(
                "%(user)s sent a new message for %(ref)s (%(hull)s)."
            )
            % {
                "user": order.requester.username,
                "ref": order.order_reference,
                "hull": order.ship_type_name,
            },
            level="info",
            link=f"/indy_hub/material-exchange/capital-orders/admin/?open_chat={chat.id}",
        )
    else:
        notify_user(
            order.requester,
            _("Capital Order Chat Message"),
            _(
                "A material exchange admin sent a new message for order %(ref)s."
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
    order = get_object_or_404(CapitalShipOrder.objects.select_related("requester"), id=order_id)
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
        _notify_material_exchange_admins(
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
            level="success",
            link=f"/indy_hub/material-exchange/capital-orders/admin/?open_chat={chat.id}",
        )
        return JsonResponse({"status": "accepted"})

    order.user_offer_confirmed_at = None
    order.user_offer_confirmed_by = None
    order.agreed_price_isk = None
    order.likely_eta_min_days = None
    order.likely_eta_max_days = None
    order.agreement_locked_at = None
    order.agreement_locked_by = None
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
        event_type=CapitalShipOrderEvent.EventType.OFFER_REJECTED_BY_USER,
        actor=request.user,
        payload={},
    )
    _create_chat_system_message(
        order,
        _("Requester declined the current offer and asked for revisions."),
    )
    _notify_material_exchange_admins(
        title=_("Capital Offer Declined"),
        body=_(
            "%(user)s declined the offer for %(ref)s. Review chat and update proposal."
        )
        % {
            "user": order.requester.username,
            "ref": order.order_reference,
        },
        level="warning",
        link=f"/indy_hub/material-exchange/capital-orders/admin/?open_chat={chat.id}",
    )
    return JsonResponse({"status": "rejected"})
