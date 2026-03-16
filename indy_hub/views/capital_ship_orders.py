"""Capital ship order views."""

from __future__ import annotations

# Django
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.cache import cache
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST

# Alliance Auth
from allianceauth.authentication.models import UserProfile
from allianceauth.services.hooks import get_extension_logger
from eve_sde.models import ItemType

# AA Example App
from indy_hub.decorators import indy_hub_permission_required
from indy_hub.models import CapitalShipOrder, MaterialExchangeConfig, MaterialExchangeSettings
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
    expanded = set(int(group_id) for group_id in root_ids)
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


def _load_capital_ship_options() -> list[dict[str, object]]:
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
            ship_class = str(entry.get("ship_class") or "").strip().lower()
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
    root_group_ids_by_class: dict[str, set[int]] = {"dread": set(), "carrier": set(), "fax": set()}

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
            group__category_id=6,  # ships
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

    ship_options = _load_capital_ship_options()
    ship_options_by_id = {int(row["type_id"]): row for row in ship_options}

    if request.method == "POST":
        ship_type_id_raw = (request.POST.get("ship_type_id") or "").strip()
        reason = (request.POST.get("reason") or "").strip()
        selected_ship = None
        selected_ship_type_id = 0

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
        )
        messages.success(
            request,
            f"Capital order {order.order_reference} created for {order.ship_type_name}.",
        )
        return redirect("indy_hub:capital_ship_orders")

    my_orders = (
        CapitalShipOrder.objects.filter(requester=request.user, config=config)
        .select_related("in_production_by")
        .order_by("-created_at")
    )

    ship_options_by_class: dict[str, list[dict[str, object]]] = {
        "dread": [],
        "carrier": [],
        "fax": [],
    }
    for option in ship_options:
        option_class = str(option["ship_class"])
        if option_class not in ship_options_by_class:
            ship_options_by_class[option_class] = []
        ship_options_by_class[option_class].append(option)

    context = {
        "ship_options_by_class": ship_options_by_class,
        "reason_choices": CapitalShipOrder.Reason.choices,
        "my_orders": my_orders,
        "can_manage_material_hub": request.user.has_perm(
            "indy_hub.can_manage_material_hub"
        ),
    }
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

    orders = CapitalShipOrder.objects.filter(config=config).select_related(
        "requester", "in_production_by"
    ).order_by("-created_at")
    if not include_completed:
        orders = orders.exclude(status__in=list(_CAPITAL_TERMINAL_STATUSES))

    orders = list(orders)
    for order in orders:
        order.requester_main_character = _resolve_main_character_name(order.requester)

    context = {
        "orders": orders,
        "include_completed": include_completed,
    }
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
def capital_ship_order_set_in_production(request, order_id: int):
    emit_view_analytics_event(
        view_name="capital_ship_orders.set_in_production",
        request=request,
    )
    order = get_object_or_404(CapitalShipOrder, id=order_id)

    if order.status != CapitalShipOrder.Status.WAITING:
        messages.warning(
            request,
            f"Order {order.order_reference} is not in waiting status.",
        )
        return redirect("indy_hub:capital_ship_orders_admin")

    order.status = CapitalShipOrder.Status.IN_PRODUCTION
    order.in_production_by = request.user
    order.in_production_at = timezone.now()
    order.save(
        update_fields=[
            "status",
            "in_production_by",
            "in_production_at",
            "updated_at",
        ]
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
    previous_notes = str(order.notes or "").strip()
    status_note = (
        f"{action_label} by manager {manager_name} at "
        f"{timezone.now().strftime('%Y-%m-%d %H:%M:%S %Z')}"
    )
    order.status = target_status
    order.anomaly_reason = ""
    order.notes = f"{previous_notes}\n{status_note}".strip()
    order.save(update_fields=["status", "anomaly_reason", "notes", "updated_at"])

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
