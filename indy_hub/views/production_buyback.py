"""Buyback availability and order submission for the production simulator.

These are thin JSON adapters. Stock, reservations and pricing come from the
buyback browse snapshot, and orders are created by the same
``create_buy_order`` service the buyback page uses, so the simulator never has
its own order rules.
"""

from __future__ import annotations

# Standard Library
import json
import re
from decimal import Decimal, InvalidOperation

# Django
from django.contrib.auth.decorators import login_required
from django.core.cache import cache
from django.http import JsonResponse
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_http_methods

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

from ..decorators import (
    indy_hub_access_required,
    indy_hub_permission_required,
    tokens_required,
)
from ..models import MaterialExchangeBuyOrder
from ..services.material_exchange_buy_orders import (
    TERMINAL_FAILED_BUY_STATUSES,
    create_buy_order,
    get_recipient_characters,
)

logger = get_extension_logger(__name__)

# Matches the buyback page, which auto-refreshes stock older than an hour.
STOCK_STALE_AFTER_SECONDS = 3600
MAX_TYPE_IDS = 200
MAX_ORDER_IDS = 50
IDEMPOTENCY_TTL_SECONDS = 10 * 60
_CLIENT_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")

# HTTP status per service error code; every error body is
# {"error": <code>, "message": <text>, ...details}.
_ERROR_STATUS = {
    "no_items": 400,
    "invalid_recipient": 400,
    "mixed_locations": 400,
    "stale_row": 409,
    "insufficient_stock": 409,
    "price_changed": 409,
    "no_price": 409,
}


def _error(code: str, message: str, status: int, **details) -> JsonResponse:
    return JsonResponse({"error": code, "message": message, **details}, status=status)


def _parse_int_list(raw_value: str, limit: int) -> list[int]:
    values: list[int] = []
    for raw in str(raw_value or "").split(",")[:limit]:
        try:
            value = int(str(raw).strip())
        except (TypeError, ValueError):
            continue
        if value > 0 and value not in values:
            values.append(value)
    return values


def _get_buy_config_or_reason():
    """Return (config, None) when simulator orders are possible, else (None, reason)."""
    # AA Example App
    from indy_hub.views.material_exchange import (
        _get_material_exchange_config,
        _is_material_exchange_enabled,
    )

    if not _is_material_exchange_enabled():
        return None, "buyback_disabled"
    config = _get_material_exchange_config()
    if not config:
        return None, "buyback_not_configured"
    if not bool(getattr(config, "buy_enabled", True)):
        return None, "buy_orders_disabled"
    return config, None


def _order_progress(order: MaterialExchangeBuyOrder) -> str:
    """Collapse the order workflow into the simulator's three states.

    Only the order workflow decides these: a pending order is never shown as
    owned, and even a completed one is only "delivered" (owned stock still
    comes from assets or the materials bay).
    """
    if order.status in TERMINAL_FAILED_BUY_STATUSES:
        return "failed"
    if order.status == MaterialExchangeBuyOrder.Status.COMPLETED:
        return "delivered"
    return "pending"


def _serialize_order(order: MaterialExchangeBuyOrder) -> dict:
    items = [
        {
            "type_id": int(item.type_id),
            "type_name": str(item.type_name or ""),
            "quantity": int(item.quantity or 0),
            "unit_price": str(item.unit_price),
        }
        for item in order.items.all()
    ]
    return {
        "id": int(order.id),
        "reference": str(order.order_reference or ""),
        "status": str(order.status),
        "status_label": str(order.get_status_display()),
        "progress": _order_progress(order),
        "items": items,
        "total": str(order.rounded_total_price or 0),
        "detail_url": reverse("indy_hub:buy_order_detail", args=[order.id]),
    }


def _pick_plain_rows(stock_rows) -> dict[int, dict]:
    """Best orderable row per type: loose items only, most available first.

    Blueprint and in-container rows are left to the buyback page, where the
    user can see exactly which copy or container they are buying.
    """
    best: dict[int, dict] = {}
    for row in stock_rows or []:
        if str(row.get("row_kind") or "") != "item":
            continue
        if str(row.get("blueprint_variant") or "").strip():
            continue
        if str(row.get("container_path") or "").strip():
            continue
        type_id = int(row.get("type_id") or 0)
        if type_id <= 0:
            continue
        current = best.get(type_id)
        if current is None or int(row.get("available_quantity") or 0) > int(
            current.get("available_quantity") or 0
        ):
            best[type_id] = row
    return best


@indy_hub_access_required
@indy_hub_permission_required("can_access_indy_hub")
@login_required
@require_http_methods(["GET"])
def production_buyback_availability(request):
    """Buyback stock, member price and recipients for the simulator's needed types.

    Also reports the current workflow state of simulator-created orders passed
    in ``order_ids`` (filtered to the current user) so pending quantities can
    be reconciled against the authoritative order data.
    """
    order_ids = _parse_int_list(request.GET.get("order_ids", ""), MAX_ORDER_IDS)
    orders = [
        _serialize_order(order)
        for order in MaterialExchangeBuyOrder.objects.filter(
            buyer=request.user, id__in=order_ids
        ).prefetch_related("items")
    ]

    config, reason = _get_buy_config_or_reason()
    if config is None:
        return JsonResponse(
            {"enabled": False, "reason": reason, "items": {}, "orders": orders}
        )

    # AA Example App
    from indy_hub.views.material_exchange import (
        _get_buy_stock_snapshot_for_submission,
    )

    type_ids = _parse_int_list(request.GET.get("type_ids", ""), MAX_TYPE_IDS)
    items: dict[str, dict] = {}
    ore_suggestions: dict[str, list[dict]] = {}
    if type_ids:
        snapshot = _get_buy_stock_snapshot_for_submission(
            config=config,
            submitted_type_ids=None,
            user_id=int(request.user.id),
        )
        plain_rows = _pick_plain_rows(snapshot.get("stock_rows"))
        for type_id in type_ids:
            row = plain_rows.get(type_id)
            if not row or int(row.get("available_quantity") or 0) <= 0:
                continue
            try:
                unit_price = Decimal(str(row.get("display_sell_price_to_member") or 0))
            except (InvalidOperation, ValueError):
                unit_price = Decimal("0")
            if unit_price <= 0:
                continue
            items[str(type_id)] = {
                "type_id": type_id,
                "type_name": str(row.get("display_type_name") or ""),
                "row_index": int(row.get("row_index")),
                "available_quantity": int(row.get("available_quantity") or 0),
                "reserved_quantity": int(row.get("reserved_quantity") or 0),
                "unit_price": str(unit_price),
                "price_source": (
                    "buyback_override"
                    if row.get("has_buy_price_override")
                    else "buyback_market"
                ),
                "location_label": str(row.get("buy_location_label") or ""),
            }

        # AA Example App
        from ..services.reprocessing import (
            get_ore_type_ids,
            get_portion_size_map,
            get_reprocessing_outputs_map,
        )

        all_plain_type_ids = set(plain_rows.keys())
        candidate_type_ids = get_ore_type_ids(all_plain_type_ids)
        if candidate_type_ids:
            outputs_map = get_reprocessing_outputs_map(candidate_type_ids)
            portion_size_map = get_portion_size_map(candidate_type_ids)
            requested_set = set(type_ids)
            for stock_type_id, outputs in outputs_map.items():
                if not outputs:
                    continue
                row = plain_rows.get(stock_type_id)
                if not row:
                    continue
                available_qty = int(row.get("available_quantity") or 0)
                if available_qty <= 0:
                    continue
                portion_size = portion_size_map.get(stock_type_id) or 100
                portions_in_stock = available_qty // portion_size
                if portions_in_stock <= 0:
                    continue
                try:
                    unit_price = Decimal(
                        str(row.get("display_sell_price_to_member") or 0)
                    )
                except (InvalidOperation, ValueError):
                    unit_price = Decimal("0")
                if unit_price <= 0:
                    continue

                # Make sure the ore item is in items map so simulator modals can order it
                if str(stock_type_id) not in items:
                    items[str(stock_type_id)] = {
                        "type_id": stock_type_id,
                        "type_name": str(row.get("display_type_name") or ""),
                        "row_index": int(row.get("row_index")),
                        "available_quantity": available_qty,
                        "reserved_quantity": int(row.get("reserved_quantity") or 0),
                        "unit_price": str(unit_price),
                        "price_source": (
                            "buyback_override"
                            if row.get("has_buy_price_override")
                            else "buyback_market"
                        ),
                        "location_label": str(row.get("buy_location_label") or ""),
                    }

                for mineral_type_id, yield_per_portion in outputs.items():
                    if yield_per_portion <= 0 or mineral_type_id not in requested_set:
                        continue
                    est_mineral_in_stock = portions_in_stock * yield_per_portion
                    suggestion = {
                        "type_id": stock_type_id,
                        "type_name": str(row.get("display_type_name") or ""),
                        "row_index": int(row.get("row_index")),
                        "available_quantity": available_qty,
                        "portion_size": portion_size,
                        "unit_price": str(unit_price),
                        "price_source": (
                            "buyback_override"
                            if row.get("has_buy_price_override")
                            else "buyback_market"
                        ),
                        "location_label": str(row.get("buy_location_label") or ""),
                        "yield_per_portion": int(yield_per_portion),
                        "portions_in_stock": int(portions_in_stock),
                        "estimated_mineral_in_stock": int(est_mineral_in_stock),
                    }
                    ore_suggestions.setdefault(str(mineral_type_id), []).append(
                        suggestion
                    )

            for mineral_id_str in ore_suggestions:
                ore_suggestions[mineral_id_str].sort(
                    key=lambda s: (
                        s["estimated_mineral_in_stock"],
                        s["available_quantity"],
                    ),
                    reverse=True,
                )

    last_sync = getattr(config, "last_stock_sync", None)
    stock_age = (timezone.now() - last_sync).total_seconds() if last_sync else None
    return JsonResponse(
        {
            "enabled": True,
            "items": items,
            "orders": orders,
            "recipients": get_recipient_characters(request.user),
            "last_stock_sync": last_sync.isoformat() if last_sync else "",
            "stock_stale": stock_age is None or stock_age > STOCK_STALE_AFTER_SECONDS,
            "buy_page_url": reverse("indy_hub:material_exchange_buy"),
            "ore_suggestions": ore_suggestions,
        }
    )


@indy_hub_access_required
@indy_hub_permission_required("can_access_indy_hub")
@login_required
@require_http_methods(["POST"])
@tokens_required(scopes="esi-assets.read_corporation_assets.v1")
def submit_production_buyback_order(request, tokens):
    """Create a single-item buyback order from the simulator.

    The same token requirement, validation and draft-order workflow as the
    buyback page apply; the extra guards here are the displayed-price check and
    an idempotency key so a double click or retry cannot create two orders.
    """
    try:
        payload = json.loads(request.body or "{}")
    except json.JSONDecodeError:
        return _error("invalid_json", "Invalid JSON data", 400)
    if not isinstance(payload, dict):
        return _error("invalid_json", "Invalid JSON data", 400)

    config, reason = _get_buy_config_or_reason()
    if config is None:
        return _error(reason, "Buyback orders are not available right now.", 409)

    try:
        type_id = int(payload.get("type_id") or 0)
        quantity = int(payload.get("quantity") or 0)
        row_index = int(payload.get("row_index"))
        expected_price = Decimal(str(payload.get("expected_unit_price")))
    except (TypeError, ValueError, InvalidOperation):
        return _error(
            "invalid_payload",
            "type_id, quantity, row_index and expected_unit_price are required.",
            400,
        )
    if (
        type_id <= 0
        or quantity <= 0
        or row_index < 0
        or not expected_price.is_finite()
        or expected_price <= 0
    ):
        return _error(
            "invalid_payload",
            "type_id, quantity, row_index and expected_unit_price are required.",
            400,
        )

    client_request_id = str(payload.get("client_request_id") or "").strip()
    if not _CLIENT_REQUEST_ID_RE.match(client_request_id):
        return _error("invalid_payload", "client_request_id is required.", 400)
    idempotency_key = f"indy_hub:sim_buyback:{request.user.id}:{client_request_id}"
    if not cache.add(
        idempotency_key, {"state": "in_progress"}, IDEMPOTENCY_TTL_SECONDS
    ):
        previous = cache.get(idempotency_key) or {}
        previous_order_id = int(previous.get("order_id") or 0)
        order = (
            MaterialExchangeBuyOrder.objects.filter(
                id=previous_order_id, buyer=request.user
            )
            .prefetch_related("items")
            .first()
        )
        if order is not None:
            return JsonResponse(
                {"success": True, "duplicate": True, "order": _serialize_order(order)}
            )
        return _error(
            "duplicate_submission", "This order is already being submitted.", 409
        )

    try:
        result = create_buy_order(
            user=request.user,
            config=config,
            submitted_entries=[
                {
                    "type_id": type_id,
                    "quantity": quantity,
                    "row_index": row_index,
                    "blueprint_variant": "",
                    "in_container": False,
                }
            ],
            recipient_character_id=payload.get("recipient_character_id"),
            # The model generates a unique reference when none is given.
            expected_unit_prices={type_id: expected_price},
        )
    except Exception:
        cache.delete(idempotency_key)
        logger.exception(
            "Simulator buyback order failed for user %s type %s",
            request.user.id,
            type_id,
        )
        return _error(
            "order_failed",
            "The order could not be created. Try again from the buyback page.",
            500,
        )

    if not result.ok:
        # Failed attempts may be retried with the same key after a fix.
        cache.delete(idempotency_key)
        first = result.errors[0]
        return _error(
            first.code,
            str(first.message),
            _ERROR_STATUS.get(first.code, 400),
            **{key: value for key, value in first.details.items() if key != "type_id"},
        )

    cache.set(
        idempotency_key,
        {"state": "done", "order_id": int(result.order.id)},
        IDEMPOTENCY_TTL_SECONDS,
    )
    logger.info(
        "Simulator buyback order %s created by user %s (type %s x%s)",
        result.order.id,
        request.user.id,
        type_id,
        quantity,
    )
    order = MaterialExchangeBuyOrder.objects.prefetch_related("items").get(
        id=result.order.id
    )
    return JsonResponse(
        {"success": True, "duplicate": False, "order": _serialize_order(order)}
    )
