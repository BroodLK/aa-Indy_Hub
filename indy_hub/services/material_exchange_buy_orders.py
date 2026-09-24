"""Buy-order creation shared by the buyback page and the production simulator.

The validation here used to live inline in ``material_exchange_buy``. It is the
single place that turns submitted stock-row quantities into a
``MaterialExchangeBuyOrder``, so both callers get the same locking,
reservation, stock-snapshot, pricing and single-location rules.

The stock-snapshot helpers still live in ``views.material_exchange``; they are
imported lazily to avoid an import cycle with that module.
"""

from __future__ import annotations

# Standard Library
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, Decimal

# Django
from django.db import transaction
from django.utils.translation import gettext_lazy as _

# AA Example App
from indy_hub.models import (
    MaterialExchangeBuyOrder,
    MaterialExchangeBuyOrderItem,
    MaterialExchangeConfig,
)

# Order statuses that mean the simulator's pending quantity will never arrive.
TERMINAL_FAILED_BUY_STATUSES = frozenset(
    {
        MaterialExchangeBuyOrder.Status.REJECTED,
        MaterialExchangeBuyOrder.Status.CANCELLED,
    }
)


@dataclass
class BuyOrderError:
    code: str
    message: str
    details: dict = field(default_factory=dict)


@dataclass
class BuyOrderResult:
    order: MaterialExchangeBuyOrder | None = None
    errors: list[BuyOrderError] = field(default_factory=list)
    total_cost: Decimal = Decimal("0")
    rounded_total_cost: Decimal = Decimal("0")
    item_count: int = 0

    @property
    def ok(self) -> bool:
        return self.order is not None and not self.errors


def get_recipient_characters(user) -> list[dict]:
    """Characters the user may name as the contract recipient."""
    # Alliance Auth
    from allianceauth.authentication.models import CharacterOwnership

    recipients = []
    for ownership in CharacterOwnership.objects.select_related("character").filter(
        user=user
    ):
        character = ownership.character
        if character and getattr(character, "character_id", None):
            recipients.append(
                {
                    "id": int(character.character_id),
                    "name": str(character.character_name or character.character_id),
                }
            )
    recipients.sort(key=lambda row: row["name"].lower())
    return recipients


def create_buy_order(
    *,
    user,
    config: MaterialExchangeConfig,
    submitted_entries: list[dict],
    recipient_character_id,
    order_reference: str = "",
    snapshot_token: str = "",
    buy_name_map: dict[int, str] | None = None,
    recipient_characters: list[dict] | None = None,
    expected_unit_prices: dict[int, Decimal] | None = None,
) -> BuyOrderResult:
    """Validate submitted rows and create ONE draft order with all items.

    ``expected_unit_prices`` (type_id -> price) lets a caller that showed the
    user a price refuse to create the order if the price moved since; the
    buyback page itself does not pass it, as it re-renders prices on submit.
    On any error nothing is created, so an order never has an unexpected total.
    """
    # AA Example App
    from indy_hub.utils.eve import get_type_name
    from indy_hub.views.material_exchange import (
        _get_buy_location_display_context,
        _get_buy_stock_snapshot_for_submission,
        _get_reserved_buy_quantities,
        _normalize_source_structure_ids,
        _resolve_selected_buy_source_location_from_groups,
        _selected_buy_source_groups_share_source_location,
    )

    result = BuyOrderResult()
    no_quantity_error = BuyOrderError(
        "no_items",
        _("Please enter a quantity greater than 0 for at least one item."),
    )
    if not submitted_entries:
        result.errors.append(no_quantity_error)
        return result

    if buy_name_map is None:
        buy_name_map, _label = _get_buy_location_display_context(config)
    if recipient_characters is None:
        recipient_characters = get_recipient_characters(user)

    submitted_type_ids = {
        int(submitted_entry.get("type_id") or 0)
        for submitted_entry in submitted_entries
        if int(submitted_entry.get("type_id") or 0) > 0
    }

    with transaction.atomic():
        recipient_id_raw = str(recipient_character_id or "").strip()
        recipient = next(
            (row for row in recipient_characters if str(row["id"]) == recipient_id_raw),
            None,
        )
        if not recipient:
            result.errors.append(
                BuyOrderError(
                    "invalid_recipient",
                    _("Please select a validated character to receive the contract."),
                )
            )
            return result
        locked_stock_items = list(
            config.stock_items.select_for_update().filter(
                type_id__in=submitted_type_ids, quantity__gt=0
            )
        )
        locked_reserved_quantities = (
            _get_reserved_buy_quantities(
                config=config,
                type_ids=submitted_type_ids,
                stock_synced_at=config.last_stock_sync,
            )
            if submitted_type_ids
            else {}
        )
        current_available_by_type: dict[int, int] = {}
        current_reserved_by_type: dict[int, int] = {}
        for stock_item in locked_stock_items:
            type_id = int(stock_item.type_id)
            reserved_qty = int(locked_reserved_quantities.get(type_id, 0) or 0)
            current_reserved_by_type[type_id] = reserved_qty
            # A type can have multiple stock rows (for example, one per
            # source structure).  Reservations are type-wide, so sum all
            # stacks first and subtract the reservation once.
            current_available_by_type[type_id] = current_available_by_type.get(
                type_id, 0
            ) + int(stock_item.quantity)

        for type_id, reserved_qty in current_reserved_by_type.items():
            current_available_by_type[type_id] = max(
                current_available_by_type.get(type_id, 0) - reserved_qty,
                0,
            )

        buy_stock_snapshot = _get_buy_stock_snapshot_for_submission(
            config=config,
            submitted_type_ids=submitted_type_ids,
            reserved_quantities=locked_reserved_quantities,
            submission_snapshot_token=snapshot_token,
            user_id=int(user.id),
        )
        stock_row_by_index = buy_stock_snapshot["stock_row_by_index"]

        items_to_create = []
        total_cost = Decimal("0")
        selected_source_structure_groups: list[list[int]] = []
        row_refresh_error = _(
            "One or more selected items no longer match the current stock view. Refresh the page and try again."
        )
        for submitted_entry in submitted_entries:
            try:
                type_id = int(submitted_entry.get("type_id") or 0)
                qty = int(submitted_entry.get("quantity") or 0)
                # Row indices start at 0; ``or -1`` used to discard that row,
                # so the first stock row could never be ordered.
                raw_row_index = submitted_entry.get("row_index")
                row_index = int(raw_row_index) if raw_row_index is not None else -1
            except (TypeError, ValueError):
                continue
            if type_id <= 0 or qty <= 0 or row_index < 0:
                continue

            row_data = stock_row_by_index.get(int(row_index))
            if not row_data:
                result.errors.append(
                    BuyOrderError("stale_row", row_refresh_error, {"type_id": type_id})
                )
                continue

            row_type_id = int(row_data.get("type_id") or 0)
            requested_variant = (
                str(submitted_entry.get("blueprint_variant") or "").strip().lower()
            )
            blueprint_variant = (
                requested_variant if requested_variant in {"bpc", "bpo"} else ""
            )
            row_variant = str(row_data.get("blueprint_variant") or "").strip().lower()
            row_in_container = bool(str(row_data.get("container_path") or "").strip())
            requested_in_container = bool(submitted_entry.get("in_container"))
            if row_type_id != int(type_id):
                result.errors.append(
                    BuyOrderError("stale_row", row_refresh_error, {"type_id": type_id})
                )
                continue
            if (blueprint_variant or row_variant) and blueprint_variant != row_variant:
                result.errors.append(
                    BuyOrderError("stale_row", row_refresh_error, {"type_id": type_id})
                )
                continue
            if requested_in_container != row_in_container:
                result.errors.append(
                    BuyOrderError("stale_row", row_refresh_error, {"type_id": type_id})
                )
                continue

            display_type_name = str(
                row_data.get("display_type_name") or get_type_name(type_id)
            )
            snapshot_available_qty = int(row_data.get("available_quantity") or 0)
            current_available_qty = int(current_available_by_type.get(type_id, 0) or 0)
            available_qty = min(snapshot_available_qty, current_available_qty)
            reserved_qty = int(
                current_reserved_by_type.get(
                    type_id, row_data.get("reserved_quantity") or 0
                )
                or 0
            )
            if qty > available_qty:
                result.errors.append(
                    BuyOrderError(
                        "insufficient_stock",
                        _(
                            f"Insufficient unlocked stock for {display_type_name}. "
                            f"Available now: {available_qty:,}, reserved in open orders: {reserved_qty:,}, requested: {qty:,}"
                        ),
                        {"type_id": type_id, "available": int(available_qty)},
                    )
                )
                continue

            if row_variant == "bpc":
                unit_price = Decimal("0")
            else:
                unit_price = Decimal(row_data.get("display_sell_price_to_member") or 0)
                if unit_price <= 0:
                    result.errors.append(
                        BuyOrderError(
                            "no_price",
                            _(f"{display_type_name} has no valid market price."),
                            {"type_id": type_id},
                        )
                    )
                    continue
            if expected_unit_prices is not None and type_id in expected_unit_prices:
                expected = Decimal(str(expected_unit_prices[type_id]))
                if unit_price.quantize(Decimal("0.01")) != expected.quantize(
                    Decimal("0.01")
                ):
                    result.errors.append(
                        BuyOrderError(
                            "price_changed",
                            _(
                                f"The buyback price for {display_type_name} changed. Review the new price and confirm again."
                            ),
                            {"type_id": type_id, "current_price": str(unit_price)},
                        )
                    )
                    continue
            total_price = unit_price * qty
            total_cost += total_price
            row_source_structure_ids = _normalize_source_structure_ids(
                row_data.get("source_structure_ids") or []
            )
            selected_source_structure_groups.append(row_source_structure_ids)

            items_to_create.append(
                {
                    "type_id": type_id,
                    "type_name": display_type_name,
                    "quantity": qty,
                    "unit_price": unit_price,
                    "total_price": total_price,
                    "stock_available_at_creation": int(available_qty),
                }
            )

        if result.errors:
            # Prevent creating a partial order with an unexpected (lower) total.
            return result

        if not items_to_create:
            result.errors.append(no_quantity_error)
            return result

        if len(
            selected_source_structure_groups
        ) > 1 and not _selected_buy_source_groups_share_source_location(
            selected_source_structure_groups
        ):
            result.errors.append(
                BuyOrderError(
                    "mixed_locations",
                    _(
                        "Selected items must come from one buy location. Deselect items from other stations and try again."
                    ),
                )
            )
            return result

        selected_buy_location_id, selected_buy_location_name = (
            _resolve_selected_buy_source_location_from_groups(
                selected_source_structure_groups,
                buy_name_map=buy_name_map,
            )
        )

        client_order_ref = str(order_reference or "").strip()

        order = MaterialExchangeBuyOrder.objects.create(
            config=config,
            buyer=user,
            status=MaterialExchangeBuyOrder.Status.DRAFT,
            order_reference=client_order_ref if client_order_ref else None,
            source_location_id=selected_buy_location_id,
            source_location_name=selected_buy_location_name,
            recipient_character_id=recipient["id"],
            recipient_character_name=recipient["name"],
        )

        for item_data in items_to_create:
            MaterialExchangeBuyOrderItem.objects.create(order=order, **item_data)

        rounded_total_cost = total_cost.quantize(Decimal("1"), rounding=ROUND_CEILING)
        order.rounded_total_price = rounded_total_cost
        order.save(update_fields=["rounded_total_price", "updated_at"])

    result.order = order
    result.total_cost = total_cost
    result.rounded_total_cost = rounded_total_cost
    result.item_count = len(items_to_create)
    return result
