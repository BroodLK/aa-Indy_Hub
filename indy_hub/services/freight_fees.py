"""Service for calculating import fees using aa-freight pricing."""

from __future__ import annotations

from math import floor, isfinite
from typing import Optional

from allianceauth.services.hooks import get_extension_logger

logger = get_extension_logger(__name__)

# Jita 4-4 station ID
JITA_4_4_STATION_ID = 60003760


def _get_numeric_attr(obj, *names: str) -> float | None:
    for name in names:
        if not hasattr(obj, name):
            continue
        raw_value = getattr(obj, name)
        try:
            numeric_value = float(raw_value)
        except (TypeError, ValueError):
            continue
        if isfinite(numeric_value):
            return numeric_value
    return None


def _get_positive_limit(obj, *names: str) -> float | None:
    value = _get_numeric_attr(obj, *names)
    if value is None or value <= 0:
        return None
    return value


def _normalize_line_item(raw_item: dict) -> dict | None:
    if not isinstance(raw_item, dict):
        return None

    type_id = int(raw_item.get("type_id") or raw_item.get("typeId") or 0)
    quantity = int(raw_item.get("quantity") or raw_item.get("qty") or 0)
    if type_id <= 0 or quantity <= 0:
        return None

    unit_volume_m3 = max(
        0.0,
        float(raw_item.get("unit_volume_m3") or raw_item.get("unitVolumeM3") or 0.0),
    )
    unit_collateral_isk = max(
        0.0,
        float(
            raw_item.get("unit_collateral_isk")
            or raw_item.get("unitCollateralIsk")
            or raw_item.get("unit_price")
            or raw_item.get("unitPrice")
            or 0.0
        ),
    )

    return {
        "type_id": type_id,
        "type_name": str(raw_item.get("type_name") or raw_item.get("typeName") or f"Type {type_id}"),
        "quantity": quantity,
        "unit_volume_m3": unit_volume_m3,
        "unit_collateral_isk": unit_collateral_isk,
    }


def _get_max_units_for_constraint(remaining_capacity: float | None, unit_value: float) -> int:
    if remaining_capacity is None:
        return 10**12
    if unit_value <= 0:
        return 10**12
    return max(0, floor((remaining_capacity + 1e-9) / unit_value))


def _initialize_contract(index: int) -> dict:
    return {
        "contract_number": index,
        "items": [],
        "volume_m3": 0.0,
        "collateral_isk": 0.0,
        "freight_cost": 0.0,
        "issues": [],
    }


def _allocate_line_items_to_contracts(
    *,
    line_items: list[dict],
    max_volume_m3: float | None,
    max_collateral_isk: float | None,
) -> tuple[list[dict], list[str]]:
    contracts: list[dict] = []
    issues: list[str] = []

    def append_item_to_contract(contract: dict, item: dict, quantity: int) -> None:
        if quantity <= 0:
            return
        contract["items"].append({
            "type_id": item["type_id"],
            "type_name": item["type_name"],
            "quantity": quantity,
            "unit_volume_m3": item["unit_volume_m3"],
            "unit_collateral_isk": item["unit_collateral_isk"],
            "total_volume_m3": item["unit_volume_m3"] * quantity,
            "total_collateral_isk": item["unit_collateral_isk"] * quantity,
        })
        contract["volume_m3"] += item["unit_volume_m3"] * quantity
        contract["collateral_isk"] += item["unit_collateral_isk"] * quantity

    for item in line_items:
        remaining_quantity = int(item["quantity"])
        if remaining_quantity <= 0:
            continue

        single_unit_exceeds_volume = (
            max_volume_m3 is not None
            and item["unit_volume_m3"] > max_volume_m3 + 1e-9
        )
        single_unit_exceeds_collateral = (
            max_collateral_isk is not None
            and item["unit_collateral_isk"] > max_collateral_isk + 1e-9
        )
        if single_unit_exceeds_volume or single_unit_exceeds_collateral:
            limits = []
            if single_unit_exceeds_volume:
                limits.append("volume")
            if single_unit_exceeds_collateral:
                limits.append("collateral")
            issues.append(
                f'{item["type_name"]} exceeds the per-contract {" and ".join(limits)} maximum and cannot be split cleanly.'
            )

        while remaining_quantity > 0:
            if not contracts:
                contracts.append(_initialize_contract(1))

            contract = contracts[-1]
            remaining_volume_capacity = (
                None if max_volume_m3 is None else max(0.0, max_volume_m3 - contract["volume_m3"])
            )
            remaining_collateral_capacity = (
                None
                if max_collateral_isk is None
                else max(0.0, max_collateral_isk - contract["collateral_isk"])
            )

            max_units_by_volume = _get_max_units_for_constraint(
                remaining_volume_capacity,
                item["unit_volume_m3"],
            )
            max_units_by_collateral = _get_max_units_for_constraint(
                remaining_collateral_capacity,
                item["unit_collateral_isk"],
            )
            quantity_to_add = min(remaining_quantity, max_units_by_volume, max_units_by_collateral)

            if quantity_to_add <= 0:
                if contract["items"]:
                    contracts.append(_initialize_contract(len(contracts) + 1))
                    continue
                quantity_to_add = 1

            append_item_to_contract(contract, item, quantity_to_add)
            remaining_quantity -= quantity_to_add

    return contracts, issues


def calculate_import_fees(
    *,
    destination_location_id: int | None = None,
    pricing_id: int | None = None,
    total_volume_m3: float,
    total_collateral_isk: float,
    line_items: list[dict] | None = None,
) -> Optional[dict]:
    """
    Calculate import fees from Jita 4-4 to destination using aa-freight pricing.

    Args:
        destination_location_id: The destination location (station or structure) ID
        pricing_id: Explicit aa-freight Pricing ID to use
        total_volume_m3: Total volume in m3
        total_collateral_isk: Total collateral value in ISK
        line_items: Optional item rows used to split the shipment into contracts

    Returns:
        Dictionary with:
        - 'freight_cost': Calculated freight cost in ISK
        - 'route_name': Name of the route
        - 'pricing_id': ID of the pricing used
        - 'issues': List of issues if any (volume/collateral limits exceeded)
        - 'contracts': One-way contract breakdown respecting pricing limits

        Returns None if no route exists from Jita 4-4 to destination.
    """
    try:
        from freight.models import Pricing
    except ImportError:
        logger.warning("aa-freight not installed, cannot calculate import fees")
        return None

    pricing = None

    if pricing_id:
        pricing = Pricing.objects.filter(
            pk=pricing_id,
            is_active=True,
        ).first()
    elif destination_location_id:
        # First try direct route (Jita -> Destination)
        pricing = Pricing.objects.filter(
            start_location_id=JITA_4_4_STATION_ID,
            end_location_id=destination_location_id,
            is_active=True,
        ).first()

        # If not found, try bidirectional route (Destination -> Jita)
        if not pricing:
            pricing = Pricing.objects.filter(
                start_location_id=destination_location_id,
                end_location_id=JITA_4_4_STATION_ID,
                is_active=True,
                is_bidirectional=True,
            ).first()

    if not pricing:
        if pricing_id:
            logger.debug("No active freight pricing found for pricing_id=%s", pricing_id)
        else:
            logger.debug(
                f"No freight route found from Jita 4-4 ({JITA_4_4_STATION_ID}) "
                f"to location {destination_location_id}"
            )
        return None

    max_volume_m3 = _get_positive_limit(pricing, "volume_max", "max_volume")
    max_collateral_isk = _get_positive_limit(pricing, "collateral_max", "max_collateral")
    normalized_items = [
        item for item in (
            _normalize_line_item(raw_item)
            for raw_item in (line_items or [])
        )
        if item
    ]

    if normalized_items:
        contracts, allocation_issues = _allocate_line_items_to_contracts(
            line_items=normalized_items,
            max_volume_m3=max_volume_m3,
            max_collateral_isk=max_collateral_isk,
        )
    else:
        contracts = [_initialize_contract(1)]
        contracts[0]["volume_m3"] = max(0.0, float(total_volume_m3 or 0.0))
        contracts[0]["collateral_isk"] = max(0.0, float(total_collateral_isk or 0.0))
        allocation_issues = []

    total_freight_cost = 0.0
    aggregated_issues = list(allocation_issues)

    for contract in contracts:
        contract_freight_cost = pricing.get_calculated_price(
            volume=contract["volume_m3"],
            collateral=contract["collateral_isk"],
        )
        contract["freight_cost"] = float(contract_freight_cost)
        total_freight_cost += contract["freight_cost"]

        contract_issues = pricing.get_contract_price_check_issues(
            volume=contract["volume_m3"],
            collateral=contract["collateral_isk"],
            reward=contract_freight_cost,
        )
        contract["issues"] = contract_issues if contract_issues else []
        aggregated_issues.extend(contract["issues"])

    unique_issues: list[str] = []
    for issue in aggregated_issues:
        issue_text = str(issue).strip()
        if issue_text and issue_text not in unique_issues:
            unique_issues.append(issue_text)

    return {
        'freight_cost': float(total_freight_cost),
        'route_name': pricing.name,
        'pricing_id': pricing.pk,
        'issues': unique_issues,
        'contract_count': len(contracts),
        'contracts': contracts,
        'max_volume_m3': max_volume_m3,
        'max_collateral_isk': max_collateral_isk,
        'total_volume_m3': max(0.0, float(total_volume_m3 or 0.0)),
        'total_collateral_isk': max(0.0, float(total_collateral_isk or 0.0)),
    }


def get_available_routes() -> list[dict]:
    """
    Get all available active freight routes from aa-freight.

    Returns:
        List of dicts with route metadata for each active pricing entry.
    """
    try:
        from freight.models import Pricing
    except ImportError:
        logger.warning("aa-freight not installed, cannot get routes")
        return []

    routes = []

    for pricing in Pricing.objects.filter(
        is_active=True,
    ).select_related('start_location', 'end_location'):
        start_name = str(getattr(pricing.start_location, "name", "") or "")
        end_name = str(getattr(pricing.end_location, "name", "") or "")
        direction = "<->" if getattr(pricing, "is_bidirectional", False) else "->"
        routes.append({
            'pricing_id': pricing.pk,
            'route_name': pricing.name,
            'start_location_id': pricing.start_location_id,
            'start_location_name': start_name,
            'end_location_id': pricing.end_location_id,
            'end_location_name': end_name,
            'destination_id': pricing.end_location_id,
            'destination_name': end_name,
            'is_bidirectional': bool(getattr(pricing, "is_bidirectional", False)),
            'route_label': f"{start_name} {direction} {end_name}".strip(),
        })

    routes.sort(
        key=lambda route: (
            str(route.get("start_location_name") or "").lower(),
            str(route.get("end_location_name") or "").lower(),
            str(route.get("route_name") or "").lower(),
            int(route.get("pricing_id") or 0),
        )
    )
    return routes


def get_available_routes_from_jita() -> list[dict]:
    """
    Backward-compatible alias for older callers.
    """
    return get_available_routes()
