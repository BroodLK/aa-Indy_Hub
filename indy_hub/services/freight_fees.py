"""Service for calculating import fees using aa-freight pricing."""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from allianceauth.services.hooks import get_extension_logger

logger = get_extension_logger(__name__)

# Jita 4-4 station ID
JITA_4_4_STATION_ID = 60003760


def calculate_import_fees(
    *,
    destination_location_id: int,
    total_volume_m3: float,
    total_collateral_isk: float,
) -> Optional[dict]:
    """
    Calculate import fees from Jita 4-4 to destination using aa-freight pricing.

    Args:
        destination_location_id: The destination location (station or structure) ID
        total_volume_m3: Total volume in m3
        total_collateral_isk: Total collateral value in ISK

    Returns:
        Dictionary with:
        - 'freight_cost': Calculated freight cost in ISK
        - 'route_name': Name of the route
        - 'pricing_id': ID of the pricing used
        - 'issues': List of issues if any (volume/collateral limits exceeded)

        Returns None if no route exists from Jita 4-4 to destination.
    """
    try:
        from freight.models import Pricing, Location
    except ImportError:
        logger.warning("aa-freight not installed, cannot calculate import fees")
        return None

    # Try to find pricing from Jita 4-4 to destination
    pricing = None

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
        logger.debug(
            f"No freight route found from Jita 4-4 ({JITA_4_4_STATION_ID}) "
            f"to location {destination_location_id}"
        )
        return None

    # Calculate freight cost
    freight_cost = pricing.get_calculated_price(
        volume=total_volume_m3,
        collateral=total_collateral_isk,
    )

    # Check for any issues (volume/collateral limits)
    issues = pricing.get_contract_price_check_issues(
        volume=total_volume_m3,
        collateral=total_collateral_isk,
        reward=freight_cost,
    )

    return {
        'freight_cost': float(freight_cost),
        'route_name': pricing.name,
        'pricing_id': pricing.pk,
        'issues': issues if issues else [],
    }


def get_available_routes_from_jita() -> list[dict]:
    """
    Get all available active freight routes from Jita 4-4.

    Returns:
        List of dicts with 'pricing_id', 'route_name', 'destination_id', 'destination_name'
    """
    try:
        from freight.models import Pricing
    except ImportError:
        logger.warning("aa-freight not installed, cannot get routes")
        return []

    routes = []

    # Get all active pricings starting from Jita
    for pricing in Pricing.objects.filter(
        start_location_id=JITA_4_4_STATION_ID,
        is_active=True,
    ).select_related('start_location', 'end_location'):
        routes.append({
            'pricing_id': pricing.pk,
            'route_name': pricing.name,
            'destination_id': pricing.end_location_id,
            'destination_name': pricing.end_location.name,
        })

    # Get all bidirectional pricings ending at Jita
    for pricing in Pricing.objects.filter(
        end_location_id=JITA_4_4_STATION_ID,
        is_active=True,
        is_bidirectional=True,
    ).select_related('start_location', 'end_location'):
        routes.append({
            'pricing_id': pricing.pk,
            'route_name': pricing.name,
            'destination_id': pricing.start_location_id,
            'destination_name': pricing.start_location.name,
        })

    return routes
