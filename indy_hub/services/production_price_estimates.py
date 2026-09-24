"""Derived price estimates for simulator items with no usable market price.

Kept out of the views so the Buy tab's Estimate column can state *where* a
number came from. `get_capital_ship_effective_estimated_price_map()` is a plain
`{**auto, **manual}` merge, so on its own it cannot tell you whether a price was
hand-entered by an admin or derived by the sync task, nor how old it is.
"""

from __future__ import annotations

# Standard Library
from typing import Any, Iterable

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

logger = get_extension_logger(__name__)

SOURCE_MANUAL = "capital_manual"
SOURCE_AUTO = "capital_auto"


def capital_estimates_for_types(type_ids: Iterable[int]) -> dict[int, dict[str, Any]]:
    """Capital price estimates keyed by type ID, with provenance.

    Returns `{type_id: {"price": float, "source": ..., "updated_at": str|None}}`
    for the requested types only. Manual admin overrides win over the synced
    auto estimate, matching `get_capital_ship_effective_estimated_price_map()`.

    Only auto rows carry `updated_at`; manual overrides have no timestamp at
    all, so the caller must not imply freshness for them.
    """
    wanted = {int(value) for value in type_ids if int(value) > 0}
    if not wanted:
        return {}

    try:
        from ..models import MaterialExchangeConfig

        config = MaterialExchangeConfig.objects.first()
    except Exception:
        logger.debug("Capital estimates unavailable", exc_info=True)
        return {}
    if config is None:
        return {}

    try:
        manual_map = config.get_capital_ship_estimated_price_map()
        auto_rows = config.get_capital_ship_auto_estimate_row_map()
    except Exception:
        logger.debug("Unable to read capital estimate maps", exc_info=True)
        return {}

    estimates: dict[int, dict[str, Any]] = {}
    for type_id in sorted(wanted):
        if type_id in manual_map:
            price = manual_map[type_id]
            estimates[type_id] = {
                "price": float(price),
                "source": SOURCE_MANUAL,
                # Manual overrides genuinely have no timestamp.
                "updated_at": None,
            }
            continue
        row = auto_rows.get(type_id)
        if not row:
            continue
        price = row.get("price_isk")
        if price is None:
            continue
        estimates[type_id] = {
            "price": float(price),
            "source": SOURCE_AUTO,
            # An ISO-8601 string, written by the estimate sync task.
            "updated_at": row.get("updated_at") or None,
            "contract_count": row.get("contract_count"),
        }
    return estimates
