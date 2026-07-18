"""Helpers for consistent Buyback pricing.

The goal is to keep pricing logic identical across:
- MaterialExchangeStock computed properties
- Buyback buy/sell views (when using live Fuzzwork prices)

Prices are based on Jita buy/sell plus a configurable markup, with an optional
"bounds" mode that clamps prices inside the Jita buy/sell spread.
"""

from __future__ import annotations

# Standard Library
from decimal import Decimal


def _to_decimal(value) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value or 0))
    except Exception:
        return Decimal("0")


def apply_markup_with_jita_bounds(
    *,
    jita_buy: Decimal,
    jita_sell: Decimal,
    base_choice: str,
    percent: Decimal,
    enforce_bounds: bool,
) -> Decimal:
    """Return price after applying markup and optional Jita buy/sell bounds.

    Rules (when enforce_bounds=True):
    - If base is Jita Sell and percent is negative: don't go below Jita Buy.
    - If base is Jita Buy and percent is positive: don't go above Jita Sell.

    This keeps computed prices inside the buy/sell spread.
    """

    jita_buy_d = _to_decimal(jita_buy)
    jita_sell_d = _to_decimal(jita_sell)
    percent_d = _to_decimal(percent)

    base = jita_sell_d if base_choice == "sell" else jita_buy_d
    price = base * (Decimal("1") + (percent_d / Decimal("100")))

    if enforce_bounds:
        if base_choice == "sell" and percent_d < 0 and jita_buy_d:
            price = max(price, jita_buy_d)
        if base_choice == "buy" and percent_d > 0 and jita_sell_d:
            price = min(price, jita_sell_d)

    return price


def compute_sell_price_to_member(*, config, jita_buy: Decimal, jita_sell: Decimal) -> Decimal:
    """Price when member buys FROM hub (uses config.buy_markup_*)."""

    return apply_markup_with_jita_bounds(
        jita_buy=jita_buy,
        jita_sell=jita_sell,
        base_choice=getattr(config, "buy_markup_base", "buy"),
        percent=getattr(config, "buy_markup_percent", Decimal("0")),
        enforce_bounds=bool(getattr(config, "enforce_jita_price_bounds", False)),
    )


def compute_buy_price_from_member(*, config, jita_buy: Decimal, jita_sell: Decimal) -> Decimal:
    """Price when member sells TO hub (uses config.sell_markup_*)."""

    return apply_markup_with_jita_bounds(
        jita_buy=jita_buy,
        jita_sell=jita_sell,
        base_choice=getattr(config, "sell_markup_base", "buy"),
        percent=getattr(config, "sell_markup_percent", Decimal("0")),
        enforce_bounds=bool(getattr(config, "enforce_jita_price_bounds", False)),
    )


def compute_refined_ore_price(
    *,
    reprocessing_outputs: dict[int, int],
    portion_size: int,
    refine_rate_percent: Decimal,
    mineral_effective_prices: dict[int, Decimal],
) -> Decimal | None:
    """Return per-ore-unit refined price from precomputed per-mineral hub prices.

    Each mineral's effective hub price should already reflect any relevant
    override (or the hub's markup applied to Jita) — this function does not add
    any further markup on top. The result is:

        sum(qty_per_portion * refine_rate / 100 * mineral_effective_price) / portion_size

    Returns None when the inputs cannot produce a usable price:
    - portion_size <= 0 or no outputs
    - refine rate <= 0
    - any output mineral has no effective price in ``mineral_effective_prices``
    - the resulting price sums to zero
    """

    try:
        portion_size_int = int(portion_size or 0)
    except (TypeError, ValueError):
        return None
    if portion_size_int <= 0 or not reprocessing_outputs:
        return None

    ratio = _to_decimal(refine_rate_percent) / Decimal("100")
    if ratio <= 0:
        return None

    total = Decimal("0")
    for mineral_id, qty_per_portion in reprocessing_outputs.items():
        try:
            mineral_key = int(mineral_id)
            qty = Decimal(int(qty_per_portion or 0))
        except (TypeError, ValueError):
            return None
        if qty <= 0:
            continue
        if mineral_key not in mineral_effective_prices:
            return None
        effective_price = _to_decimal(mineral_effective_prices.get(mineral_key))
        if effective_price <= 0:
            return None
        yield_per_ore_unit = (qty * ratio) / Decimal(portion_size_int)
        total += yield_per_ore_unit * effective_price

    if total <= 0:
        return None
    return total
