"""Automated capital order estimates from craft buy cost."""

from __future__ import annotations

# Standard Library
from collections import defaultdict
from decimal import Decimal, ROUND_CEILING
from math import ceil
from typing import Any

# Django
from django.db import connection, transaction
from django.utils import timezone

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

# Local
from indy_hub.models import MaterialExchangeConfig
from indy_hub.services.fuzzwork import fetch_fuzzwork_prices
from indy_hub.services.public_contracts_store import get_public_jita_bpc_offers

logger = get_extension_logger(__name__)

_AUTO_ESTIMATE_MARKUP_MULTIPLIER = Decimal("1.10")
_AUTO_ESTIMATE_CEILING_STEP = Decimal("100000000")
_DEFAULT_BLUEPRINT_ME = 0
_DEFAULT_ENVIRONMENT_MATERIAL_MULTIPLIER = 1.0


def _to_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _format_price(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.01")), "f")


def _ceil_price_to_step(value: Decimal, *, step: Decimal) -> Decimal:
    normalized_value = _to_decimal(value)
    normalized_step = _to_decimal(step)
    if normalized_step <= 0 or normalized_value <= 0:
        return normalized_value.quantize(Decimal("0.01"))
    return (
        (normalized_value / normalized_step).to_integral_value(rounding=ROUND_CEILING)
        * normalized_step
    ).quantize(Decimal("0.01"))


def _requires_blueprint_copy_cost_for_capital_hull(
    hull_type_id: int,
    *,
    copy_cost_required_cache: dict[int, bool],
) -> bool:
    clean_hull_type_id = int(hull_type_id or 0)
    if clean_hull_type_id <= 0:
        return False

    cached = copy_cost_required_cache.get(clean_hull_type_id)
    if cached is not None:
        return bool(cached)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COALESCE(t.name, ''),
                COALESCE(g.name, ''),
                COALESCE(t.meta_group_id_raw, 0)
            FROM eve_sde_itemtype t
            LEFT JOIN eve_sde_itemgroup g
              ON g.id = t.group_id
            WHERE t.id = %s
            LIMIT 1
            """,
            [clean_hull_type_id],
        )
        row = cursor.fetchone()

    if not row:
        copy_cost_required_cache[clean_hull_type_id] = False
        return False

    type_name = str(row[0] or "").strip().lower()
    group_name = str(row[1] or "").strip().lower()
    meta_group_id = int(row[2] or 0)

    requires_copy_cost = False
    if meta_group_id not in {0, 1}:
        requires_copy_cost = True
    elif "lancer" in group_name:
        requires_copy_cost = True
    elif "navy issue" in type_name or "fleet issue" in type_name:
        requires_copy_cost = True

    copy_cost_required_cache[clean_hull_type_id] = requires_copy_cost
    return requires_copy_cost


def _get_blueprint_copy_cost_per_unit(
    blueprint_id: int,
    *,
    blueprint_output_qty_cache: dict[int, int],
    blueprint_copy_cost_cache: dict[int, Decimal | None],
) -> Decimal | None:
    clean_blueprint_id = int(blueprint_id or 0)
    if clean_blueprint_id <= 0:
        return None

    if clean_blueprint_id in blueprint_copy_cost_cache:
        return blueprint_copy_cost_cache[clean_blueprint_id]

    offers = get_public_jita_bpc_offers(
        blueprint_type_id=clean_blueprint_id,
        max_offers=25,
    )
    positive_prices: list[Decimal] = []
    for offer in offers:
        price_per_run = _to_decimal(offer.get("price_per_run"))
        if price_per_run > 0:
            positive_prices.append(price_per_run)
    positive_prices.sort()
    if not positive_prices:
        blueprint_copy_cost_cache[clean_blueprint_id] = None
        return None

    output_qty = _get_blueprint_output_qty(
        clean_blueprint_id,
        blueprint_output_qty_cache=blueprint_output_qty_cache,
    )
    if output_qty <= 0:
        output_qty = 1

    copy_cost = (positive_prices[0] / Decimal(output_qty)).quantize(Decimal("0.01"))
    if copy_cost <= 0:
        blueprint_copy_cost_cache[clean_blueprint_id] = None
        return None

    blueprint_copy_cost_cache[clean_blueprint_id] = copy_cost
    return copy_cost


def _load_capital_ship_options(config: MaterialExchangeConfig) -> list[dict[str, object]]:
    from indy_hub.views.capital_ship_orders import _load_capital_ship_options as _loader

    return list(_loader(config=config))


def _prime_blueprint_cache_for_products(
    product_type_ids: set[int],
    *,
    blueprint_by_product_cache: dict[int, int | None],
    blueprint_output_qty_cache: dict[int, int],
) -> None:
    if not product_type_ids:
        return

    normalized_type_ids = sorted({int(type_id) for type_id in product_type_ids if int(type_id) > 0})
    if not normalized_type_ids:
        return

    placeholders = ",".join(["%s"] * len(normalized_type_ids))
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT product_eve_type_id, eve_type_id, quantity
            FROM indy_hub_sdeindustryactivityproduct
            WHERE activity_id IN (1, 11)
              AND product_eve_type_id IN ({placeholders})
            ORDER BY product_eve_type_id ASC, eve_type_id ASC
            """,
            normalized_type_ids,
        )
        for product_type_id, blueprint_id, output_qty in cursor.fetchall():
            clean_product_type_id = int(product_type_id or 0)
            clean_blueprint_id = int(blueprint_id or 0)
            if clean_product_type_id <= 0 or clean_blueprint_id <= 0:
                continue
            blueprint_by_product_cache.setdefault(clean_product_type_id, clean_blueprint_id)
            blueprint_output_qty_cache.setdefault(
                clean_blueprint_id,
                max(1, int(output_qty or 1)),
            )

    for product_type_id in normalized_type_ids:
        blueprint_by_product_cache.setdefault(int(product_type_id), None)


def _get_blueprint_for_product(
    product_type_id: int,
    *,
    blueprint_by_product_cache: dict[int, int | None],
    blueprint_output_qty_cache: dict[int, int],
) -> int | None:
    clean_product_type_id = int(product_type_id or 0)
    if clean_product_type_id <= 0:
        return None

    cached = blueprint_by_product_cache.get(clean_product_type_id, None)
    if clean_product_type_id in blueprint_by_product_cache:
        return cached

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT eve_type_id, quantity
            FROM indy_hub_sdeindustryactivityproduct
            WHERE product_eve_type_id = %s
              AND activity_id IN (1, 11)
            ORDER BY eve_type_id ASC
            LIMIT 1
            """,
            [clean_product_type_id],
        )
        row = cursor.fetchone()

    if not row:
        blueprint_by_product_cache[clean_product_type_id] = None
        return None

    blueprint_id = int(row[0] or 0)
    if blueprint_id <= 0:
        blueprint_by_product_cache[clean_product_type_id] = None
        return None

    blueprint_by_product_cache[clean_product_type_id] = blueprint_id
    blueprint_output_qty_cache.setdefault(blueprint_id, max(1, int(row[1] or 1)))
    return blueprint_id


def _get_blueprint_output_qty(
    blueprint_id: int,
    *,
    blueprint_output_qty_cache: dict[int, int],
) -> int:
    clean_blueprint_id = int(blueprint_id or 0)
    if clean_blueprint_id <= 0:
        return 1

    cached = blueprint_output_qty_cache.get(clean_blueprint_id)
    if cached is not None:
        return max(1, int(cached))

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT quantity
            FROM indy_hub_sdeindustryactivityproduct
            WHERE eve_type_id = %s
              AND activity_id IN (1, 11)
            LIMIT 1
            """,
            [clean_blueprint_id],
        )
        row = cursor.fetchone()

    output_qty = max(1, int(row[0] or 1)) if row else 1
    blueprint_output_qty_cache[clean_blueprint_id] = output_qty
    return output_qty


def _get_blueprint_material_rows(
    blueprint_id: int,
    *,
    blueprint_material_cache: dict[int, list[tuple[int, int]]],
) -> list[tuple[int, int]]:
    clean_blueprint_id = int(blueprint_id or 0)
    if clean_blueprint_id <= 0:
        return []

    cached = blueprint_material_cache.get(clean_blueprint_id)
    if cached is not None:
        return list(cached)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT material_eve_type_id, quantity
            FROM indy_hub_sdeindustryactivitymaterial
            WHERE eve_type_id = %s
              AND activity_id IN (1, 11)
            """,
            [clean_blueprint_id],
        )
        rows = [
            (int(material_type_id or 0), int(quantity or 0))
            for material_type_id, quantity in cursor.fetchall()
            if int(material_type_id or 0) > 0 and int(quantity or 0) > 0
        ]

    blueprint_material_cache[clean_blueprint_id] = rows
    return list(rows)


def _compute_material_quantity(
    *,
    base_quantity_per_run: int,
    runs: int,
    blueprint_me: int,
    environment_material_multiplier: float,
) -> int:
    base_total_qty = int(base_quantity_per_run or 0) * int(runs or 0)
    if base_total_qty <= 0:
        return 0

    me_multiplier = max(0.0, (100 - int(blueprint_me or 0)) / 100.0)
    total_multiplier = me_multiplier * float(environment_material_multiplier or 0.0)
    if total_multiplier <= 0:
        return 0

    return max(0, int(ceil(base_total_qty * total_multiplier)))


def _collect_leaf_buy_requirements(
    blueprint_id: int,
    runs: int,
    *,
    blueprint_me: int,
    environment_material_multiplier: float,
    blueprint_by_product_cache: dict[int, int | None],
    blueprint_material_cache: dict[int, list[tuple[int, int]]],
    blueprint_output_qty_cache: dict[int, int],
    material_totals: dict[int, int],
    seen: set[int] | None = None,
) -> None:
    clean_blueprint_id = int(blueprint_id or 0)
    clean_runs = int(runs or 0)
    if clean_blueprint_id <= 0 or clean_runs <= 0:
        return

    path = set(seen or set())
    if clean_blueprint_id in path:
        return
    path.add(clean_blueprint_id)

    for material_type_id, base_quantity_per_run in _get_blueprint_material_rows(
        clean_blueprint_id,
        blueprint_material_cache=blueprint_material_cache,
    ):
        required_qty = _compute_material_quantity(
            base_quantity_per_run=base_quantity_per_run,
            runs=clean_runs,
            blueprint_me=blueprint_me,
            environment_material_multiplier=environment_material_multiplier,
        )
        if required_qty <= 0:
            continue

        sub_blueprint_id = _get_blueprint_for_product(
            material_type_id,
            blueprint_by_product_cache=blueprint_by_product_cache,
            blueprint_output_qty_cache=blueprint_output_qty_cache,
        )
        if sub_blueprint_id and sub_blueprint_id not in path:
            output_qty = _get_blueprint_output_qty(
                sub_blueprint_id,
                blueprint_output_qty_cache=blueprint_output_qty_cache,
            )
            sub_material_rows = _get_blueprint_material_rows(
                sub_blueprint_id,
                blueprint_material_cache=blueprint_material_cache,
            )
            if output_qty > 0 and sub_material_rows:
                cycles = max(1, int(ceil(required_qty / output_qty)))
                _collect_leaf_buy_requirements(
                    sub_blueprint_id,
                    cycles,
                    blueprint_me=_DEFAULT_BLUEPRINT_ME,
                    environment_material_multiplier=environment_material_multiplier,
                    blueprint_by_product_cache=blueprint_by_product_cache,
                    blueprint_material_cache=blueprint_material_cache,
                    blueprint_output_qty_cache=blueprint_output_qty_cache,
                    material_totals=material_totals,
                    seen=path,
                )
                continue

        material_totals[int(material_type_id)] = material_totals.get(int(material_type_id), 0) + int(
            required_qty
        )


def _build_capital_buy_cost_map(
    allowed_hull_type_ids: set[int],
) -> tuple[dict[int, Decimal], dict[str, int]]:
    stats = {
        "types_requested": len({int(type_id) for type_id in allowed_hull_type_ids if int(type_id) > 0}),
        "blueprints_found": 0,
        "requirements_built": 0,
        "material_types_needed": 0,
        "material_price_hits": 0,
        "material_price_misses": 0,
        "bpc_eligible_types": 0,
        "bpc_price_hits": 0,
        "bpc_price_misses": 0,
        "types_priced": 0,
        "types_priced_with_bpc": 0,
        "types_skipped_missing_prices": 0,
    }
    if not allowed_hull_type_ids:
        return {}, stats

    blueprint_by_product_cache: dict[int, int | None] = {}
    blueprint_material_cache: dict[int, list[tuple[int, int]]] = {}
    blueprint_output_qty_cache: dict[int, int] = {}
    blueprint_by_hull_type: dict[int, int] = {}
    copy_cost_required_cache: dict[int, bool] = {}
    blueprint_copy_cost_cache: dict[int, Decimal | None] = {}

    normalized_type_ids = {int(type_id) for type_id in allowed_hull_type_ids if int(type_id) > 0}
    _prime_blueprint_cache_for_products(
        normalized_type_ids,
        blueprint_by_product_cache=blueprint_by_product_cache,
        blueprint_output_qty_cache=blueprint_output_qty_cache,
    )

    buy_requirements_by_type: dict[int, dict[int, int]] = {}
    all_material_type_ids: set[int] = set()

    for hull_type_id in sorted(normalized_type_ids):
        blueprint_id = _get_blueprint_for_product(
            hull_type_id,
            blueprint_by_product_cache=blueprint_by_product_cache,
            blueprint_output_qty_cache=blueprint_output_qty_cache,
        )
        if not blueprint_id:
            continue

        stats["blueprints_found"] += 1
        blueprint_by_hull_type[int(hull_type_id)] = int(blueprint_id)
        material_totals: dict[int, int] = defaultdict(int)
        _collect_leaf_buy_requirements(
            blueprint_id,
            1,
            blueprint_me=_DEFAULT_BLUEPRINT_ME,
            environment_material_multiplier=_DEFAULT_ENVIRONMENT_MATERIAL_MULTIPLIER,
            blueprint_by_product_cache=blueprint_by_product_cache,
            blueprint_material_cache=blueprint_material_cache,
            blueprint_output_qty_cache=blueprint_output_qty_cache,
            material_totals=material_totals,
        )
        if not material_totals:
            continue

        buy_requirements_by_type[int(hull_type_id)] = {
            int(material_type_id): int(quantity)
            for material_type_id, quantity in material_totals.items()
            if int(material_type_id) > 0 and int(quantity) > 0
        }
        if not buy_requirements_by_type[int(hull_type_id)]:
            continue

        all_material_type_ids.update(buy_requirements_by_type[int(hull_type_id)].keys())
        stats["requirements_built"] += 1

    stats["material_types_needed"] = len(all_material_type_ids)
    if not all_material_type_ids:
        return {}, stats

    material_prices = fetch_fuzzwork_prices(sorted(all_material_type_ids))
    for material_type_id in all_material_type_ids:
        sell_price = _to_decimal(material_prices.get(int(material_type_id), {}).get("sell"))
        if sell_price > 0:
            stats["material_price_hits"] += 1
        else:
            stats["material_price_misses"] += 1

    cost_by_hull_type: dict[int, Decimal] = {}
    for hull_type_id, requirements in buy_requirements_by_type.items():
        total_cost = Decimal("0")
        missing_prices = False

        for material_type_id, quantity in requirements.items():
            sell_price = _to_decimal(material_prices.get(int(material_type_id), {}).get("sell"))
            if sell_price <= 0:
                missing_prices = True
                break
            total_cost += sell_price * Decimal(int(quantity))

        if missing_prices or total_cost <= 0:
            stats["types_skipped_missing_prices"] += 1
            continue

        blueprint_id = int(blueprint_by_hull_type.get(int(hull_type_id), 0) or 0)
        if blueprint_id > 0 and _requires_blueprint_copy_cost_for_capital_hull(
            int(hull_type_id),
            copy_cost_required_cache=copy_cost_required_cache,
        ):
            stats["bpc_eligible_types"] += 1
            copy_cost = _get_blueprint_copy_cost_per_unit(
                blueprint_id,
                blueprint_output_qty_cache=blueprint_output_qty_cache,
                blueprint_copy_cost_cache=blueprint_copy_cost_cache,
            )
            if copy_cost is not None and copy_cost > 0:
                total_cost += copy_cost
                stats["bpc_price_hits"] += 1
                stats["types_priced_with_bpc"] += 1
            else:
                stats["bpc_price_misses"] += 1

        cost_by_hull_type[int(hull_type_id)] = total_cost.quantize(Decimal("0.01"))
        stats["types_priced"] += 1

    return cost_by_hull_type, stats


def sync_capital_ship_auto_estimates(
    *,
    max_pages: int = 2000,
    config_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Sync automated capital order estimates from default craft buy cost."""
    queryset = MaterialExchangeConfig.objects.all().order_by("id")
    if config_ids:
        queryset = queryset.filter(id__in=[int(config_id) for config_id in config_ids])
    configs = list(queryset)

    config_type_ids: dict[int, set[int]] = {}
    all_allowed_type_ids: set[int] = set()
    for config in configs:
        ship_options = _load_capital_ship_options(config=config)
        allowed_type_ids = {
            int(option.get("type_id") or 0)
            for option in ship_options
            if int(option.get("type_id") or 0) > 0
        }
        config_type_ids[int(config.id)] = allowed_type_ids
        all_allowed_type_ids.update(allowed_type_ids)

    base_costs_by_type, stats = _build_capital_buy_cost_map(all_allowed_type_ids)

    synced_at = timezone.now()
    synced_at_text = synced_at.isoformat()
    configs_updated = 0
    types_updated = 0

    for config in configs:
        existing_rows = config.get_capital_ship_auto_estimate_row_map()
        next_rows: dict[int, dict[str, object]] = {
            int(type_id): {
                "type_id": int(type_id),
                "price_isk": _format_price(Decimal(str(row.get("price_isk")))),
                **(
                    {"contract_count": int(row.get("contract_count"))}
                    if int(row.get("contract_count") or 0) > 0
                    else {}
                ),
                **(
                    {"updated_at": str(row.get("updated_at")).strip()}
                    if str(row.get("updated_at") or "").strip()
                    else {}
                ),
            }
            for type_id, row in existing_rows.items()
        }

        updated_type_ids_for_config: set[int] = set()
        for type_id in config_type_ids.get(int(config.id), set()):
            base_cost = base_costs_by_type.get(int(type_id))
            if base_cost is None or base_cost <= 0:
                continue

            estimate_price = _ceil_price_to_step(
                base_cost * _AUTO_ESTIMATE_MARKUP_MULTIPLIER,
                step=_AUTO_ESTIMATE_CEILING_STEP,
            )
            next_rows[int(type_id)] = {
                "type_id": int(type_id),
                "price_isk": _format_price(estimate_price),
                "updated_at": synced_at_text,
            }
            updated_type_ids_for_config.add(int(type_id))

        if updated_type_ids_for_config:
            ordered_rows = [next_rows[type_id] for type_id in sorted(next_rows.keys())]
            with transaction.atomic():
                config.capital_ship_auto_estimated_prices = ordered_rows
                config.save(
                    update_fields=[
                        "capital_ship_auto_estimated_prices",
                        "updated_at",
                    ]
                )
            configs_updated += 1
            types_updated += len(updated_type_ids_for_config)

    result = {
        "ok": True,
        "synced_at": synced_at_text,
        "configs_seen": len(configs),
        "configs_updated": configs_updated,
        "types_updated": types_updated,
        "input_max_pages": max(1, int(max_pages or 1)),
        **stats,
    }
    logger.info("Capital ship auto estimate sync summary: %s", result)
    return result
