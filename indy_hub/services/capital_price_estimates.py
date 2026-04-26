"""Weekly capital order estimate sync from public Forge contracts."""

from __future__ import annotations

# Standard Library
from collections import defaultdict
from decimal import Decimal
from typing import Any

# Django
from django.db import transaction
from django.utils import timezone

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

# Alliance Auth / EVE SDE
from eve_sde.models import ItemType

# Local
from indy_hub.models import MaterialExchangeConfig
from indy_hub.services.public_contracts import (
    PublicContractsError,
    _fetch_public_contract_items_cached,
    _fetch_public_contract_page_cached,
    _parse_esi_datetime,
    _resolve_operation,
    _row_value,
)

logger = get_extension_logger(__name__)

_FUEL_TEXT_MARKERS = (
    "fuel",
    "isotope",
    "heavy water",
    "liquid ozone",
    "stront",
    "clathrate",
)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _median_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    size = len(ordered)
    mid = size // 2
    if size % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / Decimal("2")


def _load_capital_ship_options(config: MaterialExchangeConfig) -> list[dict[str, object]]:
    from indy_hub.views.capital_ship_orders import _load_capital_ship_options as _loader

    return list(_loader(config=config))


def _is_public_capital_contract_candidate(
    contract: dict[str, Any],
    *,
    now: timezone.datetime,
) -> bool:
    contract_type = str(_row_value(contract, "contract_type", "type") or "").strip().lower()
    if contract_type and contract_type != "item_exchange":
        return False

    status = str(_row_value(contract, "status") or "").strip().lower()
    if status and status != "outstanding":
        return False

    date_expired = _parse_esi_datetime(_row_value(contract, "date_expired"))
    if date_expired is not None and date_expired <= now:
        return False

    total_price = _to_decimal(_row_value(contract, "price")) + _to_decimal(
        _row_value(contract, "reward")
    )
    return total_price > 0


def _extract_item_quantity(item_row: dict[str, Any]) -> int:
    quantity = _to_int(_row_value(item_row, "quantity"), 0)
    if quantity > 0:
        return quantity
    if bool(_row_value(item_row, "is_singleton") or False):
        return 1
    return 0


def _resolve_fuel_like_type_ids(type_ids: set[int]) -> set[int]:
    if not type_ids:
        return set()

    rows = ItemType.objects.filter(id__in=list(type_ids)).values_list(
        "id",
        "name",
        "group__name",
        "group__category__name",
        "market_group__name",
    )

    fuel_like_ids: set[int] = set()
    for type_id, type_name, group_name, category_name, market_group_name in rows:
        haystacks = [
            str(type_name or "").strip().lower(),
            str(group_name or "").strip().lower(),
            str(category_name or "").strip().lower(),
            str(market_group_name or "").strip().lower(),
        ]
        if any(
            marker in haystack
            for haystack in haystacks
            for marker in _FUEL_TEXT_MARKERS
        ):
            fuel_like_ids.add(int(type_id))

    return fuel_like_ids


def _allowed_extra_type_ids(
    extra_type_ids: set[int],
    *,
    fuel_like_cache: dict[int, bool],
) -> set[int]:
    unresolved = [
        int(type_id)
        for type_id in extra_type_ids
        if int(type_id) > 0 and int(type_id) not in fuel_like_cache
    ]
    if unresolved:
        fuel_like_ids = _resolve_fuel_like_type_ids(set(unresolved))
        for type_id in unresolved:
            fuel_like_cache[int(type_id)] = int(type_id) in fuel_like_ids

    return {
        int(type_id)
        for type_id in extra_type_ids
        if bool(fuel_like_cache.get(int(type_id)))
    }


def _extract_contract_price_sample(
    contract: dict[str, Any],
    items_payload: list[dict[str, Any]],
    *,
    allowed_hull_type_ids: set[int],
    fuel_like_cache: dict[int, bool],
) -> tuple[int, Decimal] | None:
    if not items_payload:
        return None

    if any(not bool(_row_value(item, "is_included") or False) for item in items_payload):
        return None

    included_quantities_by_type: dict[int, int] = defaultdict(int)
    for item in items_payload:
        if not isinstance(item, dict):
            continue
        if not bool(_row_value(item, "is_included") or False):
            continue
        type_id = _to_int(_row_value(item, "type_id"), 0)
        if type_id <= 0:
            continue
        quantity = _extract_item_quantity(item)
        if quantity <= 0:
            continue
        included_quantities_by_type[int(type_id)] += int(quantity)

    if not included_quantities_by_type:
        return None

    hull_type_ids = {
        int(type_id)
        for type_id in included_quantities_by_type.keys()
        if int(type_id) in allowed_hull_type_ids
    }
    if len(hull_type_ids) != 1:
        return None

    hull_type_id = next(iter(hull_type_ids))
    hull_quantity = int(included_quantities_by_type.get(hull_type_id) or 0)
    if hull_quantity <= 0:
        return None

    extra_type_ids = {
        int(type_id)
        for type_id in included_quantities_by_type.keys()
        if int(type_id) != hull_type_id
    }
    if extra_type_ids:
        allowed_extra_ids = _allowed_extra_type_ids(
            extra_type_ids,
            fuel_like_cache=fuel_like_cache,
        )
        if allowed_extra_ids != extra_type_ids:
            return None

    total_price = _to_decimal(_row_value(contract, "price")) + _to_decimal(
        _row_value(contract, "reward")
    )
    if total_price <= 0:
        return None

    price_per_hull = (total_price / Decimal(hull_quantity)).quantize(Decimal("0.01"))
    if price_per_hull <= 0:
        return None

    return hull_type_id, price_per_hull


def _collect_public_capital_contract_samples(
    *,
    allowed_hull_type_ids: set[int],
    max_pages: int,
) -> tuple[dict[int, list[Decimal]], dict[str, int]]:
    if not allowed_hull_type_ids:
        return {}, {
            "pages_scanned": 0,
            "rows_scanned": 0,
            "candidate_contracts": 0,
            "contracts_with_items": 0,
            "contracts_matched": 0,
            "item_fetch_failures": 0,
        }

    get_public_contracts = _resolve_operation("Contracts", "get_contracts_public_region_id")
    get_public_contract_items = _resolve_operation(
        "Contracts",
        "get_contracts_public_items_contract_id",
    )
    if not callable(get_public_contracts):
        raise PublicContractsError("Contracts public region operation is unavailable")
    if not callable(get_public_contract_items):
        raise PublicContractsError("Contracts public items operation is unavailable")

    now = timezone.now()
    candidate_rows: dict[int, dict[str, Any]] = {}
    stats = {
        "pages_scanned": 0,
        "rows_scanned": 0,
        "candidate_contracts": 0,
        "contracts_with_items": 0,
        "contracts_matched": 0,
        "item_fetch_failures": 0,
    }
    empty_pages_seen = 0

    for page in range(1, max(1, int(max_pages)) + 1):
        payload = _fetch_public_contract_page_cached(
            get_public_contracts=get_public_contracts,
            page=page,
        )
        if not payload:
            empty_pages_seen += 1
            if page == 1 or empty_pages_seen >= 2:
                break
            continue

        stats["pages_scanned"] += 1
        empty_pages_seen = 0

        for row in payload:
            stats["rows_scanned"] += 1
            if not isinstance(row, dict):
                continue
            contract_id = _to_int(_row_value(row, "contract_id"), 0)
            if contract_id <= 0:
                continue
            if not _is_public_capital_contract_candidate(row, now=now):
                continue
            candidate_rows[int(contract_id)] = row

    stats["candidate_contracts"] = len(candidate_rows)

    prices_by_type: dict[int, list[Decimal]] = defaultdict(list)
    fuel_like_cache: dict[int, bool] = {}

    for contract_id, contract in candidate_rows.items():
        try:
            items_payload = _fetch_public_contract_items_cached(
                get_public_contract_items=get_public_contract_items,
                contract_id=int(contract_id),
            )
        except Exception as exc:
            logger.debug(
                "Skipping capital estimate contract items contract_id=%s due to unexpected error: %s",
                contract_id,
                exc,
            )
            stats["item_fetch_failures"] += 1
            continue

        if not items_payload:
            continue

        stats["contracts_with_items"] += 1
        sample = _extract_contract_price_sample(
            contract,
            items_payload,
            allowed_hull_type_ids=allowed_hull_type_ids,
            fuel_like_cache=fuel_like_cache,
        )
        if sample is None:
            continue

        hull_type_id, price_per_hull = sample
        prices_by_type[int(hull_type_id)].append(price_per_hull)
        stats["contracts_matched"] += 1

    return dict(prices_by_type), stats


def sync_capital_ship_auto_estimates(
    *,
    max_pages: int = 2000,
    config_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Sync automated capital order estimates from public Forge contracts."""
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

    prices_by_type, stats = _collect_public_capital_contract_samples(
        allowed_hull_type_ids=all_allowed_type_ids,
        max_pages=max_pages,
    )

    synced_at = timezone.now()
    synced_at_text = synced_at.isoformat()
    configs_updated = 0
    types_updated = 0

    for config in configs:
        existing_rows = config.get_capital_ship_auto_estimate_row_map()
        next_rows: dict[int, dict[str, object]] = {
            int(type_id): {
                "type_id": int(type_id),
                "price_isk": format(Decimal(str(row.get("price_isk"))).quantize(Decimal("0.01")), "f"),
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
            samples = prices_by_type.get(int(type_id), [])
            median_price = _median_decimal(samples)
            if median_price is None or median_price <= 0:
                continue
            next_rows[int(type_id)] = {
                "type_id": int(type_id),
                "price_isk": format(median_price.quantize(Decimal("0.01")), "f"),
                "contract_count": len(samples),
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
        **stats,
    }
    logger.info("Capital ship auto estimate sync summary: %s", result)
    return result
