"""DB-backed cache/sync for public Jita contracts."""

from __future__ import annotations

# Standard Library
from decimal import Decimal
from typing import Any
from datetime import timedelta

# Django
from django.core.cache import cache
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

# Local
from indy_hub.models import PublicJitaContract, PublicJitaContractItem
from indy_hub.services.public_contracts import (
    PUBLIC_BPC_OFFERS_CACHE_TTL_SECONDS,
    PublicContractsError,
    _fetch_public_contract_items_cached,
    _fetch_public_contract_page_cached,
    _is_jita_contract,
    _parse_esi_datetime,
    _resolve_operation,
    _row_value,
)

logger = get_extension_logger(__name__)

SYNC_LOCK_KEY = "indy_hub:public_jita_contracts:sync_lock:v1"
SYNC_META_KEY = "indy_hub:public_jita_contracts:sync_meta:v1"
SYNC_LOCK_TTL_SECONDS = 55 * 60
SYNC_META_TTL_SECONDS = 6 * 60 * 60
MIN_SYNC_INTERVAL_SECONDS = PUBLIC_BPC_OFFERS_CACHE_TTL_SECONDS


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


def _is_candidate_contract(contract: dict, now: timezone.datetime) -> bool:
    contract_type = str(_row_value(contract, "contract_type", "type") or "").strip().lower()
    if contract_type and contract_type != "item_exchange":
        return False

    status = str(_row_value(contract, "status") or "").strip().lower()
    if status and status != "outstanding":
        return False

    date_expired = _parse_esi_datetime(_row_value(contract, "date_expired"))
    if date_expired is not None and date_expired <= now:
        return False

    return _is_jita_contract(contract)


def _collect_candidate_contracts(
    *,
    max_pages: int,
) -> tuple[dict[int, dict[str, Any]], dict[str, int]]:
    operation = _resolve_operation("Contracts", "get_contracts_public_region_id")
    if not callable(operation):
        raise PublicContractsError("Contracts public region operation is unavailable")

    now = timezone.now()
    candidates: dict[int, dict[str, Any]] = {}
    stats = {
        "pages_scanned": 0,
        "rows_scanned": 0,
        "rows_matched": 0,
    }
    empty_pages_seen = 0

    for page in range(1, max(1, int(max_pages)) + 1):
        payload = _fetch_public_contract_page_cached(
            get_public_contracts=operation,
            page=page,
        )
        if not payload:
            empty_pages_seen += 1
            if page == 1:
                break
            if empty_pages_seen >= 2:
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
            if not _is_candidate_contract(row, now):
                continue
            stats["rows_matched"] += 1
            candidates[contract_id] = row

    return candidates, stats


def _get_last_sync_at() -> timezone.datetime | None:
    meta = cache.get(SYNC_META_KEY)
    if isinstance(meta, dict):
        synced_at = _parse_esi_datetime(meta.get("synced_at") or meta.get("cached_at"))
        if synced_at is not None:
            return synced_at

    return (
        PublicJitaContract.objects.order_by("-last_synced")
        .values_list("last_synced", flat=True)
        .first()
    )


def _build_rate_limited_result(*, now: timezone.datetime) -> dict[str, Any] | None:
    last_sync_at = _get_last_sync_at()
    if last_sync_at is None:
        return None

    if timezone.is_naive(last_sync_at):
        last_sync_at = timezone.make_aware(last_sync_at, timezone=timezone.utc)

    next_allowed_at = last_sync_at + timedelta(seconds=MIN_SYNC_INTERVAL_SECONDS)
    retry_in_seconds = int((next_allowed_at - now).total_seconds())
    if retry_in_seconds <= 0:
        return None

    return {
        "ok": False,
        "skipped": "rate_limited",
        "retry_in_seconds": retry_in_seconds,
        "last_synced_at": last_sync_at.isoformat(),
        "next_allowed_at": next_allowed_at.isoformat(),
    }


def _upsert_contract_rows(
    *,
    candidate_rows: dict[int, dict[str, Any]],
    now: timezone.datetime,
) -> tuple[int, int]:
    if not candidate_rows:
        return 0, 0

    existing = PublicJitaContract.objects.in_bulk(candidate_rows.keys())
    to_create: list[PublicJitaContract] = []
    to_update: list[PublicJitaContract] = []

    update_fields = [
        "region_id",
        "contract_type",
        "status",
        "title",
        "issuer_id",
        "issuer_corporation_id",
        "for_corporation",
        "start_location_id",
        "end_location_id",
        "is_jita",
        "price",
        "reward",
        "collateral",
        "buyout",
        "date_issued",
        "date_expired",
        "is_active",
        "last_seen_at",
        "last_synced",
    ]

    for contract_id, row in candidate_rows.items():
        payload = {
            "region_id": _to_int(_row_value(row, "region_id"), 10000002),
            "contract_type": str(
                _row_value(row, "contract_type", "type") or ""
            ).strip(),
            "status": str(_row_value(row, "status") or "").strip(),
            "title": str(_row_value(row, "title") or "").strip(),
            "issuer_id": _to_int(_row_value(row, "issuer_id"), 0),
            "issuer_corporation_id": _to_int(_row_value(row, "issuer_corporation_id"), 0),
            "for_corporation": bool(_row_value(row, "for_corporation") or False),
            "start_location_id": _to_int(_row_value(row, "start_location_id"), 0) or None,
            "end_location_id": _to_int(_row_value(row, "end_location_id"), 0) or None,
            "is_jita": True,
            "price": _to_decimal(_row_value(row, "price"), Decimal("0")),
            "reward": _to_decimal(_row_value(row, "reward"), Decimal("0")),
            "collateral": _to_decimal(_row_value(row, "collateral"), Decimal("0")),
            "buyout": _to_decimal(_row_value(row, "buyout"), Decimal("0")),
            "date_issued": _parse_esi_datetime(_row_value(row, "date_issued")),
            "date_expired": _parse_esi_datetime(_row_value(row, "date_expired")),
            "is_active": True,
            "last_seen_at": now,
        }
        existing_obj = existing.get(contract_id)
        if existing_obj is None:
            to_create.append(
                PublicJitaContract(
                    contract_id=contract_id,
                    **payload,
                )
            )
            continue
        for field_name, value in payload.items():
            setattr(existing_obj, field_name, value)
        to_update.append(existing_obj)

    if to_create:
        PublicJitaContract.objects.bulk_create(
            to_create,
            batch_size=500,
            ignore_conflicts=True,
        )
    if to_update:
        PublicJitaContract.objects.bulk_update(
            to_update,
            fields=update_fields,
            batch_size=500,
        )
    return len(to_create), len(to_update)


def _sync_contract_items(*, contract_ids: list[int]) -> tuple[int, int, int]:
    if not contract_ids:
        return 0, 0, 0

    operation = _resolve_operation("Contracts", "get_contracts_public_items_contract_id")
    if not callable(operation):
        raise PublicContractsError("Contracts public items operation is unavailable")

    item_rows_created = 0
    contracts_with_items_synced = 0
    item_sync_failures = 0

    for contract_id in contract_ids:
        try:
            items_payload = _fetch_public_contract_items_cached(
                get_public_contract_items=operation,
                contract_id=int(contract_id),
            )
        except Exception as exc:
            item_sync_failures += 1
            logger.debug(
                "Skipping public contract items sync contract_id=%s due to unexpected error: %s",
                contract_id,
                exc,
            )
            continue
        if not items_payload:
            continue

        parsed_items: list[PublicJitaContractItem] = []
        for row in items_payload:
            if not isinstance(row, dict):
                continue
            record_id = _to_int(_row_value(row, "record_id"), 0)
            if record_id <= 0:
                continue

            parsed_items.append(
                PublicJitaContractItem(
                    contract_id=int(contract_id),
                    record_id=record_id,
                    type_id=_to_int(_row_value(row, "type_id"), 0),
                    quantity=max(0, _to_int(_row_value(row, "quantity"), 0)),
                    runs=max(0, _to_int(_row_value(row, "runs"), 0)),
                    is_included=bool(_row_value(row, "is_included") or False),
                    is_blueprint_copy=bool(_row_value(row, "is_blueprint_copy") or False),
                    is_singleton=bool(_row_value(row, "is_singleton") or False),
                    material_efficiency=_to_int(_row_value(row, "material_efficiency"), 0),
                    time_efficiency=_to_int(_row_value(row, "time_efficiency"), 0),
                )
            )

        if not parsed_items:
            continue

        with transaction.atomic():
            PublicJitaContractItem.objects.filter(contract_id=int(contract_id)).delete()
            PublicJitaContractItem.objects.bulk_create(parsed_items, batch_size=1000)
        contracts_with_items_synced += 1
        item_rows_created += len(parsed_items)

    return contracts_with_items_synced, item_rows_created, item_sync_failures


def sync_public_jita_contract_cache(
    *,
    force: bool = False,
    max_pages: int = 2000,
) -> dict[str, Any]:
    """Sync public Jita contracts into DB cache."""
    now = timezone.now()
    rate_limited_result = _build_rate_limited_result(now=now)
    if rate_limited_result is not None:
        return {
            **rate_limited_result,
            "force": bool(force),
            "max_pages": int(max_pages),
        }

    lock_value = f"{timezone.now().timestamp()}"
    if not cache.add(SYNC_LOCK_KEY, lock_value, SYNC_LOCK_TTL_SECONDS):
        return {"ok": False, "skipped": "locked"}

    try:
        # Re-check after lock acquisition to enforce strict 1-hour minimum globally.
        now = timezone.now()
        rate_limited_result = _build_rate_limited_result(now=now)
        if rate_limited_result is not None:
            return {
                **rate_limited_result,
                "force": bool(force),
                "max_pages": int(max_pages),
            }

        candidate_rows, stats = _collect_candidate_contracts(max_pages=max_pages)
        candidate_ids = sorted(candidate_rows.keys())

        created_count = 0
        updated_count = 0
        contracts_with_items_synced = 0
        item_rows_created = 0
        stale_deleted = 0
        expired_deleted = 0

        with transaction.atomic():
            created_count, updated_count = _upsert_contract_rows(
                candidate_rows=candidate_rows,
                now=now,
            )

        (
            contracts_with_items_synced,
            item_rows_created,
            item_sync_failures,
        ) = _sync_contract_items(
            contract_ids=candidate_ids,
        )

        with transaction.atomic():
            if candidate_ids:
                stale_qs = PublicJitaContract.objects.exclude(
                    contract_id__in=candidate_ids
                )
                stale_deleted = stale_qs.count()
                stale_qs.delete()

            expired_qs = PublicJitaContract.objects.filter(
                date_expired__isnull=False,
                date_expired__lte=now,
            )
            expired_deleted = expired_qs.count()
            expired_qs.delete()

        active_contracts = PublicJitaContract.objects.count()
        active_items = PublicJitaContractItem.objects.count()
        meta = {
            "ok": True,
            "synced_at": timezone.now().isoformat(),
            "pages_scanned": int(stats.get("pages_scanned") or 0),
            "rows_scanned": int(stats.get("rows_scanned") or 0),
            "rows_matched": int(stats.get("rows_matched") or 0),
            "contracts_seen": len(candidate_ids),
            "contracts_created": created_count,
            "contracts_updated": updated_count,
            "contracts_with_items_synced": contracts_with_items_synced,
            "item_rows_created": item_rows_created,
            "item_sync_failures": item_sync_failures,
            "stale_deleted": int(stale_deleted or 0),
            "expired_deleted": int(expired_deleted or 0),
            "active_contracts": active_contracts,
            "active_items": active_items,
            "force": bool(force),
            "max_pages": int(max_pages),
        }
        cache.set(SYNC_META_KEY, meta, SYNC_META_TTL_SECONDS)
        logger.info("Public Jita contract sync summary: %s", meta)
        return meta
    finally:
        cache.delete(SYNC_LOCK_KEY)


def get_public_jita_contract_cache_meta() -> dict[str, Any]:
    meta = cache.get(SYNC_META_KEY)
    if isinstance(meta, dict):
        synced_at = _parse_esi_datetime(meta.get("synced_at"))
        if synced_at is not None:
            expires_at = synced_at + timedelta(seconds=PUBLIC_BPC_OFFERS_CACHE_TTL_SECONDS)
            return {
                **meta,
                "cached_at": synced_at.isoformat(),
                "expires_at": expires_at.isoformat(),
                "cache_ttl_seconds": PUBLIC_BPC_OFFERS_CACHE_TTL_SECONDS,
                "is_cached": timezone.now() <= expires_at,
            }

    latest_sync = (
        PublicJitaContract.objects.order_by("-last_synced")
        .values_list("last_synced", flat=True)
        .first()
    )
    if latest_sync is None:
        return {
            "cache_ttl_seconds": PUBLIC_BPC_OFFERS_CACHE_TTL_SECONDS,
            "cached_at": "",
            "expires_at": "",
            "is_cached": False,
            "active_contracts": 0,
            "active_items": 0,
        }

    expires_at = latest_sync + timedelta(seconds=PUBLIC_BPC_OFFERS_CACHE_TTL_SECONDS)
    return {
        "cache_ttl_seconds": PUBLIC_BPC_OFFERS_CACHE_TTL_SECONDS,
        "cached_at": latest_sync.isoformat(),
        "expires_at": expires_at.isoformat(),
        "is_cached": timezone.now() <= expires_at,
        "active_contracts": PublicJitaContract.objects.count(),
        "active_items": PublicJitaContractItem.objects.count(),
    }


def get_public_jita_bpc_offers(
    *,
    blueprint_type_id: int,
    max_offers: int = 500,
) -> list[dict]:
    """Return normalized offers for one blueprint from local DB cache."""
    blueprint_type_id = int(blueprint_type_id or 0)
    if blueprint_type_id <= 0:
        return []

    now = timezone.now()
    queryset = (
        PublicJitaContractItem.objects.select_related("contract")
        .filter(
            type_id=blueprint_type_id,
            is_included=True,
            contract__is_jita=True,
            contract__is_active=True,
            contract__contract_type__iexact="item_exchange",
            contract__date_expired__isnull=False,
            contract__date_expired__gt=now,
        )
        .filter(Q(is_blueprint_copy=True) | Q(runs__gt=0))
        .order_by("contract_id", "record_id")
    )

    offers_by_contract: dict[int, dict[str, Any]] = {}
    for item in queryset.iterator():
        contract = item.contract
        contract_id = int(contract.contract_id)
        runs_per_copy = max(1, int(item.runs or 0))
        copies = max(1, int(item.quantity or 0))
        additional_runs = runs_per_copy * copies
        total_price = Decimal(contract.price or 0) + Decimal(contract.reward or 0)

        offer = offers_by_contract.get(contract_id)
        if offer is None:
            offer = {
                "contract_id": contract_id,
                "title": str(contract.title or "").strip(),
                "issuer_id": int(contract.issuer_id or 0),
                "start_location_id": int(contract.start_location_id or 0),
                "end_location_id": int(contract.end_location_id or 0),
                "price_total": float(total_price),
                "runs": 0,
                "copies": 0,
                "me": int(item.material_efficiency or 0),
                "te": int(item.time_efficiency or 0),
                "mixed_stats": False,
                "issued_at": contract.date_issued.isoformat() if contract.date_issued else "",
                "expires_at": contract.date_expired.isoformat() if contract.date_expired else "",
                "_me_values": set(),
                "_te_values": set(),
            }
            offers_by_contract[contract_id] = offer

        offer["runs"] = int(offer["runs"]) + additional_runs
        offer["copies"] = int(offer["copies"]) + copies
        me_value = int(item.material_efficiency or 0)
        te_value = int(item.time_efficiency or 0)
        offer["_me_values"].add(me_value)
        offer["_te_values"].add(te_value)
        offer["me"] = min(int(offer["me"]), me_value)
        offer["te"] = min(int(offer["te"]), te_value)

    offers: list[dict] = []
    for offer in offers_by_contract.values():
        runs = max(1, int(offer.get("runs") or 0))
        price_total = float(offer.get("price_total") or 0)
        offer["price_per_run"] = price_total / runs if runs > 0 else price_total
        offer["mixed_stats"] = (
            len(offer.get("_me_values") or set()) > 1
            or len(offer.get("_te_values") or set()) > 1
        )
        offer.pop("_me_values", None)
        offer.pop("_te_values", None)
        offers.append(offer)

    offers.sort(
        key=lambda offer: (
            float(offer.get("price_per_run") or 0),
            -int(offer.get("me") or 0),
            -int(offer.get("te") or 0),
        )
    )
    return offers[: max(1, int(max_offers or 0))]
