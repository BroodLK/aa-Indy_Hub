"""Helpers for querying public ESI contract data for blueprint copies."""

from __future__ import annotations

# Standard Library
from decimal import Decimal
import inspect

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger
from esi.exceptions import HTTPNotModified

# Local
from indy_hub.services.cache_utils import get_or_set_cache_with_lock
from indy_hub.services.esi_client import shared_client

logger = get_extension_logger(__name__)

ESI_DATASOURCE = "tranquility"
THE_FORGE_REGION_ID = 10000002
JITA_STATION_ID = 60003760
PUBLIC_CONTRACTS_ROUTE_CACHE_TTL_SECONDS = 1800
PUBLIC_CONTRACT_ITEMS_ROUTE_CACHE_TTL_SECONDS = 1800
PUBLIC_BPC_OFFERS_CACHE_TTL_SECONDS = 1800


class PublicContractsError(Exception):
    """Raised when public contract fetching fails."""


def _resolve_operation(resource: str, snake_name: str):
    client = shared_client.client
    resource_obj = getattr(client, resource, None)
    if resource_obj is None:
        return None

    operation = getattr(resource_obj, snake_name, None)
    if callable(operation):
        return operation

    camel_name = "".join(part.capitalize() for part in snake_name.split("_"))
    operation = getattr(resource_obj, camel_name, None)
    return operation if callable(operation) else None


def _coerce_openapi_value(value, *, _depth: int = 0):
    if _depth > 8:
        return None
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {
            str(key): _coerce_openapi_value(item, _depth=_depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [
            _coerce_openapi_value(item, _depth=_depth + 1)
            for item in value
        ]

    for attr in ("model_dump", "dict", "to_dict"):
        converter = getattr(value, attr, None)
        if callable(converter):
            try:
                converted = converter()
            except Exception:
                converted = None
            if converted is not None:
                return _coerce_openapi_value(converted, _depth=_depth + 1)

    object_dict = getattr(value, "__dict__", None)
    if isinstance(object_dict, dict) and object_dict:
        return {
            str(key): _coerce_openapi_value(item, _depth=_depth + 1)
            for key, item in object_dict.items()
            if not str(key).startswith("_")
        }

    return str(value)


def _normalize_openapi_rows(payload) -> list[dict]:
    base_payload = payload
    if isinstance(payload, tuple) and len(payload) > 0:
        base_payload = payload[0]

    coerced = _coerce_openapi_value(base_payload)
    if not isinstance(coerced, list):
        return []

    rows: list[dict] = []
    for item in coerced:
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _is_openapi_rows_payload(payload) -> bool:
    if isinstance(payload, list):
        return True
    if isinstance(payload, tuple) and len(payload) > 0 and isinstance(payload[0], list):
        return True
    return False


def _run_openapi_operation(operation, **kwargs):
    def _invoke(
        call_kwargs: dict,
        *,
        use_cache: bool = False,
        disable_etag: bool = False,
    ):
        result_obj = operation(**call_kwargs)
        if not hasattr(result_obj, "results"):
            return result_obj

        if disable_etag:
            try:
                return result_obj.results(use_etag=False)
            except TypeError:
                return result_obj.results()

        if use_cache:
            try:
                return result_obj.results(use_cache=True)
            except TypeError:
                return result_obj.results()

        return result_obj.results()

    def _is_not_modified_error(exc: Exception) -> bool:
        if isinstance(exc, HTTPNotModified):
            return True
        status_code = getattr(exc, "status_code", None)
        if status_code == 304:
            return True
        return "HTTPNotModified" in type(exc).__name__

    attempts: list[dict] = [dict(kwargs)]
    if "datasource" in kwargs:
        without_datasource = dict(kwargs)
        without_datasource.pop("datasource", None)
        attempts.append(without_datasource)

    try:
        signature = inspect.signature(operation)
    except Exception:
        signature = None

    if signature is not None:
        params = signature.parameters
        accepts_var_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in params.values()
        )
        if not accepts_var_kwargs:
            filtered_attempts: list[dict] = []
            for attempt_kwargs in attempts:
                filtered = {
                    key: value
                    for key, value in attempt_kwargs.items()
                    if key in params
                }
                filtered_attempts.append(filtered)
            attempts.extend(filtered_attempts)

    deduped_attempts: list[dict] = []
    seen_signatures: set[tuple[tuple[str, str], ...]] = set()
    for attempt_kwargs in attempts:
        marker = tuple(
            sorted((str(key), repr(value)) for key, value in attempt_kwargs.items())
        )
        if marker in seen_signatures:
            continue
        seen_signatures.add(marker)
        deduped_attempts.append(attempt_kwargs)

    last_error: Exception | None = None
    saw_not_modified = False
    for attempt_kwargs in deduped_attempts:
        try:
            return _invoke(attempt_kwargs)
        except Exception as exc:
            status_code = getattr(exc, "status_code", None)
            if status_code == 404:
                return None
            if _is_not_modified_error(exc):
                saw_not_modified = True
                try:
                    return _invoke(attempt_kwargs, use_cache=True)
                except Exception as cache_exc:
                    if getattr(cache_exc, "status_code", None) == 404:
                        return None
                    if not _is_not_modified_error(cache_exc):
                        last_error = cache_exc
                    logger.debug(
                        "OpenAPI 304 cache fallback failed for %s (%s): %s",
                        getattr(operation, "__name__", repr(operation)),
                        attempt_kwargs,
                        cache_exc,
                    )
                try:
                    bypass_payload = _invoke(attempt_kwargs, disable_etag=True)
                    if bypass_payload is not None:
                        logger.debug(
                            "OpenAPI ETag bypass succeeded after 304 for %s (%s)",
                            getattr(operation, "__name__", repr(operation)),
                            attempt_kwargs,
                        )
                        return bypass_payload
                except Exception as bypass_exc:
                    if getattr(bypass_exc, "status_code", None) == 404:
                        return None
                    if not _is_not_modified_error(bypass_exc):
                        last_error = bypass_exc
                    logger.debug(
                        "OpenAPI ETag bypass failed after 304 for %s (%s): %s",
                        getattr(operation, "__name__", repr(operation)),
                        attempt_kwargs,
                        bypass_exc,
                    )
                continue
            last_error = exc
            continue

    if saw_not_modified:
        if last_error is not None:
            raise PublicContractsError(
                f"HTTPNotModified unresolved for {getattr(operation, '__name__', repr(operation))}: {last_error}"
            ) from last_error
        raise PublicContractsError(
            f"HTTPNotModified unresolved for {getattr(operation, '__name__', repr(operation))}"
        )

    if last_error is not None:
        raise PublicContractsError(
            f"{type(last_error).__name__}: {last_error}"
        ) from last_error

    raise PublicContractsError("OpenAPI operation call failed without an explicit exception")


def _fetch_public_contract_page_cached(
    *,
    get_public_contracts,
    page: int,
) -> list[dict]:
    cache_key = (
        f"indy_hub:esi:contracts:public:{THE_FORGE_REGION_ID}:"
        f"datasource:{ESI_DATASOURCE}:page:{int(page)}:v5"
    )

    def _loader() -> list[dict]:
        payload = _run_openapi_operation(
            get_public_contracts,
            region_id=THE_FORGE_REGION_ID,
            datasource=ESI_DATASOURCE,
            page=int(page),
        )
        rows = _normalize_openapi_rows(payload)
        if rows:
            return rows
        if payload is not None and not _is_openapi_rows_payload(payload):
            logger.debug(
                "Unexpected public contracts payload type for page %s: %s",
                page,
                type(payload).__name__,
            )
        return []

    rows = get_or_set_cache_with_lock(
        cache_key=cache_key,
        ttl_seconds=PUBLIC_CONTRACTS_ROUTE_CACHE_TTL_SECONDS,
        loader=_loader,
        lock_ttl_seconds=25,
        wait_timeout_seconds=10.0,
        poll_interval_seconds=0.2,
    )
    return rows if isinstance(rows, list) else []


def _fetch_public_contract_items_cached(
    *,
    get_public_contract_items,
    contract_id: int,
) -> list[dict]:
    cache_key = (
        f"indy_hub:esi:contracts:public_items:"
        f"datasource:{ESI_DATASOURCE}:contract:{int(contract_id)}:v5"
    )

    def _loader() -> list[dict]:
        payload = _run_openapi_operation(
            get_public_contract_items,
            contract_id=int(contract_id),
            datasource=ESI_DATASOURCE,
        )
        rows = _normalize_openapi_rows(payload)
        if rows:
            return rows
        if payload is not None and not _is_openapi_rows_payload(payload):
            logger.debug(
                "Unexpected public contract items payload type for contract %s: %s",
                contract_id,
                type(payload).__name__,
            )
        return []

    rows = get_or_set_cache_with_lock(
        cache_key=cache_key,
        ttl_seconds=PUBLIC_CONTRACT_ITEMS_ROUTE_CACHE_TTL_SECONDS,
        loader=_loader,
        lock_ttl_seconds=20,
        wait_timeout_seconds=8.0,
        poll_interval_seconds=0.2,
    )
    return rows if isinstance(rows, list) else []


def _normalize_title(value: object) -> str:
    return str(value or "").strip().lower()


def _snake_to_camel(name: str) -> str:
    parts = str(name or "").split("_")
    if not parts:
        return str(name or "")
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


def _row_value(row: dict, *keys: str):
    if not isinstance(row, dict):
        return None
    for key in keys:
        if key in row:
            return row.get(key)
        camel_key = _snake_to_camel(key)
        if camel_key in row:
            return row.get(camel_key)
    return None


def _is_jita_contract(contract: dict) -> bool:
    start_location_id = int(_row_value(contract, "start_location_id") or 0)
    end_location_id = int(_row_value(contract, "end_location_id") or 0)
    return start_location_id == JITA_STATION_ID or end_location_id == JITA_STATION_ID


def _extract_price(contract: dict) -> Decimal:
    price = Decimal(str(_row_value(contract, "price") or 0))
    reward = Decimal(str(_row_value(contract, "reward") or 0))
    return price + reward


def _extract_matching_bpc_items(items: list[dict], *, blueprint_type_id: int) -> list[dict]:
    matches: list[dict] = []
    for item in items:
        if int(_row_value(item, "type_id") or 0) != blueprint_type_id:
            continue
        if not bool(_row_value(item, "is_included") or False):
            continue

        runs = int(_row_value(item, "runs") or 0)
        copies = max(1, int(_row_value(item, "quantity") or 1))
        me = int(_row_value(item, "material_efficiency") or 0)
        te = int(_row_value(item, "time_efficiency") or 0)
        is_copy = bool(_row_value(item, "is_blueprint_copy") or False) or runs > 0
        if not is_copy:
            continue

        total_runs = runs if runs > 0 else 1
        if copies > 1:
            total_runs = total_runs * copies

        matches.append(
            {
                "runs": max(1, int(total_runs)),
                "copies": copies,
                "me": me,
                "te": te,
            }
        )
    return matches


def fetch_jita_public_bpc_contracts(
    *,
    blueprint_type_id: int,
    blueprint_name: str,
    max_pages: int = 10,
    max_candidates: int = 160,
    timeout: int = 8,
) -> list[dict]:
    """Return public Jita contract offers for a specific blueprint copy type."""
    # Kept for API compatibility; OpenAPI client timeout is configured in shared_client.
    _ = timeout
    normalized_name = _normalize_title(blueprint_name)
    if blueprint_type_id <= 0:
        return []

    get_public_contracts = _resolve_operation(
        "Contracts",
        "get_contracts_public_region_id",
    )
    get_public_contract_items = _resolve_operation(
        "Contracts",
        "get_contracts_public_items_contract_id",
    )
    if not callable(get_public_contracts) or not callable(get_public_contract_items):
        logger.warning("Contracts OpenAPI operations are unavailable for public BPC lookup")
        raise PublicContractsError("Required Contracts OpenAPI operations are unavailable")

    cache_key = (
        f"indy_hub:craft_bpc_offers:v8:"
        f"blueprint:{int(blueprint_type_id)}:"
        f"pages:{int(max_pages)}:candidates:{int(max_candidates)}"
    )

    def _loader() -> list[dict]:
        def _collect_candidates(
            *,
            require_title_hint: bool,
            pages: int,
            candidates_limit: int,
        ) -> tuple[list[dict], dict]:
            candidates_local: list[dict] = []
            stats = {
                "scanned": 0,
                "type_filtered": 0,
                "status_filtered": 0,
                "location_filtered": 0,
                "title_filtered": 0,
            }
            for page in range(1, max(1, pages) + 1):
                payload = _fetch_public_contract_page_cached(
                    get_public_contracts=get_public_contracts,
                    page=page,
                )
                if not payload:
                    break

                for contract in payload:
                    stats["scanned"] += 1

                    # ESI public contracts commonly uses "type", while some wrappers expose "contract_type".
                    contract_type = str(
                        _row_value(contract, "contract_type", "type") or ""
                    ).strip().lower()
                    if contract_type and contract_type != "item_exchange":
                        stats["type_filtered"] += 1
                        continue

                    # Some payload variants do not include status for public contracts.
                    status = str(_row_value(contract, "status") or "").strip().lower()
                    if status and status != "outstanding":
                        stats["status_filtered"] += 1
                        continue
                    if not _is_jita_contract(contract):
                        stats["location_filtered"] += 1
                        continue

                    if require_title_hint and normalized_name:
                        title = _normalize_title(_row_value(contract, "title"))
                        short_name = normalized_name.replace(" blueprint", "").strip()
                        if (
                            not title
                            or (normalized_name not in title and (not short_name or short_name not in title))
                        ):
                            stats["title_filtered"] += 1
                            continue

                    candidates_local.append(contract)
                    if len(candidates_local) >= candidates_limit:
                        break

                if len(candidates_local) >= candidates_limit:
                    break
            return candidates_local, stats

        candidates, candidate_stats = _collect_candidates(
            require_title_hint=True,
            pages=max_pages,
            candidates_limit=max_candidates,
        )
        used_fallback_candidate_scan = False
        if not candidates:
            candidates, fallback_stats = _collect_candidates(
                require_title_hint=False,
                pages=max(max_pages * 2, 12),
                candidates_limit=max(max_candidates * 2, 200),
            )
            candidate_stats = {
                key: int(candidate_stats.get(key, 0)) + int(fallback_stats.get(key, 0))
                for key in set(candidate_stats.keys()).union(fallback_stats.keys())
            }
            used_fallback_candidate_scan = True

        offers: list[dict] = []
        checked_contracts = 0
        matched_contracts = 0
        matched_item_rows = 0
        for contract in candidates:
            contract_id = int(_row_value(contract, "contract_id") or 0)
            if not contract_id:
                continue
            checked_contracts += 1

            items_payload = _fetch_public_contract_items_cached(
                get_public_contract_items=get_public_contract_items,
                contract_id=contract_id,
            )
            if not items_payload:
                continue

            matches = _extract_matching_bpc_items(
                items_payload,
                blueprint_type_id=blueprint_type_id,
            )
            if not matches:
                continue
            matched_contracts += 1
            matched_item_rows += len(matches)

            total_price = _extract_price(contract)
            issued_at = str(_row_value(contract, "date_issued") or "")
            expires_at = str(_row_value(contract, "date_expired") or "")
            runs = max(1, sum(max(1, int(match.get("runs") or 1)) for match in matches))
            copies = max(1, sum(max(1, int(match.get("copies") or 1)) for match in matches))
            me_values = [int(match.get("me") or 0) for match in matches]
            te_values = [int(match.get("te") or 0) for match in matches]
            me = min(me_values) if me_values else 0
            te = min(te_values) if te_values else 0
            mixed_stats = len(set(me_values)) > 1 or len(set(te_values)) > 1
            total_price_float = float(total_price)
            offers.append(
                {
                    "contract_id": contract_id,
                    "title": str(_row_value(contract, "title") or "").strip(),
                    "issuer_id": int(_row_value(contract, "issuer_id") or 0),
                    "start_location_id": int(_row_value(contract, "start_location_id") or 0),
                    "end_location_id": int(_row_value(contract, "end_location_id") or 0),
                    "price_total": total_price_float,
                    "price_per_run": (total_price_float / runs) if runs > 0 else total_price_float,
                    "runs": runs,
                    "copies": copies,
                    "me": me,
                    "te": te,
                    "mixed_stats": mixed_stats,
                    "issued_at": issued_at,
                    "expires_at": expires_at,
                }
            )

        offers.sort(
            key=lambda offer: (
                float(offer.get("price_per_run") or 0),
                -int(offer.get("me") or 0),
                -int(offer.get("te") or 0),
            )
        )
        logger.info(
            (
                "Public BPC contracts lookup bp=%s name='%s' "
                "scanned=%s candidates=%s checked=%s matched_contracts=%s matched_rows=%s offers=%s fallback_scan=%s "
                "filtered(type=%s status=%s location=%s title=%s)"
            ),
            blueprint_type_id,
            blueprint_name,
            int(candidate_stats.get("scanned") or 0),
            len(candidates),
            checked_contracts,
            matched_contracts,
            matched_item_rows,
            len(offers),
            used_fallback_candidate_scan,
            int(candidate_stats.get("type_filtered") or 0),
            int(candidate_stats.get("status_filtered") or 0),
            int(candidate_stats.get("location_filtered") or 0),
            int(candidate_stats.get("title_filtered") or 0),
        )
        return offers

    result = get_or_set_cache_with_lock(
        cache_key=cache_key,
        ttl_seconds=PUBLIC_BPC_OFFERS_CACHE_TTL_SECONDS,
        loader=_loader,
        lock_ttl_seconds=30,
        wait_timeout_seconds=10.0,
        poll_interval_seconds=0.2,
    )
    return result if isinstance(result, list) else []
