"""Helpers for querying public ESI contract data for blueprint copies."""

from __future__ import annotations

# Standard Library
from decimal import Decimal
import inspect

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

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


def _run_openapi_operation(operation, **kwargs):
    def _invoke(call_kwargs: dict):
        result_obj = operation(**call_kwargs)
        return result_obj.results() if hasattr(result_obj, "results") else result_obj

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
    for attempt_kwargs in deduped_attempts:
        try:
            return _invoke(attempt_kwargs)
        except Exception as exc:
            status_code = getattr(exc, "status_code", None)
            if status_code == 404:
                return None
            last_error = exc
            continue

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
        f"datasource:{ESI_DATASOURCE}:page:{int(page)}:v1"
    )

    def _loader() -> list[dict]:
        payload = _run_openapi_operation(
            get_public_contracts,
            region_id=THE_FORGE_REGION_ID,
            datasource=ESI_DATASOURCE,
            page=int(page),
        )
        return payload if isinstance(payload, list) else []

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
        f"datasource:{ESI_DATASOURCE}:contract:{int(contract_id)}:v1"
    )

    def _loader() -> list[dict]:
        payload = _run_openapi_operation(
            get_public_contract_items,
            contract_id=int(contract_id),
            datasource=ESI_DATASOURCE,
        )
        return payload if isinstance(payload, list) else []

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


def _is_jita_contract(contract: dict) -> bool:
    start_location_id = int(contract.get("start_location_id") or 0)
    end_location_id = int(contract.get("end_location_id") or 0)
    return start_location_id == JITA_STATION_ID or end_location_id == JITA_STATION_ID


def _extract_price(contract: dict) -> Decimal:
    price = Decimal(str(contract.get("price") or 0))
    reward = Decimal(str(contract.get("reward") or 0))
    return price + reward


def _extract_matching_bpc_items(items: list[dict], *, blueprint_type_id: int) -> list[dict]:
    matches: list[dict] = []
    for item in items:
        if int(item.get("type_id") or 0) != blueprint_type_id:
            continue
        if not bool(item.get("is_included", False)):
            continue

        runs = int(item.get("runs") or 0)
        copies = max(1, int(item.get("quantity") or 1))
        me = int(item.get("material_efficiency") or 0)
        te = int(item.get("time_efficiency") or 0)
        is_copy = bool(item.get("is_blueprint_copy", False)) or runs > 0
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
    max_pages: int = 8,
    max_candidates: int = 120,
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
        f"indy_hub:craft_bpc_offers:v3:"
        f"blueprint:{int(blueprint_type_id)}:"
        f"pages:{int(max_pages)}:candidates:{int(max_candidates)}"
    )

    def _loader() -> list[dict]:
        def _collect_candidates(
            *,
            require_title_hint: bool,
            pages: int,
            candidates_limit: int,
        ) -> list[dict]:
            candidates_local: list[dict] = []
            for page in range(1, max(1, pages) + 1):
                payload = _fetch_public_contract_page_cached(
                    get_public_contracts=get_public_contracts,
                    page=page,
                )
                if not payload:
                    break

                for contract in payload:
                    if str(contract.get("contract_type") or "").strip().lower() != "item_exchange":
                        continue
                    if str(contract.get("status") or "").strip().lower() != "outstanding":
                        continue
                    if not _is_jita_contract(contract):
                        continue

                    if require_title_hint and normalized_name:
                        title = _normalize_title(contract.get("title"))
                        if title and normalized_name not in title and "bpc" not in title:
                            continue

                    candidates_local.append(contract)
                    if len(candidates_local) >= candidates_limit:
                        break

                if len(candidates_local) >= candidates_limit:
                    break
            return candidates_local

        candidates = _collect_candidates(
            require_title_hint=True,
            pages=max_pages,
            candidates_limit=max_candidates,
        )
        used_fallback_candidate_scan = False
        if not candidates:
            candidates = _collect_candidates(
                require_title_hint=False,
                pages=max(max_pages * 2, 12),
                candidates_limit=max(max_candidates * 2, 200),
            )
            used_fallback_candidate_scan = True

        offers: list[dict] = []
        for contract in candidates:
            contract_id = int(contract.get("contract_id") or 0)
            if not contract_id:
                continue

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

            total_price = _extract_price(contract)
            issued_at = str(contract.get("date_issued") or "")
            expires_at = str(contract.get("date_expired") or "")
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
                    "title": str(contract.get("title") or "").strip(),
                    "issuer_id": int(contract.get("issuer_id") or 0),
                    "start_location_id": int(contract.get("start_location_id") or 0),
                    "end_location_id": int(contract.get("end_location_id") or 0),
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
            "Public BPC contracts lookup bp=%s name='%s' candidates=%s offers=%s fallback_scan=%s",
            blueprint_type_id,
            blueprint_name,
            len(candidates),
            len(offers),
            used_fallback_candidate_scan,
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
