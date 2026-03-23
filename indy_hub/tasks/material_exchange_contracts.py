"""
Material Exchange contract validation and processing tasks.
Handles ESI contract checking, validation, and PM notifications for sell/buy orders.
"""

# Standard Library
import hashlib
import re
from datetime import timedelta
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR

# Third Party
from celery import shared_task

try:
    try:
        # Alliance Auth
        from esi.decorators import rate_limit_retry_task, wait_for_esi_errorlimit_reset
    except ImportError:  # pragma: no cover - older django-esi

        def rate_limit_retry_task(func):
            return func

        def wait_for_esi_errorlimit_reset(*args, **kwargs):
            def decorator(func):
                return func

            return decorator

except ImportError:  # pragma: no cover - older django-esi

    def rate_limit_retry_task(func):
        return func

    def wait_for_esi_errorlimit_reset(*args, **kwargs):
        def decorator(func):
            return func

        return decorator


# Django
from django.contrib.auth.models import User
from django.core.cache import cache
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

# AA Example App
# Local
from indy_hub.models import (
    CapitalShipOrder,
    CachedStructureName,
    ESIContract,
    ESIContractItem,
    MaterialExchangeBuyOrder,
    MaterialExchangeConfig,
    MaterialExchangeSellOrder,
    MaterialExchangeSettings,
    MaterialExchangeStock,
    MaterialExchangeTransaction,
    NotificationWebhook,
    NotificationWebhookMessage,
    ReprocessingServiceRequest,
)
from indy_hub.notifications import (
    notify_multi,
    notify_user,
    send_discord_webhook,
    send_discord_webhook_with_message_id,
)
from indy_hub.services.asset_cache import (
    add_cached_corp_assets_for_sell_completion,
    consume_cached_corp_assets_for_buy_completion,
    resolve_structure_names,
)
from indy_hub.services.esi_client import (
    ESIClientError,
    ESIForbiddenError,
    ESIRateLimitError,
    ESITokenError,
    ESIUnmodifiedError,
    get_retry_after_seconds,
    shared_client,
)
from indy_hub.services.reprocessing import (
    aggregate_contract_items_by_type,
    contract_items_match_exact,
    contract_items_match_with_tolerance,
)
from indy_hub.utils.analytics import emit_analytics_event
from indy_hub.utils.eve import get_type_name

logger = get_extension_logger(__name__)

# Cache for structure names to avoid repeated ESI lookups
_structure_name_cache: dict[int, str] = {}
_type_market_group_cache: dict[int, int | None] = {}
_market_group_children_cache: dict[int | None, set[int]] | None = None
_expanded_group_cache: dict[tuple[int, ...], set[int]] = {}

_REPROCESSING_SENT_STATUSES = {
    "outstanding",
    "in_progress",
    "finished",
    "finished_issuer",
    "finished_contractor",
}
_REPROCESSING_ACCEPTED_STATUSES = {
    "in_progress",
    "finished",
    "finished_issuer",
    "finished_contractor",
}
_REPROCESSING_FAILED_STATUSES = {
    "cancelled",
    "rejected",
    "failed",
    "expired",
    "deleted",
    "reversed",
}
_REPROCESSING_CONTRACT_ITEM_SYNC_STATUSES = {
    "outstanding",
    "in_progress",
}
_ACTIVE_REPROCESSING_STATUSES = [
    ReprocessingServiceRequest.Status.REQUEST_SUBMITTED,
    ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT,
    ReprocessingServiceRequest.Status.INBOUND_CONTRACT_VERIFIED,
    ReprocessingServiceRequest.Status.PROCESSING,
    ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT,
]


def _floor_isk_amount(value: Decimal | str | int | float | None) -> Decimal:
    try:
        parsed = Decimal(str(value or 0))
    except (InvalidOperation, TypeError, ValueError):
        parsed = Decimal("0")
    if parsed < 0:
        parsed = Decimal("0")
    return parsed.quantize(Decimal("1"), rounding=ROUND_FLOOR)


def _normalize_esi_mapping(payload, *, context: str) -> dict | None:
    """Return a dict from an ESI payload or None if unsupported."""
    if isinstance(payload, dict):
        return payload
    for attr in ("model_dump", "dict", "to_dict"):
        converter = getattr(payload, attr, None)
        if callable(converter):
            try:
                result = converter()
            except Exception:  # pragma: no cover - defensive
                result = None
            if isinstance(result, dict):
                return result
    logger.warning(
        "Unexpected %s payload type for material exchange contracts: %s",
        context,
        type(payload).__name__,
    )
    return None


def _log_sell_order_transactions(order: MaterialExchangeSellOrder) -> None:
    if MaterialExchangeTransaction.objects.filter(sell_order=order).exists():
        return

    added_quantities_by_type: dict[int, int] = {}
    for item in order.items.all():
        snapshot = MaterialExchangeTransaction.build_jita_snapshot(
            config=order.config,
            type_id=item.type_id,
            quantity=item.quantity,
        )
        MaterialExchangeTransaction.objects.create(
            config=order.config,
            transaction_type="sell",
            sell_order=order,
            user=order.seller,
            type_id=item.type_id,
            type_name=item.type_name,
            quantity=item.quantity,
            unit_price=item.unit_price,
            total_price=item.total_price,
            **snapshot,
        )
        added_quantities_by_type[int(item.type_id)] = (
            int(added_quantities_by_type.get(int(item.type_id), 0))
            + int(item.quantity or 0)
        )

        stock_item, _created = MaterialExchangeStock.objects.get_or_create(
            config=order.config,
            type_id=item.type_id,
            defaults={"type_name": item.type_name},
        )
        stock_item.quantity += item.quantity
        stock_item.save()

    try:
        add_cached_corp_assets_for_sell_completion(
            corporation_id=int(order.config.corporation_id),
            sell_structure_ids=order.config.get_sell_structure_ids(),
            hangar_division=int(getattr(order.config, "hangar_division", 1) or 1),
            added_quantities_by_type=added_quantities_by_type,
            preferred_structure_id=int(getattr(order, "source_location_id", 0) or 0),
        )
    except Exception as exc:
        logger.warning(
            "Failed to add cached corp assets for completed sell order %s: %s",
            order.id,
            exc,
        )


def _log_buy_order_transactions(order: MaterialExchangeBuyOrder) -> None:
    if MaterialExchangeTransaction.objects.filter(buy_order=order).exists():
        return

    consumed_quantities_by_type: dict[int, int] = {}
    for item in order.items.all():
        snapshot = MaterialExchangeTransaction.build_jita_snapshot(
            config=order.config,
            type_id=item.type_id,
            quantity=item.quantity,
        )
        MaterialExchangeTransaction.objects.create(
            config=order.config,
            transaction_type="buy",
            buy_order=order,
            user=order.buyer,
            type_id=item.type_id,
            type_name=item.type_name,
            quantity=item.quantity,
            unit_price=item.unit_price,
            total_price=item.total_price,
            **snapshot,
        )
        consumed_quantities_by_type[int(item.type_id)] = (
            int(consumed_quantities_by_type.get(int(item.type_id), 0))
            + int(item.quantity or 0)
        )

        try:
            stock_item = order.config.stock_items.get(type_id=item.type_id)
            stock_item.quantity = max(stock_item.quantity - item.quantity, 0)
            stock_item.save()
        except MaterialExchangeStock.DoesNotExist:
            continue

    try:
        consume_cached_corp_assets_for_buy_completion(
            corporation_id=int(order.config.corporation_id),
            buy_structure_ids=order.config.get_buy_structure_ids(),
            hangar_division=int(getattr(order.config, "hangar_division", 1) or 1),
            consumed_quantities_by_type=consumed_quantities_by_type,
        )
    except Exception as exc:
        logger.warning(
            "Failed to consume cached corp assets for completed buy order %s: %s",
            order.id,
            exc,
        )


def _get_location_name(
    location_id: int, esi_client=None, *, corporation_id: int | None = None
) -> str | None:
    """Resolve a location name from ESI, with caching and signed/unsigned support."""

    # Handle potential unsigned IDs coming from ESI
    def to_signed(n: int) -> int:
        if n > 9223372036854775807:
            return n - 18446744073709551616
        return n

    def to_unsigned(n: int) -> int:
        if n < 0:
            return n + 18446744073709551616
        return n

    # Try original ID
    name = _get_structure_name(location_id, esi_client, corporation_id=corporation_id)
    if name:
        return name

    # Try variant (signed/unsigned) via ESI
    variant = to_signed(location_id) if location_id > 0 else to_unsigned(location_id)
    if variant != location_id:
        return _get_structure_name(variant, esi_client, corporation_id=corporation_id)

    return None


def _get_structure_name(
    location_id: int, esi_client, *, corporation_id: int | None = None
) -> str | None:
    """
    Get the name of a structure from ESI, with caching.

    Returns the structure name or None if lookup fails.
    Uses cache to avoid repeated ESI calls for the same structure.
    """
    if location_id in _structure_name_cache:
        return _structure_name_cache[location_id]

    # Prefer persistent DB cache first
    try:
        cached = (
            CachedStructureName.objects.filter(structure_id=int(location_id))
            .values_list("name", flat=True)
            .first()
        )
        if cached:
            _structure_name_cache[int(location_id)] = str(cached)
            return str(cached)
    except Exception:
        pass

    # Prefer shared Indy Hub resolver (handles corp structure cache + token selection,
    # and supports managed negative hangar ids when corporation_id is provided).
    try:
        resolved = resolve_structure_names(
            [int(location_id)],
            corporation_id=int(corporation_id) if corporation_id is not None else None,
        ).get(int(location_id))
        if resolved:
            _structure_name_cache[int(location_id)] = str(resolved)
            return str(resolved)
    except Exception:
        pass

    if not esi_client:
        return None

    try:
        get_structure_info = getattr(esi_client, "get_structure_info", None)
        if callable(get_structure_info):
            structure_info = get_structure_info(location_id)
            structure_name = (
                structure_info.get("name") if isinstance(structure_info, dict) else None
            )
            if structure_name:
                _structure_name_cache[int(location_id)] = str(structure_name)
                try:
                    CachedStructureName.objects.update_or_create(
                        structure_id=int(location_id),
                        defaults={
                            "name": str(structure_name),
                            "last_resolved": timezone.now(),
                        },
                    )
                except Exception:
                    pass
                return str(structure_name)
    except Exception as exc:
        logger.debug(
            "Failed to fetch structure name for location %s: %s",
            location_id,
            exc,
        )

    return None


@shared_task(
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 10},
    rate_limit="500/m",
    time_limit=600,
    soft_time_limit=580,
)
@rate_limit_retry_task
def sync_esi_contracts():
    """
    Fetch corporation contracts from ESI and store/update them in the database.

    This task:
    1. Fetches all active Material Exchange configs
    2. For each config, fetches corporation contracts from ESI
    3. Stores/updates contracts and their items in the database
    4. Removes stale contracts (expired/deleted from ESI)

    Should be run periodically (e.g., every 5-15 minutes).
    """
    try:
        if not MaterialExchangeSettings.get_solo().is_enabled:
            logger.info("Material Exchange disabled; skipping contract sync.")
            return
    except Exception:
        pass

    configs = MaterialExchangeConfig.objects.all()
    if not configs.exists():
        logger.info("Material Exchange not configured; skipping contract sync.")
        return

    for config in configs:
        try:
            _sync_contracts_for_corporation(config.corporation_id)
        except ESIRateLimitError as exc:
            delay = get_retry_after_seconds(exc)
            logger.warning(
                "ESI rate limit reached during contract sync; retrying in %ss: %s",
                delay,
                exc,
            )
            sync_esi_contracts.apply_async(countdown=delay)
            return
        except Exception as exc:
            logger.error(
                "Failed to sync contracts for corporation %s: %s",
                config.corporation_id,
                exc,
                exc_info=True,
            )


def _reprocessing_request_detail_link(service_request: ReprocessingServiceRequest) -> str:
    return f"/indy_hub/reprocessing-services/requests/{int(service_request.id)}/"


def _contract_title_contains_request_reference(
    *,
    contract_title: str | None,
    request_reference: str | None,
) -> bool:
    title = str(contract_title or "").strip().lower()
    reference = str(request_reference or "").strip().lower()
    if not title or not reference:
        return False
    return reference in title


def _reprocessing_has_note_marker(
    service_request: ReprocessingServiceRequest,
    marker: str,
) -> bool:
    notes = str(service_request.notes or "")
    return str(marker or "") in notes


def _reprocessing_add_note_marker(
    service_request: ReprocessingServiceRequest,
    marker: str,
) -> bool:
    marker_text = str(marker or "").strip()
    if not marker_text:
        return False
    if _reprocessing_has_note_marker(service_request, marker_text):
        return False
    notes = str(service_request.notes or "").strip()
    service_request.notes = (
        f"{notes}\n{marker_text}" if notes else marker_text
    )
    return True


def _reprocessing_expected_input_map(
    service_request: ReprocessingServiceRequest,
) -> dict[int, int]:
    return {
        int(item.type_id): int(item.quantity)
        for item in service_request.items.all()
        if int(item.quantity or 0) > 0
    }


def _reprocessing_expected_output_map(
    service_request: ReprocessingServiceRequest,
) -> dict[int, int]:
    return {
        int(output.type_id): int(output.expected_quantity)
        for output in service_request.expected_outputs.all()
        if int(output.expected_quantity or 0) > 0
    }


def _build_reprocessing_items_mismatch_details(
    *,
    expected_by_type: dict[int, int],
    actual_by_type: dict[int, int],
    tolerance_percent: Decimal | None = None,
) -> str:
    expected: dict[int, int] = {
        int(type_id): int(qty)
        for type_id, qty in (expected_by_type or {}).items()
        if int(qty or 0) > 0
    }
    actual: dict[int, int] = {
        int(type_id): int(qty)
        for type_id, qty in (actual_by_type or {}).items()
        if int(qty or 0) > 0
    }

    if expected == actual and tolerance_percent is None:
        return ""

    missing_lines: list[str] = []
    surplus_lines: list[str] = []
    tolerance_lines: list[str] = []

    tolerance_ratio = (
        None
        if tolerance_percent is None
        else (Decimal(str(tolerance_percent or 0)) / Decimal("100"))
    )

    for type_id in sorted(set(expected.keys()) | set(actual.keys())):
        expected_qty = int(expected.get(type_id, 0))
        actual_qty = int(actual.get(type_id, 0))
        type_name = str(get_type_name(int(type_id)) or f"Type {int(type_id)}")

        if expected_qty > actual_qty:
            missing_lines.append(
                f"- {int(expected_qty - actual_qty):,} {type_name} "
                f"(expected {expected_qty:,}, actual {actual_qty:,})"
            )
        elif actual_qty > expected_qty:
            surplus_lines.append(
                f"- {int(actual_qty - expected_qty):,} {type_name} "
                f"(expected {expected_qty:,}, actual {actual_qty:,})"
            )

        if (
            tolerance_ratio is not None
            and expected_qty > 0
            and actual_qty > 0
            and expected_qty != actual_qty
        ):
            max_delta = max(
                1,
                int(
                    (Decimal(expected_qty) * tolerance_ratio)
                    .to_integral_value(rounding=ROUND_CEILING)
                ),
            )
            delta = abs(actual_qty - expected_qty)
            if delta > max_delta:
                tolerance_lines.append(
                    f"- {type_name}: expected {expected_qty:,}, actual {actual_qty:,} "
                    f"(allowed +/- {max_delta:,})"
                )

    sections: list[str] = []
    if missing_lines:
        sections.append("Missing:\n" + "\n".join(missing_lines))
    if surplus_lines:
        sections.append("Surplus:\n" + "\n".join(surplus_lines))
    if tolerance_lines:
        sections.append("Out of Tolerance:\n" + "\n".join(tolerance_lines))
    return "\n\n".join(sections)


def _reprocessing_find_contract_candidate(
    *,
    request_reference: str,
    issuer_id: int,
    assignee_id: int,
    exclude_contract_id: int = 0,
) -> ESIContract | None:
    if not request_reference or issuer_id <= 0 or assignee_id <= 0:
        return None
    queryset = ESIContract.objects.filter(
        contract_type__iexact="item_exchange",
        issuer_id=int(issuer_id),
        assignee_id=int(assignee_id),
        title__icontains=str(request_reference),
    ).order_by("-date_issued", "-contract_id")
    if int(exclude_contract_id or 0) > 0:
        queryset = queryset.exclude(contract_id=int(exclude_contract_id))
    return queryset.prefetch_related("items").first()


def _is_reprocessing_contract_accepted(
    *,
    contract: ESIContract,
    expected_acceptor_id: int,
) -> bool:
    if int(contract.acceptor_id or 0) > 0 and int(expected_acceptor_id or 0) > 0:
        return int(contract.acceptor_id) == int(expected_acceptor_id)
    if contract.date_accepted is not None:
        return True
    return str(contract.status or "").strip().lower() in _REPROCESSING_ACCEPTED_STATUSES


def _validate_reprocessing_inbound_contract(
    *,
    service_request: ReprocessingServiceRequest,
    contract: ESIContract,
    expected_items_by_type: dict[int, int],
) -> tuple[bool, str]:
    if str(contract.contract_type or "").strip().lower() != "item_exchange":
        return False, "Inbound contract must be Item Exchange."
    if not _contract_title_contains_request_reference(
        contract_title=str(contract.title or ""),
        request_reference=str(service_request.request_reference or ""),
    ):
        return False, "Inbound contract title/description is missing request reference."
    if int(contract.issuer_id or 0) != int(service_request.requester_character_id or 0):
        return False, "Inbound contract issuer does not match requester character."
    if int(contract.assignee_id or 0) != int(service_request.processor_character_id or 0):
        return False, "Inbound contract assignee does not match reprocessor character."

    price = Decimal(str(contract.price or 0)).quantize(Decimal("0.01"))
    reward = Decimal(str(contract.reward or 0)).quantize(Decimal("0.01"))
    if price != Decimal("0.00") or reward != Decimal("0.00"):
        return False, "Inbound contract must have 0 ISK price and 0 ISK reward."

    if str(contract.status or "").strip().lower() in _REPROCESSING_FAILED_STATUSES:
        return (
            False,
            f"Inbound contract moved to {contract.status}.",
        )

    matches_exact = contract_items_match_exact(
        contract_items=contract.items.all(),
        expected_by_type=expected_items_by_type,
    )
    if not matches_exact:
        mismatch_details = _build_reprocessing_items_mismatch_details(
            expected_by_type=expected_items_by_type,
            actual_by_type=aggregate_contract_items_by_type(contract.items.all()),
        )
        return (
            False,
            (
                "Inbound contract items do not exactly match the submitted source item list."
                + (f"\n\n{mismatch_details}" if mismatch_details else "")
            ),
        )
    return True, ""


def _validate_reprocessing_return_contract(
    *,
    service_request: ReprocessingServiceRequest,
    contract: ESIContract,
    expected_outputs_by_type: dict[int, int],
) -> tuple[bool, str]:
    if str(contract.contract_type or "").strip().lower() != "item_exchange":
        return False, "Return contract must be Item Exchange."
    if not _contract_title_contains_request_reference(
        contract_title=str(contract.title or ""),
        request_reference=str(service_request.request_reference or ""),
    ):
        return False, "Return contract title/description is missing request reference."
    if int(contract.issuer_id or 0) != int(service_request.processor_character_id or 0):
        return False, "Return contract issuer does not match reprocessor character."
    if int(contract.assignee_id or 0) != int(service_request.requester_character_id or 0):
        return False, "Return contract assignee does not match requester character."

    expected_price = _floor_isk_amount(service_request.reward_isk)
    contract_price = _floor_isk_amount(contract.price)
    contract_reward = _floor_isk_amount(contract.reward)
    if contract_price != expected_price or contract_reward != Decimal("0"):
        return (
            False,
            "Return contract price/reward does not match expected reprocessing reward.",
        )

    if str(contract.status or "").strip().lower() in _REPROCESSING_FAILED_STATUSES:
        return (
            False,
            f"Return contract moved to {contract.status}.",
        )

    matches_tolerance, tolerance_errors = contract_items_match_with_tolerance(
        contract_items=contract.items.all(),
        expected_by_type=expected_outputs_by_type,
        tolerance_percent=Decimal(str(service_request.tolerance_percent or 1)),
    )
    if not matches_tolerance:
        mismatch_details = _build_reprocessing_items_mismatch_details(
            expected_by_type=expected_outputs_by_type,
            actual_by_type=aggregate_contract_items_by_type(contract.items.all()),
            tolerance_percent=Decimal(str(service_request.tolerance_percent or 1)),
        )
        base_error = "; ".join(tolerance_errors)
        if mismatch_details:
            return False, f"{base_error}\n\n{mismatch_details}".strip()
        return False, base_error
    return True, ""


def _reprocessing_validation_summary_for_notification(
    *,
    stage: str,
    tolerance_percent: Decimal | None = None,
) -> str:
    stage_key = str(stage or "").strip().lower()
    if stage_key == "inbound":
        return (
            "Validation passed: title/reference, issuer/assignee, "
            "0 ISK price/reward, and exact submitted input items matched."
        )
    tolerance_value = Decimal(str(tolerance_percent or 1)).quantize(Decimal("0.01"))
    return (
        "Validation passed: title/reference, issuer/assignee, reward price, "
        f"0 ISK reward field, and expected output items matched within {tolerance_value}% tolerance."
    )


def _set_reprocessing_request_anomaly(
    *,
    service_request: ReprocessingServiceRequest,
    stage: str,
    reason: str,
    contract: ESIContract | None = None,
) -> None:
    contract_id = int(getattr(contract, "contract_id", 0) or 0)
    reason_text = str(reason or "Unknown contract validation failure").strip()
    signature = hashlib.sha1(reason_text.encode("utf-8")).hexdigest()[:10]
    marker = (
        f"[AUTO-REPROC-ANOMALY:{stage}:{contract_id}:{signature}]"
    )
    if _reprocessing_has_note_marker(service_request, marker):
        return

    # Persist the disputed status first to avoid sending misleading notifications
    # if the database write fails.
    service_request.status = ReprocessingServiceRequest.Status.DISPUTED
    service_request.dispute_reason = reason_text
    notes_changed = _reprocessing_add_note_marker(service_request, marker)
    update_fields = ["status", "dispute_reason", "updated_at"]
    if notes_changed:
        update_fields.append("notes")
    service_request.save(update_fields=update_fields)

    detail_link = _reprocessing_request_detail_link(service_request)
    contract_label = f" Contract #{contract_id}." if contract_id > 0 else ""
    message = (
        f"Request {service_request.request_reference} has a contract anomaly during {stage}."
        f"{contract_label}\n\nReason: {reason_text}\n\n"
        "This request was automatically flagged as a contract anomaly for admin review."
    )
    notify_user(
        service_request.requester,
        _("Reprocessing contract anomaly"),
        _(message),
        level="warning",
        link=detail_link,
    )
    notify_user(
        service_request.processor_user,
        _("Reprocessing contract anomaly"),
        _(message),
        level="warning",
        link=detail_link,
    )


def _is_reprocessing_contract_relevant(
    *,
    contract_title: str | None,
    active_references_upper: set[str],
) -> bool:
    title_upper = str(contract_title or "").strip().upper()
    if not title_upper:
        return False
    if any(ref in title_upper for ref in active_references_upper):
        return True
    return "REPROCESSING" in title_upper or "REPROC" in title_upper


def sync_reprocessing_character_contracts() -> None:
    active_rows = list(
        ReprocessingServiceRequest.objects.filter(status__in=_ACTIVE_REPROCESSING_STATUSES)
        .values("request_reference", "requester_character_id", "processor_character_id")
    )
    if not active_rows:
        return

    active_references_upper = {
        str(row.get("request_reference") or "").strip().upper()
        for row in active_rows
        if str(row.get("request_reference") or "").strip()
    }
    participant_character_ids = sorted(
        {
            int(row.get("requester_character_id") or 0)
            for row in active_rows
            if int(row.get("requester_character_id") or 0) > 0
        }
        | {
            int(row.get("processor_character_id") or 0)
            for row in active_rows
            if int(row.get("processor_character_id") or 0) > 0
        }
    )
    if not participant_character_ids:
        return

    synced_contract_count = 0
    synced_item_count = 0
    synced_items_contract_ids: set[int] = set()
    for character_id in participant_character_ids:
        try:
            contracts = shared_client.fetch_character_contracts(character_id=character_id)
        except ESIUnmodifiedError:
            continue
        except ESIRateLimitError as exc:
            logger.warning(
                "ESI rate limit during reprocessing character contract sync for %s: %s",
                character_id,
                exc,
            )
            continue
        except (ESITokenError, ESIForbiddenError) as exc:
            logger.debug(
                "Skipping reprocessing character contract sync for %s: %s",
                character_id,
                exc,
            )
            continue
        except Exception as exc:
            logger.warning(
                "Failed fetching character contracts for reprocessing character %s: %s",
                character_id,
                exc,
            )
            continue

        if not isinstance(contracts, list):
            logger.warning(
                "Unexpected payload type for character contracts (%s): %s",
                character_id,
                type(contracts).__name__,
            )
            continue

        for contract_data in contracts:
            contract_payload = _normalize_esi_mapping(
                contract_data,
                context=f"reprocessing character contract ({character_id})",
            )
            if not contract_payload:
                continue

            contract_id = int(contract_payload.get("contract_id") or 0)
            if contract_id <= 0:
                continue

            contract_title = str(contract_payload.get("title") or "")
            if not _is_reprocessing_contract_relevant(
                contract_title=contract_title,
                active_references_upper=active_references_upper,
            ):
                continue

            issuer_corporation_id = int(contract_payload.get("issuer_corporation_id") or 0)
            contract, _created = ESIContract.objects.update_or_create(
                contract_id=contract_id,
                defaults={
                    "issuer_id": int(contract_payload.get("issuer_id") or 0),
                    "issuer_corporation_id": issuer_corporation_id,
                    "assignee_id": int(contract_payload.get("assignee_id") or 0),
                    "acceptor_id": int(contract_payload.get("acceptor_id") or 0),
                    "contract_type": str(contract_payload.get("type") or "unknown"),
                    "status": str(contract_payload.get("status") or "unknown"),
                    "title": contract_title,
                    "start_location_id": contract_payload.get("start_location_id"),
                    "end_location_id": contract_payload.get("end_location_id"),
                    "price": Decimal(str(contract_payload.get("price") or 0)),
                    "reward": Decimal(str(contract_payload.get("reward") or 0)),
                    "collateral": Decimal(str(contract_payload.get("collateral") or 0)),
                    "date_issued": contract_payload.get("date_issued") or timezone.now(),
                    "date_expired": contract_payload.get("date_expired")
                    or (timezone.now() + timedelta(days=7)),
                    "date_accepted": contract_payload.get("date_accepted"),
                    "date_completed": contract_payload.get("date_completed"),
                    # Character contracts are not owned by a specific corp cache scope.
                    "corporation_id": 0,
                },
            )
            synced_contract_count += 1

            contract_status = str(contract_payload.get("status") or "").strip().lower()
            if (
                str(contract_payload.get("type") or "").strip().lower() != "item_exchange"
                or contract_status not in _REPROCESSING_CONTRACT_ITEM_SYNC_STATUSES
                or contract_id in synced_items_contract_ids
            ):
                continue

            try:
                contract_items = shared_client.fetch_character_contract_items(
                    character_id=character_id,
                    contract_id=contract_id,
                )
            except ESIUnmodifiedError:
                synced_items_contract_ids.add(contract_id)
                continue
            except ESIClientError as exc:
                if "404" in str(exc):
                    logger.debug(
                        "No character contract items available for %s (404).",
                        contract_id,
                    )
                else:
                    logger.warning(
                        "Failed fetching character contract items for %s: %s",
                        contract_id,
                        exc,
                    )
                continue
            except Exception as exc:
                logger.warning(
                    "Failed fetching character contract items for %s: %s",
                    contract_id,
                    exc,
                )
                continue

            if not isinstance(contract_items, list):
                logger.warning(
                    "Unexpected payload type for character contract items (%s): %s",
                    contract_id,
                    type(contract_items).__name__,
                )
                continue

            ESIContractItem.objects.filter(contract=contract).delete()
            for item_data in contract_items:
                item_payload = _normalize_esi_mapping(
                    item_data,
                    context=f"reprocessing character contract item ({contract_id})",
                )
                if not item_payload:
                    continue
                ESIContractItem.objects.create(
                    contract=contract,
                    record_id=int(item_payload.get("record_id") or 0),
                    type_id=int(item_payload.get("type_id") or 0),
                    quantity=int(item_payload.get("quantity") or 0),
                    raw_quantity=item_payload.get("raw_quantity"),
                    is_included=bool(item_payload.get("is_included", False)),
                    is_singleton=bool(item_payload.get("is_singleton", False)),
                )
                synced_item_count += 1
            synced_items_contract_ids.add(contract_id)

    if synced_contract_count or synced_item_count:
        logger.info(
            "Reprocessing character contract sync completed: %s contracts, %s items.",
            synced_contract_count,
            synced_item_count,
        )


def _process_reprocessing_request_contracts(
    service_request: ReprocessingServiceRequest,
) -> None:
    if service_request.is_terminal:
        return
    requester_character_id = int(service_request.requester_character_id or 0)
    processor_character_id = int(service_request.processor_character_id or 0)
    if requester_character_id <= 0 or processor_character_id <= 0:
        return

    detail_link = _reprocessing_request_detail_link(service_request)
    expected_inputs_by_type = _reprocessing_expected_input_map(service_request)
    expected_outputs_by_type = _reprocessing_expected_output_map(service_request)
    now = timezone.now()

    inbound_contract = None
    inbound_contract_id = int(service_request.inbound_contract_id or 0)
    if inbound_contract_id > 0:
        inbound_contract = (
            ESIContract.objects.filter(contract_id=inbound_contract_id)
            .prefetch_related("items")
            .first()
        )
    if inbound_contract is None:
        inbound_contract = _reprocessing_find_contract_candidate(
            request_reference=str(service_request.request_reference or ""),
            issuer_id=requester_character_id,
            assignee_id=processor_character_id,
        )

    if inbound_contract is not None:
        inbound_ok, inbound_error = _validate_reprocessing_inbound_contract(
            service_request=service_request,
            contract=inbound_contract,
            expected_items_by_type=expected_inputs_by_type,
        )
        if not inbound_ok:
            _set_reprocessing_request_anomaly(
                service_request=service_request,
                stage="inbound",
                reason=inbound_error,
                contract=inbound_contract,
            )
            return

        updated_fields: set[str] = set()
        if int(service_request.inbound_contract_id or 0) != int(inbound_contract.contract_id):
            service_request.inbound_contract_id = int(inbound_contract.contract_id)
            updated_fields.add("inbound_contract_id")
        if service_request.status == ReprocessingServiceRequest.Status.REQUEST_SUBMITTED:
            service_request.status = ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT
            updated_fields.add("status")

        inbound_status = str(inbound_contract.status or "").strip().lower()
        sent_marker = f"[AUTO-REPROC-INBOUND-SENT:{int(inbound_contract.contract_id)}]"
        if (
            inbound_status in _REPROCESSING_SENT_STATUSES
            and not _reprocessing_has_note_marker(service_request, sent_marker)
        ):
            validation_summary = _reprocessing_validation_summary_for_notification(
                stage="inbound",
            )
            notify_user(
                service_request.processor_user,
                _("Inbound reprocessing contract sent"),
                _(
                    "Requester %(character)s sent inbound contract %(contract)s for %(reference)s.\n%(validation)s"
                )
                % {
                    "character": service_request.requester_character_name
                    or service_request.requester.username,
                    "contract": f"#{inbound_contract.contract_id}",
                    "reference": service_request.request_reference,
                    "validation": validation_summary,
                },
                level="info",
                link=detail_link,
            )
            if _reprocessing_add_note_marker(service_request, sent_marker):
                updated_fields.add("notes")

        accepted_marker = (
            f"[AUTO-REPROC-INBOUND-ACCEPTED:{int(inbound_contract.contract_id)}]"
        )
        if _is_reprocessing_contract_accepted(
            contract=inbound_contract,
            expected_acceptor_id=processor_character_id,
        ):
            if service_request.inbound_contract_verified_at is None:
                service_request.inbound_contract_verified_at = now
                updated_fields.add("inbound_contract_verified_at")
            if service_request.status in {
                ReprocessingServiceRequest.Status.REQUEST_SUBMITTED,
                ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT,
                ReprocessingServiceRequest.Status.INBOUND_CONTRACT_VERIFIED,
            }:
                service_request.status = ReprocessingServiceRequest.Status.PROCESSING
                updated_fields.add("status")
            if not _reprocessing_has_note_marker(service_request, accepted_marker):
                notify_user(
                    service_request.requester,
                    _("Inbound reprocessing contract accepted"),
                    _(
                        "Inbound contract %(contract)s for %(reference)s was accepted by %(processor)s."
                    )
                    % {
                        "contract": f"#{inbound_contract.contract_id}",
                        "reference": service_request.request_reference,
                        "processor": service_request.processor_character_name
                        or service_request.processor_user.username,
                    },
                    level="success",
                    link=detail_link,
                )
                if _reprocessing_add_note_marker(service_request, accepted_marker):
                    updated_fields.add("notes")

        if updated_fields:
            service_request.save(update_fields=sorted(updated_fields | {"updated_at"}))

    inbound_complete = bool(service_request.inbound_contract_verified_at)
    if not inbound_complete:
        return

    return_contract = None
    return_contract_id = int(service_request.return_contract_id or 0)
    if return_contract_id > 0:
        return_contract = (
            ESIContract.objects.filter(contract_id=return_contract_id)
            .prefetch_related("items")
            .first()
        )
    if return_contract is None:
        return_contract = _reprocessing_find_contract_candidate(
            request_reference=str(service_request.request_reference or ""),
            issuer_id=processor_character_id,
            assignee_id=requester_character_id,
            exclude_contract_id=int(service_request.inbound_contract_id or 0),
        )
    if return_contract is None:
        return

    return_ok, return_error = _validate_reprocessing_return_contract(
        service_request=service_request,
        contract=return_contract,
        expected_outputs_by_type=expected_outputs_by_type,
    )
    if not return_ok:
        _set_reprocessing_request_anomaly(
            service_request=service_request,
            stage="return",
            reason=return_error,
            contract=return_contract,
        )
        return

    updated_fields: set[str] = set()
    if int(service_request.return_contract_id or 0) != int(return_contract.contract_id):
        service_request.return_contract_id = int(return_contract.contract_id)
        updated_fields.add("return_contract_id")

    return_sent_marker = f"[AUTO-REPROC-RETURN-SENT:{int(return_contract.contract_id)}]"
    if not _reprocessing_has_note_marker(service_request, return_sent_marker):
        validation_summary = _reprocessing_validation_summary_for_notification(
            stage="return",
            tolerance_percent=Decimal(str(service_request.tolerance_percent or 1)),
        )
        notify_user(
            service_request.requester,
            _("Return reprocessing contract sent"),
            _(
                "Reprocessor %(processor)s sent return contract %(contract)s for %(reference)s.\n%(validation)s"
            )
            % {
                "processor": service_request.processor_character_name
                or service_request.processor_user.username,
                "contract": f"#{return_contract.contract_id}",
                "reference": service_request.request_reference,
                "validation": validation_summary,
            },
            level="success",
            link=detail_link,
        )
        if _reprocessing_add_note_marker(service_request, return_sent_marker):
            updated_fields.add("notes")

    if service_request.return_contract_verified_at is None:
        service_request.return_contract_verified_at = now
        updated_fields.add("return_contract_verified_at")
    if service_request.completed_at is None:
        service_request.completed_at = now
        updated_fields.add("completed_at")
    if service_request.status != ReprocessingServiceRequest.Status.COMPLETED:
        service_request.status = ReprocessingServiceRequest.Status.COMPLETED
        updated_fields.add("status")

    actual_by_type = aggregate_contract_items_by_type(
        return_contract.items.filter(is_included=True)
    )
    outputs_to_update: list = []
    for output in service_request.expected_outputs.all():
        actual_quantity = int(actual_by_type.get(int(output.type_id), 0))
        if int(output.actual_quantity or 0) != actual_quantity:
            output.actual_quantity = actual_quantity
            outputs_to_update.append(output)
    if outputs_to_update:
        for output in outputs_to_update:
            output.save(update_fields=["actual_quantity"])

    if updated_fields:
        service_request.save(update_fields=sorted(updated_fields | {"updated_at"}))


def auto_progress_reprocessing_requests() -> None:
    requests = (
        ReprocessingServiceRequest.objects.filter(status__in=_ACTIVE_REPROCESSING_STATUSES)
        .select_related("requester", "processor_user", "processor_profile")
        .prefetch_related("items", "expected_outputs")
        .order_by("updated_at")
    )
    for service_request in requests:
        try:
            _process_reprocessing_request_contracts(service_request)
        except Exception as exc:
            logger.error(
                "Failed automated reprocessing contract processing for request %s: %s",
                service_request.request_reference or service_request.id,
                exc,
                exc_info=True,
            )


@shared_task(
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 2, "countdown": 5},
    rate_limit="300/m",
    time_limit=900,
    soft_time_limit=880,
)
def run_material_exchange_cycle():
    """
    End-to-end cycle: sync contracts, validate pending sell orders,
    validate pending buy orders, then check completion of approved orders.
    Reprocessing contract automation runs regardless of Material Exchange config.
    Intended to be scheduled in Celery Beat to simplify orchestration.
    """
    material_exchange_enabled = True
    try:
        material_exchange_enabled = bool(MaterialExchangeSettings.get_solo().is_enabled)
    except Exception:
        material_exchange_enabled = True

    material_exchange_has_config = bool(MaterialExchangeConfig.objects.exists())

    if material_exchange_enabled and material_exchange_has_config:
        # Step 1: sync cached contracts
        sync_esi_contracts()

        # Step 2: validate pending sell orders using cached contracts
        validate_material_exchange_sell_orders()

        # Step 3: validate pending buy orders using cached contracts
        validate_material_exchange_buy_orders()

        # Step 4: process capital ship order workflow
        process_capital_ship_orders()

        # Step 5: check completion/payment for approved orders
        check_completed_material_exchange_contracts()
    else:
        logger.info(
            "Skipping Material Exchange contract workflow (enabled=%s, has_config=%s).",
            material_exchange_enabled,
            material_exchange_has_config,
        )

    # Step 6: sync character contracts used by active reprocessing requests
    sync_reprocessing_character_contracts()

    # Step 7: auto-process reprocessing request contracts
    auto_progress_reprocessing_requests()


def _sync_contracts_for_corporation(corporation_id: int):
    """Sync ESI contracts for a single corporation."""
    logger.info("Syncing ESI contracts for corporation %s", corporation_id)

    try:
        # Get character with required scope
        character_id = _get_character_for_scope(
            corporation_id,
            "esi-contracts.read_corporation_contracts.v1",
        )

        has_cached_contracts = ESIContract.objects.filter(
            corporation_id=corporation_id
        ).exists()

        # Fetch contracts from ESI
        contracts = shared_client.fetch_corporation_contracts(
            corporation_id=corporation_id,
            character_id=character_id,
            force_refresh=not has_cached_contracts,
        )
        if not isinstance(contracts, list):
            logger.warning(
                "Skipping contract sync for corporation %s: unexpected payload type %s",
                corporation_id,
                type(contracts).__name__,
            )
            return

        logger.info(
            "Fetched %s contracts from ESI for corporation %s",
            len(contracts),
            corporation_id,
        )

    except ESITokenError as exc:
        logger.warning(
            "Cannot sync contracts for corporation %s - missing ESI scope: %s",
            corporation_id,
            exc,
        )
        return
    except ESIUnmodifiedError:
        logger.debug(
            "Contracts not modified for corporation %s; skipping sync",
            corporation_id,
        )
        return
    except ESIRateLimitError as exc:
        logger.warning(
            "ESI rate limit reached while syncing contracts for corporation %s: %s",
            corporation_id,
            exc,
        )
        raise
    except (ESIClientError, ESIForbiddenError) as exc:
        logger.error(
            "Failed to fetch contracts from ESI for corporation %s: %s",
            corporation_id,
            exc,
            exc_info=True,
        )
        return

    # Track synced contract IDs
    synced_contract_ids = []
    indy_contracts_count = 0

    with transaction.atomic():
        for contract_data in contracts:
            contract_payload = _normalize_esi_mapping(
                contract_data,
                context=f"contract ({corporation_id})",
            )
            if not contract_payload:
                continue

            contract_id = contract_payload.get("contract_id")
            if not contract_id:
                continue

            # Filter: process Indy Hub contract titles for material exchange, reprocessing, and capitals.
            contract_title = contract_payload.get("title", "")
            contract_title_upper = str(contract_title or "").upper()
            if not any(
                marker in contract_title_upper
                for marker in ("INDY", "REPROCESSING", "REPROC")
            ):
                continue

            indy_contracts_count += 1
            synced_contract_ids.append(contract_id)

            # Create or update contract
            contract, created = ESIContract.objects.update_or_create(
                contract_id=contract_id,
                defaults={
                    "issuer_id": contract_payload.get("issuer_id", 0),
                    "issuer_corporation_id": contract_payload.get(
                        "issuer_corporation_id", 0
                    ),
                    "assignee_id": contract_payload.get("assignee_id", 0),
                    "acceptor_id": contract_payload.get("acceptor_id", 0),
                    "contract_type": contract_payload.get("type", "unknown"),
                    "status": contract_payload.get("status", "unknown"),
                    "title": contract_payload.get("title", ""),
                    "start_location_id": contract_payload.get("start_location_id"),
                    "end_location_id": contract_payload.get("end_location_id"),
                    "price": Decimal(str(contract_payload.get("price") or 0)),
                    "reward": Decimal(str(contract_payload.get("reward") or 0)),
                    "collateral": Decimal(str(contract_payload.get("collateral") or 0)),
                    "date_issued": contract_payload.get("date_issued"),
                    "date_expired": contract_payload.get("date_expired"),
                    "date_accepted": contract_payload.get("date_accepted"),
                    "date_completed": contract_payload.get("date_completed"),
                    "corporation_id": corporation_id,
                },
            )

            # Fetch and store contract items for item_exchange contracts
            # Only fetch items for contracts where items are accessible (outstanding/in_progress)
            # Completed/expired contracts return 404 for items endpoint
            contract_status = contract_payload.get("status", "")
            if contract_payload.get("type") == "item_exchange" and contract_status in [
                "outstanding",
                "in_progress",
            ]:
                try:
                    has_cached_items = ESIContractItem.objects.filter(
                        contract=contract
                    ).exists()
                    contract_items = shared_client.fetch_corporation_contract_items(
                        corporation_id=corporation_id,
                        contract_id=contract_id,
                        character_id=character_id,
                        force_refresh=not has_cached_items,
                    )
                    if not isinstance(contract_items, list):
                        logger.warning(
                            "Skipping contract items for %s: unexpected payload type %s",
                            contract_id,
                            type(contract_items).__name__,
                        )
                        continue

                    # Clear existing items and create new ones
                    ESIContractItem.objects.filter(contract=contract).delete()

                    for item_data in contract_items:
                        item_payload = _normalize_esi_mapping(
                            item_data,
                            context=f"contract item ({contract_id})",
                        )
                        if not item_payload:
                            continue
                        ESIContractItem.objects.create(
                            contract=contract,
                            record_id=item_payload.get("record_id", 0),
                            type_id=item_payload.get("type_id", 0),
                            quantity=item_payload.get("quantity", 0),
                            raw_quantity=item_payload.get("raw_quantity"),
                            is_included=item_payload.get("is_included", False),
                            is_singleton=item_payload.get("is_singleton", False),
                        )

                    logger.info(
                        "Contract %s: synced %s items",
                        contract_id,
                        len(contract_items),
                    )

                except ESIUnmodifiedError:
                    logger.debug(
                        "Contract items not modified for %s; skipping items sync",
                        contract_id,
                    )
                except ESIClientError as exc:
                    # 404 is normal for contracts without items or expired contracts
                    if "404" in str(exc):
                        logger.debug(
                            "Contract %s has no items (404) - skipping items sync",
                            contract_id,
                        )
                    else:
                        logger.warning(
                            "Failed to fetch items for contract %s: %s",
                            contract_id,
                            exc,
                        )
                except Exception as exc:
                    logger.warning(
                        "Failed to fetch items for contract %s: %s",
                        contract_id,
                        exc,
                    )

        # Remove contracts that are no longer in ESI response
        # Keep contracts from the last 30 days to maintain history
        cutoff_date = timezone.now() - timezone.timedelta(days=30)
        deleted_count, _ = (
            ESIContract.objects.filter(
                corporation_id=corporation_id,
                last_synced__lt=timezone.now() - timezone.timedelta(minutes=20),
                date_issued__gte=cutoff_date,
            )
            .exclude(contract_id__in=synced_contract_ids)
            .delete()
        )

        if deleted_count > 0:
            logger.info(
                "Removed %s stale contracts for corporation %s",
                deleted_count,
                corporation_id,
            )

    logger.info(
        "Successfully synced %s INDY contracts (filtered from %s total) for corporation %s",
        indy_contracts_count,
        len(contracts),
        corporation_id,
    )


@shared_task(
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 10},
    rate_limit="500/m",
    time_limit=600,
    soft_time_limit=580,
)
@rate_limit_retry_task
def validate_material_exchange_sell_orders():
    """
    Validate pending sell orders against cached ESI contracts in the database.

    Workflow:
    1. Find all pending sell orders
    2. Query cached contracts from database
    3. Match contracts to orders by:
        - Contract type = item_exchange
        - Contract issuer = member
        - Contract acceptor = corporation
        - Items match (type_id, quantity)
    4. Update order status & notify users

    Note: Contracts are synced separately by sync_esi_contracts task.
    """
    try:
        if not MaterialExchangeSettings.get_solo().is_enabled:
            logger.info("Material Exchange disabled; skipping sell validation.")
            return
    except Exception:
        pass

    config = MaterialExchangeConfig.objects.first()
    if not config:
        logger.warning("No Material Exchange config found")
        return

    pending_orders = MaterialExchangeSellOrder.objects.filter(
        config=config,
        status__in=[
            MaterialExchangeSellOrder.Status.DRAFT,
            MaterialExchangeSellOrder.Status.AWAITING_VALIDATION,
            MaterialExchangeSellOrder.Status.ANOMALY,
            MaterialExchangeSellOrder.Status.ANOMALY_REJECTED,
        ],
    )

    if not pending_orders.exists():
        logger.debug("No pending sell orders to validate")
        return

    # Get contracts from database instead of ESI
    # Filter to item_exchange contracts for this corporation
    contracts = ESIContract.objects.filter(
        corporation_id=config.corporation_id,
        contract_type="item_exchange",
    ).prefetch_related("items")

    if not contracts.exists():
        logger.warning(
            "No cached contracts found for corporation %s. "
            "Run sync_esi_contracts task first.",
            config.corporation_id,
        )
        return

    logger.info(
        "Validating %s pending sell orders against %s cached contracts",
        pending_orders.count(),
        contracts.count(),
    )

    # Create ESI client for structure name lookups
    try:
        esi_client = shared_client
    except Exception:
        esi_client = None
        logger.warning("ESI client not available for structure name lookups")

    # Process each pending order
    for order in pending_orders:
        try:
            _validate_sell_order_from_db(config, order, contracts, esi_client)
        except Exception as exc:
            logger.error(
                "Error validating sell order %s: %s",
                order.id,
                exc,
                exc_info=True,
            )


@shared_task(
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 10},
    rate_limit="500/m",
    time_limit=600,
    soft_time_limit=580,
)
@rate_limit_retry_task
def validate_material_exchange_buy_orders():
    """
    Validate pending buy orders against cached ESI contracts in the database.

    Workflow:
    1. Find all buy orders awaiting validation
    2. Query cached contracts from database
    3. Match contracts to orders by:
        - Contract type = item_exchange
        - Issuer corporation = config.corporation_id
        - Assignee = buyer's character
        - Items and price match
    4. Update order status & notify users

    Note: Contracts are synced separately by sync_esi_contracts task.
    """
    try:
        if not MaterialExchangeSettings.get_solo().is_enabled:
            logger.info("Material Exchange disabled; skipping buy validation.")
            return
    except Exception:
        pass

    config = MaterialExchangeConfig.objects.first()
    if not config:
        logger.warning("No Material Exchange config found")
        return

    pending_orders = MaterialExchangeBuyOrder.objects.filter(
        config=config,
        status__in=[
            MaterialExchangeBuyOrder.Status.DRAFT,
            MaterialExchangeBuyOrder.Status.AWAITING_VALIDATION,
        ],
    )

    if not pending_orders.exists():
        logger.debug("No pending buy orders to validate")
        return

    # Notify buyers of awaiting validation orders on first processing.
    # Draft orders are intentionally not pinged: they may still be awaiting
    # an admin decision, but can still be auto-validated if a matching contract
    # already exists.
    for order in pending_orders:
        if order.status != MaterialExchangeBuyOrder.Status.AWAITING_VALIDATION:
            continue
        reminder_key = (
            f"material_exchange:buy_order:{order.id}:awaiting_validation_ping"
        )
        if not cache.add(reminder_key, timezone.now().timestamp(), 60 * 60 * 24):
            continue
        items_str = ", ".join(item.type_name for item in order.items.all())
        notify_user(
            order.buyer,
            _("⏳ Buy Order Awaiting Validation"),
            _(
                f"Your buy order {order.order_reference} is awaiting validation.\n"
                f"Items: {items_str}\n"
                f"Total cost: {order.total_price:,.0f} ISK\n\n"
                f"The corporation is preparing your contract. Stand by."
            ),
            level="info",
            link=f"/indy_hub/material-exchange/my-orders/buy/{order.id}/",
        )

    contracts = ESIContract.objects.filter(
        corporation_id=config.corporation_id,
        contract_type="item_exchange",
    ).prefetch_related("items")

    if not contracts.exists():
        logger.warning(
            "No cached contracts found for corporation %s. "
            "Run sync_esi_contracts task first.",
            config.corporation_id,
        )
        return

    logger.info(
        "Validating %s pending buy orders against %s cached contracts",
        pending_orders.count(),
        contracts.count(),
    )

    try:
        esi_client = shared_client
    except Exception:
        esi_client = None
        logger.warning("ESI client not available for structure name lookups")

    for order in pending_orders:
        try:
            _validate_buy_order_from_db(config, order, contracts, esi_client)
        except Exception as exc:
            logger.error(
                "Error validating buy order %s: %s",
                order.id,
                exc,
                exc_info=True,
            )


def _validate_sell_order_from_db(config, order, contracts, esi_client=None):
    """
    Validate a single sell order against cached database contracts.

    Contract matching criteria:
    - type = item_exchange
    - issuer_id = seller's main character
    - assignee_id = config.corporation_id (recipient)
    - start_location_id or end_location_id = structure_id (matched by name if available)
    - items match exactly
    - price matches
    """
    order_ref = order.order_reference or f"INDY-{order.id}"
    notify_admins_on_sell_anomaly = bool(
        getattr(config, "notify_admins_on_sell_anomaly", True)
    )
    (
        expected_sell_location_ids,
        expected_sell_location_names,
        expected_sell_locations_label,
    ) = _get_sell_order_expected_locations(order, config)
    finished_statuses = {"finished", "finished_issuer", "finished_contractor"}
    rejected_statuses = {"cancelled", "rejected", "failed", "expired", "deleted"}

    def _format_contract_location(
        start_location_id: int | None,
        end_location_id: int | None,
    ) -> str:
        location_ids: list[int] = []
        for raw in [start_location_id, end_location_id]:
            try:
                loc_id = int(raw or 0)
            except (TypeError, ValueError):
                continue
            if loc_id <= 0 or loc_id in location_ids:
                continue
            location_ids.append(loc_id)

        if not location_ids:
            return ""

        labels: list[str] = []
        for loc_id in location_ids:
            loc_name = _get_location_name(
                loc_id,
                esi_client,
                corporation_id=int(config.corporation_id),
            )
            if loc_name:
                labels.append(f"{loc_name} ({loc_id})")
            else:
                labels.append(f"Location {loc_id}")
        return " / ".join(labels)

    def _set_sell_order_validated(
        *,
        contract_id: int,
        contract_price,
        override: bool,
        contract_location: str = "",
    ):
        order.status = MaterialExchangeSellOrder.Status.VALIDATED
        order.contract_validated_at = timezone.now()
        order.esi_contract_id = contract_id

        if override:
            try:
                price_label = (
                    f"{Decimal(str(contract_price)).quantize(Decimal('1')):,.0f}"
                )
            except (InvalidOperation, TypeError):
                price_label = f"{order.total_price:,.0f}"
            order.notes = (
                f"Contract accepted in-game despite anomaly: {contract_id} @ "
                f"{price_label} ISK"
            )
        else:
            order.notes = (
                f"Contract validated: {contract_id} @ {order.total_price:,.0f} ISK"
            )

        order.save(
            update_fields=[
                "status",
                "esi_contract_id",
                "contract_validated_at",
                "notes",
                "updated_at",
            ]
        )

        if override:
            notify_user(
                order.seller,
                _("✅ Sell Order Accepted In-Game"),
                _(
                    f"Your sell order {order.order_reference} was in anomaly, but the corporation accepted contract #{contract_id} in-game. "
                    f"The order has been moved back to validated status."
                    + (f"\nLocation: {contract_location}" if contract_location else "")
                ),
                level="success",
                link=f"/indy_hub/material-exchange/my-orders/sell/{order.id}/",
            )

            _notify_material_exchange_admins(
                config,
                _("Sell Order Validated by In-Game Acceptance"),
                _(
                    f"{order.seller.username}'s anomalous order {order_ref} has been accepted in-game via contract #{contract_id}.\n"
                    f"Order moved to validated status."
                    + (f"\nLocation: {contract_location}" if contract_location else "")
                ),
                level="info",
                link=(
                    f"/indy_hub/material-exchange/my-orders/sell/{order.id}/"
                    f"?next=/indy_hub/material-exchange/%23admin-panel"
                ),
            )

            logger.info(
                "Sell order %s validated by in-game acceptance of anomalous contract %s",
                order.id,
                contract_id,
            )
            emit_analytics_event(
                task="material_exchange.sell_order_validated",
                label="override_in_game_accept",
                result="success",
            )
            return

        notify_user(
            order.seller,
            _("✅ Sell Order Validated"),
            _(
                f"Your sell order {order.order_reference} has been validated!\n"
                f"Contract #{contract_id} for {order.total_price:,.0f} ISK verified.\n\n"
                + (f"Location: {contract_location}\n" if contract_location else "")
                + f"Status: Awaiting corporation to accept the contract.\n"
                f"Once accepted, you will receive payment."
            ),
            level="success",
            link=f"/indy_hub/material-exchange/my-orders/sell/{order.id}/",
        )

        _notify_material_exchange_admins(
            config,
            _("Sell Order Validated"),
            _(
                f"{order.seller.username} has created a contract and it has been validated as correct.\n"
                f"Total: {order.total_price:,.0f} ISK\n"
                f"Contract #{contract_id} verified from database.\n"
                + (f"Location: {contract_location}\n" if contract_location else "")
                + "\n"
                f"Awaiting corporation to accept the contract."
            ),
            level="success",
            link=(
                f"/indy_hub/material-exchange/my-orders/sell/{order.id}/"
                f"?next=/indy_hub/material-exchange/%23admin-panel"
            ),
        )

        logger.info(
            "Sell order %s validated: contract %s verified",
            order.id,
            contract_id,
        )
        emit_analytics_event(
            task="material_exchange.sell_order_validated",
            label="standard",
            result="success",
        )

    def _set_sell_order_anomaly_rejected(*, contract_id: int, contract_status: str):
        anomaly_rejected_notes = (
            f"Anomaly contract {contract_id} was {contract_status} in-game. "
            "Order remains open so user can submit a new compliant contract."
        )
        anomaly_rejected_updated = (
            order.status != MaterialExchangeSellOrder.Status.ANOMALY_REJECTED
            or order.notes != anomaly_rejected_notes
        )

        order.status = MaterialExchangeSellOrder.Status.ANOMALY_REJECTED
        order.notes = anomaly_rejected_notes
        order.save(update_fields=["status", "notes", "updated_at"])

        if anomaly_rejected_updated:
            notify_user(
                order.seller,
                _("Sell Order: Contract Refused In-Game"),
                _(
                    f"Contract #{contract_id} linked to your sell order {order_ref} was {contract_status} in-game.\n\n"
                    f"Your order is NOT cancelled in Auth. Please create a new compliant contract with the same order reference."
                ),
                level="warning",
                link=f"/indy_hub/material-exchange/my-orders/sell/{order.id}/",
            )

        logger.warning(
            "Sell order %s moved to anomaly_rejected: contract %s status is %s",
            order.id,
            contract_id,
            contract_status,
        )
        emit_analytics_event(
            task="material_exchange.sell_order_anomaly_rejected",
            label=contract_status,
            result="warning",
        )

    # Find seller's characters
    seller_character_ids = _get_user_character_ids(order.seller)
    if not seller_character_ids:
        logger.warning(
            "Sell order %s: seller %s has no character", order.id, order.seller
        )
        anomaly_notes = "Anomaly: seller has no linked EVE character"
        anomaly_updated = (
            order.status != MaterialExchangeSellOrder.Status.ANOMALY
            or order.notes != anomaly_notes
        )
        order.status = MaterialExchangeSellOrder.Status.ANOMALY
        order.notes = anomaly_notes
        order.save(update_fields=["status", "notes", "updated_at"])

        if anomaly_updated:
            notify_user(
                order.seller,
                _("Sell Order Error"),
                _(
                    "Your sell order cannot be validated: no linked EVE character found."
                ),
                level="warning",
            )

        if notify_admins_on_sell_anomaly and anomaly_updated:
            _notify_material_exchange_admins(
                config,
                _("Material Hub Order Requires Intervention"),
                _(
                    f"Order {order_ref} requires your intervention.\n"
                    f"Please contact user {order.seller.username} regarding this anomaly: seller has no linked EVE character."
                ),
                level="warning",
                link=(
                    f"/indy_hub/material-exchange/my-orders/sell/{order.id}/"
                    f"?next=/indy_hub/material-exchange/%23admin-panel"
                ),
            )
        return

    items_list = "\n".join(
        f"- {item.type_name}: {item.quantity}x @ {item.unit_price:,.2f} ISK each"
        for item in order.items.all()
    )

    matching_contract = None
    last_price_issue: str | None = None
    last_reason: str | None = None
    contract_with_correct_ref_wrong_structure: dict | None = None
    contract_with_correct_ref_wrong_price: dict | None = None
    contract_with_correct_ref_items_mismatch: dict | None = None
    contract_with_wrong_ref_only: dict | None = None

    order_ref_lower = str(order_ref or "").strip().lower()
    for contract in contracts:
        # Track contracts with correct order reference in title (for better diagnostics)
        title = str(contract.title or "")
        has_correct_ref = bool(order_ref_lower and order_ref_lower in title.lower())

        # Basic criteria
        criteria_match = _matches_sell_order_criteria_db(
            contract,
            order,
            config,
            seller_character_ids,
            esi_client,
            expected_location_ids=expected_sell_location_ids,
            expected_location_names=expected_sell_location_names,
        )
        if not criteria_match:
            # Store contract info if it has correct ref but wrong structure
            if has_correct_ref and not contract_with_correct_ref_wrong_structure:
                contract_with_correct_ref_wrong_structure = {
                    "contract_id": contract.contract_id,
                    "issue": "structure location mismatch",
                    "start_location_id": contract.start_location_id,
                    "end_location_id": contract.end_location_id,
                    "status": contract.status,
                    "price": contract.price,
                }
            continue

        # Items check
        if not _contract_items_match_order_db(contract, order):
            last_reason = "items mismatch"
            mismatch_details = _build_items_mismatch_details(contract, order)
            _missing_by_type, surplus_by_type, mismatch_type_names = (
                _get_items_mismatch_breakdown(contract, order)
            )
            if has_correct_ref and not contract_with_correct_ref_items_mismatch:
                contract_with_correct_ref_items_mismatch = {
                    "contract_id": contract.contract_id,
                    "issue": "items mismatch",
                    "status": contract.status,
                    "price": contract.price,
                    "start_location_id": contract.start_location_id,
                    "end_location_id": contract.end_location_id,
                    "details": mismatch_details,
                    "surplus_type_ids": sorted(
                        int(type_id) for type_id in surplus_by_type.keys()
                    ),
                    "type_names": {
                        str(int(type_id)): str(name)
                        for type_id, name in (mismatch_type_names or {}).items()
                        if str(name).strip()
                    },
                }
            continue

        # Price check
        price_ok, price_msg = _contract_price_matches_db(contract, order)
        if not price_ok:
            last_price_issue = price_msg
            last_reason = price_msg
            if has_correct_ref and not contract_with_correct_ref_wrong_price:
                contract_with_correct_ref_wrong_price = {
                    "contract_id": contract.contract_id,
                    "price_msg": price_msg,
                    "contract_price": contract.price,
                    "expected_price": order.total_price,
                    "status": contract.status,
                    "start_location_id": contract.start_location_id,
                    "end_location_id": contract.end_location_id,
                }
            continue

        # Strict full match (issuer/assignee/location/items/price)
        if has_correct_ref:
            matching_contract = contract
            break

        # Near match: all strict fields match but order reference is missing/wrong
        if not contract_with_wrong_ref_only:
            contract_with_wrong_ref_only = {
                "contract_id": contract.contract_id,
                "title": title,
                "status": contract.status,
                "price": contract.price,
            }

    if matching_contract:
        matching_contract_location = _format_contract_location(
            matching_contract.start_location_id,
            matching_contract.end_location_id,
        )
        _set_sell_order_validated(
            contract_id=matching_contract.contract_id,
            contract_price=matching_contract.price,
            override=False,
            contract_location=matching_contract_location,
        )
    elif contract_with_correct_ref_wrong_structure:
        contract_status = str(
            contract_with_correct_ref_wrong_structure.get("status") or ""
        ).lower()
        if contract_status in finished_statuses:
            contract_location = _format_contract_location(
                contract_with_correct_ref_wrong_structure.get("start_location_id"),
                contract_with_correct_ref_wrong_structure.get("end_location_id"),
            )
            _set_sell_order_validated(
                contract_id=contract_with_correct_ref_wrong_structure["contract_id"],
                contract_price=contract_with_correct_ref_wrong_structure.get("price"),
                override=True,
                contract_location=contract_location,
            )
            return
        if contract_status in rejected_statuses:
            _set_sell_order_anomaly_rejected(
                contract_id=contract_with_correct_ref_wrong_structure["contract_id"],
                contract_status=contract_status,
            )
            return

        # Contract found with correct title but wrong structure
        anomaly_notes = (
            f"Anomaly: contract {contract_with_correct_ref_wrong_structure['contract_id']} has the correct title ({order_ref}) "
            f"but wrong location. Expected: {expected_sell_locations_label}\n"
            f"Contract is at location {contract_with_correct_ref_wrong_structure.get('start_location_id') or contract_with_correct_ref_wrong_structure.get('end_location_id')}"
        )
        anomaly_updated = (
            order.status != MaterialExchangeSellOrder.Status.ANOMALY
            or order.notes != anomaly_notes
        )
        order.status = MaterialExchangeSellOrder.Status.ANOMALY
        order.notes = anomaly_notes
        order.save(update_fields=["status", "notes", "updated_at"])

        admin_link = (
            f"/indy_hub/material-exchange/my-orders/sell/{order.id}/"
            f"?next=/indy_hub/material-exchange/%23admin-panel"
        )

        if anomaly_updated:
            notify_user(
                order.seller,
                _("Sell Order Anomaly: Wrong Contract Location"),
                (
                    _(
                        f"Your sell order {order_ref} is in anomaly status.\n\n"
                        f"You submitted contract #{contract_with_correct_ref_wrong_structure['contract_id']} which has the correct title, "
                        f"but it's located at the wrong structure.\n\n"
                        f"Required location(s): {expected_sell_locations_label}\n"
                        f"Your contract is at location {contract_with_correct_ref_wrong_structure.get('start_location_id') or contract_with_correct_ref_wrong_structure.get('end_location_id')}\n\n"
                        f"You can either create a new contract at the correct location, or contact a Material Hub admin (they have been notified)."
                    )
                    if notify_admins_on_sell_anomaly
                    else _(
                        f"Your sell order {order_ref} is in anomaly status.\n\n"
                        f"You submitted contract #{contract_with_correct_ref_wrong_structure['contract_id']} which has the correct title, "
                        f"but it's located at the wrong structure.\n\n"
                        f"Required location(s): {expected_sell_locations_label}\n"
                        f"Your contract is at location {contract_with_correct_ref_wrong_structure.get('start_location_id') or contract_with_correct_ref_wrong_structure.get('end_location_id')}\n\n"
                        f"Please create a new compliant contract at the correct location."
                    )
                ),
                level="warning",
                link=f"/indy_hub/material-exchange/my-orders/sell/{order.id}/",
            )

        if notify_admins_on_sell_anomaly and anomaly_updated:
            _notify_material_exchange_admins(
                config,
                _("Material Hub Order Requires Intervention"),
                _(
                    f"Order {order_ref} requires your intervention.\n"
                    f"Please contact user {order.seller.username} regarding this anomaly: wrong contract location."
                ),
                level="warning",
                link=admin_link,
            )

        logger.warning(
            "Sell order %s anomaly: contract %s has correct title but wrong structure",
            order.id,
            contract_with_correct_ref_wrong_structure["contract_id"],
        )
    elif contract_with_correct_ref_wrong_price:
        contract_status = str(
            contract_with_correct_ref_wrong_price.get("status") or ""
        ).lower()
        if contract_status in finished_statuses:
            contract_location = _format_contract_location(
                contract_with_correct_ref_wrong_price.get("start_location_id"),
                contract_with_correct_ref_wrong_price.get("end_location_id"),
            )
            _set_sell_order_validated(
                contract_id=contract_with_correct_ref_wrong_price["contract_id"],
                contract_price=contract_with_correct_ref_wrong_price.get(
                    "contract_price"
                ),
                override=True,
                contract_location=contract_location,
            )
            return
        if contract_status in rejected_statuses:
            _set_sell_order_anomaly_rejected(
                contract_id=contract_with_correct_ref_wrong_price["contract_id"],
                contract_status=contract_status,
            )
            return

        anomaly_notes = (
            f"Anomaly: contract {contract_with_correct_ref_wrong_price['contract_id']} has the correct title ({order_ref}) "
            f"but wrong price ({contract_with_correct_ref_wrong_price['price_msg']})."
        )
        anomaly_updated = (
            order.status != MaterialExchangeSellOrder.Status.ANOMALY
            or order.notes != anomaly_notes
        )
        order.status = MaterialExchangeSellOrder.Status.ANOMALY
        order.notes = anomaly_notes
        order.save(update_fields=["status", "notes", "updated_at"])

        admin_link = (
            f"/indy_hub/material-exchange/my-orders/sell/{order.id}/"
            f"?next=/indy_hub/material-exchange/%23admin-panel"
        )

        expected_value = contract_with_correct_ref_wrong_price.get("expected_price")
        contract_value = contract_with_correct_ref_wrong_price.get("contract_price")
        try:
            expected_price = (
                f"{Decimal(str(expected_value)).quantize(Decimal('1')):,.0f} ISK"
            )
        except (InvalidOperation, TypeError):
            expected_price = str(expected_value)

        try:
            contract_price = (
                f"{Decimal(str(contract_value)).quantize(Decimal('1')):,.0f} ISK"
            )
        except (InvalidOperation, TypeError):
            contract_price = str(contract_value)

        if anomaly_updated:
            notify_user(
                order.seller,
                _("Sell Order Anomaly: Price Mismatch"),
                (
                    _(
                        f"Your sell order {order_ref} is in anomaly status.\n\n"
                        f"You submitted contract #{contract_with_correct_ref_wrong_price['contract_id']} with the correct title, but the price does not match the agreed total.\n\n"
                        f"Expected price: {expected_price}\n"
                        f"Contract price: {contract_price}\n\n"
                        f"You can either create a new contract with the correct price at {expected_sell_locations_label}, or wait for admin review (admins have been notified)."
                    )
                    if notify_admins_on_sell_anomaly
                    else _(
                        f"Your sell order {order_ref} is in anomaly status.\n\n"
                        f"You submitted contract #{contract_with_correct_ref_wrong_price['contract_id']} with the correct title, but the price does not match the agreed total.\n\n"
                        f"Expected price: {expected_price}\n"
                        f"Contract price: {contract_price}\n\n"
                        f"Please create a new compliant contract with the correct price at {expected_sell_locations_label}."
                    )
                ),
                level="warning",
                link=f"/indy_hub/material-exchange/my-orders/sell/{order.id}/",
            )

        if notify_admins_on_sell_anomaly and anomaly_updated:
            _notify_material_exchange_admins(
                config,
                _("Material Hub Order Requires Intervention"),
                _(
                    f"Order {order_ref} requires your intervention.\n"
                    f"Please contact user {order.seller.username} regarding this anomaly: contract price mismatch."
                ),
                level="warning",
                link=admin_link,
            )

        logger.warning(
            "Sell order %s anomaly: contract %s has correct title but wrong price (%s)",
            order.id,
            contract_with_correct_ref_wrong_price["contract_id"],
            contract_with_correct_ref_wrong_price["price_msg"],
        )
    elif contract_with_correct_ref_items_mismatch:
        contract_status = str(
            contract_with_correct_ref_items_mismatch.get("status") or ""
        ).lower()
        if contract_status in finished_statuses:
            contract_location = _format_contract_location(
                contract_with_correct_ref_items_mismatch.get("start_location_id"),
                contract_with_correct_ref_items_mismatch.get("end_location_id"),
            )
            _set_sell_order_validated(
                contract_id=contract_with_correct_ref_items_mismatch["contract_id"],
                contract_price=contract_with_correct_ref_items_mismatch.get("price"),
                override=True,
                contract_location=contract_location,
            )
            return
        if contract_status in rejected_statuses:
            _set_sell_order_anomaly_rejected(
                contract_id=contract_with_correct_ref_items_mismatch["contract_id"],
                contract_status=contract_status,
            )
            return

        mismatch_type_names_raw = contract_with_correct_ref_items_mismatch.get(
            "type_names"
        ) or {}
        mismatch_type_names: dict[int, str] = {}
        if isinstance(mismatch_type_names_raw, dict):
            for raw_type_id, raw_name in mismatch_type_names_raw.items():
                try:
                    parsed_type_id = int(raw_type_id)
                except (TypeError, ValueError):
                    continue
                parsed_name = str(raw_name or "").strip()
                if parsed_name:
                    mismatch_type_names[parsed_type_id] = parsed_name

        surplus_type_ids = []
        for raw_type_id in (
            contract_with_correct_ref_items_mismatch.get("surplus_type_ids") or []
        ):
            try:
                parsed_type_id = int(raw_type_id)
            except (TypeError, ValueError):
                continue
            if parsed_type_id <= 0:
                continue
            if parsed_type_id not in surplus_type_ids:
                surplus_type_ids.append(parsed_type_id)

        effective_contract_location_id = _get_effective_contract_location_id(
            start_location_id=contract_with_correct_ref_items_mismatch.get(
                "start_location_id"
            ),
            end_location_id=contract_with_correct_ref_items_mismatch.get(
                "end_location_id"
            ),
            expected_location_ids=expected_sell_location_ids,
        )
        location_guidance_block = _build_sell_surplus_item_location_guidance(
            config=config,
            contract_location_id=effective_contract_location_id,
            surplus_type_ids=surplus_type_ids,
            type_names=mismatch_type_names,
        )

        anomaly_notes = (
            f"Anomaly: contract {contract_with_correct_ref_items_mismatch['contract_id']} has the correct title ({order_ref}) "
            f"but item list/quantities do not match this order."
            + (
                f"\n\n{contract_with_correct_ref_items_mismatch.get('details')}"
                if contract_with_correct_ref_items_mismatch.get("details")
                else ""
            )
            + (f"\n\n{location_guidance_block}" if location_guidance_block else "")
        )
        mismatch_details_block = (
            f"{contract_with_correct_ref_items_mismatch.get('details')}\n\n"
            if contract_with_correct_ref_items_mismatch.get("details")
            else ""
        )
        guidance_details_block = (
            f"{location_guidance_block}\n\n" if location_guidance_block else ""
        )
        anomaly_updated = (
            order.status != MaterialExchangeSellOrder.Status.ANOMALY
            or order.notes != anomaly_notes
        )
        order.status = MaterialExchangeSellOrder.Status.ANOMALY
        order.notes = anomaly_notes
        order.save(update_fields=["status", "notes", "updated_at"])

        admin_link = (
            f"/indy_hub/material-exchange/my-orders/sell/{order.id}/"
            f"?next=/indy_hub/material-exchange/%23admin-panel"
        )

        if anomaly_updated:
            seller_message = (
                _(
                    f"Your sell order {order_ref} is in anomaly status.\n\n"
                    f"Contract #{contract_with_correct_ref_items_mismatch['contract_id']} has the correct reference, but item list/quantities do not match this order.\n\n"
                    f"{mismatch_details_block}"
                    f"{guidance_details_block}"
                    "Please create a corrected contract, or contact a Material Hub admin (they have been notified)."
                )
                if notify_admins_on_sell_anomaly
                else _(
                    f"Your sell order {order_ref} is in anomaly status.\n\n"
                    f"Contract #{contract_with_correct_ref_items_mismatch['contract_id']} has the correct reference, but item list/quantities do not match this order.\n\n"
                    f"{mismatch_details_block}"
                    f"{guidance_details_block}"
                    "Please create a corrected and compliant contract."
                )
            )
            notify_user(
                order.seller,
                _("Sell Order Anomaly: Items Mismatch"),
                seller_message,
                level="warning",
                link=f"/indy_hub/material-exchange/my-orders/sell/{order.id}/",
            )

        if notify_admins_on_sell_anomaly and anomaly_updated:
            _notify_material_exchange_admins(
                config,
                _("Material Hub Order Requires Intervention"),
                _(
                    f"Order {order_ref} requires your intervention.\n"
                    f"Please contact user {order.seller.username} regarding this anomaly: contract items mismatch."
                    + (
                        f"\n\n{contract_with_correct_ref_items_mismatch.get('details')}"
                        if contract_with_correct_ref_items_mismatch.get("details")
                        else ""
                    )
                    + (
                        f"\n\n{location_guidance_block}"
                        if location_guidance_block
                        else ""
                    )
                ),
                level="warning",
                link=admin_link,
            )

        logger.warning(
            "Sell order %s anomaly: contract %s has correct title but items mismatch",
            order.id,
            contract_with_correct_ref_items_mismatch["contract_id"],
        )
        emit_analytics_event(
            task="material_exchange.sell_order_items_mismatch",
            label="correct_ref",
            result="warning",
        )
    elif contract_with_wrong_ref_only:
        contract_id = contract_with_wrong_ref_only["contract_id"]
        found_title = (contract_with_wrong_ref_only.get("title") or "").strip()
        title_display = found_title or _("(empty title)")
        contract_status = str(contract_with_wrong_ref_only.get("status") or "").lower()

        if contract_status in finished_statuses:
            _set_sell_order_validated(
                contract_id=contract_id,
                contract_price=contract_with_wrong_ref_only.get("price"),
                override=True,
            )
            return

        if contract_status in rejected_statuses:
            _set_sell_order_anomaly_rejected(
                contract_id=contract_id,
                contract_status=contract_status,
            )
            return

        anomaly_notes = (
            f"Anomaly: contract {contract_id} matches seller/corp/location/items/price but title reference is incorrect. "
            f"Found title: '{title_display}'. Expected reference: '{order_ref}'."
        )
        anomaly_updated = (
            order.status != MaterialExchangeSellOrder.Status.ANOMALY
            or order.notes != anomaly_notes
        )
        order.status = MaterialExchangeSellOrder.Status.ANOMALY
        order.notes = anomaly_notes
        order.save(update_fields=["status", "notes", "updated_at"])

        if anomaly_updated:
            notify_user(
                order.seller,
                _("Sell Order Anomaly: Wrong Contract Reference"),
                _(
                    f"We found contract #{contract_id} that matches your sell order items, structure and price, "
                    f"but the title/reference is incorrect.\n\n"
                    f"Found title: {title_display}\n"
                    f"Expected reference: {order_ref}\n\n"
                    f"Please recreate/update the contract title with the exact order reference."
                ),
                level="warning",
                link=f"/indy_hub/material-exchange/my-orders/sell/{order.id}/",
            )

            if notify_admins_on_sell_anomaly:
                _notify_material_exchange_admins(
                    config,
                    _("Material Hub Order Requires Intervention"),
                    _(
                        f"Order {order_ref} has a near-match contract #{contract_id} with wrong reference in title.\n"
                        f"Found title: {title_display}\n"
                        f"Expected reference: {order_ref}\n"
                        f"Please contact user {order.seller.username}."
                    ),
                    level="warning",
                    link=(
                        f"/indy_hub/material-exchange/my-orders/sell/{order.id}/"
                        f"?next=/indy_hub/material-exchange/%23admin-panel"
                    ),
                )

        logger.warning(
            "Sell order %s anomaly: contract %s near-match found but wrong title reference (found=%r expected=%r)",
            order.id,
            contract_id,
            title_display,
            order_ref,
        )
    else:
        # No contract found - only notify if status is changing or notes have significantly changed
        new_notes = (
            "Waiting for matching contract. Please create an item exchange contract with:\n"
            f"- Title including {order_ref}\n"
            f"- Recipient (assignee): {_get_corp_name(config.corporation_id)}\n"
            f"- Location(s): {expected_sell_locations_label}\n"
            f"- Price: {order.total_price:,.0f} ISK\n"
            f"- Items: {', '.join(item.type_name for item in order.items.all())}"
            + (f"\nLast checked issue: {last_price_issue}" if last_price_issue else "")
        )

        # Only notify on first pending status (when notes change significantly)
        notes_changed = order.notes != new_notes
        order.notes = new_notes
        order.save(update_fields=["notes", "updated_at"])

        reminder_key = f"material_exchange:sell_order:{order.id}:contract_reminder"
        now = timezone.now()
        reminder_set = cache.add(reminder_key, now.timestamp(), 60 * 60 * 24)
        if notes_changed:
            cache.set(reminder_key, now.timestamp(), 60 * 60 * 24)

        delete_link = f"/indy_hub/material-exchange/my-orders/sell/{order.id}/delete/"

        should_notify = False
        if notes_changed or reminder_set:
            created_at = getattr(order, "created_at", None)
            if created_at:
                should_notify = now - created_at >= timedelta(hours=24)
            else:
                should_notify = True

        if should_notify:
            notify_user(
                order.seller,
                _("Sell Order Pending: waiting for contract"),
                _(
                    f"We still don't see a matching contract for your sell order {order_ref}.\n"
                    f"Please submit an item exchange contract matching the requirements above."
                    + (f"\nLatest issue seen: {last_reason}" if last_reason else "")
                    + "\n\nDon't need this order anymore? You can delete it from your orders page."
                ),
                level="warning",
                link=delete_link,
            )

        logger.info("Sell order %s pending: no matching contract yet", order.id)


def _validate_buy_order_from_db(config, order, contracts, esi_client=None):
    """Validate a single buy order against cached database contracts."""

    order_ref = order.order_reference or f"INDY-{order.id}"
    finished_statuses = {"finished", "finished_issuer", "finished_contractor"}
    (
        expected_buy_location_ids,
        expected_buy_location_names,
        expected_buy_location_label,
    ) = _get_buy_order_expected_locations(order, config)

    buyer_character_ids = _get_user_character_ids(order.buyer)
    if not buyer_character_ids:
        logger.warning("Buy order %s: buyer %s has no character", order.id, order.buyer)
        notify_user(
            order.buyer,
            _("Buy Order Error"),
            _("Your buy order cannot be validated: no linked EVE character found."),
            level="warning",
        )
        order.status = MaterialExchangeBuyOrder.Status.REJECTED
        order.notes = "Buyer has no linked EVE character"
        order.save(update_fields=["status", "notes", "updated_at"])
        return

    items_list = "\n".join(
        f"- {item.type_name}: {item.quantity}x @ {item.unit_price:,.2f} ISK each"
        for item in order.items.all()
    )

    matching_contract = None
    finished_contract_ref_mismatch = None
    finished_contract_items_mismatch = None
    finished_contract_price_mismatch = None
    finished_contract_criteria_mismatch = None
    finished_contract_items_mismatch_details: str | None = None
    last_price_issue: str | None = None
    last_reason: str | None = None
    last_items_mismatch_details: str | None = None

    def _set_buy_order_validated(
        contract,
        *,
        override: bool,
        override_reason: str = "",
        override_details: str | None = None,
    ):
        now = timezone.now()

        order.status = MaterialExchangeBuyOrder.Status.VALIDATED
        order.contract_validated_at = now
        order.esi_contract_id = contract.contract_id

        if override:
            order.notes = (
                f"Contract accepted in-game despite anomaly: {contract.contract_id} @ "
                f"{contract.price:,.0f} ISK"
                + (f" ({override_reason})" if override_reason else "")
                + (f"\n\n{override_details}" if override_details else "")
            )
        else:
            order.notes = (
                f"Contract validated: {contract.contract_id} @ "
                f"{contract.price:,.0f} ISK"
            )

        order.save(
            update_fields=[
                "status",
                "esi_contract_id",
                "contract_validated_at",
                "notes",
                "updated_at",
            ]
        )

        order.items.update(
            esi_contract_id=contract.contract_id,
            esi_contract_validated=True,
            esi_validation_checked_at=now,
        )

        if override:
            notify_user(
                order.buyer,
                _("✅ Buy Order Accepted In-Game"),
                _(
                    f"Your buy order {order.order_reference} had a validation anomaly, but contract #{contract.contract_id} was accepted in-game. "
                    f"The order has been moved back to validated status and completion sync will follow."
                    + (f"\n\n{override_details}" if override_details else "")
                ),
                level="success",
                link=f"/indy_hub/material-exchange/my-orders/buy/{order.id}/",
            )

            _notify_material_exchange_admins(
                config,
                _("Buy Order Validated by In-Game Acceptance"),
                _(
                    f"{order.buyer.username}'s anomalous buy order {order_ref} has been accepted in-game via contract #{contract.contract_id}.\n"
                    f"Order moved to validated status."
                    + (f"\n\n{override_details}" if override_details else "")
                ),
                level="info",
                link=(
                    f"/indy_hub/material-exchange/my-orders/buy/{order.id}/"
                    f"?next=/indy_hub/material-exchange/%23admin-panel"
                ),
            )

            logger.info(
                "Buy order %s validated by in-game acceptance of anomalous contract %s (%s)",
                order.id,
                contract.contract_id,
                override_reason or "no reason",
            )
            emit_analytics_event(
                task="material_exchange.buy_order_validated",
                label=f"override_{override_reason or 'unknown'}",
                result="success",
            )
            return

        notify_user(
            order.buyer,
            _("Buy Contract Created"),
            _(
                f"The corporation created your in-game contract for buy order {order.order_reference}.\n"
                f"Contract #{contract.contract_id} for {order.total_price:,.0f} ISK is now available.\n\n"
                f"Next step: accept the in-game contract to receive your items.\n"
                f"You will receive another notification once delivery is completed."
            ),
            level="success",
        )

        _notify_material_exchange_admins(
            config,
            _("Buy Contract Created"),
            _(
                f"Buy contract created for {order.buyer.username}.\n"
                f"Order: {order.order_reference}\n"
                f"Contract: #{contract.contract_id}\n"
                f"Total: {order.total_price:,.0f} ISK\n\n"
            ),
            level="success",
            link=(
                f"/indy_hub/material-exchange/my-orders/buy/{order.id}/"
                f"?next=/indy_hub/material-exchange/%23admin-panel"
            ),
        )

        logger.info(
            "Buy order %s validated: contract %s verified",
            order.id,
            contract.contract_id,
        )
        emit_analytics_event(
            task="material_exchange.buy_order_validated",
            label="standard",
            result="success",
        )

    order_ref_lower = str(order_ref or "").strip().lower()
    for contract in contracts:
        title = str(contract.title or "")
        has_correct_ref = bool(order_ref_lower and order_ref_lower in title.lower())

        if not has_correct_ref:
            criteria_match_without_ref = _matches_buy_order_criteria_db(
                contract,
                order,
                config,
                buyer_character_ids,
                esi_client,
                expected_location_ids=expected_buy_location_ids,
                expected_location_names=expected_buy_location_names,
            )
            if criteria_match_without_ref and _contract_items_match_order_db(
                contract, order
            ):
                price_ok_without_ref, _price_msg_unused = _contract_price_matches_db(
                    contract, order
                )
                if price_ok_without_ref:
                    contract_status = str(contract.status or "").lower()
                    if (
                        contract_status in finished_statuses
                        and finished_contract_ref_mismatch is None
                    ):
                        finished_contract_ref_mismatch = contract
                        last_reason = "wrong contract reference"

        # Require title reference before further checks.
        if not has_correct_ref:
            continue

        criteria_match = _matches_buy_order_criteria_db(
            contract,
            order,
            config,
            buyer_character_ids,
            esi_client,
            expected_location_ids=expected_buy_location_ids,
            expected_location_names=expected_buy_location_names,
        )
        if not criteria_match:
            contract_status = str(contract.status or "").lower()
            if (
                contract_status in finished_statuses
                and finished_contract_criteria_mismatch is None
            ):
                finished_contract_criteria_mismatch = contract
                last_reason = "contract criteria mismatch"
            continue

        if not _contract_items_match_order_db(contract, order):
            last_reason = "items mismatch"
            mismatch_details = _build_items_mismatch_details(contract, order)
            if mismatch_details and last_items_mismatch_details is None:
                last_items_mismatch_details = mismatch_details
            contract_status = str(contract.status or "").lower()
            if (
                contract_status in finished_statuses
                and finished_contract_items_mismatch is None
            ):
                finished_contract_items_mismatch = contract
                finished_contract_items_mismatch_details = mismatch_details
            continue

        price_ok, price_msg = _contract_price_matches_db(contract, order)
        if not price_ok:
            last_price_issue = price_msg
            last_reason = price_msg
            contract_status = str(contract.status or "").lower()
            if (
                contract_status in finished_statuses
                and finished_contract_price_mismatch is None
            ):
                finished_contract_price_mismatch = contract
            continue

        matching_contract = contract
        break

    if matching_contract:
        _set_buy_order_validated(matching_contract, override=False)
        return

    if finished_contract_ref_mismatch:
        _set_buy_order_validated(
            finished_contract_ref_mismatch,
            override=True,
            override_reason="wrong contract reference",
        )
        return

    if finished_contract_criteria_mismatch:
        _set_buy_order_validated(
            finished_contract_criteria_mismatch,
            override=True,
            override_reason="contract criteria mismatch",
        )
        return

    if finished_contract_items_mismatch:
        _set_buy_order_validated(
            finished_contract_items_mismatch,
            override=True,
            override_reason="items mismatch",
            override_details=finished_contract_items_mismatch_details,
        )
        return

    if finished_contract_price_mismatch:
        _set_buy_order_validated(
            finished_contract_price_mismatch,
            override=True,
            override_reason="price mismatch",
        )
        return

    # No matching contract found yet
    issues: list[str] = []
    for issue in [last_price_issue, last_reason]:
        if issue and issue not in issues:
            issues.append(issue)

    issue_line = f"Issue(s): {'; '.join(issues)}" if issues else ""
    mismatch_block = (
        f"\n\n{last_items_mismatch_details}" if last_items_mismatch_details else ""
    )

    new_notes = "\n".join(
        [
            f"Pending contract for {order_ref}.",
            f"Required contract location: {expected_buy_location_label}",
            "Ensure corp issues item exchange contract to buyer at this location.",
            f"Expected price: {order.total_price:,.0f} ISK",
            f"{issue_line}{mismatch_block}".strip(),
        ]
    ).strip()

    notes_changed = order.notes != new_notes
    order.notes = new_notes
    order.save(update_fields=["notes", "updated_at"])

    now = timezone.now()
    immediate_issue_alert_sent = False
    if issues:
        issue_fingerprint = hashlib.sha1(
            (
                f"{order_ref}|{';'.join(issues)}|{last_items_mismatch_details or ''}"
            ).encode("utf-8")
        ).hexdigest()[:16]
        issue_alert_key = (
            f"material_exchange:buy_order:{order.id}:contract_issue:{issue_fingerprint}"
        )
        immediate_issue_alert_sent = cache.add(
            issue_alert_key, now.timestamp(), 60 * 60 * 24
        )

    if immediate_issue_alert_sent:
        _notify_material_exchange_admins(
            config,
            _("Buy Order Contract Issue Detected"),
            _(
                f"Buy order {order.order_reference} has a contract mismatch.\n"
                f"Buyer: {order.buyer.username}\n"
                f"Required location: {expected_buy_location_label}\n"
                f"Expected price: {order.total_price:,.0f} ISK"
                + (f"\nIssue(s): {'; '.join(issues)}" if issues else "")
                + (
                    f"\n\n{last_items_mismatch_details}"
                    if last_items_mismatch_details
                    else ""
                )
            ),
            level="warning",
            link=(
                f"/indy_hub/material-exchange/my-orders/buy/{order.id}/"
                f"?next=/indy_hub/material-exchange/%23admin-panel"
            ),
        )
        emit_analytics_event(
            task="material_exchange.buy_order_pending_mismatch",
            label="issues_present_immediate",
            result="warning",
        )

    reminder_key = f"material_exchange:buy_order:{order.id}:contract_reminder"
    reminder_set = cache.add(reminder_key, now.timestamp(), 60 * 60 * 24)
    if notes_changed:
        cache.set(reminder_key, now.timestamp(), 60 * 60 * 24)

    should_notify = False
    if reminder_set:
        created_at = getattr(order, "created_at", None)
        if created_at:
            should_notify = now - created_at >= timedelta(hours=24)
        else:
            should_notify = True

    if should_notify and not immediate_issue_alert_sent:
        _notify_material_exchange_admins(
            config,
            _("Buy Order Pending: contract mismatch"),
            _(
                f"Buy order {order.order_reference} has no matching contract yet.\n"
                f"Buyer: {order.buyer.username}\n"
                f"Required location: {expected_buy_location_label}\n"
                f"Expected price: {order.total_price:,.0f} ISK"
                + (f"\nIssue(s): {'; '.join(issues)}" if issues else "")
                + (
                    f"\n\n{last_items_mismatch_details}"
                    if last_items_mismatch_details
                    else ""
                )
            ),
            level="warning",
            link=(
                f"/indy_hub/material-exchange/my-orders/buy/{order.id}/"
                f"?next=/indy_hub/material-exchange/%23admin-panel"
            ),
        )
        emit_analytics_event(
            task="material_exchange.buy_order_pending_mismatch",
            label="issues_present" if issues else "no_issues",
            result="warning",
        )

    logger.info("Buy order %s pending: no matching contract yet", order.id)


def _get_location_match_mode(config: MaterialExchangeConfig) -> str:
    mode = (getattr(config, "location_match_mode", None) or "name_or_id").strip()
    if mode not in {"name_or_id", "strict_id"}:
        return "name_or_id"
    return mode


def _get_type_market_group_id(type_id: int) -> int | None:
    """Return ItemType.market_group_id_raw for the given type ID."""
    type_id_int = int(type_id)
    if type_id_int in _type_market_group_cache:
        return _type_market_group_cache[type_id_int]

    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import ItemType

        market_group_id = (
            ItemType.objects.filter(id=type_id_int)
            .values_list("market_group_id_raw", flat=True)
            .first()
        )
        market_group_value = int(market_group_id) if market_group_id else None
    except Exception:
        market_group_value = None

    _type_market_group_cache[type_id_int] = market_group_value
    return market_group_value


def _get_market_group_children_map() -> dict[int | None, set[int]]:
    """Return a parent->children map for market groups (cached)."""
    global _market_group_children_cache
    if _market_group_children_cache is not None:
        return _market_group_children_cache

    try:
        # AA Example App
        from indy_hub.models import SdeMarketGroup

        children_map: dict[int | None, set[int]] = {}
        for group_id, parent_id in SdeMarketGroup.objects.values_list(
            "id", "parent_id"
        ):
            children_map.setdefault(parent_id, set()).add(int(group_id))
    except Exception:
        children_map = {}

    _market_group_children_cache = children_map
    return children_map


def _expand_market_group_ids(group_ids: set[int]) -> set[int]:
    """Expand grouped market IDs to include all descendant groups."""
    if not group_ids:
        return set()

    cache_key = tuple(sorted(int(gid) for gid in group_ids if int(gid) > 0))
    if cache_key in _expanded_group_cache:
        return _expanded_group_cache[cache_key]

    children_map = _get_market_group_children_map()
    expanded = set(cache_key)
    stack = list(cache_key)

    while stack:
        current = int(stack.pop())
        for child_id in children_map.get(current, set()):
            child_int = int(child_id)
            if child_int in expanded:
                continue
            expanded.add(child_int)
            stack.append(child_int)

    _expanded_group_cache[cache_key] = expanded
    return expanded


def _normalize_group_ids(raw_group_ids: list[int] | tuple[int, ...] | set[int] | None) -> list[int]:
    normalized: list[int] = []
    for raw in raw_group_ids or []:
        try:
            group_id = int(raw)
        except (TypeError, ValueError):
            continue
        if group_id <= 0 or group_id in normalized:
            continue
        normalized.append(group_id)
    return normalized


def _get_sell_group_ids_for_location(
    config: MaterialExchangeConfig, location_id: int
) -> list[int] | None:
    """Return grouped sell market IDs for a location, or None for explicit all."""
    group_map = config.get_sell_market_group_map()
    location_key = int(location_id)
    if location_key in group_map:
        rule = group_map[location_key]
        if rule is None:
            return None
        return _normalize_group_ids(rule)

    return _normalize_group_ids(list(getattr(config, "allowed_market_groups_sell", []) or []))


def _is_type_accepted_for_sell_location(
    *,
    config: MaterialExchangeConfig,
    location_id: int,
    type_id: int,
) -> bool:
    """Return whether a type is accepted for selling at a specific location."""
    grouped_ids = _get_sell_group_ids_for_location(config, int(location_id))
    if grouped_ids is None:
        return True
    if not grouped_ids:
        return False

    market_group_id = _get_type_market_group_id(int(type_id))
    if not market_group_id:
        return False

    expanded_group_ids = _expand_market_group_ids(set(grouped_ids))
    return int(market_group_id) in expanded_group_ids


def _get_effective_contract_location_id(
    *,
    start_location_id: int | None,
    end_location_id: int | None,
    expected_location_ids: list[int] | None = None,
) -> int | None:
    """Pick the most relevant location ID for sell-location guidance.

    When location matching succeeds by name (name_or_id mode), contract start/end IDs
    may differ from configured sell location IDs for the same structure. In that case,
    prefer a configured expected location ID so market-group acceptance checks evaluate
    against configured location rules.
    """
    location_ids: list[int] = []
    for raw in [start_location_id, end_location_id]:
        try:
            loc_id = int(raw or 0)
        except (TypeError, ValueError):
            continue
        if loc_id <= 0 or loc_id in location_ids:
            continue
        location_ids.append(loc_id)

    if not location_ids:
        return None

    expected_ids: list[int] = []
    for raw in expected_location_ids or []:
        try:
            expected_id = int(raw or 0)
        except (TypeError, ValueError):
            continue
        if expected_id <= 0 or expected_id in expected_ids:
            continue
        expected_ids.append(expected_id)

    if expected_ids:
        expected_set = set(expected_ids)
        for loc_id in location_ids:
            if loc_id in expected_set:
                return loc_id
        # No contract ID match: use first configured expected location deterministically.
        return expected_ids[0]

    return location_ids[0]


def _build_sell_surplus_item_location_guidance(
    *,
    config: MaterialExchangeConfig,
    contract_location_id: int | None,
    surplus_type_ids: list[int],
    type_names: dict[int, str] | None = None,
) -> str:
    """Build guidance for surplus sell-contract items and accepted locations."""
    if not surplus_type_ids:
        return ""

    sell_location_ids = config.get_sell_structure_ids()
    if not sell_location_ids:
        return ""

    sell_name_map = config.get_sell_structure_name_map()
    guidance_lines: list[str] = []

    for raw_type_id in surplus_type_ids:
        type_id = int(raw_type_id)
        item_name = str((type_names or {}).get(type_id) or "").strip() or str(
            get_type_name(type_id) or f"Type {type_id}"
        )

        not_accepted_here = False
        if contract_location_id:
            not_accepted_here = not _is_type_accepted_for_sell_location(
                config=config,
                location_id=int(contract_location_id),
                type_id=type_id,
            )
        if not not_accepted_here:
            continue

        accepted_elsewhere: list[str] = []
        for raw_loc_id in sell_location_ids:
            loc_id = int(raw_loc_id)
            if contract_location_id and loc_id == int(contract_location_id):
                continue
            if _is_type_accepted_for_sell_location(
                config=config,
                location_id=loc_id,
                type_id=type_id,
            ):
                accepted_elsewhere.append(
                    str(sell_name_map.get(loc_id) or f"Structure {loc_id}")
                )

        if accepted_elsewhere:
            guidance_lines.append(
                f"- {item_name}: not accepted at this contract location; accepted at {', '.join(accepted_elsewhere)}."
            )
        else:
            guidance_lines.append(
                f"- {item_name}: not accepted at this contract location or any configured sell location."
            )

    if not guidance_lines:
        return ""

    return "Sell-location guidance:\n" + "\n".join(guidance_lines)


def _normalize_location_name(name: str | None) -> str:
    return str(name or "").strip().lower()


def _get_expected_location_ids(config: MaterialExchangeConfig, *, side: str) -> list[int]:
    if side == "sell":
        raw_ids = config.get_sell_structure_ids()
    elif side == "buy":
        raw_ids = config.get_buy_structure_ids()
    else:
        raw_ids = []

    normalized_ids: list[int] = []
    for raw in raw_ids:
        try:
            sid = int(raw)
        except (TypeError, ValueError):
            continue
        if sid <= 0 or sid in normalized_ids:
            continue
        normalized_ids.append(sid)
    return normalized_ids


def _get_expected_location_name_set(
    config: MaterialExchangeConfig, *, side: str
) -> set[str]:
    if side == "sell":
        name_map = config.get_sell_structure_name_map()
    elif side == "buy":
        name_map = config.get_buy_structure_name_map()
    else:
        name_map = {}

    names = {
        _normalize_location_name(name)
        for name in (name_map or {}).values()
        if _normalize_location_name(name)
    }
    return names


def _get_expected_locations_label(config: MaterialExchangeConfig, *, side: str) -> str:
    if side == "sell":
        name_map = config.get_sell_structure_name_map()
    elif side == "buy":
        name_map = config.get_buy_structure_name_map()
    else:
        name_map = {}

    labels = []
    for sid in _get_expected_location_ids(config, side=side):
        name = str((name_map or {}).get(int(sid), "") or "").strip()
        labels.append(name or f"Structure {sid}")

    return ", ".join(labels) if labels else (
        config.structure_name or f"Structure {config.structure_id}"
    )


def _get_sell_order_expected_locations(
    order: MaterialExchangeSellOrder, config: MaterialExchangeConfig
) -> tuple[list[int], set[str], str]:
    source_name = str(getattr(order, "source_location_name", "") or "").strip()
    source_location_id = None
    try:
        source_location_id = int(getattr(order, "source_location_id", 0) or 0)
    except (TypeError, ValueError):
        source_location_id = 0

    if source_location_id > 0:
        labels_name = source_name or f"Structure {source_location_id}"
        name_set = {_normalize_location_name(source_name)} if source_name else set()
        return [source_location_id], name_set, labels_name

    expected_ids = _get_expected_location_ids(config, side="sell")
    expected_name_set = _get_expected_location_name_set(config, side="sell")
    expected_label = _get_expected_locations_label(config, side="sell")
    return expected_ids, expected_name_set, expected_label


def _infer_buy_order_source_location_from_stock(
    order: MaterialExchangeBuyOrder, config: MaterialExchangeConfig
) -> tuple[int | None, str]:
    try:
        order_type_ids = {
            int(type_id)
            for type_id in order.items.values_list("type_id", flat=True)
            if int(type_id) > 0
        }
    except Exception:
        order_type_ids = set()
    if not order_type_ids:
        return None, ""

    source_ids_by_type: dict[int, set[int]] = {}
    try:
        stock_rows = config.stock_items.filter(type_id__in=list(order_type_ids)).values_list(
            "type_id", "source_structure_ids"
        )
    except Exception:
        stock_rows = []

    for type_id, source_structure_ids in stock_rows:
        try:
            type_id_int = int(type_id)
        except (TypeError, ValueError):
            continue
        ids_for_type: set[int] = set()
        for raw_id in source_structure_ids or []:
            try:
                structure_id = int(raw_id)
            except (TypeError, ValueError):
                continue
            if structure_id > 0:
                ids_for_type.add(structure_id)
        if ids_for_type:
            source_ids_by_type[type_id_int] = ids_for_type

    common_location_ids: set[int] | None = None
    for type_id in sorted(order_type_ids):
        ids_for_type = source_ids_by_type.get(int(type_id), set())
        if not ids_for_type:
            continue
        if common_location_ids is None:
            common_location_ids = set(ids_for_type)
        else:
            common_location_ids &= ids_for_type
        if not common_location_ids:
            return None, ""

    if not common_location_ids:
        return None, ""

    selected_location_id = sorted(common_location_ids)[0]
    name_map = config.get_buy_structure_name_map() or {}
    selected_location_name = str(
        name_map.get(int(selected_location_id), "") or ""
    ).strip()
    if not selected_location_name:
        selected_location_name = f"Structure {int(selected_location_id)}"
    return int(selected_location_id), selected_location_name


def _get_buy_order_expected_locations(
    order: MaterialExchangeBuyOrder, config: MaterialExchangeConfig
) -> tuple[list[int], set[str], str]:
    source_name = str(getattr(order, "source_location_name", "") or "").strip()
    source_location_id = None
    try:
        source_location_id = int(getattr(order, "source_location_id", 0) or 0)
    except (TypeError, ValueError):
        source_location_id = 0

    if source_location_id > 0:
        name_map = config.get_buy_structure_name_map() or {}
        mapped_name = str(name_map.get(source_location_id, "") or "").strip()
        label_name = source_name or mapped_name or f"Structure {source_location_id}"
        name_set = {
            normalized
            for normalized in [
                _normalize_location_name(source_name),
                _normalize_location_name(mapped_name),
            ]
            if normalized
        }
        return [source_location_id], name_set, label_name

    inferred_location_id, inferred_location_name = _infer_buy_order_source_location_from_stock(
        order,
        config,
    )
    if inferred_location_id and inferred_location_id > 0:
        inferred_label = str(inferred_location_name or "").strip() or (
            f"Structure {int(inferred_location_id)}"
        )
        inferred_name_set = {_normalize_location_name(inferred_label)}
        return [int(inferred_location_id)], inferred_name_set, inferred_label

    expected_ids = _get_expected_location_ids(config, side="buy")
    expected_name_set = _get_expected_location_name_set(config, side="buy")
    expected_label = _get_expected_locations_label(config, side="buy")
    return expected_ids, expected_name_set, expected_label


def _contract_matches_expected_locations(
    *,
    start_location_id: int | None,
    end_location_id: int | None,
    start_location_name: str | None,
    end_location_name: str | None,
    expected_location_ids: list[int],
    expected_location_names: set[str],
    match_mode: str,
) -> bool:
    expected_id_set = {int(sid) for sid in expected_location_ids if sid}
    if expected_id_set:
        try:
            if int(start_location_id or 0) in expected_id_set:
                return True
        except (TypeError, ValueError):
            pass
        try:
            if int(end_location_id or 0) in expected_id_set:
                return True
        except (TypeError, ValueError):
            pass

    if match_mode != "name_or_id":
        return False
    if not expected_location_names:
        return False

    start_name = _normalize_location_name(start_location_name)
    if start_name and start_name in expected_location_names:
        return True
    end_name = _normalize_location_name(end_location_name)
    if end_name and end_name in expected_location_names:
        return True

    return False


def _matches_sell_order_criteria_db(
    contract,
    order,
    config,
    seller_character_ids,
    esi_client=None,
    *,
    expected_location_ids: list[int] | None = None,
    expected_location_names: set[str] | None = None,
):
    """
    Check if a database contract matches sell order basic criteria.

    Location matching respects config.location_match_mode:
    - strict_id: contract start/end location must match configured sell IDs.
    - name_or_id: match by ID OR by resolved location name.
    """
    # Issuer must be the seller
    if contract.issuer_id not in seller_character_ids:
        return False

    # Assignee must be the corporation (recipient of the contract)
    if contract.assignee_id != config.corporation_id:
        return False

    location_match_mode = _get_location_match_mode(config)
    expected_ids = (
        list(expected_location_ids)
        if expected_location_ids is not None
        else _get_expected_location_ids(config, side="sell")
    )
    expected_name_set = (
        set(expected_location_names)
        if expected_location_names is not None
        else _get_expected_location_name_set(config, side="sell")
    )

    contract_start_name = None
    contract_end_name = None
    if location_match_mode == "name_or_id":
        contract_start_name = _get_location_name(
            contract.start_location_id,
            esi_client,
            corporation_id=int(config.corporation_id),
        )
        contract_end_name = _get_location_name(
            contract.end_location_id,
            esi_client,
            corporation_id=int(config.corporation_id),
        )

    return _contract_matches_expected_locations(
        start_location_id=contract.start_location_id,
        end_location_id=contract.end_location_id,
        start_location_name=contract_start_name,
        end_location_name=contract_end_name,
        expected_location_ids=expected_ids,
        expected_location_names=expected_name_set,
        match_mode=location_match_mode,
    )


def _matches_buy_order_criteria_db(
    contract,
    order,
    config,
    buyer_character_ids,
    esi_client=None,
    *,
    expected_location_ids: list[int] | None = None,
    expected_location_names: set[str] | None = None,
):
    """Check if a database contract matches buy order basic criteria."""

    # Issuer corporation must be the hub corporation
    issuer_corporation_id = int(getattr(contract, "issuer_corporation_id", 0) or 0)
    config_corporation_id = int(getattr(config, "corporation_id", 0) or 0)
    if issuer_corporation_id not in {0, config_corporation_id}:
        return False

    # Assignee must be one of the buyer's characters
    if contract.assignee_id not in buyer_character_ids:
        return False

    location_match_mode = _get_location_match_mode(config)
    expected_ids = (
        list(expected_location_ids)
        if expected_location_ids is not None
        else _get_expected_location_ids(config, side="buy")
    )
    expected_name_set = (
        set(expected_location_names)
        if expected_location_names is not None
        else _get_expected_location_name_set(config, side="buy")
    )

    contract_start_name = None
    contract_end_name = None
    if location_match_mode == "name_or_id":
        contract_start_name = _get_location_name(
            contract.start_location_id,
            esi_client,
            corporation_id=int(config.corporation_id),
        )
        contract_end_name = _get_location_name(
            contract.end_location_id,
            esi_client,
            corporation_id=int(config.corporation_id),
        )

    return _contract_matches_expected_locations(
        start_location_id=contract.start_location_id,
        end_location_id=contract.end_location_id,
        start_location_name=contract_start_name,
        end_location_name=contract_end_name,
        expected_location_ids=expected_ids,
        expected_location_names=expected_name_set,
        match_mode=location_match_mode,
    )


def _contract_items_match_order_db(contract, order):
    """Check if contract included items match order quantities by type.

    Containers are excluded from the comparison.
    """
    # Only validate included items (not requested)
    included_items = contract.items.filter(is_included=True)
    if not included_items.exists():
        # Finished contracts may no longer expose items via ESI; allow match
        # based on other criteria (title/location/price) in that case.
        return contract.status in [
            "finished",
            "finished_issuer",
            "finished_contractor",
        ]

    order_items = list(order.items.all())
    expected_by_type: dict[int, int] = {}
    for order_item in order_items:
        type_id = int(order_item.type_id)
        expected_by_type[type_id] = expected_by_type.get(type_id, 0) + int(
            order_item.quantity
        )

    actual_by_type: dict[int, int] = {}
    for contract_item in included_items:
        type_id = int(contract_item.type_id)

        # Skip containers - they shouldn't affect item matching
        if _is_container_type(type_id):
            logger.debug(
                "Excluding container type_id %s from contract item matching",
                type_id
            )
            continue

        # Skip items that are inside containers (raw_quantity < 0)
        if _is_item_inside_container(contract_item):
            logger.debug(
                "Excluding type_id %s from matching because it's inside a container (raw_quantity=%s)",
                type_id,
                getattr(contract_item, 'raw_quantity', None)
            )
            continue

        actual_by_type[type_id] = actual_by_type.get(type_id, 0) + int(
            contract_item.quantity
        )

    return expected_by_type == actual_by_type


def _is_item_inside_container(contract_item) -> bool:
    """Check if a contract item is inside a container based on raw_quantity.

    In ESI contract items, items inside containers have raw_quantity of -1 or -2.
    """
    raw_qty = getattr(contract_item, 'raw_quantity', None)
    if raw_qty is None:
        return False
    try:
        return int(raw_qty) < 0
    except (TypeError, ValueError):
        return False


def _is_container_type(type_id: int) -> bool:
    """Check if a type_id is a container that should be excluded from surplus/missing calculations."""
    try:
        from eve_sde.models import ItemType

        # Common container group IDs in EVE SDE
        CONTAINER_GROUP_IDS = {
            12,   # Cargo Container
            340,  # Freight Container
            448,  # Audit Log Secure Container
            649,  # Secure Cargo Container
            1226, # Station Container
            1246, # Station Vault Container
            1248, # Station Warehouse Container
        }

        try:
            item_type = ItemType.objects.filter(id=int(type_id)).first()
            if not item_type:
                return False

            group_id = getattr(item_type, 'group_id', None) or getattr(item_type.group, 'id', None) if hasattr(item_type, 'group') else None
            if group_id and int(group_id) in CONTAINER_GROUP_IDS:
                return True

            # Also check if the item name contains "Container" as a fallback
            item_name = str(getattr(item_type, 'name', '') or '').lower()
            if 'container' in item_name and 'packaged' not in item_name:
                return True

            return False
        except Exception:
            return False
    except ImportError:
        # If eve_sde is not available, fall back to name-based detection
        try:
            type_name = str(get_type_name(int(type_id)) or '').lower()
            return 'container' in type_name and 'packaged' not in type_name
        except Exception:
            return False


def _get_items_mismatch_breakdown(
    contract, order
) -> tuple[dict[int, int], dict[int, int], dict[int, str]]:
    """Return (missing_by_type, surplus_by_type, type_names) for order vs contract items.

    Containers and their contents are excluded from surplus calculations.
    """
    order_items = list(order.items.all())
    included_items = list(contract.items.filter(is_included=True))

    if not order_items and not included_items:
        return {}, {}, {}

    expected_by_type: dict[int, int] = {}
    actual_by_type: dict[int, int] = {}
    type_names: dict[int, str] = {}

    def _resolved_type_name(type_id: int, preferred_name: str = "") -> str:
        name = str(preferred_name or "").strip()
        if name:
            return name
        try:
            resolved = str(get_type_name(int(type_id)) or "").strip()
        except Exception:
            resolved = ""
        return resolved or f"Type {int(type_id)}"

    for order_item in order_items:
        type_id = int(order_item.type_id)
        expected_by_type[type_id] = expected_by_type.get(type_id, 0) + int(
            order_item.quantity
        )
        if type_id not in type_names:
            type_names[type_id] = _resolved_type_name(type_id, order_item.type_name)

    for contract_item in included_items:
        type_id = int(contract_item.type_id)

        # Skip containers - they shouldn't be counted as surplus
        if _is_container_type(type_id):
            logger.debug(
                "Excluding container type_id %s from contract item comparison",
                type_id
            )
            continue

        # Skip items that are inside containers (raw_quantity < 0)
        if _is_item_inside_container(contract_item):
            logger.debug(
                "Excluding type_id %s from surplus/missing because it's inside a container (raw_quantity=%s)",
                type_id,
                getattr(contract_item, 'raw_quantity', None)
            )
            continue

        actual_by_type[type_id] = actual_by_type.get(type_id, 0) + int(
            contract_item.quantity
        )
        if type_id not in type_names:
            type_names[type_id] = _resolved_type_name(type_id)

    all_type_ids = sorted(set(expected_by_type.keys()) | set(actual_by_type.keys()))
    missing_lines: list[str] = []
    surplus_lines: list[str] = []

    for type_id in all_type_ids:
        expected_qty = expected_by_type.get(type_id, 0)
        actual_qty = actual_by_type.get(type_id, 0)

        if expected_qty > actual_qty:
            missing_lines.append(type_id)
        elif actual_qty > expected_qty:
            surplus_lines.append(type_id)

    missing_by_type = {
        int(type_id): int(expected_by_type.get(type_id, 0) - actual_by_type.get(type_id, 0))
        for type_id in missing_lines
        if expected_by_type.get(type_id, 0) > actual_by_type.get(type_id, 0)
    }
    surplus_by_type = {
        int(type_id): int(actual_by_type.get(type_id, 0) - expected_by_type.get(type_id, 0))
        for type_id in surplus_lines
        if actual_by_type.get(type_id, 0) > expected_by_type.get(type_id, 0)
    }
    return missing_by_type, surplus_by_type, type_names


def _build_items_mismatch_details(contract, order) -> str:
    """Build a human-readable item delta between order and contract included items."""
    missing_by_type, surplus_by_type, type_names = _get_items_mismatch_breakdown(
        contract, order
    )
    if not missing_by_type and not surplus_by_type:
        return ""

    missing_lines: list[str] = []
    surplus_lines: list[str] = []
    for type_id, qty in sorted(missing_by_type.items()):
        type_name = (
            str(type_names.get(type_id) or "").strip()
            or str(get_type_name(int(type_id)) or "")
            or f"Type {int(type_id)}"
        )
        missing_lines.append(f"- {int(qty):,} {type_name}")
    for type_id, qty in sorted(surplus_by_type.items()):
        type_name = (
            str(type_names.get(type_id) or "").strip()
            or str(get_type_name(int(type_id)) or "")
            or f"Type {int(type_id)}"
        )
        surplus_lines.append(f"- {int(qty):,} {type_name}")

    sections: list[str] = []
    if missing_lines:
        sections.append("Missing:\n" + "\n".join(missing_lines))
    if surplus_lines:
        sections.append("Surplus:\n" + "\n".join(surplus_lines))

    return "\n\n".join(sections)


def _contract_price_matches_db(contract, order) -> tuple[bool, str]:
    """Validate database contract price against order total."""
    try:
        contract_price = Decimal(str(contract.price)).quantize(Decimal("0.01"))
        expected_price = Decimal(str(order.total_price)).quantize(Decimal("0.01"))
    except (InvalidOperation, TypeError):
        return False, "invalid contract price"

    if contract_price != expected_price:
        return False, (
            f"price {contract_price:,.0f} ISK vs expected {expected_price:,.0f} ISK"
        )

    return True, f"price {contract_price:,.0f} ISK OK"


@shared_task(
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 5},
    rate_limit="1000/m",
    time_limit=300,
    soft_time_limit=280,
)
def handle_material_exchange_buy_order_created(order_id):
    """
    Send immediate notification to admins when a buy order is created.
    Resilient task with auto-retry and rate limiting.
    """
    try:
        order = (
            MaterialExchangeBuyOrder.objects.select_related("config", "buyer")
            .prefetch_related("items")
            .get(id=order_id)
        )
    except MaterialExchangeBuyOrder.DoesNotExist:
        logger.warning("Buy order %s not found", order_id)
        return

    config = order.config

    items = list(order.items.all())
    total_qty = order.total_quantity
    total_price = order.total_price

    preview_lines = []
    for item in items[:5]:
        preview_lines.append(
            f"- {item.type_name or item.type_id}: {item.quantity:,}x @ {item.unit_price:,.2f} ISK"
        )
    if len(items) > 5:
        preview_lines.append(_("…"))

    preview = "\n".join(preview_lines) if preview_lines else _("(no items)")

    title = _("New Buy Order")
    message = _(
        f"{order.buyer.username} created a buy order {order.order_reference}.\n"
        f"Items: {len(items)} (qty: {total_qty:,})\n"
        f"Total: {total_price:,.2f} ISK\n\n"
        f"Preview:\n{preview}\n\n"
        f"Review and approve to proceed with delivery."
    )
    link = (
        f"/indy_hub/material-exchange/my-orders/buy/{order.id}/"
        f"?next=/indy_hub/material-exchange/%23admin-panel"
    )

    webhook = NotificationWebhook.get_material_exchange_webhook()
    if webhook and webhook.webhook_url:
        sent, message_id = send_discord_webhook_with_message_id(
            webhook.webhook_url,
            title,
            message,
            level="info",
            link=link,
            embed_title=f"🛒 {title}",
            mention_everyone=bool(getattr(webhook, "ping_here", False)),
        )
        if sent:
            if message_id:
                NotificationWebhookMessage.objects.create(
                    webhook_type=NotificationWebhook.TYPE_MATERIAL_EXCHANGE,
                    webhook_url=webhook.webhook_url,
                    message_id=message_id,
                    buy_order=order,
                )
            logger.info("Buy order %s notification sent to webhook", order_id)
            emit_analytics_event(
                task="material_exchange.buy_order_created",
                label="webhook",
                result="success",
                value=max(len(items), 1),
            )
            return

    admins = _get_admins_for_config(config)
    notify_multi(
        admins,
        title,
        message,
        level="info",
        link=link,
    )

    logger.info("Buy order %s notification sent to admins", order_id)
    emit_analytics_event(
        task="material_exchange.buy_order_created",
        label="admin_notify",
        result="success",
        value=max(len(items), 1),
    )


@shared_task(
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 5},
    rate_limit="1000/m",
    time_limit=300,
    soft_time_limit=280,
)
def handle_material_exchange_sell_order_created(order_id):
    """
    Send immediate notification to admins when a sell order is created.
    Resilient task with auto-retry and rate limiting.
    """
    try:
        order = (
            MaterialExchangeSellOrder.objects.select_related("config", "seller")
            .prefetch_related("items")
            .get(id=order_id)
        )
    except MaterialExchangeSellOrder.DoesNotExist:
        logger.warning("Sell order %s not found", order_id)
        return

    config = order.config

    items = list(order.items.all())
    total_qty = order.total_quantity
    total_price = order.total_price

    preview_lines = []
    for item in items[:5]:
        preview_lines.append(
            f"- {item.type_name or item.type_id}: {item.quantity:,}x @ {item.unit_price:,.2f} ISK"
        )
    if len(items) > 5:
        preview_lines.append("...")

    preview = "\n".join(preview_lines) if preview_lines else _("(no items)")
    source_location = str(getattr(order, "source_location_name", "") or "").strip()

    title = _("New Sell Order")
    message = _(
        f"{order.seller.username} wants to sell with order {order.order_reference}.\n"
        f"Items: {len(items)} (qty: {total_qty:,})\n"
        f"Total: {total_price:,.2f} ISK"
        + (f"\nLocation: {source_location}" if source_location else "")
        + f"\n\nPreview:\n{preview}\n\n"
        f"Review and approve to start contract validation."
    )
    link = (
        f"/indy_hub/material-exchange/my-orders/sell/{order.id}/"
        f"?next=/indy_hub/material-exchange/%23admin-panel"
    )

    _notify_material_exchange_admins(
        config,
        title,
        message,
        level="info",
        link=link,
    )

    logger.info("Sell order %s notification sent to admins/webhook", order_id)
    emit_analytics_event(
        task="material_exchange.sell_order_created",
        label="admin_or_webhook",
        result="success",
        value=max(len(items), 1),
    )


@shared_task(
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 5},
    rate_limit="1000/m",
    time_limit=300,
    soft_time_limit=280,
)
def handle_capital_ship_order_created(order_id):
    """Notify admins/webhook when a capital order is created."""
    try:
        order = CapitalShipOrder.objects.select_related("config", "requester").get(
            id=order_id
        )
    except CapitalShipOrder.DoesNotExist:
        logger.warning("Capital ship order %s not found", order_id)
        return

    title = _("Capital Order Created")
    message = _(
        f"{order.requester.username} created capital order {order.order_reference}.\n"
        f"Hull: {order.ship_type_name} ({order.get_ship_class_display()})\n"
        f"Reason: {order.get_reason_display()}\n"
        f"Status: {order.get_status_display()}"
    )
    _notify_material_exchange_admins(
        order.config,
        title,
        message,
        level="info",
        link="/indy_hub/material-exchange/capital-orders/admin/",
    )
    notify_user(
        order.requester,
        _("Capital Order Submitted"),
        _(
            f"Order {order.order_reference} for {order.ship_type_name} was submitted.\n"
            "A manager will move it to in production when work starts."
        ),
        level="info",
        link="/indy_hub/material-exchange/capital-orders/",
    )


@shared_task(
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 5},
    rate_limit="1000/m",
    time_limit=300,
    soft_time_limit=280,
)
def handle_capital_ship_order_marked_in_production(order_id):
    """Notify user/admins when manager marks a capital order in production."""
    try:
        order = CapitalShipOrder.objects.select_related(
            "config", "requester", "in_production_by"
        ).get(id=order_id)
    except CapitalShipOrder.DoesNotExist:
        logger.warning("Capital ship order %s not found", order_id)
        return

    manager_name = (
        str(getattr(order.in_production_by, "username", "") or "").strip()
        or "Manager"
    )
    notify_user(
        order.requester,
        _("Capital Order In Production"),
        _(
            f"Order {order.order_reference} ({order.ship_type_name}) is now in production.\n"
            f"Set by: {manager_name}"
        ),
        level="info",
        link="/indy_hub/material-exchange/capital-orders/",
    )
    _notify_material_exchange_admins(
        order.config,
        _("Capital Order In Production"),
        _(
            f"{manager_name} moved capital order {order.order_reference} to in production.\n"
            f"User: {order.requester.username}\n"
            f"Hull: {order.ship_type_name}"
        ),
        level="info",
        link="/indy_hub/material-exchange/capital-orders/admin/",
    )


@shared_task(
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 5},
    rate_limit="1000/m",
    time_limit=300,
    soft_time_limit=280,
)
def handle_capital_ship_order_closed_by_manager(
    order_id: int,
    status: str,
    manager_user_id: int | None = None,
):
    """Notify user/admins when manager rejects or cancels a capital order."""
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in {
        CapitalShipOrder.Status.REJECTED,
        CapitalShipOrder.Status.CANCELLED,
    }:
        return

    try:
        order = CapitalShipOrder.objects.select_related("config", "requester").get(
            id=order_id
        )
    except CapitalShipOrder.DoesNotExist:
        logger.warning("Capital ship order %s not found", order_id)
        return

    if str(order.status or "").strip().lower() != normalized_status:
        # Order moved again before task execution; skip stale notification.
        return

    manager_name = "Manager"
    if manager_user_id:
        try:
            manager_name = (
                User.objects.filter(id=int(manager_user_id))
                .values_list("username", flat=True)
                .first()
                or manager_name
            )
        except Exception:
            pass

    if normalized_status == CapitalShipOrder.Status.REJECTED:
        user_title = _("Capital Order Rejected")
        user_message = _(
            f"Order {order.order_reference} ({order.ship_type_name}) was rejected by {manager_name}."
        )
        admin_title = _("Capital Order Rejected")
        admin_message = _(
            f"{manager_name} rejected capital order {order.order_reference}.\n"
            f"User: {order.requester.username}\n"
            f"Hull: {order.ship_type_name}"
        )
        level = "warning"
    else:
        user_title = _("Capital Order Cancelled")
        user_message = _(
            f"Order {order.order_reference} ({order.ship_type_name}) was cancelled by {manager_name}."
        )
        admin_title = _("Capital Order Cancelled")
        admin_message = _(
            f"{manager_name} cancelled capital order {order.order_reference}.\n"
            f"User: {order.requester.username}\n"
            f"Hull: {order.ship_type_name}"
        )
        level = "info"

    notify_user(
        order.requester,
        user_title,
        user_message,
        level=level,
        link="/indy_hub/material-exchange/capital-orders/",
    )
    _notify_material_exchange_admins(
        order.config,
        admin_title,
        admin_message,
        level=level,
        link="/indy_hub/material-exchange/capital-orders/admin/",
    )


def _get_user_main_character_id(user: User) -> int | None:
    """Resolve user's main character ID when available."""
    try:
        # Alliance Auth
        from allianceauth.authentication.models import UserProfile

        profile = UserProfile.objects.select_related("main_character").get(user=user)
        main_character = getattr(profile, "main_character", None)
        main_character_id = int(getattr(main_character, "character_id", 0) or 0)
        return main_character_id if main_character_id > 0 else None
    except Exception:
        return None


def _capital_contract_has_requested_hull(contract: ESIContract, ship_type_id: int) -> bool:
    included_items = contract.items.filter(is_included=True)
    if not included_items.exists():
        return False

    requested_type_id = int(ship_type_id)
    for contract_item in included_items:
        contract_type_id = int(getattr(contract_item, "type_id", 0) or 0)
        if contract_type_id <= 0:
            continue
        if _is_container_type(contract_type_id) or _is_item_inside_container(contract_item):
            continue
        quantity = int(getattr(contract_item, "quantity", 0) or 0)
        if contract_type_id == requested_type_id and quantity > 0:
            return True
    return False


def _set_capital_order_anomaly(
    order: CapitalShipOrder,
    *,
    reason: str,
    contract_id: int | None = None,
    contract_status: str | None = None,
) -> None:
    previous_status = str(order.status or "")
    previous_reason = str(order.anomaly_reason or "")
    if previous_status == CapitalShipOrder.Status.ANOMALY and previous_reason == reason:
        return

    order.status = CapitalShipOrder.Status.ANOMALY
    order.anomaly_reason = reason
    if contract_id and not order.esi_contract_id:
        order.esi_contract_id = int(contract_id)
    status_note = f" ({contract_status})" if contract_status else ""
    order.notes = (
        f"Anomaly detected for order {order.order_reference}: {reason}{status_note}"
    )
    order.save(
        update_fields=[
            "status",
            "anomaly_reason",
            "esi_contract_id",
            "notes",
            "updated_at",
        ]
    )

    notify_user(
        order.requester,
        _("Capital Order Anomaly"),
        _(
            f"Order {order.order_reference} is now in anomaly status.\n"
            f"Reason: {reason}"
        ),
        level="warning",
        link="/indy_hub/material-exchange/capital-orders/",
    )
    _notify_material_exchange_admins(
        order.config,
        _("Capital Order Anomaly"),
        _(
            f"Order {order.order_reference} requires intervention.\n"
            f"User: {order.requester.username}\n"
            f"Hull: {order.ship_type_name}\n"
            f"Reason: {reason}"
        ),
        level="warning",
        link="/indy_hub/material-exchange/capital-orders/admin/",
    )


@shared_task(
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 10},
    rate_limit="500/m",
    time_limit=600,
    soft_time_limit=580,
)
@rate_limit_retry_task
def process_capital_ship_orders():
    """Auto-advance capital ship orders from contracts and contract status."""
    try:
        if not MaterialExchangeSettings.get_solo().is_enabled:
            logger.info("Material Exchange disabled; skipping capital order processing.")
            return
    except Exception:
        pass

    config = MaterialExchangeConfig.objects.first()
    if not config:
        return

    active_orders = CapitalShipOrder.objects.filter(
        config=config,
        status__in=[
            CapitalShipOrder.Status.WAITING,
            CapitalShipOrder.Status.IN_PRODUCTION,
            CapitalShipOrder.Status.CONTRACT_CREATED,
        ],
    ).select_related("requester")
    if not active_orders.exists():
        return

    contracts = list(
        ESIContract.objects.filter(
            corporation_id=config.corporation_id,
            contract_type="item_exchange",
            title__icontains="INDY-CAP",
        ).prefetch_related("items")
    )
    contracts_by_id = {int(contract.contract_id): contract for contract in contracts}

    finished_statuses = {"finished", "finished_issuer", "finished_contractor"}
    failed_statuses = {"cancelled", "rejected", "failed", "expired", "deleted", "reversed"}

    for order in active_orders:
        order_ref_lower = str(order.order_reference or "").strip().lower()
        matching_by_title = [
            contract
            for contract in contracts
            if order_ref_lower and order_ref_lower in str(contract.title or "").lower()
        ]

        if order.status in {
            CapitalShipOrder.Status.WAITING,
            CapitalShipOrder.Status.IN_PRODUCTION,
        }:
            if not matching_by_title:
                continue

            expected_assignee_id = _get_user_main_character_id(order.requester)
            if not expected_assignee_id:
                character_ids = _get_user_character_ids(order.requester)
                expected_assignee_id = character_ids[0] if character_ids else None
            if not expected_assignee_id:
                _set_capital_order_anomaly(
                    order,
                    reason="User has no linked/main character for contract assignment.",
                )
                continue

            candidate_contract = None
            mismatch_reason = None
            sorted_matches = sorted(
                matching_by_title,
                key=lambda c: (str(getattr(c, "date_issued", "") or ""), int(c.contract_id)),
                reverse=True,
            )
            for contract in sorted_matches:
                assignee_id = int(getattr(contract, "assignee_id", 0) or 0)
                if assignee_id != int(expected_assignee_id):
                    mismatch_reason = (
                        f"Contract #{contract.contract_id} assignee mismatch "
                        f"(expected main character ID {int(expected_assignee_id)}, got {assignee_id})."
                    )
                    continue

                if not _capital_contract_has_requested_hull(
                    contract,
                    int(order.ship_type_id),
                ):
                    mismatch_reason = (
                        f"Contract #{contract.contract_id} does not contain the requested hull "
                        f"{order.ship_type_name}."
                    )
                    continue

                candidate_contract = contract
                break

            if not candidate_contract:
                if mismatch_reason:
                    _set_capital_order_anomaly(order, reason=mismatch_reason)
                continue

            contract_status = str(candidate_contract.status or "").lower()
            if contract_status in failed_statuses:
                _set_capital_order_anomaly(
                    order,
                    reason=f"Matched contract #{candidate_contract.contract_id} is {contract_status}.",
                    contract_id=int(candidate_contract.contract_id),
                    contract_status=contract_status,
                )
                continue

            if contract_status in finished_statuses:
                order.status = CapitalShipOrder.Status.COMPLETED
                order.esi_contract_id = int(candidate_contract.contract_id)
                if not order.contract_created_at:
                    order.contract_created_at = (
                        getattr(candidate_contract, "date_accepted", None)
                        or getattr(candidate_contract, "date_issued", None)
                        or timezone.now()
                    )
                order.contract_completed_at = (
                    getattr(candidate_contract, "date_completed", None) or timezone.now()
                )
                order.anomaly_reason = ""
                order.notes = (
                    f"Contract #{candidate_contract.contract_id} completed for "
                    f"{order.ship_type_name}."
                )
                order.save(
                    update_fields=[
                        "status",
                        "esi_contract_id",
                        "contract_created_at",
                        "contract_completed_at",
                        "anomaly_reason",
                        "notes",
                        "updated_at",
                    ]
                )
                notify_user(
                    order.requester,
                    _("Capital Contract Completed"),
                    _(
                        f"Your capital order {order.order_reference} is complete.\n"
                        f"Contract #{candidate_contract.contract_id} was accepted."
                    ),
                    level="success",
                    link="/indy_hub/material-exchange/capital-orders/",
                )
                _notify_material_exchange_admins(
                    order.config,
                    _("Capital Contract Completed"),
                    _(
                        f"Capital order {order.order_reference} completed.\n"
                        f"User: {order.requester.username}\n"
                        f"Hull: {order.ship_type_name}\n"
                        f"Contract: #{candidate_contract.contract_id}"
                    ),
                    level="success",
                    link="/indy_hub/material-exchange/capital-orders/admin/",
                )
                continue

            status_changed = (
                order.status != CapitalShipOrder.Status.CONTRACT_CREATED
                or int(order.esi_contract_id or 0) != int(candidate_contract.contract_id)
            )
            order.status = CapitalShipOrder.Status.CONTRACT_CREATED
            order.esi_contract_id = int(candidate_contract.contract_id)
            if not order.contract_created_at:
                order.contract_created_at = (
                    getattr(candidate_contract, "date_accepted", None)
                    or getattr(candidate_contract, "date_issued", None)
                    or timezone.now()
                )
            order.anomaly_reason = ""
            order.notes = (
                f"Contract #{candidate_contract.contract_id} detected for "
                f"{order.ship_type_name}. Awaiting acceptance."
            )
            order.save(
                update_fields=[
                    "status",
                    "esi_contract_id",
                    "contract_created_at",
                    "anomaly_reason",
                    "notes",
                    "updated_at",
                ]
            )
            if status_changed:
                notify_user(
                    order.requester,
                    _("Capital Contract Created"),
                    _(
                        f"The corporation created your contract for {order.order_reference}.\n"
                        f"Contract #{candidate_contract.contract_id} is now available."
                    ),
                    level="success",
                    link="/indy_hub/material-exchange/capital-orders/",
                )
                _notify_material_exchange_admins(
                    order.config,
                    _("Capital Contract Created"),
                    _(
                        f"Capital contract created for order {order.order_reference}.\n"
                        f"User: {order.requester.username}\n"
                        f"Hull: {order.ship_type_name}\n"
                        f"Contract: #{candidate_contract.contract_id}"
                    ),
                    level="success",
                    link="/indy_hub/material-exchange/capital-orders/admin/",
                )
            continue

        # CONTRACT_CREATED status monitoring
        contract_id = int(order.esi_contract_id or 0)
        if contract_id <= 0:
            continue

        contract = contracts_by_id.get(contract_id)
        if not contract:
            continue

        contract_status = str(contract.status or "").lower()
        if contract_status in finished_statuses:
            if order.status == CapitalShipOrder.Status.COMPLETED:
                continue
            order.status = CapitalShipOrder.Status.COMPLETED
            order.contract_completed_at = (
                getattr(contract, "date_completed", None) or timezone.now()
            )
            order.anomaly_reason = ""
            order.notes = f"Contract #{contract_id} completed for {order.ship_type_name}."
            order.save(
                update_fields=[
                    "status",
                    "contract_completed_at",
                    "anomaly_reason",
                    "notes",
                    "updated_at",
                ]
            )
            notify_user(
                order.requester,
                _("Capital Contract Completed"),
                _(
                    f"Your capital order {order.order_reference} is complete.\n"
                    f"Contract #{contract_id} was accepted."
                ),
                level="success",
                link="/indy_hub/material-exchange/capital-orders/",
            )
            _notify_material_exchange_admins(
                order.config,
                _("Capital Contract Completed"),
                _(
                    f"Capital order {order.order_reference} completed.\n"
                    f"User: {order.requester.username}\n"
                    f"Hull: {order.ship_type_name}\n"
                    f"Contract: #{contract_id}"
                ),
                level="success",
                link="/indy_hub/material-exchange/capital-orders/admin/",
            )
            continue

        if contract_status in failed_statuses:
            _set_capital_order_anomaly(
                order,
                reason=f"Contract #{contract_id} moved to {contract_status}.",
                contract_id=contract_id,
                contract_status=contract_status,
            )
            continue

        if contract_status in {"outstanding", "in_progress"} and not _capital_contract_has_requested_hull(
            contract, int(order.ship_type_id)
        ):
            _set_capital_order_anomaly(
                order,
                reason=(
                    f"Contract #{contract_id} no longer contains requested hull "
                    f"{order.ship_type_name}."
                ),
                contract_id=contract_id,
                contract_status=contract_status,
            )


@shared_task(
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 10},
    rate_limit="500/m",
    time_limit=600,
    soft_time_limit=580,
)
@rate_limit_retry_task
def check_completed_material_exchange_contracts():
    """
    Check if corp contracts for approved sell orders have been completed.
    Update order status and notify users when payment is verified.
    """
    try:
        if not MaterialExchangeSettings.get_solo().is_enabled:
            logger.info("Material Exchange disabled; skipping completion check.")
            return
    except Exception:
        pass

    config = MaterialExchangeConfig.objects.first()
    if not config:
        return

    approved_orders = MaterialExchangeSellOrder.objects.filter(
        config=config,
        status=MaterialExchangeSellOrder.Status.VALIDATED,
    )

    try:
        has_cached_contracts = ESIContract.objects.filter(
            corporation_id=config.corporation_id
        ).exists()
        contracts = shared_client.fetch_corporation_contracts(
            corporation_id=config.corporation_id,
            character_id=_get_character_for_scope(
                config.corporation_id,
                "esi-contracts.read_corporation_contracts.v1",
            ),
            force_refresh=not has_cached_contracts,
        )
    except ESIUnmodifiedError:
        contracts = list(
            ESIContract.objects.filter(corporation_id=config.corporation_id).values(
                "contract_id",
                "status",
            )
        )
        if not contracts:
            logger.debug(
                "Contracts not modified for corporation %s; no cached contracts available",
                config.corporation_id,
            )
            return
    except ESIRateLimitError as exc:
        delay = get_retry_after_seconds(exc)
        logger.warning(
            "ESI rate limit reached while checking contract status; retrying in %ss: %s",
            delay,
            exc,
        )
        check_completed_material_exchange_contracts.apply_async(countdown=delay)
        return
    except (ESITokenError, ESIForbiddenError, ESIClientError) as exc:
        if "304" in str(exc):
            contracts = list(
                ESIContract.objects.filter(corporation_id=config.corporation_id).values(
                    "contract_id", "status"
                )
            )
            if not contracts:
                logger.debug(
                    "Contracts not modified for corporation %s; no cached contracts available",
                    config.corporation_id,
                )
                return
        else:
            logger.error("Failed to check contract status: %s", exc)
            return

    for order in approved_orders:
        # Extract contract ID from stored field or notes
        contract_id = order.esi_contract_id or _extract_contract_id(order.notes)
        if not contract_id:
            continue

        contract = next(
            (c for c in contracts if c["contract_id"] == contract_id),
            None,
        )
        if not contract:
            continue

        # Handle contract status
        contract_status = contract.get("status", "")

        # Contract completed successfully
        if contract_status in ["finished", "finished_issuer", "finished_contractor"]:
            order.status = MaterialExchangeSellOrder.Status.COMPLETED
            order.payment_verified_at = timezone.now()
            order.save(
                update_fields=[
                    "status",
                    "payment_verified_at",
                    "updated_at",
                ]
            )

            _log_sell_order_transactions(order)
            sell_items = list(order.items.all())
            sell_items_preview = "\n".join(
                f"- {item.type_name}: {item.quantity:,}x"
                for item in sell_items[:8]
            )
            if len(sell_items) > 8:
                sell_items_preview += "\n- ..."

            notify_user(
                order.seller,
                _("Sell Order Completed"),
                _(
                    f"Your sell order {order.order_reference} is complete.\n"
                    f"Contract #{contract_id} has been accepted by the corporation."
                    + (
                        f"\n\nItems received:\n{sell_items_preview}"
                        if sell_items_preview
                        else ""
                    )
                ),
                level="success",
                link=f"/indy_hub/material-exchange/my-orders/sell/{order.id}/",
            )
            _notify_material_exchange_admins(
                config,
                _("Sell Order Completed"),
                _(
                    f"{order.seller.username}'s sell order {order.order_reference} is completed.\n"
                ),
                level="success",
                link=(
                    f"/indy_hub/material-exchange/my-orders/sell/{order.id}/"
                    f"?next=/indy_hub/material-exchange/%23admin-panel"
                ),
            )

            logger.info(
                "Sell order %s completed: contract %s accepted (status: %s)",
                order.id,
                contract_id,
                contract_status,
            )
            emit_analytics_event(
                task="material_exchange.sell_order_completed",
                label=contract_status,
                result="success",
            )

        # Contract cancelled, rejected, failed, expired or deleted
        elif contract_status in [
            "cancelled",
            "rejected",
            "failed",
            "expired",
            "deleted",
        ]:
            order.status = MaterialExchangeSellOrder.Status.CANCELLED
            order.notes = f"Contract {contract_id} was {contract_status} by EVE system"
            order.save(
                update_fields=[
                    "status",
                    "notes",
                    "updated_at",
                ]
            )

            logger.warning(
                "Sell order %s cancelled: contract %s status is %s",
                order.id,
                contract_id,
                contract_status,
            )
            emit_analytics_event(
                task="material_exchange.sell_order_cancelled",
                label=contract_status,
                result="warning",
            )

        # Contract reversed (rare case - completed then reversed)
        elif contract_status == "reversed":
            order.status = MaterialExchangeSellOrder.Status.CANCELLED
            order.notes = f"Contract {contract_id} was reversed after completion"
            order.save(
                update_fields=[
                    "status",
                    "notes",
                    "updated_at",
                ]
            )

            logger.error(
                "Sell order %s reversed: contract %s was reversed",
                order.id,
                contract_id,
            )
            emit_analytics_event(
                task="material_exchange.sell_order_cancelled",
                label="reversed",
                result="error",
            )

    # Process validated buy orders (corp -> member)
    validated_buy_orders = MaterialExchangeBuyOrder.objects.filter(
        config=config,
        status=MaterialExchangeBuyOrder.Status.VALIDATED,
    )

    if not validated_buy_orders.exists():
        return

    for order in validated_buy_orders:
        contract_id = order.esi_contract_id or _extract_contract_id(order.notes)
        if not contract_id:
            continue

        contract = next(
            (c for c in contracts if c["contract_id"] == contract_id),
            None,
        )
        if not contract:
            continue

        # Handle contract status
        contract_status = contract.get("status", "")

        # Contract completed successfully
        if contract_status in ["finished", "finished_issuer", "finished_contractor"]:
            order.status = MaterialExchangeBuyOrder.Status.COMPLETED
            order.delivered_at = contract.get("date_completed") or timezone.now()
            order.save(
                update_fields=[
                    "status",
                    "delivered_at",
                    "updated_at",
                ]
            )

            _log_buy_order_transactions(order)
            buy_items = list(order.items.all())
            buy_items_preview = "\n".join(
                f"- {item.type_name}: {item.quantity:,}x" for item in buy_items[:8]
            )
            if len(buy_items) > 8:
                buy_items_preview += "\n- ..."
            notify_user(
                order.buyer,
                _("Buy Order Completed"),
                _(
                    f"Your buy order {order.order_reference} is complete.\n"
                    f"Contract #{contract_id} has been accepted in-game and your delivery is marked as received."
                    + (
                        f"\n\nItems delivered:\n{buy_items_preview}"
                        if buy_items_preview
                        else ""
                    )
                ),
                level="success",
                link=f"/indy_hub/material-exchange/my-orders/buy/{order.id}/",
            )
            _notify_material_exchange_admins(
                config,
                _("Buy Order Completed"),
                _(
                    f"{order.buyer.username}'s buy order {order.order_reference} is completed.\n"
                ),
                level="success",
                link=(
                    f"/indy_hub/material-exchange/my-orders/buy/{order.id}/"
                    f"?next=/indy_hub/material-exchange/%23admin-panel"
                ),
            )

            logger.info(
                "Buy order %s completed: contract %s accepted (status: %s)",
                order.id,
                contract_id,
                contract_status,
            )
            emit_analytics_event(
                task="material_exchange.buy_order_completed",
                label=contract_status,
                result="success",
            )

        # Contract cancelled, rejected, failed, expired or deleted
        elif contract_status in [
            "cancelled",
            "rejected",
            "failed",
            "expired",
            "deleted",
        ]:
            order.status = MaterialExchangeBuyOrder.Status.CANCELLED
            order.notes = f"Contract {contract_id} was {contract_status} by EVE system"
            order.save(
                update_fields=[
                    "status",
                    "notes",
                    "updated_at",
                ]
            )

            logger.warning(
                "Buy order %s cancelled: contract %s status is %s",
                order.id,
                contract_id,
                contract_status,
            )
            emit_analytics_event(
                task="material_exchange.buy_order_cancelled",
                label=contract_status,
                result="warning",
            )

        # Contract reversed (rare case - completed then reversed)
        elif contract_status == "reversed":
            order.status = MaterialExchangeBuyOrder.Status.CANCELLED
            order.notes = f"Contract {contract_id} was reversed after completion"
            order.save(
                update_fields=[
                    "status",
                    "notes",
                    "updated_at",
                ]
            )

            logger.error(
                "Buy order %s reversed: contract %s was reversed",
                order.id,
                contract_id,
            )
            emit_analytics_event(
                task="material_exchange.buy_order_cancelled",
                label="reversed",
                result="error",
            )


def _extract_contract_id(notes: str) -> int | None:
    """Extract contract ID from order notes (format: "Contract validated: 12345")."""
    if not notes:
        return None

    match = re.search(r"Contract validated:\s*(\d+)", notes)
    if match:
        return int(match.group(1))

    match = re.search(r"\b(\d{6,})\b", notes)
    if match:
        return int(match.group(1))

    return None


def _get_character_for_scope(corporation_id: int, scope: str) -> int:
    """
    Find a character with the required scope in the corporation.
    Used for authenticated ESI calls.

    Raises:
        ESITokenError: If no character with the scope is found
    """
    # Alliance Auth
    from allianceauth.eveonline.models import EveCharacter
    from esi.models import Token

    try:
        # Step 1: Get character IDs from the corporation
        character_ids = EveCharacter.objects.filter(
            corporation_id=corporation_id
        ).values_list("character_id", flat=True)

        if not character_ids:
            raise ESITokenError(
                f"No characters found for corporation {corporation_id}. "
                f"At least one corporation member must login to grant ESI scopes."
            )

        # Step 2: Get all tokens for these characters
        # Note: AllianceAuth's Token model does not have a 'character' FK.
        # Avoid select_related("character") to prevent FieldError.
        tokens = Token.objects.filter(character_id__in=character_ids).require_valid()

        if not tokens.exists():
            raise ESITokenError(
                f"No tokens found for corporation {corporation_id}. "
                f"At least one corporation member must login to grant ESI scopes."
            )

        # Try to find a token with the required scope
        # Token.scopes is a ManyToMany field (Scope model)
        for token in tokens:
            try:
                token_scope_names = list(token.scopes.values_list("name", flat=True))
                if scope in token_scope_names:
                    logger.debug(
                        f"Found token for {scope} via character {token.character_id}"
                    )
                    return token.character_id
            except Exception:
                continue

        # No token with required scope found
        # Build a readable list of available scopes and character names
        try:
            # Alliance Auth
            from allianceauth.eveonline.models import EveCharacter

            name_map = {
                ec.character_id: (ec.character_name or str(ec.character_id))
                for ec in EveCharacter.objects.filter(character_id__in=character_ids)
            }
        except Exception:
            name_map = {}

        available_scopes_list = []
        for token in tokens:
            try:
                scopes_str = ", ".join(token.scopes.values_list("name", flat=True))
            except Exception:
                scopes_str = "unknown"
            char_name = name_map.get(token.character_id, f"char {token.character_id}")
            available_scopes_list.append(f"{char_name}: {scopes_str}")

        raise ESITokenError(
            f"No character in corporation {corporation_id} has scope '{scope}'. "
            f"Available characters and scopes:\n" + "\n".join(available_scopes_list)
        )

    except ESITokenError:
        raise
    except Exception as exc:
        logger.error(
            f"Error checking tokens for corporation {corporation_id}: {exc}",
            exc_info=True,
        )
        raise ESITokenError(
            f"Error checking tokens for corporation {corporation_id}: {exc}"
        )


def _get_user_character_ids(user: User) -> list[int]:
    """Get character IDs for a user from valid tokens and linked ownership."""
    character_ids: set[int] = set()

    try:
        # Alliance Auth
        from esi.models import Token

        for character_id in (
            Token.objects.filter(user=user)
            .require_valid()
            .values_list("character_id", flat=True)
            .distinct()
        ):
            if character_id:
                character_ids.add(int(character_id))
    except Exception:
        pass

    try:
        # Alliance Auth
        from allianceauth.authentication.models import CharacterOwnership

        for character_id in (
            CharacterOwnership.objects.filter(user=user)
            .values_list("character__character_id", flat=True)
            .distinct()
        ):
            if character_id:
                character_ids.add(int(character_id))
    except Exception:
        pass

    return sorted(character_ids)


def _notify_material_exchange_admins(
    config: MaterialExchangeConfig,
    title: str,
    message: str,
    *,
    level: str = "info",
    link: str | None = None,
    thumbnail_url: str | None = None,
) -> None:
    """Notify Material Exchange admins or send to webhook if configured."""

    webhook = NotificationWebhook.get_material_exchange_webhook()
    if webhook and webhook.webhook_url:
        sent = send_discord_webhook(
            webhook.webhook_url,
            title,
            message,
            level=level,
            link=link,
            thumbnail_url=thumbnail_url,
            embed_title=f"🛒 {title}",
            mention_everyone=bool(getattr(webhook, "ping_here", False)),
        )
        if sent:
            return

    admins = _get_admins_for_config(config)
    notify_multi(
        admins,
        title,
        message,
        level=level,
        link=link,
        thumbnail_url=thumbnail_url,
    )


def _get_admins_for_config(config: MaterialExchangeConfig) -> list[User]:
    """
    Get users to notify about material exchange orders.
    Includes: users with explicit can_manage_material_hub permission only.
    """
    # Django
    from django.contrib.auth.models import Permission

    try:
        perm = Permission.objects.get(
            codename="can_manage_material_hub",
            content_type__app_label="indy_hub",
        )
        perm_users = list(
            User.objects.filter(
                Q(groups__permissions=perm) | Q(user_permissions=perm),
                is_active=True,
            ).distinct()
        )
    except Permission.DoesNotExist:
        return []

    return perm_users


def _get_corp_name(corporation_id: int) -> str:
    """Get corporation name, fallback to ID if not available."""
    try:
        # Alliance Auth
        from allianceauth.eveonline.models import EveCharacter

        char = EveCharacter.objects.filter(corporation_id=corporation_id).first()
        if char:
            return char.corporation_name
    except Exception:
        pass
    return f"Corp {corporation_id}"
