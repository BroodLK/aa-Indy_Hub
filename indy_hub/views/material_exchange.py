"""Buyback views for Indy Hub."""

# Standard Library
from datetime import datetime, time
import hashlib
import re
from decimal import ROUND_CEILING, Decimal
import unicodedata

# Django
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import Permission
from django.core.cache import cache
from django.core.paginator import Paginator
from django.db import transaction
from django.db.models import Count, Max, Min, Q, Sum, Value
from django.db.models.functions import Lower, Replace, TruncMonth
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from django.views.decorators.http import require_http_methods

# Alliance Auth
from allianceauth.authentication.models import UserProfile
from allianceauth.services.hooks import get_extension_logger

try:  # Optional dependency.
    from corptools.models import (
        CorporationMarketOrder,
        CorporationWalletDivision,
        CorporationWalletJournalEntry,
    )
except Exception:  # pragma: no cover - Corptools not installed/enabled.
    CorporationMarketOrder = None
    CorporationWalletDivision = None
    CorporationWalletJournalEntry = None

from ..decorators import indy_hub_permission_required, tokens_required
from ..models import (
    Blueprint,
    CapitalShipOrder,
    CachedCharacterAsset,
    ESIContract,
    ESIContractItem,
    MaterialExchangeBuyOrder,
    MaterialExchangeBuyOrderItem,
    MaterialExchangeConfig,
    MaterialExchangeItemPriceOverride,
    MaterialExchangeSellOrder,
    MaterialExchangeSellOrderItem,
    MaterialExchangeSettings,
    MaterialExchangeStock,
    MaterialExchangeTransaction,
)
from ..services.asset_cache import (
    _get_character_for_scope,
    add_cached_corp_assets_for_sell_completion,
    asset_chain_has_context,
    build_asset_index_by_item_id,
    consume_cached_corp_assets_for_buy_completion,
    get_corp_assets_cached,
    get_corp_divisions_cached,
    get_corp_wallet_divisions_cached,
    get_office_folder_item_id_from_assets,
    get_user_assets_cached,
    make_managed_hangar_location_id,
    resolve_structure_names,
)
from ..services.esi_client import ESIClientError, ESITokenError, shared_client
from ..tasks.material_exchange import (
    ESI_DOWN_COOLDOWN_SECONDS,
    ME_STOCK_SYNC_CACHE_VERSION,
    ME_USER_ASSETS_CACHE_VERSION,
    me_buy_stock_esi_cooldown_key,
    me_sell_assets_esi_cooldown_key,
    me_stock_sync_cache_version_key,
    me_user_assets_cache_version_key,
    refresh_material_exchange_buy_stock,
    refresh_material_exchange_sell_user_assets,
    sync_material_exchange_prices,
    sync_material_exchange_stock,
)
from ..utils.analytics import emit_view_analytics_event
from ..utils.eve import get_corporation_name, get_type_name
from ..utils.material_exchange_pricing import (
    apply_markup_with_jita_bounds,
    compute_buy_price_from_member,
)
from .navigation import build_nav_context

logger = get_extension_logger(__name__)
User = get_user_model()

_PRODUCTION_IDS_CACHE: set[int] | None = None
_INDUSTRY_MARKET_GROUP_IDS_CACHE: set[int] | None = None
_SELL_ESTIMATE_TYPE_LOOKUP_CACHE: dict[str, int | None] = {}
MAX_ESTIMATE_LIVE_PRICE_LOOKUPS = 400

_ACTIVE_BUY_RESERVATION_STATUSES: tuple[str, ...] = (
    MaterialExchangeBuyOrder.Status.DRAFT,
    MaterialExchangeBuyOrder.Status.AWAITING_VALIDATION,
    MaterialExchangeBuyOrder.Status.VALIDATED,
)

_ACTIVE_SELL_RESERVATION_STATUSES: tuple[str, ...] = (
    MaterialExchangeSellOrder.Status.DRAFT,
    MaterialExchangeSellOrder.Status.AWAITING_VALIDATION,
    MaterialExchangeSellOrder.Status.ANOMALY,
    MaterialExchangeSellOrder.Status.ANOMALY_REJECTED,
    MaterialExchangeSellOrder.Status.VALIDATED,
)


def _get_reserved_buy_quantities(
    *,
    config: MaterialExchangeConfig,
    type_ids: set[int] | None = None,
    exclude_order_id: int | None = None,
) -> dict[int, int]:
    """Return reserved quantities by type for active buy orders."""

    queryset = MaterialExchangeBuyOrderItem.objects.filter(
        order__config=config,
        order__status__in=_ACTIVE_BUY_RESERVATION_STATUSES,
    )
    if type_ids:
        queryset = queryset.filter(type_id__in=type_ids)
    if exclude_order_id:
        queryset = queryset.exclude(order_id=int(exclude_order_id))

    aggregated = queryset.values("type_id").annotate(total_reserved=Sum("quantity"))
    return {
        int(row["type_id"]): int(row["total_reserved"] or 0) for row in aggregated
    }


def _get_reserved_sell_quantities(
    *,
    config: MaterialExchangeConfig,
    seller,
    location_id: int | None = None,
    type_ids: set[int] | None = None,
    exclude_order_id: int | None = None,
    assets_synced_at=None,
) -> dict[int, int]:
    """Return reserved quantities by type for a seller's active sell orders.

    Active sell orders are always reserved.
    Terminal sell orders (completed/rejected/cancelled) remain reserved until the
    next successful user assets sync that happened after the order status update.
    """

    status_filter = Q(order__status__in=_ACTIVE_SELL_RESERVATION_STATUSES)
    terminal_pending_sync_filter = Q(
        order__status__in=[
            MaterialExchangeSellOrder.Status.COMPLETED,
            MaterialExchangeSellOrder.Status.REJECTED,
            MaterialExchangeSellOrder.Status.CANCELLED,
        ]
    )
    if assets_synced_at is not None:
        terminal_pending_sync_filter &= Q(order__updated_at__gt=assets_synced_at)
    status_filter |= terminal_pending_sync_filter

    queryset = MaterialExchangeSellOrderItem.objects.filter(
        order__config=config,
        order__seller=seller,
    ).filter(status_filter)
    if location_id is not None:
        queryset = queryset.filter(
            Q(order__source_location_id=int(location_id))
            | Q(order__source_location_id__isnull=True)
        )
    if type_ids:
        queryset = queryset.filter(type_id__in=type_ids)
    if exclude_order_id:
        queryset = queryset.exclude(order_id=int(exclude_order_id))

    aggregated = queryset.values("type_id").annotate(total_reserved=Sum("quantity"))
    return {
        int(row["type_id"]): int(row["total_reserved"] or 0) for row in aggregated
    }


def _resolve_main_character_name(user) -> str:
    """Return user's main character name when available, fallback to username."""
    if not user:
        return ""

    try:
        profile = UserProfile.objects.select_related("main_character").get(user=user)
        main_character = getattr(profile, "main_character", None)
        if main_character and getattr(main_character, "character_name", None):
            return str(main_character.character_name)
    except UserProfile.DoesNotExist:
        pass
    except Exception:
        pass

    return str(getattr(user, "username", ""))


def _format_duration_short(delta) -> str:
    """Format a timedelta-like value as compact d/h/m text."""
    if delta is None:
        return "-"
    try:
        total_seconds = int(delta.total_seconds())
    except Exception:
        return "-"
    if total_seconds < 0:
        return "-"

    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, _seconds = divmod(remainder, 60)

    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if days or hours:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m")
    return " ".join(parts)


def _build_timeline_breadcrumb_for_order(
    order, order_kind: str, perspective: str = "user"
):
    """Build compact timeline breadcrumb for order list cards."""
    breadcrumb = []

    if order_kind == "sell":
        breadcrumb.append(
            {
                "status": _("Order Created"),
                "completed": order.status
                in [
                    "draft",
                    "awaiting_validation",
                    "anomaly",
                    "anomaly_rejected",
                    "validated",
                    "completed",
                ],
                "icon": "fa-pen",
            }
        )
        breadcrumb.append(
            {
                "status": _("Awaiting Contract"),
                "completed": order.status
                in [
                    "awaiting_validation",
                    "anomaly",
                    "anomaly_rejected",
                    "validated",
                    "completed",
                ],
                "icon": "fa-file",
            }
        )
        breadcrumb.append(
            {
                "status": _("Auth Validation"),
                "completed": order.status in ["validated", "completed"],
                "icon": "fa-check-circle",
            }
        )
        breadcrumb.append(
            {
                "status": _("Corporation Acceptance"),
                "completed": order.status == "completed",
                "icon": "fa-flag-checkered",
            }
        )
    else:
        final_acceptance_label = (
            _("User Accept") if perspective == "admin" else _("You Accept")
        )
        breadcrumb.append(
            {
                "status": _("Order Created"),
                "completed": order.status
                in ["draft", "awaiting_validation", "validated", "completed"],
                "icon": "fa-pen",
            }
        )
        breadcrumb.append(
            {
                "status": _("Awaiting Corp Contract"),
                "completed": order.status
                in ["awaiting_validation", "validated", "completed"],
                "icon": "fa-file",
            }
        )
        breadcrumb.append(
            {
                "status": _("Auth Validation"),
                "completed": order.status in ["validated", "completed"],
                "icon": "fa-check-circle",
            }
        )
        breadcrumb.append(
            {
                "status": final_acceptance_label,
                "completed": order.status == "completed",
                "icon": "fa-hand-pointer",
            }
        )

    return breadcrumb


def _annotate_timeline_positions(timeline):
    total_steps = len(timeline)
    if total_steps <= 1:
        if total_steps == 1:
            timeline[0]["position_percent"] = 0
        return timeline

    last_index = total_steps - 1
    for index, step in enumerate(timeline):
        step["position_percent"] = round((index / last_index) * 100, 2)
    return timeline


def _attach_order_progress_data(order, order_kind: str, perspective: str = "user"):
    order.order_kind = order_kind
    order.timeline_breadcrumb = _build_timeline_breadcrumb_for_order(
        order, order_kind, perspective
    )
    order.timeline_breadcrumb = _annotate_timeline_positions(order.timeline_breadcrumb)
    order.progress_width = _calc_progress_width(order.timeline_breadcrumb)
    order.progress_total_steps = len(order.timeline_breadcrumb)
    order.progress_completed_steps = sum(
        1 for step in order.timeline_breadcrumb if step.get("completed")
    )
    order.progress_active_start = 0
    order.progress_active_width = 0

    current_step_index = 0
    for idx, step in enumerate(order.timeline_breadcrumb):
        if not step.get("completed"):
            current_step_index = idx
            break
    else:
        if order.progress_total_steps:
            current_step_index = order.progress_total_steps - 1

    if order.timeline_breadcrumb:
        order.progress_current_label = order.timeline_breadcrumb[current_step_index][
            "status"
        ]
        current_step_position = order.timeline_breadcrumb[current_step_index].get(
            "position_percent", 0
        )
        order.progress_active_start = 0
        order.progress_active_width = max(
            0, min(100, round(current_step_position, 2))
        )
    else:
        order.progress_current_label = ""

    return order


def _calc_progress_width(breadcrumb) -> int:
    if not breadcrumb:
        return 0
    total = len(breadcrumb)
    done = sum(1 for step in breadcrumb if step.get("completed"))
    if total <= 1:
        return 100 if done else 0
    ratio = max(0, min(done - 1, total - 1)) / (total - 1)
    return int(ratio * 100)


def _minutes_until_refresh(last_update, *, window_seconds: int = 3600) -> int | None:
    if not last_update:
        return None
    try:
        remaining = window_seconds - (timezone.now() - last_update).total_seconds()
    except Exception:
        return None
    if remaining <= 0:
        return 0
    return int((remaining + 59) // 60)


def _get_user_assets_last_sync(user):
    """Return latest cached assets sync timestamp for a user."""
    try:
        return (
            CachedCharacterAsset.objects.filter(user=user)
            .order_by("-synced_at")
            .values_list("synced_at", flat=True)
            .first()
        )
    except Exception:
        return None


def _get_material_exchange_settings() -> MaterialExchangeSettings | None:
    try:
        return MaterialExchangeSettings.get_solo()
    except Exception:
        return None


def _is_material_exchange_enabled() -> bool:
    settings_obj = _get_material_exchange_settings()
    if settings_obj is None:
        return True
    return bool(settings_obj.is_enabled)


def _get_material_exchange_config() -> MaterialExchangeConfig | None:
    return MaterialExchangeConfig.objects.first()


def _get_industry_market_group_ids() -> set[int]:
    """Return market group IDs used by EVE industry materials (cached)."""

    global _INDUSTRY_MARKET_GROUP_IDS_CACHE
    if _INDUSTRY_MARKET_GROUP_IDS_CACHE is not None:
        return _INDUSTRY_MARKET_GROUP_IDS_CACHE

    cache_key = "indy_hub:material_exchange:industry_market_group_ids:v1"
    cached = cache.get(cache_key)
    if cached is not None:
        try:
            _INDUSTRY_MARKET_GROUP_IDS_CACHE = {int(x) for x in cached}
            return _INDUSTRY_MARKET_GROUP_IDS_CACHE
        except Exception:
            _INDUSTRY_MARKET_GROUP_IDS_CACHE = set()
            return _INDUSTRY_MARKET_GROUP_IDS_CACHE

    try:
        # AA Example App
        from indy_hub.models import SdeIndustryActivityMaterial

        ids = set(
            SdeIndustryActivityMaterial.objects.exclude(
                material_eve_type__market_group_id__isnull=True
            )
            .values_list("material_eve_type__market_group_id", flat=True)
            .distinct()
        )
    except Exception as exc:
        logger.warning("Failed to load industry market group IDs: %s", exc)
        ids = set()

    cache.set(cache_key, list(ids), 3600)
    _INDUSTRY_MARKET_GROUP_IDS_CACHE = ids
    return ids


def _get_market_group_children_map() -> dict[int | None, set[int]]:
    """Return a mapping of parent_id -> child_ids (cached)."""

    cache_key = "indy_hub:material_exchange:market_group_children_map:v1"
    cached = cache.get(cache_key)
    if cached is not None:
        try:
            return {int(k) if k != "None" else None: set(v) for k, v in cached.items()}
        except Exception:
            pass

    try:
        # AA Example App
        from indy_hub.models import SdeMarketGroup

        children_map: dict[int | None, set[int]] = {}
        for group_id, parent_id in SdeMarketGroup.objects.values_list(
            "id", "parent_id"
        ):
            children_map.setdefault(parent_id, set()).add(group_id)
    except Exception as exc:
        logger.warning("Failed to load market group tree: %s", exc)
        return {}

    cache.set(
        cache_key,
        {"None" if k is None else str(k): list(v) for k, v in children_map.items()},
        3600,
    )
    return children_map


def _expand_market_group_ids(group_ids: set[int]) -> set[int]:
    """Expand market group IDs to include all descendants."""

    if not group_ids:
        return set()

    children_map = _get_market_group_children_map()
    expanded = set(group_ids)
    stack = list(group_ids)
    while stack:
        current = stack.pop()
        for child_id in children_map.get(current, set()):
            if child_id in expanded:
                continue
            expanded.add(child_id)
            stack.append(child_id)
    return expanded


def _get_allowed_type_ids_for_config(
    config: MaterialExchangeConfig, mode: str, *, structure_id: int | None = None
) -> set[int] | None:
    """Resolve allowed item type IDs for the given mode (sell/buy)."""

    if mode not in {"sell", "buy"}:
        return None

    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import ItemType

        explicit_all_groups = False
        if mode == "sell" and structure_id:
            structure_group_map = config.get_sell_market_group_map()
            structure_key = int(structure_id)
            if structure_key in structure_group_map:
                raw_group_ids = structure_group_map.get(structure_key)
                if raw_group_ids is None:
                    explicit_all_groups = True
            else:
                raw_group_ids = config.allowed_market_groups_sell
        else:
            raw_group_ids = (
                config.allowed_market_groups_sell
                if mode == "sell"
                else config.allowed_market_groups_buy
            )

        if explicit_all_groups:
            return None

        group_ids = {int(x) for x in (raw_group_ids or [])}
        if not group_ids:
            return set()

        expanded_group_ids = _expand_market_group_ids(group_ids)
        groups_key = ",".join(map(str, sorted(expanded_group_ids)))
        groups_hash = hashlib.md5(
            groups_key.encode("utf-8"), usedforsecurity=False
        ).hexdigest()
        cache_key = (
            "indy_hub:material_exchange:allowed_type_ids:v1:" f"{mode}:{groups_hash}"
        )
        cached = cache.get(cache_key)
        if cached is not None:
            return {int(x) for x in cached}

        allowed_type_ids = set(
            ItemType.objects.filter(
                market_group_id__in=expanded_group_ids
            ).values_list("id", flat=True)
        )
        cache.set(cache_key, list(allowed_type_ids), 3600)
        return allowed_type_ids
    except Exception as exc:
        logger.warning("Failed to resolve market group filter (%s): %s", mode, exc)
        return None


def _find_sell_locations_for_type(
    *,
    config: MaterialExchangeConfig,
    sell_structure_ids: list[int],
    sell_structure_name_map: dict[int, str],
    user_assets_by_location: dict[int, dict[int, int]],
    type_id: int,
    exclude_location_id: int | None = None,
    allowed_type_ids_cache: dict[int, set[int] | None] | None = None,
) -> list[dict[str, object]]:
    """Return sell locations where the given type is both present and accepted."""

    matches: list[dict[str, object]] = []
    cache_by_location = allowed_type_ids_cache if allowed_type_ids_cache is not None else {}

    for raw_location_id in sell_structure_ids:
        location_id = int(raw_location_id)
        if exclude_location_id is not None and int(exclude_location_id) == location_id:
            continue

        location_assets = user_assets_by_location.get(location_id, {})
        quantity = int(location_assets.get(int(type_id), 0) or 0)
        if quantity <= 0:
            continue

        if location_id in cache_by_location:
            allowed_type_ids = cache_by_location[location_id]
        else:
            allowed_type_ids = _get_allowed_type_ids_for_config(
                config,
                "sell",
                structure_id=location_id,
            )
            cache_by_location[location_id] = allowed_type_ids

        if allowed_type_ids is not None and int(type_id) not in allowed_type_ids:
            continue

        matches.append(
            {
                "id": location_id,
                "name": sell_structure_name_map.get(location_id) or f"Structure {location_id}",
                "quantity": quantity,
            }
        )

    return matches


def _get_material_exchange_admins() -> list[User]:
    """Return active admins for Buyback (explicit permission holders only)."""

    try:
        perm = Permission.objects.get(
            codename="can_manage_material_hub", content_type__app_label="indy_hub"
        )
        perm_users = User.objects.filter(
            Q(groups__permissions=perm) | Q(user_permissions=perm), is_active=True
        ).distinct()
        return list(perm_users)
    except Permission.DoesNotExist:
        return []


def _fetch_user_assets_for_structure(
    user,
    structure_ids: int | list[int],
    *,
    allow_refresh: bool = True,
    config: MaterialExchangeConfig | None = None,
) -> tuple[dict[int, int], bool]:
    """Return aggregated asset quantities for the user's characters at structure(s) using cache."""

    aggregated, _by_character, _by_location, scope_missing = (
        _fetch_user_assets_for_structure_data(
            user,
            structure_ids,
            allow_refresh=allow_refresh,
            config=config,
        )
    )
    return aggregated, scope_missing


def _get_ship_type_ids(type_ids: set[int]) -> set[int]:
    """Return type IDs that are ships (category 6) for the provided IDs."""
    if not type_ids:
        return set()
    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import ItemType

        return set(
            ItemType.objects.filter(
                id__in=type_ids,
                group__category_id=6,  # EVE SDE ship category
            ).values_list("id", flat=True)
        )
    except Exception:
        return set()


def _build_fitted_ship_excluded_item_ids(assets: list[dict]) -> set[int]:
    """Return item_ids to exclude when fitted ships are not allowed."""
    if not assets:
        return set()

    item_assets: dict[int, dict] = {}
    children_by_parent: dict[int, list[dict]] = {}
    present_type_ids: set[int] = set()

    for asset in assets:
        try:
            type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            type_id = 0
        if type_id > 0:
            present_type_ids.add(type_id)

        item_id_raw = asset.get("item_id")
        try:
            item_id = int(item_id_raw) if item_id_raw is not None else 0
        except (TypeError, ValueError):
            item_id = 0
        if item_id > 0:
            item_assets[item_id] = asset

        raw_location_id_raw = asset.get("raw_location_id")
        try:
            raw_location_id = (
                int(raw_location_id_raw) if raw_location_id_raw is not None else 0
            )
        except (TypeError, ValueError):
            raw_location_id = 0
        if raw_location_id > 0:
            children_by_parent.setdefault(raw_location_id, []).append(asset)

    ship_type_ids = _get_ship_type_ids(present_type_ids)
    if not ship_type_ids:
        return set()

    fitted_ship_item_ids: set[int] = set()
    for item_id, asset in item_assets.items():
        try:
            type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            continue
        if type_id not in ship_type_ids:
            continue
        # Consider the ship fitted if it has any direct child assets.
        if children_by_parent.get(item_id):
            fitted_ship_item_ids.add(item_id)

    if not fitted_ship_item_ids:
        return set()

    excluded_item_ids = set(fitted_ship_item_ids)
    stack = list(fitted_ship_item_ids)
    while stack:
        parent_id = stack.pop()
        for child in children_by_parent.get(parent_id, []):
            child_item_id_raw = child.get("item_id")
            try:
                child_item_id = (
                    int(child_item_id_raw) if child_item_id_raw is not None else 0
                )
            except (TypeError, ValueError):
                child_item_id = 0
            if child_item_id <= 0 or child_item_id in excluded_item_ids:
                continue
            excluded_item_ids.add(child_item_id)
            stack.append(child_item_id)

    return excluded_item_ids


def _fetch_user_assets_for_structure_data(
    user,
    structure_ids: int | list[int],
    *,
    allow_refresh: bool = True,
    config: MaterialExchangeConfig | None = None,
) -> tuple[dict[int, int], dict[int, dict[int, int]], dict[int, dict[int, int]], bool]:
    """Return aggregated and per-character asset quantities at structure(s) using cache."""

    assets, scope_missing = get_user_assets_cached(user, allow_refresh=allow_refresh)

    aggregated: dict[int, int] = {}
    by_character: dict[int, dict[int, int]] = {}
    by_location: dict[int, dict[int, int]] = {}
    if isinstance(structure_ids, (list, tuple, set)):
        structure_id_set: set[int] = set()
        for sid in structure_ids:
            try:
                sid_int = int(sid)
            except (TypeError, ValueError):
                continue
            if sid_int > 0:
                structure_id_set.add(sid_int)
    else:
        try:
            structure_id_set = {int(structure_ids)}
        except (TypeError, ValueError):
            structure_id_set = set()
    if not structure_id_set:
        return aggregated, by_character, by_location, scope_missing

    exclude_fitted_ships = not bool(getattr(config, "allow_fitted_ships", False))
    excluded_item_ids: set[int] = (
        _build_fitted_ship_excluded_item_ids(assets) if exclude_fitted_ships else set()
    )

    for asset in assets:
        if excluded_item_ids:
            item_id_raw = asset.get("item_id")
            try:
                item_id = int(item_id_raw) if item_id_raw is not None else 0
            except (TypeError, ValueError):
                item_id = 0
            if item_id > 0 and item_id in excluded_item_ids:
                continue

        try:
            location_id = int(asset.get("location_id", 0))
        except (TypeError, ValueError):
            continue
        if location_id not in structure_id_set:
            continue

        try:
            type_id = int(asset.get("type_id"))
        except (TypeError, ValueError):
            continue

        qty_raw = asset.get("quantity", 1)
        try:
            quantity = int(qty_raw or 0)
        except (TypeError, ValueError):
            quantity = 1

        if quantity <= 0:
            quantity = 1 if asset.get("is_singleton") else 0

        aggregated[type_id] = aggregated.get(type_id, 0) + quantity

        try:
            character_id = int(asset.get("character_id") or 0)
        except (TypeError, ValueError):
            character_id = 0
        if character_id > 0:
            char_assets = by_character.setdefault(character_id, {})
            char_assets[type_id] = char_assets.get(type_id, 0) + quantity

        loc_assets = by_location.setdefault(location_id, {})
        loc_assets[type_id] = loc_assets.get(type_id, 0) + quantity

    return aggregated, by_character, by_location, scope_missing


def _resolve_user_character_names_map(user) -> dict[int, str]:
    """Return owned character names keyed by character ID."""

    names: dict[int, str] = {}
    try:
        # Alliance Auth
        from allianceauth.authentication.models import CharacterOwnership

        ownerships = CharacterOwnership.objects.select_related("character").filter(
            user=user
        )
        for ownership in ownerships:
            character = getattr(ownership, "character", None)
            if not character:
                continue
            character_id = getattr(character, "character_id", None)
            if not character_id:
                continue
            character_name = (getattr(character, "character_name", "") or "").strip()
            names[int(character_id)] = character_name or str(character_id)
    except Exception:
        return names

    return names


def _me_sell_assets_progress_key(user_id: int) -> str:
    return f"indy_hub:material_exchange:sell_assets_refresh:{int(user_id)}"


def _ensure_sell_assets_refresh_started(user) -> dict:
    """Start (if needed) an async refresh of user assets and return the current progress state."""

    progress_key = _me_sell_assets_progress_key(user.id)
    ttl_seconds = 10 * 60
    state = cache.get(progress_key) or {}

    cooldown_until = cache.get(me_sell_assets_esi_cooldown_key(int(user.id)))
    if cooldown_until:
        try:
            retry_seconds = max(
                0, int(float(cooldown_until) - timezone.now().timestamp())
            )
        except (TypeError, ValueError):
            retry_seconds = int(ESI_DOWN_COOLDOWN_SECONDS)
        retry_minutes = int((retry_seconds + 59) // 60)
        state = {
            "running": False,
            "finished": True,
            "error": "esi_down",
            "retry_after_minutes": retry_minutes,
        }
        cache.set(progress_key, state, ttl_seconds)
        return state
    if state.get("running"):
        try:
            started_at = float(state.get("started_at") or 0)
            last_progress_at = float(state.get("last_progress_at") or started_at or 0)
            elapsed = timezone.now().timestamp() - last_progress_at
        except (TypeError, ValueError):
            elapsed = 0
        if not state.get("started_at") and not state.get("last_progress_at"):
            elapsed = 999999
        if elapsed <= 180:
            return state
        state.update({"running": False, "finished": True, "error": "timeout"})
        cache.set(progress_key, state, ttl_seconds)

    # Always refresh on page open unless explicitly suppressed.
    try:
        # Alliance Auth
        from allianceauth.authentication.models import CharacterOwnership
        from esi.models import Token

        total = int(
            CharacterOwnership.objects.filter(user=user)
            .values_list("character__character_id", flat=True)
            .distinct()
            .count()
        )

        has_assets_token = (
            Token.objects.filter(user=user)
            .require_scopes(["esi-assets.read_assets.v1"])
            .require_valid()
            .exists()
        )
    except Exception:
        total = 0
        has_assets_token = False

    if total > 0 and not has_assets_token:
        state = {
            "running": False,
            "finished": True,
            "error": "missing_assets_scope",
            "total": total,
            "done": 0,
            "failed": 0,
        }
        cache.set(progress_key, state, ttl_seconds)
        return state

    started_at = timezone.now().timestamp()
    state = {
        "running": True,
        "finished": False,
        "error": None,
        "total": total,
        "done": 0,
        "failed": 0,
        "started_at": started_at,
        "last_progress_at": started_at,
    }
    cache.set(progress_key, state, ttl_seconds)

    try:
        task_result = refresh_material_exchange_sell_user_assets.delay(int(user.id))
        logger.info(
            "Started asset refresh task for user %s (task_id=%s)",
            user.id,
            task_result.id,
        )
    except Exception as exc:
        # Fallback: mark as finished; UI will stop polling.
        logger.error(
            "Failed to start asset refresh task for user %s: %s",
            user.id,
            exc,
            exc_info=True,
        )
        state.update({"running": False, "finished": True, "error": "task_start_failed"})
        cache.set(progress_key, state, ttl_seconds)

    return state


@login_required
@indy_hub_permission_required("can_access_indy_hub")
def material_exchange_sell_assets_refresh_status(request):
    """Return JSON progress for sell-page user asset refresh."""
    emit_view_analytics_event(
        view_name="material_exchange.sell_assets_refresh_status", request=request
    )

    if not _is_material_exchange_enabled():
        return JsonResponse({"running": False, "finished": True, "error": "disabled"})

    progress_key = _me_sell_assets_progress_key(request.user.id)
    state = cache.get(progress_key) or {
        "running": False,
        "finished": False,
        "error": None,
        "total": 0,
        "done": 0,
        "failed": 0,
    }
    if state.get("running"):
        try:
            started_at = float(state.get("started_at") or 0)
            last_progress_at = float(state.get("last_progress_at") or started_at or 0)
            elapsed = timezone.now().timestamp() - last_progress_at
        except (TypeError, ValueError):
            elapsed = 0
        if not state.get("started_at") and not state.get("last_progress_at"):
            elapsed = 999999
        if elapsed > 180:
            state.update({"running": False, "finished": True, "error": "timeout"})
            cache.set(progress_key, state, 10 * 60)
    response = dict(state)
    try:
        last_update = (
            CachedCharacterAsset.objects.filter(user=request.user)
            .order_by("-synced_at")
            .values_list("synced_at", flat=True)
            .first()
        )
    except Exception:
        last_update = None

    if last_update:
        try:
            last_update_utc = timezone.localtime(last_update, timezone.utc)
        except Exception:
            last_update_utc = last_update
        response["last_update"] = last_update_utc.isoformat()

    return JsonResponse(response)


def _ensure_buy_stock_refresh_started(config) -> dict:
    """Start (if needed) an async refresh of buy stock and return the current progress state."""

    progress_key = (
        f"indy_hub:material_exchange:buy_stock_refresh:{int(config.corporation_id)}"
    )
    ttl_seconds = 10 * 60
    state = cache.get(progress_key) or {}

    cooldown_until = cache.get(
        me_buy_stock_esi_cooldown_key(int(config.corporation_id))
    )
    if cooldown_until:
        try:
            retry_seconds = max(
                0, int(float(cooldown_until) - timezone.now().timestamp())
            )
        except (TypeError, ValueError):
            retry_seconds = int(ESI_DOWN_COOLDOWN_SECONDS)
        retry_minutes = int((retry_seconds + 59) // 60)
        state = {
            "running": False,
            "finished": True,
            "error": "esi_down",
            "retry_after_minutes": retry_minutes,
        }
        cache.set(progress_key, state, ttl_seconds)
        return state

    if state.get("running"):
        return state

    state = {
        "running": True,
        "finished": False,
        "error": None,
    }
    cache.set(progress_key, state, ttl_seconds)

    try:
        task_result = refresh_material_exchange_buy_stock.delay(
            int(config.corporation_id)
        )
        logger.info(
            "Started buy stock refresh task for corporation %s (task_id=%s)",
            config.corporation_id,
            task_result.id,
        )
    except Exception as exc:
        logger.error(
            "Failed to start buy stock refresh task for corporation %s: %s",
            config.corporation_id,
            exc,
            exc_info=True,
        )
        state.update({"running": False, "finished": True, "error": "task_start_failed"})
        cache.set(progress_key, state, ttl_seconds)

    return state


@login_required
@indy_hub_permission_required("can_access_indy_hub")
def material_exchange_buy_stock_refresh_status(request):
    """Return JSON progress for buy-page stock refresh."""
    emit_view_analytics_event(
        view_name="material_exchange.buy_stock_refresh_status", request=request
    )
    if not _is_material_exchange_enabled():
        return JsonResponse({"running": False, "finished": True, "error": "disabled"})

    config = _get_material_exchange_config()
    if not config:
        return JsonResponse(
            {"running": False, "finished": True, "error": "not_configured"}
        )
    progress_key = (
        f"indy_hub:material_exchange:buy_stock_refresh:{int(config.corporation_id)}"
    )
    state = cache.get(progress_key) or {
        "running": False,
        "finished": False,
        "error": None,
    }
    return JsonResponse(state)


def _get_group_map(type_ids: list[int]) -> dict[int, str]:
    """Return mapping type_id -> group name using Eve SDE if available."""

    if not type_ids:
        return {}

    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import ItemType

        item_types = ItemType.objects.filter(id__in=type_ids).select_related("group")
        return {
            it.id: (it.group.name if getattr(it, "group", None) else "Other")
            for it in item_types
        }
    except Exception:
        return {}


def _get_type_name_map(type_ids: list[int]) -> dict[int, str]:
    """Return mapping type_id -> type name with minimal database round-trips."""

    cleaned_type_ids: set[int] = set()
    for raw_type_id in type_ids or []:
        try:
            type_id = int(raw_type_id)
        except (TypeError, ValueError):
            continue
        if type_id > 0:
            cleaned_type_ids.add(type_id)
    if not cleaned_type_ids:
        return {}

    type_name_map: dict[int, str] = {}
    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import ItemType

        rows = ItemType.objects.filter(id__in=cleaned_type_ids).values_list("id", "name")
        type_name_map = {int(raw_id): str(raw_name or "") for raw_id, raw_name in rows}
    except Exception:
        type_name_map = {}

    for type_id in cleaned_type_ids:
        if type_id not in type_name_map or not str(type_name_map[type_id]).strip():
            type_name_map[type_id] = get_type_name(type_id)
    return type_name_map


def _fetch_fuzzwork_prices(type_ids: list[int]) -> dict[int, dict[str, Decimal]]:
    """Batch fetch Jita buy/sell prices from Fuzzwork for given type IDs."""
    # Local
    from ..services.fuzzwork import FuzzworkError, fetch_fuzzwork_prices

    if not type_ids:
        return {}

    unique_id_set: set[int] = set()
    for raw_type_id in type_ids:
        try:
            type_id = int(raw_type_id)
        except (TypeError, ValueError):
            continue
        if type_id > 0:
            unique_id_set.add(type_id)
    unique_ids: list[int] = sorted(unique_id_set)
    if not unique_ids:
        return {}

    price_map: dict[int, dict[str, Decimal]] = {}
    batch_size = 200
    for batch_start in range(0, len(unique_ids), batch_size):
        batch_ids = unique_ids[batch_start : batch_start + batch_size]
        try:
            batch_prices = fetch_fuzzwork_prices(batch_ids, timeout=12)
        except FuzzworkError as exc:  # pragma: no cover - defensive
            logger.warning(
                "material_exchange: failed to fetch fuzzwork prices for batch %s-%s: %s",
                batch_start,
                batch_start + len(batch_ids) - 1,
                exc,
            )
            continue
        price_map.update(batch_prices)
    return price_map


def _get_stock_jita_price_map(
    *, config: MaterialExchangeConfig, type_ids: list[int]
) -> dict[int, dict[str, Decimal]]:
    """Return cached Jita buy/sell prices from MaterialExchangeStock for a config."""

    cleaned_type_ids: set[int] = set()
    for raw_type_id in type_ids or []:
        try:
            type_id = int(raw_type_id)
        except (TypeError, ValueError):
            continue
        if type_id > 0:
            cleaned_type_ids.add(type_id)
    if not cleaned_type_ids:
        return {}

    rows = MaterialExchangeStock.objects.filter(
        config=config,
        type_id__in=cleaned_type_ids,
    ).values("type_id", "jita_buy_price", "jita_sell_price")

    price_map: dict[int, dict[str, Decimal]] = {}
    for row in rows:
        try:
            type_id = int(row.get("type_id") or 0)
        except (TypeError, ValueError):
            continue
        if type_id <= 0:
            continue
        price_map[type_id] = {
            "buy": Decimal(str(row.get("jita_buy_price") or 0)),
            "sell": Decimal(str(row.get("jita_sell_price") or 0)),
        }
    return price_map


def _upsert_stock_jita_prices(
    *,
    config: MaterialExchangeConfig,
    price_data: dict[int, dict[str, Decimal]],
) -> int:
    """Persist fetched Jita prices into MaterialExchangeStock for this config."""

    normalized_prices: dict[int, tuple[Decimal, Decimal]] = {}
    for raw_type_id, raw_info in (price_data or {}).items():
        try:
            type_id = int(raw_type_id)
        except (TypeError, ValueError):
            continue
        if type_id <= 0:
            continue
        info = raw_info or {}
        normalized_prices[type_id] = (
            Decimal(str(info.get("buy") or 0)),
            Decimal(str(info.get("sell") or 0)),
        )
    if not normalized_prices:
        return 0

    now = timezone.now()
    type_ids = list(normalized_prices.keys())
    existing_rows = {
        int(row.type_id): row
        for row in MaterialExchangeStock.objects.filter(
            config=config,
            type_id__in=type_ids,
        )
    }

    rows_to_update: list[MaterialExchangeStock] = []
    rows_to_create: list[MaterialExchangeStock] = []

    for type_id, (buy_price, sell_price) in normalized_prices.items():
        stock_row = existing_rows.get(type_id)
        if stock_row is not None:
            stock_row.jita_buy_price = buy_price
            stock_row.jita_sell_price = sell_price
            stock_row.last_price_update = now
            rows_to_update.append(stock_row)
            continue

        rows_to_create.append(
            MaterialExchangeStock(
                config=config,
                type_id=type_id,
                type_name="",
                quantity=0,
                jita_buy_price=buy_price,
                jita_sell_price=sell_price,
                last_price_update=now,
            )
        )

    if rows_to_create:
        MaterialExchangeStock.objects.bulk_create(rows_to_create, ignore_conflicts=True)
    if rows_to_update:
        MaterialExchangeStock.objects.bulk_update(
            rows_to_update,
            ["type_name", "jita_buy_price", "jita_sell_price", "last_price_update"],
        )
    return len(rows_to_create) + len(rows_to_update)


def _parse_submitted_quantities(post_data) -> dict[int, int]:
    """Parse `qty_*` POST fields and return summed quantities by type_id."""

    submitted_quantities: dict[int, int] = {}
    for key, values in post_data.lists():
        if not key.startswith("qty_"):
            continue
        type_id_part = key[4:].split("_", 1)[0]
        if not type_id_part.isdigit():
            continue
        type_id = int(type_id_part)
        for raw_value in values:
            qty_raw = (raw_value or "").strip()
            if not qty_raw:
                continue
            try:
                qty = int(qty_raw)
            except Exception:
                continue
            if qty <= 0:
                continue
            submitted_quantities[type_id] = submitted_quantities.get(type_id, 0) + qty
    return submitted_quantities


def _parse_submitted_sell_item_quantities(post_data) -> list[dict[str, object]]:
    """Parse sell-form `qty_*` fields and keep blueprint variant when present.

    Supports both:
    - `qty_<type_id>_<row_index>` (legacy)
    - `qty_<type_id>_<variant>_<row_index>` where variant is `std|bpo|bpc`
    - `qty_<type_id>_<variant>_<scope>_<row_index>` where scope is `root|incan`
    """

    grouped: dict[tuple[int, str, bool], int] = {}
    for key, values in post_data.lists():
        if not key.startswith("qty_"):
            continue

        suffix = key[4:]
        parts = suffix.split("_")
        if not parts:
            continue
        type_id_part = parts[0]
        if not type_id_part.isdigit():
            continue
        type_id = int(type_id_part)

        variant = ""
        is_in_container = False
        if len(parts) >= 4 and str(parts[2] or "").strip().lower() in {
            "incan",
            "root",
        }:
            raw_variant = str(parts[1] or "").strip().lower()
            if raw_variant in {"std", "bpo", "bpc"}:
                variant = "" if raw_variant == "std" else raw_variant
            is_in_container = str(parts[2] or "").strip().lower() == "incan"
        elif len(parts) >= 3:
            raw_variant = str(parts[1] or "").strip().lower()
            if raw_variant in {"std", "bpo", "bpc"}:
                variant = "" if raw_variant == "std" else raw_variant

        for raw_value in values:
            qty_raw = (raw_value or "").strip()
            if not qty_raw:
                continue
            try:
                qty = int(qty_raw)
            except Exception:
                continue
            if qty <= 0:
                continue
            key_tuple = (type_id, variant, is_in_container)
            grouped[key_tuple] = grouped.get(key_tuple, 0) + qty

    entries: list[dict[str, object]] = []
    for (type_id, variant, is_in_container), quantity in grouped.items():
        entries.append(
            {
                "type_id": int(type_id),
                "blueprint_variant": str(variant or ""),
                "in_container": bool(is_in_container),
                "quantity": int(quantity),
            }
        )
    return entries


def _normalize_sell_estimate_item_text(value: str | None) -> str:
    """Normalize user-pasted item text before matching a type."""

    text = unicodedata.normalize("NFKC", str(value or ""))
    text = (
        text.replace("\u00A0", " ")
        .replace("\u202F", " ")
        .replace("\u2009", " ")
        .replace("\u2010", "-")
        .replace("\u2011", "-")
        .replace("\u2012", "-")
        .replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2212", "-")
    )
    text = re.sub(r"\s*-\s*", "-", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_sell_estimate_positive_quantity(raw_value: str | int | None) -> int | None:
    """Parse positive integer quantities from pasted estimate lines."""

    text_value = str(raw_value or "").strip()
    if not text_value:
        return None
    normalized = (
        text_value.replace("\u00A0", " ")
        .replace("\u202F", " ")
        .replace("\u2009", " ")
        .replace("_", "")
        .replace("'", "")
    )
    compact = normalized.replace(" ", "")
    if compact.isdigit():
        parsed = int(compact)
        return parsed if parsed > 0 else None
    if re.match(r"^\d{1,3}(?:[.,]\d{3})+$", compact):
        parsed = int(compact.replace(",", "").replace(".", ""))
        return parsed if parsed > 0 else None
    return None


def _build_sell_estimate_candidate_keys(normalized_text: str) -> list[str]:
    """Return normalized lowercase candidate keys for type-name matching."""

    candidates: list[str] = [normalized_text]
    if "-" in normalized_text:
        candidates.append(normalized_text.replace("-", " "))
    grade_dash = re.sub(r"(?i)\b([ivx]+)\s+grade\b", r"\1-Grade", normalized_text)
    grade_space = re.sub(r"(?i)\b([ivx]+)-grade\b", r"\1 Grade", normalized_text)
    candidates.extend([grade_dash, grade_space])

    deduped_keys: list[str] = []
    seen_keys: set[str] = set()
    for candidate in candidates:
        candidate_norm = _normalize_sell_estimate_item_text(candidate)
        if not candidate_norm:
            continue
        candidate_key = candidate_norm.casefold()
        if candidate_key in seen_keys:
            continue
        seen_keys.add(candidate_key)
        deduped_keys.append(candidate_key)
    return deduped_keys


def _resolve_type_ids_for_sell_estimate_texts(
    type_texts: list[str],
) -> dict[str, int | None]:
    """Resolve many pasted item texts at once. Result keys are normalized lowercase names."""

    resolved_by_cache_key: dict[str, int | None] = {}
    unresolved_candidate_keys: dict[str, list[str]] = {}

    for raw_text in type_texts:
        normalized = _normalize_sell_estimate_item_text(raw_text)
        if not normalized:
            continue
        cache_key = normalized.casefold()

        if normalized.isdigit():
            parsed_id = int(normalized)
            resolved_by_cache_key[cache_key] = parsed_id if parsed_id > 0 else None
            continue

        if cache_key in _SELL_ESTIMATE_TYPE_LOOKUP_CACHE:
            resolved_by_cache_key[cache_key] = _SELL_ESTIMATE_TYPE_LOOKUP_CACHE[cache_key]
            continue

        unresolved_candidate_keys[cache_key] = _build_sell_estimate_candidate_keys(
            normalized
        )

    if not unresolved_candidate_keys:
        return resolved_by_cache_key

    candidate_keys: set[str] = {
        candidate_key
        for keys in unresolved_candidate_keys.values()
        for candidate_key in keys
    }
    resolved_candidate_map: dict[str, int] = {}

    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import ItemType

        exact_rows = (
            ItemType.objects.annotate(name_lookup_key=Lower("name"))
            .filter(name_lookup_key__in=candidate_keys)
            .values_list("id", "name_lookup_key")
        )
        for raw_type_id, raw_lookup_key in exact_rows:
            if raw_lookup_key not in resolved_candidate_map:
                resolved_candidate_map[str(raw_lookup_key)] = int(raw_type_id)

        unresolved_lookup_keys = {
            key for key in candidate_keys if key not in resolved_candidate_map
        }
        if unresolved_lookup_keys:
            normalized_rows = (
                ItemType.objects.annotate(
                    name_lookup_key=Lower(
                        Replace(
                            Replace(
                                Replace(
                                    Replace("name", Value("\u2011"), Value("-")),
                                    Value("\u2013"),
                                    Value("-"),
                                ),
                                Value("\u2014"),
                                Value("-"),
                            ),
                            Value("\u2212"),
                            Value("-"),
                        )
                    )
                )
                .filter(name_lookup_key__in=unresolved_lookup_keys)
                .values_list("id", "name_lookup_key")
            )
            for raw_type_id, raw_lookup_key in normalized_rows:
                if raw_lookup_key not in resolved_candidate_map:
                    resolved_candidate_map[str(raw_lookup_key)] = int(raw_type_id)
    except Exception:
        resolved_candidate_map = {}

    for cache_key, lookup_keys in unresolved_candidate_keys.items():
        resolved_type_id: int | None = None
        for lookup_key in lookup_keys:
            resolved_type_id = resolved_candidate_map.get(lookup_key)
            if resolved_type_id:
                break
        resolved_by_cache_key[cache_key] = resolved_type_id
        _SELL_ESTIMATE_TYPE_LOOKUP_CACHE[cache_key] = resolved_type_id

    return resolved_by_cache_key


def _resolve_type_id_from_sell_estimate_text(type_text: str) -> int | None:
    """Resolve type ID from pasted estimate item text."""

    normalized = _normalize_sell_estimate_item_text(type_text)
    if not normalized:
        return None
    cache_key = normalized.casefold()
    resolved = _resolve_type_ids_for_sell_estimate_texts([normalized]).get(cache_key)
    return int(resolved) if resolved else None


def _parse_sell_estimate_input(raw_text: str) -> tuple[list[dict[str, int]], list[str]]:
    """Parse pasted estimate lines into normalized type/quantity rows."""

    rows_by_type: dict[int, int] = {}
    invalid_lines: list[str] = []
    parsed_lines: list[tuple[str, str, int]] = []

    for raw_line in str(raw_text or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue

        item_part = ""
        quantity: int | None = None

        tab_parts = [
            str(part or "").strip()
            for part in line.split("\t")
            if str(part or "").strip()
        ]
        if len(tab_parts) >= 2:
            item_part = tab_parts[0]
            for quantity_candidate in tab_parts[1:]:
                quantity = _parse_sell_estimate_positive_quantity(quantity_candidate)
                if quantity is not None:
                    break

        if not item_part or quantity is None:
            normalized_line = (
                line.replace("\u00A0", " ")
                .replace("\u202F", " ")
                .replace("\u2009", " ")
                .strip()
            )
            match = re.match(r"^(.*\S)[ \t]+([0-9][0-9\s,._']*)$", normalized_line)
            if match:
                item_part = str(match.group(1) or "").strip()
                quantity = _parse_sell_estimate_positive_quantity(match.group(2))

        item_part = str(item_part or "").strip()
        if not item_part or quantity is None:
            invalid_lines.append(line)
            continue

        parsed_lines.append((line, item_part, int(quantity)))

    resolved_type_ids = _resolve_type_ids_for_sell_estimate_texts(
        [item_text for _line, item_text, _qty in parsed_lines]
    )
    for line, item_part, quantity in parsed_lines:
        item_cache_key = _normalize_sell_estimate_item_text(item_part).casefold()
        type_id = resolved_type_ids.get(item_cache_key)
        if not type_id:
            invalid_lines.append(line)
            continue

        rows_by_type[int(type_id)] = rows_by_type.get(int(type_id), 0) + int(quantity)

    rows = [
        {"type_id": int(type_id), "quantity": int(quantity)}
        for type_id, quantity in sorted(
            rows_by_type.items(),
            key=lambda pair: int(pair[0]),
        )
    ]
    return rows, invalid_lines


def _get_effective_sell_structure_ids(config: MaterialExchangeConfig) -> list[int]:
    """Return configured sell structure IDs with a safe primary fallback."""

    sell_structure_ids = [int(sid) for sid in (config.get_sell_structure_ids() or [])]
    if sell_structure_ids:
        return sell_structure_ids
    try:
        primary_id = int(config.structure_id or 0)
    except (TypeError, ValueError):
        primary_id = 0
    return [primary_id] if primary_id > 0 else []


def _get_estimate_accepting_sell_locations(
    *,
    config: MaterialExchangeConfig,
    type_id: int,
    sell_structure_ids: list[int],
    sell_structure_name_map: dict[int, str],
    allowed_type_ids_cache: dict[int, set[int] | None],
) -> list[str]:
    """Return configured sell locations that accept the given type."""

    accepted_locations: list[str] = []
    for structure_id in sell_structure_ids:
        if structure_id not in allowed_type_ids_cache:
            allowed_type_ids_cache[structure_id] = _get_allowed_type_ids_for_config(
                config,
                "sell",
                structure_id=int(structure_id),
            )
        allowed_type_ids = allowed_type_ids_cache[structure_id]
        if allowed_type_ids is None or int(type_id) in allowed_type_ids:
            accepted_locations.append(
                str(
                    sell_structure_name_map.get(int(structure_id))
                    or f"Structure {int(structure_id)}"
                )
            )
    return accepted_locations


def _build_price_override_entry(
    *,
    fixed_price_raw,
    markup_percent_raw,
    markup_base_raw,
) -> dict[str, object] | None:
    """Return normalized override payload or None when unset/invalid."""

    def _coerce_decimal(raw_value) -> Decimal | None:
        if raw_value is None:
            return None
        normalized = str(raw_value).strip()
        if not normalized:
            return None
        try:
            return Decimal(normalized)
        except Exception:
            return None

    fixed_price = _coerce_decimal(fixed_price_raw)
    if fixed_price is not None:
        return {"kind": "fixed", "price": fixed_price}

    markup_percent = _coerce_decimal(markup_percent_raw)
    if markup_percent is None:
        return None
    markup_base = str(markup_base_raw or "buy").strip().lower()
    if markup_base not in {"buy", "sell"}:
        markup_base = "buy"
    return {"kind": "markup", "percent": markup_percent, "base": markup_base}


def _build_sell_variant_quantities(
    *, assets: list[dict], location_id: int | None
) -> dict[tuple[int, str], int]:
    """Return available quantities keyed by (type_id, blueprint_variant)."""

    quantities: dict[tuple[int, str], int] = {}
    location_filter = int(location_id or 0)

    for asset in assets:
        try:
            asset_location_id = int(asset.get("location_id") or 0)
        except (TypeError, ValueError):
            continue
        if location_filter > 0 and asset_location_id != location_filter:
            continue

        try:
            type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            continue
        if type_id <= 0:
            continue

        quantity = _asset_quantity(asset)
        if quantity <= 0:
            continue

        blueprint_variant = _asset_blueprint_variant(asset)
        variant_key = (int(type_id), str(blueprint_variant or ""))
        quantities[variant_key] = quantities.get(variant_key, 0) + int(quantity)

    return quantities


def _build_scoped_variant_quantities(
    *,
    assets: list[dict],
    location_id: int | None = None,
    explicit_variant_by_item_id: dict[int, str] | None = None,
) -> dict[tuple[int, str, bool], int]:
    """Return quantities keyed by (type_id, blueprint_variant, in_container)."""

    if not assets:
        return {}

    asset_by_item_id: dict[int, dict] = {}
    for asset in assets:
        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        if item_id > 0:
            asset_by_item_id[item_id] = asset

    def _resolve_parent_item_id(asset: dict) -> int:
        for field_name in ("raw_location_id", "location_id"):
            try:
                candidate = int(asset.get(field_name) or 0)
            except (TypeError, ValueError):
                candidate = 0
            if candidate > 0 and candidate in asset_by_item_id:
                return candidate
        return 0

    parent_by_item_id: dict[int, int] = {}
    for asset in assets:
        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        if item_id <= 0:
            continue
        parent_item_id = _resolve_parent_item_id(asset)
        if parent_item_id > 0:
            parent_by_item_id[item_id] = parent_item_id

    normalized_explicit_variants = {
        int(item_id): str(variant or "").strip().lower()
        for item_id, variant in (explicit_variant_by_item_id or {}).items()
        if int(item_id) > 0 and str(variant or "").strip().lower() in {"bpc", "bpo"}
    }

    location_filter = int(location_id or 0)
    quantities: dict[tuple[int, str, bool], int] = {}
    for asset in assets:
        if location_filter > 0:
            try:
                asset_location_id = int(asset.get("location_id") or 0)
            except (TypeError, ValueError):
                continue
            if asset_location_id != location_filter:
                continue

        try:
            type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            continue
        if type_id <= 0:
            continue

        quantity = _asset_quantity(asset)
        if quantity <= 0:
            continue

        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        blueprint_variant = normalized_explicit_variants.get(item_id)
        if blueprint_variant not in {"bpc", "bpo"}:
            blueprint_variant = _asset_blueprint_variant(asset)
        in_container = int(parent_by_item_id.get(item_id, 0) or 0) > 0
        key = (int(type_id), str(blueprint_variant or ""), bool(in_container))
        quantities[key] = quantities.get(key, 0) + int(quantity)

    return quantities


def _get_item_price_override_maps(
    config: MaterialExchangeConfig,
) -> tuple[dict[int, dict[str, object]], dict[int, dict[str, object]]]:
    """Return per-item override maps for sell and buy sides."""

    sell_overrides: dict[int, dict[str, object]] = {}
    buy_overrides: dict[int, dict[str, object]] = {}
    try:
        rows = MaterialExchangeItemPriceOverride.objects.filter(config=config).values_list(
            "type_id",
            "sell_markup_percent_override",
            "sell_markup_base_override",
            "buy_markup_percent_override",
            "buy_markup_base_override",
            "sell_price_override",
            "buy_price_override",
        )
    except Exception:
        return sell_overrides, buy_overrides

    for (
        type_id,
        sell_markup_percent_override,
        sell_markup_base_override,
        buy_markup_percent_override,
        buy_markup_base_override,
        sell_override,
        buy_override,
    ) in rows:
        type_id_int = int(type_id)
        sell_override_value = _build_price_override_entry(
            fixed_price_raw=sell_override,
            markup_percent_raw=sell_markup_percent_override,
            markup_base_raw=sell_markup_base_override,
        )
        if sell_override_value is not None:
            sell_overrides[type_id_int] = sell_override_value

        buy_override_value = _build_price_override_entry(
            fixed_price_raw=buy_override,
            markup_percent_raw=buy_markup_percent_override,
            markup_base_raw=buy_markup_base_override,
        )
        if buy_override_value is not None:
            buy_overrides[type_id_int] = buy_override_value

    return sell_overrides, buy_overrides


def _get_market_group_price_override_maps(
    config: MaterialExchangeConfig,
) -> tuple[dict[int, dict[str, object]], dict[int, dict[str, object]]]:
    """Return per-market-group override maps for sell and buy sides."""

    raw_rows = list(getattr(config, "market_group_price_overrides", []) or [])
    sell_overrides: dict[int, dict[str, object]] = {}
    buy_overrides: dict[int, dict[str, object]] = {}
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict):
            continue
        try:
            market_group_id = int(
                raw_row.get("market_group_id") or raw_row.get("group_id") or 0
            )
        except (TypeError, ValueError):
            continue
        if market_group_id <= 0:
            continue

        sell_override = _build_price_override_entry(
            fixed_price_raw=raw_row.get("sell_price_override"),
            markup_percent_raw=raw_row.get("sell_markup_percent_override"),
            markup_base_raw=raw_row.get("sell_markup_base_override"),
        )
        if sell_override is not None:
            sell_overrides[int(market_group_id)] = sell_override

        buy_override = _build_price_override_entry(
            fixed_price_raw=raw_row.get("buy_price_override"),
            markup_percent_raw=raw_row.get("buy_markup_percent_override"),
            markup_base_raw=raw_row.get("buy_markup_base_override"),
        )
        if buy_override is not None:
            buy_overrides[int(market_group_id)] = buy_override

    return sell_overrides, buy_overrides


def _get_container_price_override_maps(
    config: MaterialExchangeConfig,
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    """Return container-only (in-can) override payloads for sell and buy sides."""

    raw_payload = getattr(config, "container_price_overrides", {}) or {}
    if not isinstance(raw_payload, dict):
        return None, None

    sell_override = _build_price_override_entry(
        fixed_price_raw=raw_payload.get("sell_price_override"),
        markup_percent_raw=raw_payload.get("sell_markup_percent_override"),
        markup_base_raw=raw_payload.get("sell_markup_base_override"),
    )
    buy_override = _build_price_override_entry(
        fixed_price_raw=raw_payload.get("buy_price_override"),
        markup_percent_raw=raw_payload.get("buy_markup_percent_override"),
        markup_base_raw=raw_payload.get("buy_markup_base_override"),
    )
    return sell_override, buy_override


def _get_market_group_parent_map() -> dict[int, int | None]:
    """Return market-group parent mapping keyed by market_group_id."""

    cache_key = "indy_hub:material_exchange:market_group_parent_map:v1"
    cached = cache.get(cache_key)
    if cached is not None:
        try:
            return {
                int(group_id): (
                    int(parent_id) if parent_id not in (None, "", 0, "0") else None
                )
                for group_id, parent_id in cached.items()
            }
        except Exception:
            return {}

    try:
        from ..models import SdeMarketGroup

        parent_map = {
            int(group_id): (
                int(parent_id) if parent_id not in (None, "", 0, "0") else None
            )
            for group_id, parent_id in SdeMarketGroup.objects.values_list(
                "id", "parent_id"
            )
        }
    except Exception as exc:
        logger.warning("Failed to load market-group parent map: %s", exc)
        return {}

    cache.set(
        cache_key,
        {str(group_id): parent_id for group_id, parent_id in parent_map.items()},
        3600,
    )
    return parent_map


def _get_type_market_group_path_map(type_ids: set[int] | list[int]) -> dict[int, list[int]]:
    """Return type_id -> market-group path IDs (root to leaf)."""

    cleaned_type_ids: set[int] = set()
    for raw_type_id in type_ids or []:
        try:
            type_id = int(raw_type_id)
        except (TypeError, ValueError):
            continue
        if type_id > 0:
            cleaned_type_ids.add(type_id)
    if not cleaned_type_ids:
        return {}

    try:
        from eve_sde.models import ItemType
    except Exception:
        return {}

    parent_map = _get_market_group_parent_map()
    path_map: dict[int, list[int]] = {}
    rows = ItemType.objects.filter(id__in=cleaned_type_ids).values_list(
        "id", "market_group_id"
    )
    for raw_type_id, raw_market_group_id in rows:
        try:
            type_id = int(raw_type_id)
            market_group_id = int(raw_market_group_id or 0)
        except (TypeError, ValueError):
            continue
        if type_id <= 0 or market_group_id <= 0:
            continue

        path_ids: list[int] = []
        seen: set[int] = set()
        current_id = int(market_group_id)
        while current_id > 0 and current_id not in seen:
            seen.add(current_id)
            path_ids.append(current_id)
            parent_id = parent_map.get(current_id)
            if parent_id in (None, 0):
                break
            current_id = int(parent_id)
        if not path_ids:
            continue
        path_map[int(type_id)] = list(reversed(path_ids))
    return path_map


def _build_type_market_group_label_map(
    type_market_group_path_map: dict[int, list[int]] | None,
) -> dict[int, dict[str, str]]:
    """Return type_id -> market-group labels (leaf name and full path string)."""

    if not type_market_group_path_map:
        return {}

    group_ids: set[int] = set()
    for path_ids in type_market_group_path_map.values():
        for raw_group_id in path_ids or []:
            try:
                group_id = int(raw_group_id)
            except (TypeError, ValueError):
                continue
            if group_id > 0:
                group_ids.add(group_id)
    if not group_ids:
        return {}

    group_name_map: dict[int, str] = {}
    try:
        from ..models import SdeMarketGroup

        for raw_group_id, raw_name in SdeMarketGroup.objects.filter(
            id__in=group_ids
        ).values_list("id", "name"):
            try:
                group_id = int(raw_group_id)
            except (TypeError, ValueError):
                continue
            clean_name = str(raw_name or "").strip()
            if group_id > 0 and clean_name:
                group_name_map[group_id] = clean_name
    except Exception:
        group_name_map = {}

    labels_by_type: dict[int, dict[str, str]] = {}
    for raw_type_id, path_ids in (type_market_group_path_map or {}).items():
        try:
            type_id = int(raw_type_id)
        except (TypeError, ValueError):
            continue
        if type_id <= 0:
            continue
        path_names: list[str] = []
        for raw_group_id in path_ids or []:
            try:
                group_id = int(raw_group_id)
            except (TypeError, ValueError):
                continue
            if group_id <= 0:
                continue
            group_name = str(group_name_map.get(group_id) or "").strip()
            if group_name:
                path_names.append(group_name)
        if not path_names:
            continue
        labels_by_type[type_id] = {
            "name": str(path_names[-1]),
            "path": " > ".join(path_names),
        }
    return labels_by_type


def _resolve_market_group_override_for_type(
    *,
    type_id: int,
    market_group_override_map: dict[int, dict[str, object]] | None,
    type_market_group_path_map: dict[int, list[int]] | None,
) -> dict[str, object] | None:
    """Return nearest market-group override for a type, preferring leaf-most group."""

    if not market_group_override_map or not type_market_group_path_map:
        return None
    path_ids = list(type_market_group_path_map.get(int(type_id), []) or [])
    if not path_ids:
        return None
    for path_group_id in reversed(path_ids):
        override_value = market_group_override_map.get(int(path_group_id))
        if override_value is not None:
            return override_value
    return None


def _resolve_price_override_for_type(
    *,
    type_id: int,
    item_override_map: dict[int, dict[str, object]],
    market_group_override_map: dict[int, dict[str, object]] | None = None,
    type_market_group_path_map: dict[int, list[int]] | None = None,
    container_override: dict[str, object] | None = None,
    in_container: bool = False,
) -> dict[str, object] | None:
    """Resolve effective override for a type with precedence item > in-container > market-group."""

    direct_override = item_override_map.get(int(type_id))
    if direct_override is not None:
        return direct_override
    if in_container and container_override is not None:
        return container_override
    return _resolve_market_group_override_for_type(
        type_id=int(type_id),
        market_group_override_map=market_group_override_map,
        type_market_group_path_map=type_market_group_path_map,
    )


def _compute_effective_sell_unit_price(
    *,
    config: MaterialExchangeConfig,
    type_id: int,
    jita_buy: Decimal,
    jita_sell: Decimal,
    sell_override_map: dict[int, dict[str, object]],
    sell_market_group_override_map: dict[int, dict[str, object]] | None = None,
    type_market_group_path_map: dict[int, list[int]] | None = None,
    sell_container_override: dict[str, object] | None = None,
    in_container: bool = False,
) -> tuple[Decimal, Decimal, bool]:
    """Return (effective, default, has_override) for sell-page pricing."""

    default_unit_price = compute_buy_price_from_member(
        config=config,
        jita_buy=jita_buy,
        jita_sell=jita_sell,
    )
    override_value = _resolve_price_override_for_type(
        type_id=int(type_id),
        item_override_map=sell_override_map,
        market_group_override_map=sell_market_group_override_map,
        type_market_group_path_map=type_market_group_path_map,
        container_override=sell_container_override,
        in_container=bool(in_container),
    )
    if override_value is None:
        return default_unit_price, default_unit_price, False
    if str(override_value.get("kind") or "") == "markup":
        override_base = str(override_value.get("base") or "buy").strip().lower()
        if override_base not in {"buy", "sell"}:
            override_base = "buy"
        effective_price = apply_markup_with_jita_bounds(
            jita_buy=jita_buy,
            jita_sell=jita_sell,
            base_choice=override_base,
            percent=Decimal(override_value.get("percent") or 0),
            enforce_bounds=bool(getattr(config, "enforce_jita_price_bounds", False)),
        )
    else:
        effective_price = Decimal(override_value.get("price") or 0)
    return effective_price, default_unit_price, effective_price != default_unit_price


def _compute_effective_buy_unit_price(
    *,
    stock_item: MaterialExchangeStock | None = None,
    type_id: int | None = None,
    jita_buy: Decimal | None = None,
    jita_sell: Decimal | None = None,
    default_unit_price: Decimal | None = None,
    config: MaterialExchangeConfig | None = None,
    buy_override_map: dict[int, dict[str, object]],
    buy_market_group_override_map: dict[int, dict[str, object]] | None = None,
    type_market_group_path_map: dict[int, list[int]] | None = None,
    buy_container_override: dict[str, object] | None = None,
    in_container: bool = False,
) -> tuple[Decimal, Decimal, bool]:
    """Return (effective, default, has_override) for buy-page pricing."""

    resolved_type_id = int(type_id or 0)
    resolved_jita_buy = Decimal(jita_buy or 0)
    resolved_jita_sell = Decimal(jita_sell or 0)
    resolved_default_price = (
        Decimal(default_unit_price) if default_unit_price is not None else Decimal("0")
    )
    resolved_config = config

    if stock_item is not None:
        resolved_type_id = int(getattr(stock_item, "type_id", 0) or 0)
        resolved_jita_buy = Decimal(getattr(stock_item, "jita_buy_price", 0) or 0)
        resolved_jita_sell = Decimal(getattr(stock_item, "jita_sell_price", 0) or 0)
        if default_unit_price is None:
            resolved_default_price = Decimal(stock_item.sell_price_to_member or 0)
        resolved_config = resolved_config or getattr(stock_item, "config", None)

    if resolved_default_price <= 0 and resolved_config is not None:
        base_choice = str(getattr(resolved_config, "buy_markup_base", "buy") or "buy")
        percent = Decimal(getattr(resolved_config, "buy_markup_percent", 0) or 0)
        resolved_default_price = apply_markup_with_jita_bounds(
            jita_buy=resolved_jita_buy,
            jita_sell=resolved_jita_sell,
            base_choice=base_choice,
            percent=percent,
            enforce_bounds=bool(
                getattr(resolved_config, "enforce_jita_price_bounds", False)
            ),
        )

    override_value = _resolve_price_override_for_type(
        type_id=resolved_type_id,
        item_override_map=buy_override_map,
        market_group_override_map=buy_market_group_override_map,
        type_market_group_path_map=type_market_group_path_map,
        container_override=buy_container_override,
        in_container=bool(in_container),
    )
    if override_value is None:
        return resolved_default_price, resolved_default_price, False
    if str(override_value.get("kind") or "") == "markup":
        override_base = str(override_value.get("base") or "buy").strip().lower()
        if override_base not in {"buy", "sell"}:
            override_base = "buy"
        effective_price = apply_markup_with_jita_bounds(
            jita_buy=resolved_jita_buy,
            jita_sell=resolved_jita_sell,
            base_choice=override_base,
            percent=Decimal(override_value.get("percent") or 0),
            enforce_bounds=bool(
                getattr(resolved_config, "enforce_jita_price_bounds", False)
            ),
        )
    else:
        effective_price = Decimal(override_value.get("price") or 0)
    return (
        effective_price,
        resolved_default_price,
        effective_price != resolved_default_price,
    )


def _asset_quantity(asset: dict) -> int:
    try:
        quantity = int(asset.get("quantity", 0) or 0)
    except (TypeError, ValueError):
        quantity = 0
    if quantity <= 0:
        return 1 if asset.get("is_singleton") else 0
    return quantity


def _asset_is_blueprint(asset: dict) -> bool:
    """Return True when an asset row represents a blueprint."""
    if bool(asset.get("is_blueprint", False)) or bool(
        asset.get("is_blueprint_copy", False)
    ):
        return True
    explicit_variant = str(asset.get("blueprint_variant") or "").strip().lower()
    if explicit_variant in {"bpc", "bpo"}:
        return True
    try:
        raw_quantity = int(asset.get("quantity", 0) or 0)
    except (TypeError, ValueError):
        raw_quantity = 0

    # ESI asset rows encode blueprint singletons as -1 (BPO) or -2 (BPC).
    # Positive quantities are normal stackable items and must not be treated as blueprints.
    if raw_quantity in {-1, -2}:
        return True

    bp_type = str(asset.get("bp_type") or "").strip().lower()
    if bp_type in {"copy", "original", "bpc", "bpo"}:
        return True

    try:
        runs = int(asset.get("runs", 0) or 0)
    except (TypeError, ValueError):
        runs = 0
    if runs == -1 or runs > 0:
        return True

    type_name_lower = str(asset.get("type_name") or "").strip().lower()
    if "blueprint" in type_name_lower:
        return True

    # Cached corp asset rows do not always carry type_name. For singleton rows,
    # resolve the type name as a last-resort signal to avoid dropping blueprint variants.
    if bool(asset.get("is_singleton")):
        try:
            type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            type_id = 0
        if type_id > 0:
            try:
                resolved_name = str(get_type_name(type_id) or "").strip().lower()
            except Exception:
                resolved_name = ""
            if "blueprint" in resolved_name:
                return True

    return False


def _asset_blueprint_variant(asset: dict) -> str:
    """Return blueprint variant token for an asset row: 'bpo', 'bpc', or ''."""
    if not _asset_is_blueprint(asset):
        return ""
    try:
        raw_quantity = int(asset.get("quantity", 0) or 0)
    except (TypeError, ValueError):
        raw_quantity = 0

    # In asset list ESI, quantity == -2 is often BPC.
    if raw_quantity == -2:
        return "bpc"

    # In corporate blueprint ESI, quantity == -1 is BPO.
    if raw_quantity == -1:
        return "bpo"

    # In corporate blueprint ESI, quantity > 0 represents BPC runs.
    if (raw_quantity or 0) > 0:
        return "bpc"

    # Fallback to name-based check if still ambiguous, although usually
    # name-based checks are unreliable as the user noted.
    type_name_lower = str(asset.get("type_name") or asset.get("set_name") or "").lower()
    if " (bpc)" in type_name_lower or "blueprint copy" in type_name_lower:
        return "bpc"

    return "bpo"


def _build_eve_type_icon_urls(
    type_id: int, *, blueprint_variant: str = "", is_blueprint_hint: bool = False
) -> tuple[str, str]:
    """Return (primary, fallback) icon URLs for a type row."""
    type_id_int = int(type_id)
    if blueprint_variant == "bpc":
        primary_variant = "bpc"
        fallback_variant = "bp"
    elif blueprint_variant == "bpo":
        primary_variant = "bp"
        fallback_variant = "icon"
    elif is_blueprint_hint:
        primary_variant = "bp"
        fallback_variant = "icon"
    else:
        primary_variant = "icon"
        fallback_variant = "render"
    primary = (
        f"https://images.evetech.net/types/{type_id_int}/{primary_variant}?size=64"
    )
    fallback = f"https://images.evetech.net/types/{type_id_int}/{fallback_variant}?size=64"
    return primary, fallback


def _format_sell_blueprint_type_name(type_name: str, blueprint_variant: str) -> str:
    """Return display label that differentiates blueprint originals and copies."""
    clean_name = str(type_name or "").strip()
    if blueprint_variant == "bpc":
        return f"{clean_name} (BPC)"
    if blueprint_variant == "bpo":
        return f"{clean_name} (BPO)"
    return clean_name


def _build_sell_material_rows(
    *,
    assets: list[dict],
    config: MaterialExchangeConfig,
    price_data: dict[int, dict[str, Decimal]],
    reserved_quantities: dict[int, int],
    allowed_type_ids: set[int] | None,
    sell_override_map: dict[int, dict[str, object]],
    sell_market_group_override_map: dict[int, dict[str, object]] | None = None,
    sell_container_override: dict[str, object] | None = None,
    type_market_group_path_map: dict[int, list[int]] | None = None,
    type_market_group_label_map: dict[int, dict[str, str]] | None = None,
    character_name_by_id: dict[int, str] | None = None,
) -> list[dict]:
    """Build sell rows, grouping assets by containers and character owner."""

    if not assets:
        return []

    character_name_map: dict[int, str] = {}
    for raw_character_id, raw_name in (character_name_by_id or {}).items():
        try:
            character_id = int(raw_character_id)
        except (TypeError, ValueError):
            continue
        clean_name = str(raw_name or "").strip()
        if character_id > 0 and clean_name:
            character_name_map[character_id] = clean_name

    asset_by_item_id: dict[int, dict] = {}
    children_by_parent: dict[int, list[dict]] = {}
    for asset in assets:
        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        if item_id > 0:
            asset_by_item_id[item_id] = asset

    parent_by_item_id: dict[int, int] = {}

    def resolve_parent_item_id(asset: dict) -> int:
        # Prefer raw parent linkage, but tolerate legacy rows where only location_id
        # still points at a container item_id.
        for field_name in ("raw_location_id", "location_id"):
            try:
                candidate = int(asset.get(field_name) or 0)
            except (TypeError, ValueError):
                candidate = 0
            if candidate > 0 and candidate in asset_by_item_id:
                return candidate
        return 0

    for asset in assets:
        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        parent_id = resolve_parent_item_id(asset)
        if parent_id > 0:
            children_by_parent.setdefault(parent_id, []).append(asset)
            if item_id > 0:
                parent_by_item_id[item_id] = parent_id

    container_item_ids = set(children_by_parent.keys())
    reserved_by_type = {
        int(type_id): int(qty or 0) for type_id, qty in reserved_quantities.items()
    }
    allowed_types = {int(type_id) for type_id in allowed_type_ids} if allowed_type_ids else None
    price_meta_cache: dict[tuple[int, str, bool], dict[str, object] | None] = {}

    def get_price_meta(
        type_id: int,
        blueprint_variant: str = "",
        *,
        in_container: bool = False,
    ) -> dict[str, object] | None:
        type_id_int = int(type_id)
        meta_key = (type_id_int, str(blueprint_variant or ""), bool(in_container))
        if meta_key in price_meta_cache:
            return price_meta_cache[meta_key]

        if allowed_types is not None and type_id_int not in allowed_types:
            price_meta_cache[meta_key] = None
            return None

        type_name = get_type_name(type_id_int)
        icon_url, icon_fallback_url = _build_eve_type_icon_urls(
            type_id_int, blueprint_variant=blueprint_variant
        )

        if blueprint_variant == "bpc":
            meta = {
                "type_id": type_id_int,
                "type_name": _format_sell_blueprint_type_name(
                    type_name, blueprint_variant
                ),
                "buy_price_from_member": Decimal("0"),
                "default_buy_price_from_member": Decimal("0"),
                "has_sell_price_override": False,
                "is_blueprint_copy": True,
                "is_blueprint_original": False,
                "blueprint_variant": "bpc",
                "icon_url": icon_url,
                "icon_fallback_url": icon_fallback_url,
            }
            price_meta_cache[meta_key] = meta
            return meta

        fuzz_prices = price_data.get(type_id_int, {})
        jita_buy = Decimal(fuzz_prices.get("buy") or 0)
        jita_sell = Decimal(fuzz_prices.get("sell") or 0)
        has_market_price = jita_buy > 0 or jita_sell > 0
        has_group_override = (
            _resolve_market_group_override_for_type(
                type_id=type_id_int,
                market_group_override_map=sell_market_group_override_map,
                type_market_group_path_map=type_market_group_path_map,
            )
            is not None
        )
        if (
            not has_market_price
            and type_id_int not in sell_override_map
            and not (in_container and sell_container_override is not None)
            and not has_group_override
        ):
            price_meta_cache[meta_key] = None
            return None

        unit_price, default_unit_price, has_override = _compute_effective_sell_unit_price(
            config=config,
            type_id=type_id_int,
            jita_buy=jita_buy,
            jita_sell=jita_sell,
            sell_override_map=sell_override_map,
            sell_market_group_override_map=sell_market_group_override_map,
            type_market_group_path_map=type_market_group_path_map,
            sell_container_override=sell_container_override,
            in_container=bool(in_container),
        )
        if unit_price <= 0:
            price_meta_cache[meta_key] = None
            return None

        meta = {
            "type_id": type_id_int,
            "type_name": _format_sell_blueprint_type_name(type_name, blueprint_variant),
            "buy_price_from_member": unit_price,
            "default_buy_price_from_member": default_unit_price,
            "has_sell_price_override": bool(has_override),
            "is_blueprint_copy": False,
            "is_blueprint_original": blueprint_variant == "bpo",
            "blueprint_variant": "bpo" if blueprint_variant == "bpo" else "",
            "icon_url": icon_url,
            "icon_fallback_url": icon_fallback_url,
        }
        price_meta_cache[meta_key] = meta
        return meta

    total_qty_by_type: dict[int, int] = {}
    for asset in assets:
        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        if item_id > 0 and item_id in container_item_ids:
            continue
        try:
            type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            continue
        if type_id <= 0:
            continue
        quantity = _asset_quantity(asset)
        if quantity <= 0:
            continue
        blueprint_variant = _asset_blueprint_variant(asset)
        is_in_container_asset = int(parent_by_item_id.get(item_id, 0) or 0) > 0
        if (
            get_price_meta(
                type_id,
                blueprint_variant,
                in_container=is_in_container_asset,
            )
            is None
        ):
            continue
        total_qty_by_type[type_id] = total_qty_by_type.get(type_id, 0) + quantity

    remaining_by_type = {
        int(type_id): max(int(total_qty) - int(reserved_by_type.get(int(type_id), 0)), 0)
        for type_id, total_qty in total_qty_by_type.items()
    }

    def container_display_name(asset: dict) -> str:
        named = str(asset.get("set_name") or asset.get("name") or "").strip()
        if named:
            return named
        try:
            container_type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            container_type_id = 0
        if container_type_id > 0:
            return get_type_name(container_type_id)
        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        return f"Container {item_id}" if item_id > 0 else "Container"

    def resolve_character_meta(asset: dict) -> tuple[int, str]:
        try:
            character_id = int(asset.get("character_id") or 0)
        except (TypeError, ValueError):
            character_id = 0
        if character_id > 0:
            character_name = character_name_map.get(character_id)
            if not character_name:
                character_name = _("Character %(id)s") % {"id": character_id}
            return int(character_id), str(character_name)
        return 0, ""

    def resolve_asset_location_id(asset: dict) -> int:
        try:
            location_id = int(asset.get("location_id") or 0)
        except (TypeError, ValueError):
            location_id = 0
        return location_id if location_id > 0 else 0

    row_index = 0

    def next_row_index() -> int:
        nonlocal row_index
        idx = row_index
        row_index += 1
        return idx

    def build_item_row(
        *,
        type_id: int,
        quantity: int,
        blueprint_variant: str,
        ancestors: list[str],
        depth: int,
        character_id: int,
        character_name: str,
        source_location_ids: set[int] | None = None,
    ) -> dict[str, object] | None:
        if quantity <= 0:
            return None
        in_container = bool(ancestors)
        meta = get_price_meta(type_id, blueprint_variant, in_container=in_container)
        if meta is None:
            return None

        available_for_type = remaining_by_type.get(int(type_id), 0)
        available_qty = min(int(quantity), int(available_for_type))
        remaining_by_type[int(type_id)] = max(int(available_for_type) - available_qty, 0)
        reserved_qty = max(int(quantity) - available_qty, 0)
        row_idx = next_row_index()
        variant_token = str(meta.get("blueprint_variant") or "") or "std"
        container_scope_token = "incan" if in_container else "root"

        market_group_meta = (
            (type_market_group_label_map or {}).get(int(type_id), {})
            if type_market_group_label_map
            else {}
        )
        return {
            "row_kind": "item",
            "row_index": row_idx,
            "type_id": int(type_id),
            "type_name": str(meta["type_name"]),
            "buy_price_from_member": meta["buy_price_from_member"],
            "default_buy_price_from_member": meta["default_buy_price_from_member"],
            "has_sell_price_override": bool(meta["has_sell_price_override"]),
            "is_blueprint_copy": bool(meta.get("is_blueprint_copy", False)),
            "is_blueprint_original": bool(meta.get("is_blueprint_original", False)),
            "blueprint_variant": str(meta.get("blueprint_variant") or ""),
            "icon_url": str(meta.get("icon_url") or ""),
            "icon_fallback_url": str(meta.get("icon_fallback_url") or ""),
            "form_quantity_field_name": (
                f"qty_{int(type_id)}_{variant_token}_{container_scope_token}_{row_idx}"
            ),
            "user_quantity": int(quantity),
            "reserved_quantity": int(reserved_qty),
            "available_quantity": int(available_qty),
            "depth": int(depth),
            "container_path": ",".join(ancestors),
            "indent_padding_rem": round(max(0, depth) * 1.15, 2),
            "character_id": int(character_id) if int(character_id) > 0 else None,
            "character_name": str(character_name or ""),
            "market_group_name": str(market_group_meta.get("name") or ""),
            "market_group_path": str(
                market_group_meta.get("path") or market_group_meta.get("name") or ""
            ),
            "source_location_ids": sorted(
                {
                    int(location_id)
                    for location_id in (source_location_ids or set())
                    if int(location_id) > 0
                }
            ),
        }

    def build_container_branch(asset: dict, ancestors: list[str], depth: int) -> list[dict]:
        try:
            container_item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            return []
        if container_item_id <= 0:
            return []

        children = children_by_parent.get(container_item_id, [])
        if not children:
            return []

        container_key = f"c{container_item_id}"
        next_ancestors = [*ancestors, container_key]
        container_character_id, container_character_name = resolve_character_meta(asset)

        grouped_child_items: dict[tuple[int, str, int, str], int] = {}
        grouped_child_location_ids: dict[tuple[int, str, int, str], set[int]] = {}
        nested_container_assets: list[dict] = []
        for child in children:
            try:
                child_item_id = int(child.get("item_id") or 0)
            except (TypeError, ValueError):
                child_item_id = 0
            if child_item_id > 0 and child_item_id in container_item_ids:
                nested_container_assets.append(child)
                continue

            try:
                child_type_id = int(child.get("type_id") or 0)
            except (TypeError, ValueError):
                continue
            if child_type_id <= 0:
                continue
            child_qty = _asset_quantity(child)
            if child_qty <= 0:
                continue
            child_blueprint_variant = _asset_blueprint_variant(child)
            child_character_id, child_character_name = resolve_character_meta(child)
            child_key = (
                child_type_id,
                child_blueprint_variant,
                child_character_id,
                child_character_name,
            )
            grouped_child_items[child_key] = grouped_child_items.get(child_key, 0) + child_qty
            child_location_id = resolve_asset_location_id(child)
            if child_location_id > 0:
                grouped_child_location_ids.setdefault(child_key, set()).add(child_location_id)

        child_rows: list[dict] = []
        grouped_items_sorted = sorted(
            grouped_child_items.items(),
            key=lambda pair: (
                str(
                    (
                        get_price_meta(
                            pair[0][0], pair[0][1], in_container=True
                        )
                        or {}
                    ).get("type_name")
                    or get_type_name(pair[0][0])
                ).lower(),
                int(pair[0][0]),
                str(pair[0][1] or ""),
                str(pair[0][3] or "").lower(),
                int(pair[0][2] or 0),
            ),
        )
        for child_key, child_qty in grouped_items_sorted:
            child_type_id, child_blueprint_variant, child_character_id, child_character_name = (
                child_key
            )
            row = build_item_row(
                type_id=child_type_id,
                quantity=child_qty,
                blueprint_variant=child_blueprint_variant,
                ancestors=next_ancestors,
                depth=depth + 1,
                character_id=child_character_id,
                character_name=child_character_name,
                source_location_ids=grouped_child_location_ids.get(child_key, set()),
            )
            if row is not None:
                child_rows.append(row)

        nested_container_assets = sorted(
            nested_container_assets,
            key=lambda nested_asset: container_display_name(nested_asset).lower(),
        )
        for nested_asset in nested_container_assets:
            child_rows.extend(
                build_container_branch(
                    nested_asset,
                    ancestors=next_ancestors,
                    depth=depth + 1,
                )
            )

        if not child_rows:
            return []

        try:
            container_type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            container_type_id = 0
        container_icon_url = ""
        container_icon_fallback_url = ""
        if container_type_id > 0:
            container_icon_url, container_icon_fallback_url = _build_eve_type_icon_urls(
                container_type_id
            )
        container_row = {
            "row_kind": "container",
            "row_index": next_row_index(),
            "container_key": container_key,
            "container_name": container_display_name(asset),
            "container_type_id": container_type_id if container_type_id > 0 else None,
            "container_icon_url": container_icon_url,
            "container_icon_fallback_url": container_icon_fallback_url,
            "depth": int(depth),
            "container_path": ",".join(ancestors),
            "indent_padding_rem": round(max(0, depth) * 1.15, 2),
            "character_id": (
                int(container_character_id) if int(container_character_id) > 0 else None
            ),
            "character_name": str(container_character_name or ""),
        }

        return [container_row, *child_rows]

    root_container_assets: list[dict] = []
    root_items_by_key: dict[tuple[int, str, int, str], int] = {}
    root_item_location_ids: dict[tuple[int, str, int, str], set[int]] = {}
    for asset in assets:
        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        parent_id = int(parent_by_item_id.get(item_id, 0) or 0)
        is_container = item_id > 0 and item_id in container_item_ids
        has_container_parent = parent_id > 0 and parent_id in container_item_ids

        if is_container:
            if not has_container_parent:
                root_container_assets.append(asset)
            continue
        if has_container_parent:
            continue

        try:
            type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            continue
        if type_id <= 0:
            continue
        qty = _asset_quantity(asset)
        if qty <= 0:
            continue
        blueprint_variant = _asset_blueprint_variant(asset)
        character_id, character_name = resolve_character_meta(asset)
        root_key = (type_id, blueprint_variant, character_id, character_name)
        root_items_by_key[root_key] = root_items_by_key.get(root_key, 0) + qty
        root_location_id = resolve_asset_location_id(asset)
        if root_location_id > 0:
            root_item_location_ids.setdefault(root_key, set()).add(root_location_id)

    rows: list[dict] = []
    root_container_assets = sorted(
        root_container_assets,
        key=lambda container_asset: container_display_name(container_asset).lower(),
    )
    for root_container_asset in root_container_assets:
        rows.extend(build_container_branch(root_container_asset, ancestors=[], depth=0))

    root_items_sorted = sorted(
        root_items_by_key.items(),
        key=lambda pair: (
            str(
                (get_price_meta(pair[0][0], pair[0][1]) or {}).get("type_name")
                or get_type_name(pair[0][0])
            ).lower(),
            int(pair[0][0]),
            str(pair[0][1] or ""),
            str(pair[0][3] or "").lower(),
            int(pair[0][2] or 0),
        ),
    )
    for root_key, qty in root_items_sorted:
        type_id, blueprint_variant, character_id, character_name = root_key
        row = build_item_row(
            type_id=type_id,
            quantity=qty,
            blueprint_variant=blueprint_variant,
            ancestors=[],
            depth=0,
            character_id=character_id,
            character_name=character_name,
            source_location_ids=root_item_location_ids.get(root_key, set()),
        )
        if row is not None:
            rows.append(row)

    return rows


def _build_buy_stock_location_label(
    stock_item: MaterialExchangeStock,
    *,
    buy_name_map: dict[int, str],
    fallback_label: str,
) -> str:
    """Return a stable per-item location label for the buy table."""

    labels: list[str] = []
    for raw_name in getattr(stock_item, "source_structure_names", []) or []:
        clean_name = str(raw_name or "").strip()
        if clean_name and clean_name not in labels:
            labels.append(clean_name)

    for raw_structure_id in getattr(stock_item, "source_structure_ids", []) or []:
        try:
            structure_id = int(raw_structure_id)
        except (TypeError, ValueError):
            continue
        resolved_name = (buy_name_map.get(structure_id) or f"Structure {structure_id}").strip()
        if resolved_name and resolved_name not in labels:
            labels.append(resolved_name)

    return ", ".join(labels).strip() or str(fallback_label or "").strip()


def _get_corp_blueprint_details_by_item_id(
    *,
    config: MaterialExchangeConfig,
    item_ids: set[int] | None = None,
) -> dict[int, dict[str, object]]:
    """Return corp blueprint metadata by item_id (variant and optional runs)."""
    try:
        corporation_id = int(config.corporation_id)
        queryset = Blueprint.objects.filter(
            corporation_id=corporation_id,
        )
        if item_ids:
            queryset = queryset.filter(item_id__in=[int(iid) for iid in item_ids if int(iid) > 0])
        rows = queryset.values("item_id", "bp_type", "quantity", "runs", "type_name")
    except Exception:
        return {}

    details_by_item_id: dict[int, dict[str, object]] = {}
    for row in rows:
        try:
            item_id = int(row.get("item_id") or 0)
        except (TypeError, ValueError):
            continue
        if item_id <= 0:
            continue

        bp_type = str(row.get("bp_type") or "").strip().upper()
        try:
            quantity = int(row.get("quantity") or 0)
        except (TypeError, ValueError):
            quantity = 0
        try:
            runs = int(row.get("runs") or 0)
        except (TypeError, ValueError):
            runs = 0
        type_name_lower = str(row.get("type_name") or "").strip().lower()

        variant = ""
        # Prefer hard quantity/runs signals first, because bp_type can be stale
        # in rows created before classification logic updates.
        if quantity == -2:
            variant = "bpc"
        elif (runs or 0) > 0:
            variant = "bpc"
        elif (quantity or 0) > 0:
            # For corp blueprints, quantity > 0 usually represents runs for BPCs.
            variant = "bpc"
        elif quantity == -1 and runs == -1:
            variant = "bpo"
        elif quantity == -1:
            # Legacy rows may still carry copy hints in type_name.
            if "blueprint copy" in type_name_lower or " (bpc)" in type_name_lower:
                variant = "bpc"
            else:
                variant = "bpo"
        elif runs == -1:
            variant = "bpo"
        elif bp_type == str(Blueprint.BPType.COPY):
            variant = "bpc"
        elif bp_type == str(Blueprint.BPType.ORIGINAL):
            variant = "bpo"

        if not variant:
            continue

        details: dict[str, object] = {"variant": variant}
        if variant == "bpc":
            runs_value = int(runs) if runs > 0 else int(quantity) if quantity > 0 else 0
            if runs_value > 0:
                details["runs"] = int(runs_value)
        details_by_item_id[item_id] = details

    return details_by_item_id


def _get_corp_blueprint_variant_by_item_id(
    *,
    config: MaterialExchangeConfig,
    item_ids: set[int] | None = None,
) -> dict[int, str]:
    """Return blueprint variant by corp blueprint item_id: bpc/bpo."""
    details_by_item_id = _get_corp_blueprint_details_by_item_id(
        config=config,
        item_ids=item_ids,
    )
    return {
        int(item_id): str(details.get("variant") or "").strip().lower()
        for item_id, details in details_by_item_id.items()
        if str(details.get("variant") or "").strip().lower() in {"bpc", "bpo"}
    }


def _get_buy_location_scoped_corp_assets(
    *,
    config: MaterialExchangeConfig,
    corp_assets: list[dict] | None = None,
) -> list[dict]:
    """Return cached corp assets matched to configured buy locations/division."""

    if corp_assets is None:
        try:
            corp_assets, _scope_missing = get_corp_assets_cached(
                int(config.corporation_id),
                allow_refresh=True,
            )
        except Exception:
            return []
    if not corp_assets:
        return []

    try:
        target_structure_ids = [int(sid) for sid in config.get_buy_structure_ids() or []]
    except Exception:
        target_structure_ids = []
    if not target_structure_ids:
        return []

    hangar_flag_map = {
        1: "CorpSAG1",
        2: "CorpSAG2",
        3: "CorpSAG3",
        4: "CorpSAG4",
        5: "CorpSAG5",
        6: "CorpSAG6",
        7: "CorpSAG7",
    }
    target_flag = hangar_flag_map.get(int(getattr(config, "hangar_division", 0) or 0))
    if not target_flag:
        return []

    effective_location_ids: list[int] = []
    context_to_structure_ids: dict[int, set[int]] = {}
    hangar_fallback_context_ids: set[int] = set()
    for structure_id in target_structure_ids:
        structure_id_int = int(structure_id)
        office_folder_item_id = get_office_folder_item_id_from_assets(
            corp_assets,
            structure_id=structure_id_int,
        )
        candidate_context_ids: list[int] = [structure_id_int]
        if office_folder_item_id is not None:
            office_folder_item_id_int = int(office_folder_item_id)
            candidate_context_ids.append(office_folder_item_id_int)
            candidate_context_ids.append(
                make_managed_hangar_location_id(
                    office_folder_item_id_int,
                    int(config.hangar_division),
                )
            )
        else:
            hangar_fallback_context_ids.add(structure_id_int)

        for context_id in candidate_context_ids:
            context_id_int = int(context_id)
            if context_id_int not in effective_location_ids:
                effective_location_ids.append(context_id_int)
            context_to_structure_ids.setdefault(context_id_int, set()).add(
                structure_id_int
            )

    index_by_item_id = build_asset_index_by_item_id(corp_assets or [])

    def _asset_chain_contains_location(asset_row: dict, location_id: int) -> bool:
        current = asset_row
        seen: set[int] = set()
        target_id = int(location_id)
        for _ in range(25):
            try:
                current_location_id = int(current.get("location_id", 0) or 0)
            except (TypeError, ValueError):
                return False
            if current_location_id == target_id:
                return True
            parent = index_by_item_id.get(current_location_id)
            if not parent:
                return False
            if current_location_id in seen:
                return False
            seen.add(current_location_id)
            current = parent
        return False

    scoped_assets: list[dict] = []
    for raw_asset in corp_assets or []:
        matches_any_location = False
        matched_structure_ids: set[int] = set()
        for location_id in effective_location_ids:
            location_id_int = int(location_id)
            if location_id_int < 0:
                matched = _asset_chain_contains_location(raw_asset, location_id_int)
            else:
                matched = asset_chain_has_context(
                    raw_asset,
                    index_by_item_id,
                    location_id=location_id_int,
                    location_flag=str(target_flag),
                )
                if not matched and location_id_int in hangar_fallback_context_ids:
                    matched = asset_chain_has_context(
                        raw_asset,
                        index_by_item_id,
                        location_id=location_id_int,
                        location_flag="Hangar",
                    )
            if not matched:
                continue
            matches_any_location = True
            matched_structure_ids.update(context_to_structure_ids.get(location_id_int, set()))
            break

        if not matches_any_location:
            continue

        asset = dict(raw_asset)
        asset["source_structure_ids"] = sorted(int(sid) for sid in matched_structure_ids)
        scoped_assets.append(asset)

    return scoped_assets


def _format_buy_stock_type_name(type_name: str, blueprint_variant: str) -> str:
    """Return a buy-table item name with an optional blueprint suffix."""
    base_name = str(type_name or "").strip()
    for suffix in (" (BPO/BPC)", " (BPO)", " (BPC)"):
        if base_name.endswith(suffix):
            base_name = base_name[: -len(suffix)]
            break

    variant = str(blueprint_variant or "").strip().lower()
    if variant == "bpc":
        return f"{base_name} (BPC)"
    if variant == "bpo":
        return f"{base_name} (BPO)"
    if variant == "mixed":
        return f"{base_name} (BPO/BPC)"
    return base_name


def _get_buy_stock_blueprint_variant_map(
    *,
    config: MaterialExchangeConfig,
    type_ids: set[int] | None = None,
) -> dict[int, str]:
    """Return stock blueprint variant by type_id: bpo/bpc/mixed."""

    try:
        corp_assets, _scope_missing = get_corp_assets_cached(
            int(config.corporation_id),
            allow_refresh=False,
        )
    except Exception:
        return {}
    if not corp_assets:
        return {}

    wanted_type_ids: set[int] | None = None
    if type_ids:
        wanted_type_ids = {int(type_id) for type_id in type_ids if int(type_id) > 0}

    scoped_assets = _get_buy_location_scoped_corp_assets(
        config=config,
        corp_assets=corp_assets,
    )
    if not scoped_assets:
        return {}

    scoped_item_ids: set[int] = set()
    for scoped_asset in scoped_assets:
        try:
            scoped_item_id = int(scoped_asset.get("item_id") or 0)
        except (TypeError, ValueError):
            scoped_item_id = 0
        if scoped_item_id > 0:
            scoped_item_ids.add(scoped_item_id)
    blueprint_variant_by_item_id = _get_corp_blueprint_variant_by_item_id(
        config=config,
        item_ids=scoped_item_ids,
    )

    variants_by_type: dict[int, set[str]] = {}
    for asset in scoped_assets:
        try:
            type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            continue
        if type_id <= 0:
            continue
        if wanted_type_ids is not None and type_id not in wanted_type_ids:
            continue

        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        variant = str(blueprint_variant_by_item_id.get(item_id) or "").strip().lower()
        if variant not in {"bpc", "bpo"}:
            if not _asset_is_blueprint(asset):
                continue
            # Fallback for when Blueprint record is missing:
            # -2 usually means BPC in some contexts.
            # is_singleton=True is characteristic of both BPO and BPC in asset lists.
            # Without 'runs', we can't be 100% sure, but we can look for "Copy" in the name if available.
            try:
                raw_quantity = int(asset.get("quantity", 0) or 0)
            except (TypeError, ValueError):
                raw_quantity = 0

            is_bpc_likely = raw_quantity == -2
            if not is_bpc_likely and raw_quantity == -1:
                is_bpc_likely = False # Explicitly BPO
            elif not is_bpc_likely and (raw_quantity or 0) > 0:
                is_bpc_likely = True # Likely BPC runs
            elif not is_bpc_likely:
                type_name_lower = str(asset.get("type_name") or get_type_name(type_id)).lower()
                if " (bpc)" in type_name_lower or "blueprint copy" in type_name_lower:
                    is_bpc_likely = True

            variant = "bpc" if is_bpc_likely else "bpo"
        variants_by_type.setdefault(type_id, set()).add(variant)

    variant_map: dict[int, str] = {}
    for type_id, variants in variants_by_type.items():
        if not variants:
            continue
        if variants == {"bpc"}:
            variant_map[int(type_id)] = "bpc"
        elif variants == {"bpo"}:
            variant_map[int(type_id)] = "bpo"
        else:
            variant_map[int(type_id)] = "mixed"
    return variant_map


def _build_buy_material_rows(
    *,
    scoped_assets: list[dict],
    config: MaterialExchangeConfig,
    stock_meta_by_type: dict[int, dict[str, object]],
    buy_override_map: dict[int, dict[str, object]],
    buy_market_group_override_map: dict[int, dict[str, object]] | None = None,
    buy_container_override: dict[str, object] | None = None,
    type_market_group_path_map: dict[int, list[int]] | None = None,
    type_market_group_label_map: dict[int, dict[str, str]] | None = None,
    buy_name_map: dict[int, str],
    fallback_location_label: str,
    blueprint_variant_by_item_id: dict[int, str] | None = None,
    blueprint_runs_by_item_id: dict[int, int] | None = None,
) -> list[dict]:
    """Build buy-table rows, grouping corp assets by containers when possible."""

    if not scoped_assets or not stock_meta_by_type:
        return []

    asset_by_item_id: dict[int, dict] = {}
    for asset in scoped_assets:
        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        if item_id > 0:
            asset_by_item_id[item_id] = asset

    parent_by_item_id: dict[int, int] = {}
    children_by_parent: dict[int, list[dict]] = {}

    def resolve_parent_item_id(asset: dict) -> int:
        for field_name in ("raw_location_id", "location_id"):
            try:
                candidate = int(asset.get(field_name) or 0)
            except (TypeError, ValueError):
                candidate = 0
            if candidate > 0 and candidate in asset_by_item_id:
                return candidate
        return 0

    for asset in scoped_assets:
        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        parent_id = resolve_parent_item_id(asset)
        if parent_id > 0:
            children_by_parent.setdefault(parent_id, []).append(asset)
            if item_id > 0:
                parent_by_item_id[item_id] = parent_id

    container_item_ids = set(children_by_parent.keys())
    remaining_by_type: dict[int, int] = {
        int(type_id): max(int(meta.get("available_quantity") or 0), 0)
        for type_id, meta in stock_meta_by_type.items()
    }
    row_index = 0
    explicit_variant_by_item_id = {
        int(item_id): str(variant or "").strip().lower()
        for item_id, variant in (blueprint_variant_by_item_id or {}).items()
        if int(item_id) > 0 and str(variant or "").strip().lower() in {"bpc", "bpo"}
    }
    explicit_runs_by_item_id = {
        int(item_id): int(runs or 0)
        for item_id, runs in (blueprint_runs_by_item_id or {}).items()
        if int(item_id) > 0 and int(runs or 0) > 0
    }
    container_name_by_key: dict[str, str] = {}

    def next_row_index() -> int:
        nonlocal row_index
        idx = row_index
        row_index += 1
        return idx

    def _location_label_from_source_ids(source_ids: list[int]) -> str:
        labels: list[str] = []
        for source_id in source_ids:
            source_name = (buy_name_map.get(int(source_id)) or f"Structure {int(source_id)}").strip()
            if source_name and source_name not in labels:
                labels.append(source_name)
        return ", ".join(labels).strip() or str(fallback_location_label or "").strip()

    def _container_display_name(asset: dict) -> str:
        named = str(asset.get("set_name") or asset.get("name") or "").strip()
        if named:
            return named
        try:
            container_type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            container_type_id = 0
        if container_type_id > 0:
            return get_type_name(container_type_id)
        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        return f"Container {item_id}" if item_id > 0 else "Container"

    def _resolve_container_name_for_key(container_key: str) -> str:
        clean_key = str(container_key or "").strip()
        if not clean_key:
            return ""
        cached_name = str(container_name_by_key.get(clean_key, "")).strip()
        if cached_name:
            return cached_name
        if clean_key.startswith("c"):
            try:
                container_item_id = int(clean_key[1:] or 0)
            except (TypeError, ValueError):
                container_item_id = 0
            if container_item_id > 0:
                asset = asset_by_item_id.get(container_item_id)
                if asset:
                    resolved_name = _container_display_name(asset)
                    container_name_by_key[clean_key] = resolved_name
                    return resolved_name
                return f"Container {container_item_id}"
        return ""

    def _resolve_item_meta(
        type_id: int,
        blueprint_variant: str = "",
        *,
        in_container: bool = False,
    ) -> dict[str, object] | None:
        type_id_int = int(type_id)
        base_meta = stock_meta_by_type.get(type_id_int)
        if not base_meta:
            return None

        base_type_name = str(
            base_meta.get("base_type_name")
            or base_meta.get("display_type_name")
            or get_type_name(type_id_int)
        ).strip()

        variant = str(blueprint_variant or "").strip().lower()
        if variant not in {"bpc", "bpo"}:
            base_variant = str(base_meta.get("blueprint_variant") or "").strip().lower()
            if base_variant in {"bpc", "bpo"}:
                variant = base_variant
            else:
                variant = ""

        if variant == "bpc":
            unit_price = Decimal("0")
            default_unit_price = Decimal("0")
            has_override = False
        else:
            unit_price, default_unit_price, has_override = (
                _compute_effective_buy_unit_price(
                    type_id=type_id_int,
                    jita_buy=Decimal(base_meta.get("jita_buy_price") or 0),
                    jita_sell=Decimal(base_meta.get("jita_sell_price") or 0),
                    default_unit_price=Decimal(
                        base_meta.get("default_sell_price_to_member") or 0
                    ),
                    config=config,
                    buy_override_map=buy_override_map,
                    buy_market_group_override_map=buy_market_group_override_map,
                    type_market_group_path_map=type_market_group_path_map,
                    buy_container_override=buy_container_override,
                    in_container=bool(in_container),
                )
            )

        icon_variant = "bpc" if variant == "bpc" else ("bpo" if variant == "bpo" else "")
        icon_url, icon_fallback_url = _build_eve_type_icon_urls(
            type_id_int,
            blueprint_variant=icon_variant,
            is_blueprint_hint=(bool(variant) or "blueprint" in base_type_name.lower()),
        )
        return {
            "type_id": type_id_int,
            "display_type_name": _format_buy_stock_type_name(base_type_name, variant),
            "blueprint_variant": variant,
            "display_sell_price_to_member": unit_price,
            "default_sell_price_to_member": default_unit_price,
            "has_buy_price_override": has_override,
            "icon_url": icon_url,
            "icon_fallback_url": icon_fallback_url,
            "default_source_structure_ids": [
                int(sid)
                for sid in (base_meta.get("source_structure_ids") or [])
                if int(sid) > 0
            ],
            "default_buy_location_label": str(
                base_meta.get("buy_location_label") or fallback_location_label
            ),
        }

    def _resolve_asset_blueprint_variant(asset: dict) -> str:
        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        explicit_variant = str(explicit_variant_by_item_id.get(item_id) or "").strip().lower()
        if explicit_variant in {"bpc", "bpo"}:
            return explicit_variant
        return _asset_blueprint_variant(asset)

    def build_item_row(
        *,
        type_id: int,
        quantity: int,
        blueprint_variant: str,
        bpc_runs: int | None,
        source_structure_ids: list[int],
        ancestors: list[str],
        depth: int,
    ) -> dict[str, object] | None:
        if int(quantity) <= 0:
            return None

        in_container = bool(ancestors)
        item_meta = _resolve_item_meta(
            type_id,
            blueprint_variant,
            in_container=in_container,
        )
        if item_meta is None:
            return None

        available_for_type = int(remaining_by_type.get(int(type_id), 0) or 0)
        available_qty = min(int(quantity), max(available_for_type, 0))
        remaining_by_type[int(type_id)] = max(available_for_type - available_qty, 0)
        reserved_qty = max(int(quantity) - available_qty, 0)

        clean_source_ids: list[int] = []
        for raw_source_id in source_structure_ids or []:
            try:
                source_id = int(raw_source_id)
            except (TypeError, ValueError):
                continue
            if source_id > 0 and source_id not in clean_source_ids:
                clean_source_ids.append(source_id)
        if not clean_source_ids:
            clean_source_ids = list(item_meta.get("default_source_structure_ids") or [])

        location_label = (
            _location_label_from_source_ids(clean_source_ids)
            if clean_source_ids
            else str(item_meta.get("default_buy_location_label") or fallback_location_label)
        )
        row_idx = next_row_index()
        variant_token = str(item_meta.get("blueprint_variant") or "") or "std"
        container_scope_token = "incan" if in_container else "root"
        clean_bpc_runs: int | None = None
        if str(item_meta.get("blueprint_variant") or "").strip().lower() == "bpc":
            try:
                parsed_runs = int(bpc_runs or 0)
            except (TypeError, ValueError):
                parsed_runs = 0
            if parsed_runs > 0:
                clean_bpc_runs = int(parsed_runs)
        container_names = [
            _resolve_container_name_for_key(str(container_key))
            for container_key in ancestors
        ]
        container_name_path = " > ".join(
            container_name for container_name in container_names if container_name
        )

        market_group_meta = (
            (type_market_group_label_map or {}).get(int(type_id), {})
            if type_market_group_label_map
            else {}
        )
        return {
            "row_kind": "item",
            "row_index": row_idx,
            "type_id": int(type_id),
            "display_type_name": str(item_meta.get("display_type_name") or ""),
            "blueprint_variant": str(item_meta.get("blueprint_variant") or ""),
            "bpc_runs": clean_bpc_runs,
            "quantity": int(quantity),
            "reserved_quantity": int(reserved_qty),
            "available_quantity": int(available_qty),
            "display_sell_price_to_member": item_meta.get("display_sell_price_to_member"),
            "default_sell_price_to_member": item_meta.get("default_sell_price_to_member"),
            "has_buy_price_override": bool(item_meta.get("has_buy_price_override", False)),
            "icon_url": str(item_meta.get("icon_url") or ""),
            "icon_fallback_url": str(item_meta.get("icon_fallback_url") or ""),
            "source_structure_ids": clean_source_ids,
            "buy_location_label": location_label,
            "market_group_name": str(market_group_meta.get("name") or ""),
            "market_group_path": str(
                market_group_meta.get("path") or market_group_meta.get("name") or ""
            ),
            "depth": int(depth),
            "container_path": ",".join(ancestors),
            "container_name_path": str(container_name_path),
            "indent_padding_rem": round(max(0, depth) * 1.15, 2),
            "form_quantity_field_name": (
                f"qty_{int(type_id)}_{variant_token}_{container_scope_token}_{row_idx}"
            ),
        }

    def build_container_branch(asset: dict, ancestors: list[str], depth: int) -> list[dict]:
        try:
            container_item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            return []
        if container_item_id <= 0:
            return []

        children = children_by_parent.get(container_item_id, [])
        if not children:
            return []

        container_key = f"c{container_item_id}"
        container_name = _container_display_name(asset)
        container_name_by_key[container_key] = container_name
        next_ancestors = [*ancestors, container_key]
        nested_container_assets: list[dict] = []
        grouped_child_items: dict[tuple[int, str, int, tuple[int, ...]], int] = {}

        for child in children:
            try:
                child_item_id = int(child.get("item_id") or 0)
            except (TypeError, ValueError):
                child_item_id = 0
            if child_item_id > 0 and child_item_id in container_item_ids:
                nested_container_assets.append(child)
                continue

            try:
                child_type_id = int(child.get("type_id") or 0)
            except (TypeError, ValueError):
                continue
            if child_type_id <= 0 or child_type_id not in stock_meta_by_type:
                continue
            child_qty = _asset_quantity(child)
            if child_qty <= 0:
                continue
            child_variant = _resolve_asset_blueprint_variant(child)
            child_runs = 0
            if child_variant == "bpc" and child_item_id > 0:
                child_runs = int(explicit_runs_by_item_id.get(child_item_id, 0) or 0)
            child_source_ids: list[int] = []
            for raw_source_id in child.get("source_structure_ids", []) or []:
                try:
                    source_id = int(raw_source_id)
                except (TypeError, ValueError):
                    continue
                if source_id > 0 and source_id not in child_source_ids:
                    child_source_ids.append(source_id)

            group_key = (
                int(child_type_id),
                str(child_variant or ""),
                int(child_runs),
                tuple(sorted(child_source_ids)),
            )
            grouped_child_items[group_key] = grouped_child_items.get(group_key, 0) + int(child_qty)

        child_rows: list[dict] = []
        grouped_items_sorted = sorted(
            grouped_child_items.items(),
            key=lambda pair: (
                str(
                    (
                        _resolve_item_meta(
                            pair[0][0], pair[0][1], in_container=True
                        )
                        or {}
                    ).get("display_type_name")
                    or get_type_name(pair[0][0])
                ).lower(),
                int(pair[0][0]),
                str(pair[0][1] or ""),
                int(pair[0][2] or 0),
                ",".join(str(x) for x in pair[0][3]),
            ),
        )
        for grouped_key, grouped_qty in grouped_items_sorted:
            child_type_id, child_variant, child_runs, source_ids_tuple = grouped_key
            child_row = build_item_row(
                type_id=child_type_id,
                quantity=int(grouped_qty),
                blueprint_variant=child_variant,
                bpc_runs=child_runs,
                source_structure_ids=list(source_ids_tuple),
                ancestors=next_ancestors,
                depth=depth + 1,
            )
            if child_row is not None:
                child_rows.append(child_row)

        nested_container_assets = sorted(
            nested_container_assets,
            key=lambda nested_asset: _container_display_name(nested_asset).lower(),
        )
        for nested_container_asset in nested_container_assets:
            child_rows.extend(
                build_container_branch(
                    nested_container_asset,
                    ancestors=next_ancestors,
                    depth=depth + 1,
                )
            )

        if not child_rows:
            return []

        try:
            container_type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            container_type_id = 0

        container_icon_url = ""
        container_icon_fallback_url = ""
        if container_type_id > 0:
            container_icon_url, container_icon_fallback_url = _build_eve_type_icon_urls(
                container_type_id
            )

        container_row = {
            "row_kind": "container",
            "row_index": next_row_index(),
            "container_key": container_key,
            "container_name": container_name,
            "container_type_id": container_type_id if container_type_id > 0 else None,
            "container_icon_url": container_icon_url,
            "container_icon_fallback_url": container_icon_fallback_url,
            "depth": int(depth),
            "container_path": ",".join(ancestors),
            "indent_padding_rem": round(max(0, depth) * 1.15, 2),
        }

        return [container_row, *child_rows]

    root_container_assets: list[dict] = []
    root_items_by_key: dict[tuple[int, str, int, tuple[int, ...]], int] = {}
    for asset in scoped_assets:
        try:
            item_id = int(asset.get("item_id") or 0)
        except (TypeError, ValueError):
            item_id = 0
        parent_id = int(parent_by_item_id.get(item_id, 0) or 0)
        is_container = item_id > 0 and item_id in container_item_ids
        has_container_parent = parent_id > 0 and parent_id in container_item_ids

        if is_container:
            if not has_container_parent:
                root_container_assets.append(asset)
            continue
        if has_container_parent:
            continue

        try:
            type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            continue
        if type_id <= 0 or type_id not in stock_meta_by_type:
            continue

        qty = _asset_quantity(asset)
        if qty <= 0:
            continue
        blueprint_variant = _resolve_asset_blueprint_variant(asset)
        bpc_runs = 0
        if blueprint_variant == "bpc" and item_id > 0:
            bpc_runs = int(explicit_runs_by_item_id.get(item_id, 0) or 0)

        source_ids: list[int] = []
        for raw_source_id in asset.get("source_structure_ids", []) or []:
            try:
                source_id = int(raw_source_id)
            except (TypeError, ValueError):
                continue
            if source_id > 0 and source_id not in source_ids:
                source_ids.append(source_id)
        key = (
            int(type_id),
            str(blueprint_variant or ""),
            int(bpc_runs),
            tuple(sorted(source_ids)),
        )
        root_items_by_key[key] = root_items_by_key.get(key, 0) + int(qty)

    rows: list[dict] = []
    root_container_assets = sorted(
        root_container_assets,
        key=lambda container_asset: _container_display_name(container_asset).lower(),
    )
    for root_container_asset in root_container_assets:
        rows.extend(build_container_branch(root_container_asset, ancestors=[], depth=0))

    root_items_sorted = sorted(
        root_items_by_key.items(),
        key=lambda pair: (
            str(
                (_resolve_item_meta(pair[0][0], pair[0][1]) or {}).get("display_type_name")
                or get_type_name(pair[0][0])
            ).lower(),
            int(pair[0][0]),
            str(pair[0][1] or ""),
            int(pair[0][2] or 0),
            ",".join(str(x) for x in pair[0][3]),
        ),
    )
    for root_key, qty in root_items_sorted:
        type_id, blueprint_variant, bpc_runs, source_ids_tuple = root_key
        row = build_item_row(
            type_id=type_id,
            quantity=int(qty),
            blueprint_variant=blueprint_variant,
            bpc_runs=bpc_runs,
            source_structure_ids=list(source_ids_tuple),
            ancestors=[],
            depth=0,
        )
        if row is not None:
            rows.append(row)

    return rows


def _resolve_selected_buy_stock_source_location(
    stock_items: list[MaterialExchangeStock],
    *,
    buy_name_map: dict[int, str] | None = None,
) -> tuple[int | None, str]:
    """Return a deterministic common source location for selected buy stock rows."""

    common_location_ids: set[int] | None = None
    for stock_item in stock_items:
        source_location_ids: set[int] = set()
        for raw_structure_id in getattr(stock_item, "source_structure_ids", []) or []:
            try:
                structure_id = int(raw_structure_id)
            except (TypeError, ValueError):
                continue
            if structure_id > 0:
                source_location_ids.add(structure_id)

        if not source_location_ids:
            continue

        if common_location_ids is None:
            common_location_ids = set(source_location_ids)
        else:
            common_location_ids &= source_location_ids

        if not common_location_ids:
            return None, ""

    if not common_location_ids:
        return None, ""

    selected_location_id = sorted(common_location_ids)[0]
    selected_location_name = str(
        (buy_name_map or {}).get(int(selected_location_id), "") or ""
    ).strip()
    if not selected_location_name:
        selected_location_name = f"Structure {int(selected_location_id)}"
    return int(selected_location_id), selected_location_name


def _selected_buy_stock_items_share_source_location(
    stock_items: list[MaterialExchangeStock],
) -> bool:
    """Return True when selected stock rows can be sourced from one common location."""
    selected_location_id, _selected_location_name = (
        _resolve_selected_buy_stock_source_location(stock_items)
    )
    return selected_location_id is not None


@login_required
@indy_hub_permission_required("can_access_indy_hub")
def material_exchange_index(request):
    """
    Buyback hub landing page.
    Shows overview, recent transactions, and quick stats.
    """
    emit_view_analytics_event(view_name="material_exchange.index", request=request)
    config = _get_material_exchange_config()
    enabled = _is_material_exchange_enabled()

    if not enabled or not config:
        context = {
            "nav_context": _build_nav_context(request.user),
            "material_exchange_disabled": not enabled,
        }
        context.update(
            build_nav_context(
                request.user,
                active_tab="material_hub",
                can_manage_corp=request.user.has_perm(
                    "indy_hub.can_manage_corp_bp_requests"
                ),
            )
        )
        return render(
            request,
            "indy_hub/material_exchange/not_configured.html",
            context,
        )

    sell_structure_ids = config.get_sell_structure_ids()
    if not sell_structure_ids:
        try:
            sell_structure_ids = [int(config.structure_id)]
        except (TypeError, ValueError):
            sell_structure_ids = []

    sell_location_names = []
    sell_name_map = config.get_sell_structure_name_map()
    for sid in sell_structure_ids:
        sid_int = int(sid)
        sell_location_names.append(
            sell_name_map.get(sid_int) or f"Structure {sid_int}"
        )
    buy_enabled = bool(getattr(config, "buy_enabled", True))
    buy_structure_ids = config.get_buy_structure_ids() if buy_enabled else []
    buy_location_names = []
    buy_name_map = config.get_buy_structure_name_map()
    for sid in buy_structure_ids:
        sid_int = int(sid)
        buy_location_names.append(
            buy_name_map.get(sid_int) or f"Structure {sid_int}"
        )
    hub_location_label = ", ".join(sell_location_names).strip() or (
        config.structure_name or f"Structure {config.structure_id}"
    )

    # Stats (based on the user's visible sell items)
    stock_count = 0
    total_stock_value = 0

    try:
        # Avoid blocking ESI calls on index page; use cached data only
        (
            user_assets,
            _user_assets_by_character,
            user_assets_by_location,
            scope_missing,
        ) = _fetch_user_assets_for_structure_data(
            request.user,
            sell_structure_ids,
            allow_refresh=False,
            config=config,
        )

        if scope_missing:
            messages.info(
                request,
                _(
                    "Refreshing via ESI. Make sure you have granted the assets scope to at least one character."
                ),
            )

        filtered_assets: dict[int, int] = {}
        for sid in sell_structure_ids:
            loc_assets = user_assets_by_location.get(int(sid), {})
            allowed_type_ids = _get_allowed_type_ids_for_config(
                config,
                "sell",
                structure_id=int(sid),
            )
            if allowed_type_ids is None:
                loc_filtered = loc_assets
            else:
                loc_filtered = {
                    tid: qty for tid, qty in loc_assets.items() if tid in allowed_type_ids
                }
            for type_id, qty in loc_filtered.items():
                filtered_assets[type_id] = filtered_assets.get(type_id, 0) + qty
        user_assets = filtered_assets

        if user_assets:
            price_data = _fetch_fuzzwork_prices(list(user_assets.keys()))
            visible_items = 0
            total_value = Decimal(0)

            for type_id, user_qty in user_assets.items():
                fuzz_prices = price_data.get(type_id, {})
                jita_buy = fuzz_prices.get("buy") or Decimal(0)
                jita_sell = fuzz_prices.get("sell") or Decimal(0)
                base = jita_sell if config.sell_markup_base == "sell" else jita_buy
                if base <= 0:
                    continue
                unit_price = base * (1 + (config.sell_markup_percent / Decimal(100)))
                item_value = unit_price * user_qty
                total_value += item_value
                visible_items += 1

            stock_count = visible_items
            total_stock_value = total_value
    except Exception:
        # Fall back silently if user assets cannot be loaded
        pass

    pending_sell_orders = config.sell_orders.filter(
        status=MaterialExchangeSellOrder.Status.DRAFT
    ).count()
    pending_buy_orders = config.buy_orders.filter(status="draft").count()
    capital_orders_active_count = CapitalShipOrder.objects.filter(
        config=config,
    ).exclude(
        status__in=[
            CapitalShipOrder.Status.COMPLETED,
            CapitalShipOrder.Status.REJECTED,
            CapitalShipOrder.Status.CANCELLED,
        ]
    ).count()

    # User's active orders
    closed_statuses = ["completed", "rejected", "cancelled"]
    user_sell_orders = (
        request.user.material_sell_orders.filter(config=config)
        .exclude(status__in=closed_statuses)
        .prefetch_related("items")
        .order_by("-created_at")[:5]
    )
    user_buy_orders = (
        request.user.material_buy_orders.filter(config=config)
        .exclude(status__in=closed_statuses)
        .prefetch_related("items")
        .order_by("-created_at")[:5]
    )

    recent_orders = []
    for order in user_sell_orders:
        recent_orders.append(_attach_order_progress_data(order, "sell"))
    for order in user_buy_orders:
        recent_orders.append(_attach_order_progress_data(order, "buy"))
    recent_orders.sort(key=lambda order: order.created_at, reverse=True)
    recent_orders = recent_orders[:10]

    # Admin section data (if user has permission)
    can_admin = request.user.has_perm("indy_hub.can_manage_material_hub")
    explicit_manage_material_hub_perm = False
    try:
        manage_perm = Permission.objects.get(
            codename="can_manage_material_hub", content_type__app_label="indy_hub"
        )
        explicit_manage_material_hub_perm = (
            User.objects.filter(
                id=request.user.id,
                is_active=True,
            )
            .filter(
                Q(groups__permissions=manage_perm) | Q(user_permissions=manage_perm)
            )
            .exists()
        )
    except Permission.DoesNotExist:
        explicit_manage_material_hub_perm = False

    superuser_without_material_hub_manage = bool(
        request.user.is_superuser and not explicit_manage_material_hub_perm
    )
    admin_sell_orders = None
    admin_buy_orders = None
    status_filter = None

    if can_admin:
        status_filter = request.GET.get("status") or None
        # Admin panel: show only active/in-flight orders; closed ones move to history view
        admin_sell_orders = (
            config.sell_orders.exclude(status__in=closed_statuses)
            .select_related("seller")
            .prefetch_related("items")
            .order_by("-created_at")
        )
        admin_buy_orders = (
            config.buy_orders.exclude(status__in=closed_statuses)
            .select_related("buyer")
            .prefetch_related("items")
            .order_by("-created_at")
        )
        if status_filter:
            admin_sell_orders = admin_sell_orders.filter(status=status_filter)
            admin_buy_orders = admin_buy_orders.filter(status=status_filter)

        admin_sell_orders = list(admin_sell_orders)
        for order in admin_sell_orders:
            order.seller_display_name = _resolve_main_character_name(order.seller)
            _attach_order_progress_data(order, "sell", perspective="admin")

        admin_buy_orders = list(admin_buy_orders)
        for order in admin_buy_orders:
            order.buyer_display_name = _resolve_main_character_name(order.buyer)
            _attach_order_progress_data(order, "buy", perspective="admin")

    context = {
        "config": config,
        "hub_location_label": hub_location_label,
        "sell_location_names": sell_location_names,
        "buy_location_names": buy_location_names,
        "buy_enabled": buy_enabled,
        "stock_count": stock_count,
        "total_stock_value": total_stock_value,
        "pending_sell_orders": pending_sell_orders,
        "pending_buy_orders": pending_buy_orders,
        "capital_orders_active_count": capital_orders_active_count,
        "recent_orders": recent_orders,
        "can_admin": can_admin,
        "superuser_without_material_hub_manage": superuser_without_material_hub_manage,
        "admin_sell_orders": admin_sell_orders,
        "admin_buy_orders": admin_buy_orders,
        "status_filter": status_filter,
        "nav_context": _build_nav_context(request.user),
    }

    context.update(
        build_nav_context(
            request.user,
            active_tab="material_hub",
            can_manage_corp=request.user.has_perm(
                "indy_hub.can_manage_corp_bp_requests"
            ),
        )
    )

    return render(request, "indy_hub/material_exchange/index.html", context)


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_http_methods(["POST"])
def material_exchange_sell_estimate(request):
    """Estimate sell payout from pasted item rows and configured Buyback rules."""

    emit_view_analytics_event(
        view_name="material_exchange.sell_estimate",
        request=request,
    )

    if not _is_material_exchange_enabled():
        return JsonResponse(
            {"ok": False, "summary": _("Buyback is disabled.")},
            status=400,
        )

    config = _get_material_exchange_config()
    if not config:
        return JsonResponse(
            {"ok": False, "summary": _("Buyback is not configured.")},
            status=400,
        )

    sell_structure_ids = _get_effective_sell_structure_ids(config)
    if not sell_structure_ids:
        return JsonResponse(
            {"ok": False, "summary": _("Sell locations are not configured.")},
            status=400,
        )

    raw_text = str(request.POST.get("estimate_text") or "").strip()
    parsed_rows, invalid_lines = _parse_sell_estimate_input(raw_text)
    if not parsed_rows:
        return JsonResponse(
            {
                "ok": False,
                "summary": _(
                    "No valid lines were detected. Use: item name <tab> qty or item name <space> qty."
                ),
                "invalid_lines": invalid_lines,
            },
            status=400,
        )

    type_ids = [int(row["type_id"]) for row in parsed_rows]
    price_data = _get_stock_jita_price_map(config=config, type_ids=type_ids)

    missing_price_type_ids: list[int] = []
    for type_id in type_ids:
        info = price_data.get(int(type_id))
        if not info:
            missing_price_type_ids.append(int(type_id))
            continue
        buy_price = Decimal(str(info.get("buy") or 0))
        sell_price = Decimal(str(info.get("sell") or 0))
        if buy_price <= 0 and sell_price <= 0:
            missing_price_type_ids.append(int(type_id))

    fetched_price_data: dict[int, dict[str, Decimal]] = {}
    prices_backfilled = 0
    live_lookup_type_ids = missing_price_type_ids[:MAX_ESTIMATE_LIVE_PRICE_LOOKUPS]
    deferred_live_lookup_count = max(
        0, len(missing_price_type_ids) - len(live_lookup_type_ids)
    )
    if live_lookup_type_ids:
        fetched_price_data = _fetch_fuzzwork_prices(live_lookup_type_ids)
        if fetched_price_data:
            prices_backfilled = _upsert_stock_jita_prices(
                config=config,
                price_data=fetched_price_data,
            )
            price_data.update(fetched_price_data)
    sell_override_map, _buy_override_map = _get_item_price_override_maps(config)
    sell_market_group_override_map, _buy_market_group_override_map = (
        _get_market_group_price_override_maps(config)
    )
    sell_container_override, _buy_container_override = (
        _get_container_price_override_maps(config)
    )
    type_market_group_path_map = _get_type_market_group_path_map(type_ids)
    type_name_map = _get_type_name_map(type_ids)

    sell_structure_name_map = config.get_sell_structure_name_map()
    allowed_type_ids_cache: dict[int, set[int] | None] = {}

    def format_decimal(value: Decimal) -> str:
        return format(value.quantize(Decimal("0.01")), ".2f")

    estimate_rows: list[dict[str, object]] = []
    quoteable_count = 0
    not_accepted_count = 0
    no_price_count = 0
    estimated_total = Decimal("0")

    for row in parsed_rows:
        type_id = int(row["type_id"])
        quantity = max(int(row["quantity"]), 0)
        type_name = type_name_map.get(type_id) or str(type_id)
        accepted_locations = _get_estimate_accepting_sell_locations(
            config=config,
            type_id=type_id,
            sell_structure_ids=sell_structure_ids,
            sell_structure_name_map=sell_structure_name_map,
            allowed_type_ids_cache=allowed_type_ids_cache,
        )

        fuzz_prices = price_data.get(type_id, {})
        jita_buy = Decimal(fuzz_prices.get("buy") or 0)
        jita_sell = Decimal(fuzz_prices.get("sell") or 0)
        unit_price, _default_unit_price, _has_override = _compute_effective_sell_unit_price(
            config=config,
            type_id=type_id,
            jita_buy=jita_buy,
            jita_sell=jita_sell,
            sell_override_map=sell_override_map,
            sell_market_group_override_map=sell_market_group_override_map,
            type_market_group_path_map=type_market_group_path_map,
            sell_container_override=sell_container_override,
            in_container=False,
        )

        can_quote = bool(accepted_locations) and unit_price > 0
        total_price = (unit_price * quantity) if can_quote else Decimal("0")
        status = "ok"
        reason = ""
        if not accepted_locations:
            status = "not_accepted"
            not_accepted_count += 1
            reason = _("Not accepted in any configured sell citadel.")
        elif unit_price <= 0:
            status = "no_price"
            no_price_count += 1
            reason = _("No valid market price found with current pricing settings.")
        else:
            quoteable_count += 1
            estimated_total += total_price

        estimate_rows.append(
            {
                "type_id": type_id,
                "type_name": type_name,
                "quantity": quantity,
                "unit_price": format_decimal(unit_price if can_quote else Decimal("0")),
                "total_price": format_decimal(total_price),
                "accepted_locations": accepted_locations,
                "status": status,
                "reason": str(reason),
            }
        )

    rounded_estimated_total = estimated_total.quantize(
        Decimal("1"), rounding=ROUND_CEILING
    )
    summary_parts = [
        _("%(count)s valid line(s) parsed.")
        % {"count": int(len(parsed_rows))},
        _("%(count)s line(s) quoteable.")
        % {"count": int(quoteable_count)},
    ]
    if not_accepted_count:
        summary_parts.append(
            _("%(count)s line(s) not accepted in configured sell locations.")
            % {"count": int(not_accepted_count)}
        )
    if no_price_count:
        summary_parts.append(
            _("%(count)s line(s) missing price data.") % {"count": int(no_price_count)}
        )
    if invalid_lines:
        summary_parts.append(
            _("%(count)s line(s) could not be parsed.") % {"count": len(invalid_lines)}
        )

    instructions = [
        _(
            "Click Start Selling, then pick a sell location that accepts your items."
        ),
        _(
            "Enter the same quantities, submit the sell order, and copy the generated order reference."
        ),
        _(
            "Create an in-game Item Exchange contract to the listed corporation at the order location."
        ),
        _(
            "Use the exact order reference in the contract title/description, then use Paste & Check on the order page before finalizing."
        ),
    ]

    return JsonResponse(
        {
            "ok": True,
            "summary": " ".join(summary_parts),
            "items": estimate_rows,
            "invalid_lines": invalid_lines,
            "estimated_total": format_decimal(estimated_total),
            "rounded_estimated_total": str(rounded_estimated_total),
            "instructions": instructions,
            "pricing": {
                "missing_requested": int(len(missing_price_type_ids)),
                "fetched_live": int(len(fetched_price_data)),
                "backfilled_rows": int(prices_backfilled),
                "deferred_live_lookups": int(deferred_live_lookup_count),
            },
        }
    )


@login_required
@indy_hub_permission_required("can_access_indy_hub")
def material_exchange_history(request):
    """Admin-only history page showing closed (completed/rejected/cancelled) orders."""
    emit_view_analytics_event(view_name="material_exchange.history", request=request)
    if not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("You are not allowed to view this page."))
        return redirect("indy_hub:material_exchange_index")

    if not _is_material_exchange_enabled():
        messages.warning(request, _("Buyback is disabled."))
        return redirect("indy_hub:material_exchange_index")

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, _("Buyback is not configured."))
        return redirect("indy_hub:material_exchange_index")
    closed_statuses = ["completed", "rejected", "cancelled"]

    sell_history = (
        config.sell_orders.filter(status__in=closed_statuses)
        .select_related("seller")
        .order_by("-created_at")
    )
    buy_history = (
        config.buy_orders.filter(status__in=closed_statuses)
        .select_related("buyer")
        .order_by("-created_at")
    )

    context = {
        "config": config,
        "sell_history": sell_history,
        "buy_history": buy_history,
        "nav_context": _build_nav_context(request.user),
    }

    context.update(
        build_nav_context(
            request.user,
            active_tab="material_hub",
            can_manage_corp=request.user.has_perm(
                "indy_hub.can_manage_corp_bp_requests"
            ),
        )
    )

    return render(request, "indy_hub/material_exchange/history.html", context)


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@tokens_required(scopes="esi-assets.read_assets.v1")
def material_exchange_sell(request, tokens):
    """
    Sell materials TO the hub.
    Member chooses materials + quantities, creates ONE order with multiple items.
    """
    emit_view_analytics_event(view_name="material_exchange.sell", request=request)
    if not _is_material_exchange_enabled():
        messages.warning(request, _("Buyback is disabled."))
        return redirect("indy_hub:material_exchange_index")

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, _("Buyback is not configured."))
        return redirect("indy_hub:material_exchange_index")
    sell_structure_ids = config.get_sell_structure_ids()
    if not sell_structure_ids:
        messages.warning(request, _("Sell locations are not configured."))
        return redirect("indy_hub:material_exchange_index")
    sell_structure_name_map = config.get_sell_structure_name_map()
    missing_name_ids = [
        int(sid)
        for sid in sell_structure_ids
        if int(sid) not in sell_structure_name_map
    ]
    if missing_name_ids:
        try:
            resolved = resolve_structure_names(
                missing_name_ids,
                corporation_id=int(config.corporation_id),
                user=request.user,
                schedule_async=True,
            )
            for sid, name in resolved.items():
                if name and not str(name).startswith("Structure "):
                    sell_structure_name_map[int(sid)] = str(name)
        except Exception:
            pass
    materials_with_qty: list[dict] = []
    assets_refreshing = False
    sell_override_map, _buy_override_map = _get_item_price_override_maps(config)
    sell_market_group_override_map, _buy_market_group_override_map = (
        _get_market_group_price_override_maps(config)
    )
    sell_container_override, _buy_container_override = (
        _get_container_price_override_maps(config)
    )

    sell_last_update = (
        CachedCharacterAsset.objects.filter(user=request.user)
        .order_by("-synced_at")
        .values_list("synced_at", flat=True)
        .first()
    )

    user_assets_version_refresh = False
    try:
        if sell_last_update:
            current_version = int(
                cache.get(me_user_assets_cache_version_key(int(request.user.id))) or 0
            )
            user_assets_version_refresh = current_version < int(
                ME_USER_ASSETS_CACHE_VERSION
            )
    except Exception:
        user_assets_version_refresh = False

    try:
        user_assets_stale = (
            not sell_last_update
            or (timezone.now() - sell_last_update).total_seconds() > 3600
        )
    except Exception:
        user_assets_stale = True

    # Start async refresh of the user's assets on page open (GET only).
    progress_key = _me_sell_assets_progress_key(request.user.id)
    sell_assets_progress = cache.get(progress_key) or {}
    if request.method == "GET" and (user_assets_stale or user_assets_version_refresh):
        # The refreshed=1 guard prevents loops, but version migrations should override it.
        if request.GET.get("refreshed") != "1" or user_assets_version_refresh:
            sell_assets_progress = _ensure_sell_assets_refresh_started(request.user)
    assets_refreshing = bool(sell_assets_progress.get("running"))

    if sell_assets_progress.get("error") == "esi_down" and not sell_assets_progress.get(
        "retry_after_minutes"
    ):
        cooldown_until = cache.get(
            me_sell_assets_esi_cooldown_key(int(request.user.id))
        )
        if cooldown_until:
            try:
                retry_seconds = max(
                    0, int(float(cooldown_until) - timezone.now().timestamp())
                )
            except (TypeError, ValueError):
                retry_seconds = int(ESI_DOWN_COOLDOWN_SECONDS)
            sell_assets_progress["retry_after_minutes"] = int(
                (retry_seconds + 59) // 60
            )

    if request.method == "POST":
        selected_location_param = (request.POST.get("sell_location_id") or "").strip()
        selected_location_id = None
        if selected_location_param:
            try:
                selected_location_id = int(selected_location_param)
            except (TypeError, ValueError):
                selected_location_id = None
        if selected_location_id not in sell_structure_ids:
            selected_location_id = sell_structure_ids[0]
        active_location_name = (
            sell_structure_name_map.get(int(selected_location_id))
            if selected_location_id
            else ""
        )
        if not active_location_name and selected_location_id:
            active_location_name = f"Structure {selected_location_id}"
        sell_redirect_url = reverse("indy_hub:material_exchange_sell")
        if selected_location_id:
            sell_redirect_url = f"{sell_redirect_url}?location={int(selected_location_id)}"

        (
            _all_sell_assets,
            _all_sell_assets_by_character,
            all_sell_assets_by_location,
            scope_missing,
        ) = _fetch_user_assets_for_structure_data(
            request.user,
            sell_structure_ids,
            config=config,
        )
        if scope_missing:
            # Avoid transient flash messaging for missing scopes; the page already
            # renders a persistent on-page warning based on `sell_assets_progress`.
            _ensure_sell_assets_refresh_started(request.user)
            return redirect(sell_redirect_url)

        selected_location_assets_raw = all_sell_assets_by_location.get(
            int(selected_location_id), {}
        )
        if not selected_location_assets_raw:
            messages.error(
                request,
                _("No items available to sell at this location."),
            )
            return redirect(sell_redirect_url)

        allowed_type_ids_cache: dict[int, set[int] | None] = {}
        selected_location_allowed_type_ids = _get_allowed_type_ids_for_config(
            config,
            "sell",
            structure_id=selected_location_id,
        )
        allowed_type_ids_cache[int(selected_location_id)] = (
            selected_location_allowed_type_ids
        )

        user_assets = dict(selected_location_assets_raw)
        pre_filter_count = len(user_assets)

        # Apply market group filter strictly (empty config means no allowed items)
        try:
            if selected_location_allowed_type_ids is not None:
                user_assets = {
                    tid: qty
                    for tid, qty in user_assets.items()
                    if tid in selected_location_allowed_type_ids
                }
        except Exception as exc:
            logger.warning("Failed to apply market group filter: %s", exc)

        if not user_assets:
            if pre_filter_count > 0:
                messages.error(
                    request,
                    _("No accepted items available to sell at this location."),
                )
            else:
                messages.error(
                    request, _("You have no items to sell at this location.")
                )
            return redirect(sell_redirect_url)

        # Parse submitted quantities from the form. Do not iterate over `user_assets` here:
        # doing so can silently drop items if assets changed or a type was filtered out.
        submitted_item_quantities = _parse_submitted_sell_item_quantities(request.POST)
        submitted_quantities: dict[int, int] = {}
        for submitted_entry in submitted_item_quantities:
            type_id = int(submitted_entry.get("type_id") or 0)
            qty = int(submitted_entry.get("quantity") or 0)
            if type_id <= 0 or qty <= 0:
                continue
            submitted_quantities[type_id] = submitted_quantities.get(type_id, 0) + qty

        if not submitted_item_quantities or not submitted_quantities:
            messages.error(
                request,
                _("Please enter a quantity greater than 0 for at least one item."),
            )
            return redirect(sell_redirect_url)

        submitted_type_market_group_path_map = _get_type_market_group_path_map(
            set(submitted_quantities.keys())
        )

        assets_last_sync = _get_user_assets_last_sync(request.user)
        reserved_quantities = _get_reserved_sell_quantities(
            config=config,
            seller=request.user,
            location_id=selected_location_id,
            type_ids=set(submitted_quantities.keys()),
            assets_synced_at=assets_last_sync,
        )

        items_to_create: list[dict] = []
        errors: list[str] = []
        total_payout = Decimal("0")

        price_data = _fetch_fuzzwork_prices(list(submitted_quantities.keys()))

        scoped_variant_quantities_available = True
        try:
            all_cached_assets_for_pricing, _scope_missing_for_pricing = get_user_assets_cached(
                request.user,
                allow_refresh=False,
            )
        except Exception:
            all_cached_assets_for_pricing = []
            scoped_variant_quantities_available = False
        variant_quantities = _build_sell_variant_quantities(
            assets=all_cached_assets_for_pricing,
            location_id=selected_location_id,
        )
        scoped_variant_quantities = (
            _build_scoped_variant_quantities(
                assets=all_cached_assets_for_pricing,
                location_id=selected_location_id,
            )
            if scoped_variant_quantities_available
            else {}
        )

        for type_id, qty in submitted_quantities.items():
            user_qty = user_assets.get(type_id)
            if user_qty is None:
                type_name = get_type_name(type_id)
                selected_raw_qty = int(selected_location_assets_raw.get(type_id, 0) or 0)
                other_locations = _find_sell_locations_for_type(
                    config=config,
                    sell_structure_ids=sell_structure_ids,
                    sell_structure_name_map=sell_structure_name_map,
                    user_assets_by_location=all_sell_assets_by_location,
                    type_id=type_id,
                    exclude_location_id=selected_location_id,
                    allowed_type_ids_cache=allowed_type_ids_cache,
                )
                location_hints = ", ".join(
                    f"{entry['name']} ({int(entry['quantity']):,})"
                    for entry in other_locations
                )

                if (
                    selected_raw_qty > 0
                    and selected_location_allowed_type_ids is not None
                    and type_id not in selected_location_allowed_type_ids
                ):
                    if location_hints:
                        errors.append(
                            _(
                                f"{type_name} is not accepted at {active_location_name}. "
                                f"It is accepted at: {location_hints}."
                            )
                        )
                    else:
                        errors.append(
                            _(
                                f"{type_name} is not accepted at {active_location_name} "
                                "and is not accepted in other configured sell locations."
                            )
                        )
                elif location_hints:
                    errors.append(
                        _(
                            f"{type_name} is no longer available at {active_location_name}. "
                            f"You can sell it at: {location_hints}."
                        )
                    )
                else:
                    errors.append(
                        _(
                            f"{type_name} is no longer available at {active_location_name}. Please refresh the page and try again."
                        )
                    )
                continue

            if qty > user_qty:
                type_name = get_type_name(type_id)
                errors.append(
                    _(
                            f"Insufficient {type_name} in {active_location_name}. You have: {user_qty:,}, requested: {qty:,}"
                        )
                    )
                continue

            reserved_qty = int(reserved_quantities.get(int(type_id), 0) or 0)
            available_by_type = max(int(user_qty) - reserved_qty, 0)
            if qty > available_by_type:
                type_name = get_type_name(type_id)
                if reserved_qty > 0:
                    errors.append(
                        _(
                            f"Insufficient unlocked {type_name} in {active_location_name}. "
                            f"Available now: {available_by_type:,}, reserved in open orders: {reserved_qty:,}, requested: {qty:,}."
                        )
                    )
                else:
                    errors.append(
                        _(
                            f"Insufficient {type_name} in {active_location_name}. You have: {user_qty:,}, requested: {qty:,}"
                        )
                    )
                continue

        if errors:
            for err in errors:
                messages.error(request, err)
            return redirect(sell_redirect_url)

        for submitted_entry in submitted_item_quantities:
            type_id = int(submitted_entry.get("type_id") or 0)
            qty = int(submitted_entry.get("quantity") or 0)
            in_container = bool(submitted_entry.get("in_container"))
            blueprint_variant = str(
                submitted_entry.get("blueprint_variant") or ""
            ).strip().lower()
            if blueprint_variant not in {"", "bpo", "bpc"}:
                blueprint_variant = ""
            if type_id <= 0 or qty <= 0:
                continue

            type_name_base = get_type_name(type_id)
            type_name = _format_sell_blueprint_type_name(type_name_base, blueprint_variant)

            if blueprint_variant in {"bpo", "bpc"}:
                variant_available = int(
                    variant_quantities.get((type_id, blueprint_variant), 0) or 0
                )
                if qty > variant_available:
                    errors.append(
                        _(
                            f"Insufficient {type_name} in {active_location_name}. "
                            f"You have: {variant_available:,}, requested: {qty:,}."
                        )
                    )
                    continue

            if scoped_variant_quantities_available:
                scoped_available = int(
                    scoped_variant_quantities.get(
                        (type_id, blueprint_variant, in_container), 0
                    )
                    or 0
                )
                if qty > scoped_available:
                    scope_label = (
                        _("inside containers") if in_container else _("outside containers")
                    )
                    errors.append(
                        _(
                            f"Insufficient {type_name} {scope_label} in {active_location_name}. "
                            f"You have: {scoped_available:,}, requested: {qty:,}."
                        )
                    )
                    continue
            elif in_container:
                errors.append(
                    _(
                        "Unable to validate in-container quantities right now. "
                        "Please refresh and try again."
                    )
                )
                continue

            if blueprint_variant == "bpc":
                unit_price = Decimal("0")
            else:
                fuzz_prices = price_data.get(type_id, {})
                jita_buy = Decimal(fuzz_prices.get("buy") or 0)
                jita_sell = Decimal(fuzz_prices.get("sell") or 0)
                unit_price, _default_unit_price, _has_override = (
                    _compute_effective_sell_unit_price(
                        config=config,
                        type_id=type_id,
                        jita_buy=jita_buy,
                        jita_sell=jita_sell,
                        sell_override_map=sell_override_map,
                        sell_market_group_override_map=sell_market_group_override_map,
                        type_market_group_path_map=submitted_type_market_group_path_map,
                        sell_container_override=sell_container_override,
                        in_container=in_container,
                    )
                )
                if unit_price <= 0:
                    errors.append(_(f"{type_name} has no valid market price."))
                    continue

            total_price = unit_price * qty
            total_payout += total_price

            items_to_create.append(
                {
                    "type_id": type_id,
                    "type_name": type_name,
                    "quantity": qty,
                    "unit_price": unit_price,
                    "total_price": total_price,
                }
            )

        if errors:
            for err in errors:
                messages.error(request, err)

            # Prevent creating a partial order with an unexpected (lower) total.
            return redirect(sell_redirect_url)

        if items_to_create:
            # Get order reference from client (generated in JavaScript)
            client_order_ref = request.POST.get("order_reference", "").strip()

            order = MaterialExchangeSellOrder.objects.create(
                config=config,
                seller=request.user,
                status=MaterialExchangeSellOrder.Status.DRAFT,
                order_reference=client_order_ref if client_order_ref else None,
                source_location_id=selected_location_id,
                source_location_name=active_location_name or "",
            )
            for item_data in items_to_create:
                MaterialExchangeSellOrderItem.objects.create(order=order, **item_data)

            rounded_total_payout = total_payout.quantize(
                Decimal("1"), rounding=ROUND_CEILING
            )
            order.rounded_total_price = rounded_total_payout
            order.save(update_fields=["rounded_total_price", "updated_at"])

            messages.success(
                request,
                _(
                    f"Sell order created. Order reference: {order.order_reference}. "
                    f"Open your order page to follow the contract steps."
                ),
            )

            # Redirect to order detail page instead of index
            return redirect("indy_hub:sell_order_detail", order_id=order.id)

        return redirect(sell_redirect_url)

    # GET branch: trigger stock sync only if stale (> 1h) or never synced
    message_shown = False
    try:
        last_sync = config.last_stock_sync
        needs_refresh = (
            not last_sync or (timezone.now() - last_sync).total_seconds() > 3600
        )
    except Exception:
        needs_refresh = True

    stock_version_refresh = False
    try:
        # Only trigger the version refresh if there is already synced data.
        if config.last_stock_sync:
            current_version = int(
                cache.get(me_stock_sync_cache_version_key(int(config.corporation_id)))
                or 0
            )
            stock_version_refresh = current_version < int(ME_STOCK_SYNC_CACHE_VERSION)
    except Exception:
        stock_version_refresh = False

    if needs_refresh or stock_version_refresh:
        messages.info(
            request,
            _(
                "Refreshing via ESI. Make sure you have granted the assets scope to at least one character."
            ),
        )
        message_shown = True
        try:
            logger.info(
                "Starting stock sync for sell page (last_sync=%s)",
                config.last_stock_sync,
            )
            sync_material_exchange_stock()
            config.refresh_from_db()
            logger.info(
                "Stock sync completed successfully (last_sync=%s)",
                config.last_stock_sync,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Stock auto-sync failed (sell page): %s", exc, exc_info=True)

    # Avoid blocking GET requests: if a background refresh is running, don't do a synchronous refresh.
    # If we're on ?refreshed=1 and nothing is cached yet, allow a one-time sync refresh so the list
    # can still render even if the background job didn't populate anything.
    has_cached_assets = CachedCharacterAsset.objects.filter(user=request.user).exists()

    current_user_assets_version = 0
    try:
        current_user_assets_version = int(
            cache.get(me_user_assets_cache_version_key(int(request.user.id))) or 0
        )
    except Exception:
        current_user_assets_version = 0
    needs_user_assets_version_refresh = has_cached_assets and (
        current_user_assets_version < int(ME_USER_ASSETS_CACHE_VERSION)
    )

    allow_refresh = (
        not bool(sell_assets_progress.get("running"))
        or sell_assets_progress.get("error") == "task_start_failed"
    ) and (
        request.GET.get("refreshed") != "1"
        or not has_cached_assets
        or needs_user_assets_version_refresh
    )
    user_assets, user_assets_by_character, user_assets_by_location, scope_missing = (
        _fetch_user_assets_for_structure_data(
            request.user,
            sell_structure_ids,
            allow_refresh=allow_refresh,
            config=config,
        )
    )
    if sell_assets_progress.get("error") == "no_assets_fetched" and (
        has_cached_assets or user_assets
    ):
        sell_assets_progress = dict(sell_assets_progress)
        sell_assets_progress["error"] = None
        cache.set(
            progress_key,
            sell_assets_progress,
            10 * 60,
        )
    active_location_id = str(sell_structure_ids[0]) if sell_structure_ids else ""
    active_location_name = sell_structure_name_map.get(
        int(sell_structure_ids[0]), ""
    ) if sell_structure_ids else ""
    if not active_location_name and sell_structure_ids:
        active_location_name = f"Structure {sell_structure_ids[0]}"
    location_tabs: list[dict] = []

    if user_assets:
        pre_filter_count = len(user_assets)
        logger.info(
            f"SELL DEBUG: Found {len(user_assets)} unique items in assets before production filter (filter disabled)"
        )

        # Apply market group filter strictly (same as POST + Index)
        try:
            allowed_type_ids_by_location: dict[int, set[int] | None] = {
                int(sid): _get_allowed_type_ids_for_config(
                    config,
                    "sell",
                    structure_id=int(sid),
                )
                for sid in sell_structure_ids
            }

            filtered_by_location: dict[int, dict[int, int]] = {}
            for location_id, loc_assets in user_assets_by_location.items():
                allowed_type_ids = allowed_type_ids_by_location.get(int(location_id))
                if allowed_type_ids is None:
                    filtered_loc_assets = dict(loc_assets)
                else:
                    filtered_loc_assets = {
                        tid: qty
                        for tid, qty in loc_assets.items()
                        if tid in allowed_type_ids
                    }
                filtered_by_location[int(location_id)] = filtered_loc_assets
            user_assets_by_location = filtered_by_location

            filtered_user_assets: dict[int, int] = {}
            for loc_assets in user_assets_by_location.values():
                for type_id, qty in loc_assets.items():
                    filtered_user_assets[type_id] = (
                        filtered_user_assets.get(type_id, 0) + qty
                    )
            user_assets = filtered_user_assets

            if len(sell_structure_ids) <= 1 and sell_structure_ids:
                single_location_id = int(sell_structure_ids[0])
                single_location_allowed = allowed_type_ids_by_location.get(
                    single_location_id
                )
                if single_location_allowed is not None:
                    user_assets_by_character = {
                        character_id: {
                            tid: qty
                            for tid, qty in char_assets.items()
                            if tid in single_location_allowed
                        }
                        for character_id, char_assets in user_assets_by_character.items()
                    }

            logger.info(
                f"SELL DEBUG: {len(user_assets)} items after market group filter"
            )
        except Exception as exc:
            logger.warning("Failed to apply market group filter (GET): %s", exc)

        price_data = _fetch_fuzzwork_prices(list(user_assets.keys()))
        logger.info(f"SELL DEBUG: Got prices for {len(price_data)} items from Fuzzwork")
        display_type_market_group_path_map = _get_type_market_group_path_map(
            set(user_assets.keys())
        )
        display_type_market_group_label_map = _build_type_market_group_label_map(
            display_type_market_group_path_map
        )

        def _is_sellable_type(type_id: int) -> bool:
            fuzz_prices = price_data.get(type_id, {})
            jita_buy = Decimal(fuzz_prices.get("buy") or 0)
            jita_sell = Decimal(fuzz_prices.get("sell") or 0)
            buy_price, _default_buy_price, _has_override = (
                _compute_effective_sell_unit_price(
                    config=config,
                    type_id=type_id,
                    jita_buy=jita_buy,
                    jita_sell=jita_sell,
                    sell_override_map=sell_override_map,
                    sell_market_group_override_map=sell_market_group_override_map,
                    type_market_group_path_map=display_type_market_group_path_map,
                    sell_container_override=sell_container_override,
                    in_container=bool(sell_container_override is not None),
                )
            )
            return buy_price > 0

        sell_page_base_url = reverse("indy_hub:material_exchange_sell")
        location_tabs = []
        character_tabs = []
        active_character_tab = ""
        selected_character_id: int | None = None
        character_names_map = _resolve_user_character_names_map(request.user)

        selected_location_param = (request.GET.get("location") or "").strip()
        selected_location_id: int | None = None
        if selected_location_param:
            try:
                selected_location_id = int(selected_location_param)
            except (TypeError, ValueError):
                selected_location_id = None
        if selected_location_id not in sell_structure_ids:
            selected_location_id = sell_structure_ids[0]
        active_location_id = str(selected_location_id) if selected_location_id else ""

        active_location_name = sell_structure_name_map.get(int(selected_location_id), "")
        if not active_location_name and selected_location_id:
            active_location_name = f"Structure {selected_location_id}"

        show_character_tabs = len(sell_structure_ids) <= 1

        if show_character_tabs:
            sorted_characters = sorted(
                user_assets_by_character.keys(),
                key=lambda character_id: character_names_map.get(
                    character_id, str(character_id)
                ).lower(),
            )
            for character_id in sorted_characters:
                character_assets = user_assets_by_character.get(character_id, {})
                tab_count = sum(
                    1 for type_id in character_assets if _is_sellable_type(type_id)
                )
                if tab_count <= 0:
                    continue
                character_tabs.append(
                    {
                        "id": str(character_id),
                        "name": character_names_map.get(
                            character_id, _("Character %(id)s") % {"id": character_id}
                        ),
                        "item_count": tab_count,
                        "url": f"{sell_page_base_url}?character={character_id}",
                    }
                )

            selected_character_param = (request.GET.get("character") or "").strip()
            if selected_character_param:
                try:
                    selected_character_id = int(selected_character_param)
                except (TypeError, ValueError):
                    selected_character_id = None

            available_character_ids = {
                int(tab["id"])
                for tab in character_tabs
                if str(tab.get("id", "")).isdigit()
            }

            if selected_character_id in available_character_ids:
                active_character_tab = str(selected_character_id)
            elif character_tabs:
                active_character_tab = str(character_tabs[0]["id"])
                selected_character_id = int(active_character_tab)
            else:
                active_character_tab = ""
                selected_character_id = None

            if (
                selected_character_id
                and selected_character_id in user_assets_by_character
            ):
                assets_for_display = user_assets_by_character[selected_character_id]
            else:
                assets_for_display = {}
        else:
            for loc_id in sell_structure_ids:
                loc_assets = user_assets_by_location.get(int(loc_id), {})
                tab_count = sum(
                    1 for type_id in loc_assets if _is_sellable_type(type_id)
                )
                location_tabs.append(
                    {
                        "id": str(loc_id),
                        "name": sell_structure_name_map.get(int(loc_id), "")
                        or f"Structure {loc_id}",
                        "item_count": tab_count,
                        "url": f"{sell_page_base_url}?location={loc_id}",
                    }
                )
            if not selected_location_param:
                first_with_items = next(
                    (
                        int(tab.get("id"))
                        for tab in location_tabs
                        if int(tab.get("item_count") or 0) > 0
                    ),
                    None,
                )
                if first_with_items and first_with_items != int(selected_location_id):
                    selected_location_id = int(first_with_items)
                    active_location_id = str(selected_location_id)
                    active_location_name = (
                        sell_structure_name_map.get(int(selected_location_id), "")
                        or f"Structure {selected_location_id}"
                    )
            assets_for_display = user_assets_by_location.get(
                int(selected_location_id), {}
            )

        assets_last_sync = _get_user_assets_last_sync(request.user)
        reserved_quantities_for_display = _get_reserved_sell_quantities(
            config=config,
            seller=request.user,
            location_id=selected_location_id,
            assets_synced_at=assets_last_sync,
        )

        selected_location_allowed_type_ids: set[int] | None = None
        if selected_location_id:
            selected_location_allowed_type_ids = _get_allowed_type_ids_for_config(
                config,
                "sell",
                structure_id=int(selected_location_id),
            )

        display_assets_raw: list[dict] = []
        try:
            all_cached_assets, _raw_scope_missing = get_user_assets_cached(
                request.user,
                allow_refresh=False,
            )
        except Exception:
            all_cached_assets = []

        excluded_item_ids: set[int] = set()
        try:
            if not bool(getattr(config, "allow_fitted_ships", False)):
                excluded_item_ids = _build_fitted_ship_excluded_item_ids(all_cached_assets)
        except Exception:
            excluded_item_ids = set()

        for asset in all_cached_assets:
            try:
                asset_location_id = int(asset.get("location_id") or 0)
            except (TypeError, ValueError):
                continue
            if selected_location_id and asset_location_id != int(selected_location_id):
                continue

            if show_character_tabs and selected_character_id:
                try:
                    asset_character_id = int(asset.get("character_id") or 0)
                except (TypeError, ValueError):
                    continue
                if asset_character_id != int(selected_character_id):
                    continue

            if excluded_item_ids:
                try:
                    asset_item_id = int(asset.get("item_id") or 0)
                except (TypeError, ValueError):
                    asset_item_id = 0
                if asset_item_id > 0 and asset_item_id in excluded_item_ids:
                    continue

            display_assets_raw.append(asset)

        materials_with_qty = _build_sell_material_rows(
            assets=display_assets_raw,
            config=config,
            price_data=price_data,
            reserved_quantities=reserved_quantities_for_display,
            allowed_type_ids=selected_location_allowed_type_ids,
            sell_override_map=sell_override_map,
            sell_market_group_override_map=sell_market_group_override_map,
            sell_container_override=sell_container_override,
            type_market_group_path_map=display_type_market_group_path_map,
            type_market_group_label_map=display_type_market_group_label_map,
            character_name_by_id=character_names_map,
        )

        logger.info(
            f"SELL DEBUG: Final materials_with_qty count: {len(materials_with_qty)}"
        )

        if pre_filter_count > 0 and not materials_with_qty and not message_shown:
            messages.info(
                request,
                _("No accepted items available to sell at this location."),
            )
    else:
        if scope_missing and not message_shown:
            messages.info(
                request,
                _(
                    "Refreshing via ESI. Make sure you have granted the assets scope to at least one character."
                ),
            )
        elif not message_shown:
            messages.info(
                request,
                _("No items available to sell at this location."),
            )

    # Show loading spinner only while the refresh task is running.
    assets_refreshing = bool(sell_assets_progress.get("running"))

    # Get corporation name
    corporation_name = _get_corp_name_for_hub(config.corporation_id)

    context = {
        "config": config,
        "materials": materials_with_qty,
        "location_tabs": location_tabs if user_assets else [],
        "active_location_id": active_location_id,
        "active_location_name": active_location_name,
        "character_tabs": character_tabs if user_assets else [],
        "active_character_tab": active_character_tab if user_assets else "",
        "corporation_name": corporation_name,
        "assets_refreshing": assets_refreshing,
        "sell_assets_progress": sell_assets_progress,
        "sell_last_update": sell_last_update,
        "sell_next_refresh_minutes": _minutes_until_refresh(sell_last_update),
        "nav_context": _build_nav_context(request.user),
    }

    context.update(
        build_nav_context(
            request.user,
            active_tab="material_hub",
            can_manage_corp=request.user.has_perm(
                "indy_hub.can_manage_corp_bp_requests"
            ),
        )
    )

    return render(request, "indy_hub/material_exchange/sell.html", context)


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@tokens_required(scopes="esi-assets.read_corporation_assets.v1")
def material_exchange_buy(request, tokens):
    """
    Buy materials FROM the hub.
    Member chooses materials + quantities, creates ONE order with multiple items.
    """
    emit_view_analytics_event(view_name="material_exchange.buy", request=request)
    if not _is_material_exchange_enabled():
        messages.warning(request, _("Buyback is disabled."))
        return redirect("indy_hub:material_exchange_index")

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, _("Buyback is not configured."))
        return redirect("indy_hub:material_exchange_index")
    if not bool(getattr(config, "buy_enabled", True)):
        messages.info(request, _("Buy orders are currently disabled for this hub."))
        return redirect("indy_hub:material_exchange_index")
    stock_refreshing = False
    _sell_override_map, buy_override_map = _get_item_price_override_maps(config)
    _sell_market_group_override_map, buy_market_group_override_map = (
        _get_market_group_price_override_maps(config)
    )
    _sell_container_override, buy_container_override = (
        _get_container_price_override_maps(config)
    )

    buy_structure_ids = config.get_buy_structure_ids()
    buy_name_map = config.get_buy_structure_name_map()
    buy_location_names = []
    for sid in buy_structure_ids:
        sid_int = int(sid)
        buy_location_names.append(
            buy_name_map.get(sid_int) or f"Structure {sid_int}"
        )
    buy_locations_label = ", ".join(buy_location_names).strip() or (
        config.structure_name or f"Structure {config.structure_id}"
    )

    corp_assets_scope_missing = False
    try:
        # Alliance Auth
        from esi.models import Token

        corp_assets_scope_missing = not (
            Token.objects.filter(character__corporation_id=int(config.corporation_id))
            .require_scopes(["esi-assets.read_corporation_assets.v1"])
            .require_valid()
            .exists()
        )
    except Exception:
        corp_assets_scope_missing = False

    if request.method == "POST":
        # Parse submitted quantities from the form. Do not iterate over `stock_items` here:
        # doing so can silently drop items if stock changed (quantity=0) or an item is no
        # longer visible due to filters.
        submitted_entries = _parse_submitted_sell_item_quantities(request.POST)
        submitted_quantities: dict[int, int] = {}
        for submitted_entry in submitted_entries:
            try:
                type_id = int(submitted_entry.get("type_id") or 0)
                qty = int(submitted_entry.get("quantity") or 0)
            except (TypeError, ValueError):
                continue
            if type_id <= 0 or qty <= 0:
                continue
            submitted_quantities[type_id] = submitted_quantities.get(type_id, 0) + qty

        if not submitted_quantities:
            messages.error(
                request,
                _("Please enter a quantity greater than 0 for at least one item."),
            )
            return redirect("indy_hub:material_exchange_buy")

        items_to_create = []
        errors = []
        total_cost = Decimal("0")

        submitted_type_ids = set(submitted_quantities.keys())
        submitted_type_market_group_path_map = _get_type_market_group_path_map(
            submitted_type_ids
        )
        submitted_blueprint_variants = _get_buy_stock_blueprint_variant_map(
            config=config,
            type_ids=submitted_type_ids,
        )
        buy_scoped_variant_quantities_available = True
        try:
            scoped_buy_assets_for_validation = _get_buy_location_scoped_corp_assets(
                config=config
            )
            scoped_item_ids: set[int] = set()
            for scoped_asset in scoped_buy_assets_for_validation:
                try:
                    scoped_item_id = int(scoped_asset.get("item_id") or 0)
                except (TypeError, ValueError):
                    scoped_item_id = 0
                if scoped_item_id > 0:
                    scoped_item_ids.add(scoped_item_id)
            scoped_blueprint_details = _get_corp_blueprint_details_by_item_id(
                config=config,
                item_ids=scoped_item_ids,
            )
            scoped_variant_by_item_id = {
                int(item_id): str(details.get("variant") or "").strip().lower()
                for item_id, details in scoped_blueprint_details.items()
                if str(details.get("variant") or "").strip().lower() in {"bpc", "bpo"}
            }
            buy_scoped_variant_quantities = _build_scoped_variant_quantities(
                assets=scoped_buy_assets_for_validation,
                explicit_variant_by_item_id=scoped_variant_by_item_id,
            )
        except Exception:
            buy_scoped_variant_quantities_available = False
            buy_scoped_variant_quantities = {}

        with transaction.atomic():
            stock_items = list(
                config.stock_items.select_for_update()
                .filter(type_id__in=submitted_type_ids, quantity__gt=0)
            )
            stock_by_type_id = {item.type_id: item for item in stock_items}

            # Apply market group filter strictly (empty config means no allowed items)
            allowed_type_ids: set[int] | None = None
            try:
                allowed_type_ids = _get_allowed_type_ids_for_config(config, "buy")
            except Exception as exc:
                logger.warning("Failed to apply market group filter: %s", exc)

            reserved_quantities = _get_reserved_buy_quantities(
                config=config,
                type_ids=submitted_type_ids,
            )
            available_by_type: dict[int, int] = {}

            for type_id, qty in submitted_quantities.items():
                stock_item = stock_by_type_id.get(type_id)
                if stock_item is None:
                    type_name = get_type_name(type_id)
                    errors.append(
                        _(
                            f"{type_name} is no longer available in stock. Please refresh the page and try again."
                        )
                    )
                    continue

                blueprint_variant = str(
                    submitted_blueprint_variants.get(int(type_id), "")
                ).strip().lower()
                display_type_name = _format_buy_stock_type_name(
                    stock_item.type_name or get_type_name(type_id),
                    blueprint_variant,
                )

                if allowed_type_ids is not None and type_id not in allowed_type_ids:
                    errors.append(
                        _(
                            f"{display_type_name} is not available in the currently allowed categories."
                        )
                    )
                    continue

                reserved_qty = int(reserved_quantities.get(int(type_id), 0) or 0)
                available_qty = max(int(stock_item.quantity) - reserved_qty, 0)
                available_by_type[int(type_id)] = int(available_qty)
                if qty > available_qty:
                    errors.append(
                        _(
                            f"Insufficient unlocked stock for {display_type_name}. "
                            f"Available now: {available_qty:,}, reserved in open orders: {reserved_qty:,}, requested: {qty:,}"
                        )
                    )
                    continue

            if errors:
                for err in errors:
                    messages.error(request, err)
                # Prevent creating a partial order with an unexpected (lower) total.
                return redirect("indy_hub:material_exchange_buy")

            for submitted_entry in submitted_entries:
                try:
                    type_id = int(submitted_entry.get("type_id") or 0)
                    qty = int(submitted_entry.get("quantity") or 0)
                except (TypeError, ValueError):
                    continue
                if type_id <= 0 or qty <= 0:
                    continue
                in_container = bool(submitted_entry.get("in_container"))

                stock_item = stock_by_type_id.get(type_id)
                if stock_item is None:
                    continue

                requested_variant = str(
                    submitted_entry.get("blueprint_variant") or ""
                ).strip().lower()
                blueprint_variant = (
                    requested_variant
                    if requested_variant in {"bpc", "bpo"}
                    else str(submitted_blueprint_variants.get(int(type_id), "")).strip().lower()
                )
                display_type_name = _format_buy_stock_type_name(
                    stock_item.type_name or get_type_name(type_id),
                    blueprint_variant,
                )

                if buy_scoped_variant_quantities_available:
                    scoped_available = int(
                        buy_scoped_variant_quantities.get(
                            (type_id, blueprint_variant, in_container), 0
                        )
                        or 0
                    )
                    if qty > scoped_available:
                        scope_label = (
                            _("inside containers")
                            if in_container
                            else _("outside containers")
                        )
                        errors.append(
                            _(
                                f"Insufficient {display_type_name} {scope_label} in stock. "
                                f"You have: {scoped_available:,}, requested: {qty:,}."
                            )
                        )
                        continue
                elif in_container:
                    errors.append(
                        _(
                            "Unable to validate in-container stock quantities right now. "
                            "Please refresh and try again."
                        )
                    )
                    continue

                if blueprint_variant == "bpc":
                    unit_price = Decimal("0")
                else:
                    unit_price, _default_unit_price, _has_override = (
                        _compute_effective_buy_unit_price(
                            stock_item=stock_item,
                            buy_override_map=buy_override_map,
                            buy_market_group_override_map=buy_market_group_override_map,
                            type_market_group_path_map=submitted_type_market_group_path_map,
                            buy_container_override=buy_container_override,
                            in_container=in_container,
                        )
                    )
                    if unit_price <= 0:
                        errors.append(_(f"{display_type_name} has no valid market price."))
                        continue
                total_price = unit_price * qty
                total_cost += total_price

                items_to_create.append(
                    {
                        "type_id": type_id,
                        "type_name": display_type_name,
                        "quantity": qty,
                        "unit_price": unit_price,
                        "total_price": total_price,
                        "stock_available_at_creation": int(
                            available_by_type.get(int(type_id), 0)
                        ),
                    }
                )

            if errors:
                for err in errors:
                    messages.error(request, err)
                # Prevent creating a partial order with an unexpected (lower) total.
                return redirect("indy_hub:material_exchange_buy")

            if not items_to_create and not errors:
                messages.error(
                    request,
                    _("Please enter a quantity greater than 0 for at least one item."),
                )
                return redirect("indy_hub:material_exchange_buy")

            selected_stock_rows = [
                stock_by_type_id[item_data["type_id"]]
                for item_data in items_to_create
                if item_data["type_id"] in stock_by_type_id
            ]
            if (
                len(selected_stock_rows) > 1
                and not _selected_buy_stock_items_share_source_location(
                    selected_stock_rows
                )
            ):
                messages.error(
                    request,
                    _(
                        "Selected items must come from one buy location. Deselect items from other stations and try again."
                    ),
                )
                return redirect("indy_hub:material_exchange_buy")

            selected_buy_location_id, selected_buy_location_name = (
                _resolve_selected_buy_stock_source_location(
                    selected_stock_rows,
                    buy_name_map=buy_name_map,
                )
            )

            # Get order reference from client (generated in JavaScript)
            client_order_ref = request.POST.get("order_reference", "").strip()

            # Create ONE order with ALL items
            order = MaterialExchangeBuyOrder.objects.create(
                config=config,
                buyer=request.user,
                status=MaterialExchangeBuyOrder.Status.DRAFT,
                order_reference=client_order_ref if client_order_ref else None,
                source_location_id=selected_buy_location_id,
                source_location_name=selected_buy_location_name,
            )

            # Create items for this order
            for item_data in items_to_create:
                MaterialExchangeBuyOrderItem.objects.create(order=order, **item_data)

            rounded_total_cost = total_cost.quantize(
                Decimal("1"), rounding=ROUND_CEILING
            )
            order.rounded_total_price = rounded_total_cost
            order.save(update_fields=["rounded_total_price", "updated_at"])

        # Admin notifications are handled by the post_save signal + async task

        messages.success(
            request,
            _(
                f"Created buy order #{order.id} with {len(items_to_create)} item(s). Total cost: {rounded_total_cost:,.0f} ISK. Awaiting admin approval."
            ),
        )
        return redirect("indy_hub:material_exchange_index")

    # Auto-refresh stock only if stale (> 1h) or never synced; otherwise keep cache.
    # Post-deploy self-heal: if we changed stock derivation logic, trigger a one-time refresh.
    try:
        last_sync = config.last_stock_sync
        # Django
        from django.utils import timezone

        needs_refresh = (
            not last_sync or (timezone.now() - last_sync).total_seconds() > 3600
        )
    except Exception:
        needs_refresh = True

    stock_version_refresh = False
    try:
        if config.last_stock_sync:
            current_version = int(
                cache.get(me_stock_sync_cache_version_key(int(config.corporation_id)))
                or 0
            )
            stock_version_refresh = current_version < int(ME_STOCK_SYNC_CACHE_VERSION)
    except Exception:
        stock_version_refresh = False

    stock_refreshing = False
    buy_stock_progress = (
        cache.get(
            f"indy_hub:material_exchange:buy_stock_refresh:{int(config.corporation_id)}"
        )
        or {}
    )

    if request.method == "GET" and (needs_refresh or stock_version_refresh):
        # The refreshed=1 guard prevents loops, but version migrations should override it.
        if request.GET.get("refreshed") != "1" or stock_version_refresh:
            buy_stock_progress = _ensure_buy_stock_refresh_started(config)
    stock_refreshing = bool(buy_stock_progress.get("running"))

    if buy_stock_progress.get("error") == "esi_down" and not buy_stock_progress.get(
        "retry_after_minutes"
    ):
        cooldown_until = cache.get(
            me_buy_stock_esi_cooldown_key(int(config.corporation_id))
        )
        if cooldown_until:
            try:
                retry_seconds = max(
                    0, int(float(cooldown_until) - timezone.now().timestamp())
                )
            except (TypeError, ValueError):
                retry_seconds = int(ESI_DOWN_COOLDOWN_SECONDS)
            buy_stock_progress["retry_after_minutes"] = int((retry_seconds + 59) // 60)

    # GET: ensure prices are populated if stock exists without prices
    base_stock_qs = config.stock_items.filter(quantity__gt=0)
    if (
        base_stock_qs.exists()
        and not base_stock_qs.filter(jita_buy_price__gt=0).exists()
    ):
        try:
            sync_material_exchange_prices()
            config.refresh_from_db()
        except Exception as exc:  # pragma: no cover - defensive
            messages.warning(request, f"Price sync failed automatically: {exc}")

    # Show available stock.
    stock_items = list(config.stock_items.filter(quantity__gt=0))
    pre_filter_stock_count = len(stock_items)

    # Apply market group filter strictly (empty config means no allowed items)
    try:
        allowed_type_ids = _get_allowed_type_ids_for_config(config, "buy")
        if allowed_type_ids is not None:
            stock_items = [
                item for item in stock_items if item.type_id in allowed_type_ids
            ]
    except Exception as exc:
        logger.warning("Failed to apply market group filter: %s", exc)
    post_group_filter_count = len(stock_items)
    stock_items_for_meta = list(stock_items)

    stock_blueprint_variants = _get_buy_stock_blueprint_variant_map(
        config=config,
        type_ids={int(item.type_id) for item in stock_items},
    )
    displayed_type_market_group_path_map = _get_type_market_group_path_map(
        {int(item.type_id) for item in stock_items}
    )
    displayed_type_market_group_label_map = _build_type_market_group_label_map(
        displayed_type_market_group_path_map
    )

    priced_stock_items: list[MaterialExchangeStock] = []
    for stock_item in stock_items:
        blueprint_variant = str(
            stock_blueprint_variants.get(int(stock_item.type_id), "")
        ).strip().lower()
        stock_item.blueprint_variant = blueprint_variant
        stock_item.display_type_name = _format_buy_stock_type_name(
            stock_item.type_name or get_type_name(int(stock_item.type_id)),
            blueprint_variant,
        )

        icon_variant = ""
        if blueprint_variant == "bpc":
            icon_variant = "bpc"
        elif blueprint_variant in {"bpo", "mixed"}:
            icon_variant = "bpo"
        icon_url, icon_fallback_url = _build_eve_type_icon_urls(
            int(stock_item.type_id),
            blueprint_variant=icon_variant,
            is_blueprint_hint=("blueprint" in str(stock_item.type_name or "").lower()),
        )
        stock_item.icon_url = icon_url
        stock_item.icon_fallback_url = icon_fallback_url

        if blueprint_variant == "bpc":
            unit_price = Decimal("0")
            default_unit_price = Decimal("0")
            has_override = False
        else:
            (
                unit_price,
                default_unit_price,
                has_override,
            ) = _compute_effective_buy_unit_price(
                stock_item=stock_item,
                buy_override_map=buy_override_map,
                buy_market_group_override_map=buy_market_group_override_map,
                type_market_group_path_map=displayed_type_market_group_path_map,
                buy_container_override=buy_container_override,
                in_container=False,
            )
        if unit_price <= 0 and blueprint_variant != "bpc":
            continue
        stock_item.display_sell_price_to_member = unit_price
        stock_item.default_sell_price_to_member = default_unit_price
        stock_item.has_buy_price_override = bool(has_override)
        priced_stock_items.append(stock_item)
    stock_items = priced_stock_items

    group_map = _get_group_map([item.type_id for item in stock_items])
    stock_items.sort(
        key=lambda i: (
            group_map.get(i.type_id, "Other").lower(),
            (str(getattr(i, "display_type_name", "") or i.type_name or "")).lower(),
        )
    )
    reserved_quantities = _get_reserved_buy_quantities(
        config=config,
        type_ids={int(item.type_id) for item in stock_items_for_meta},
    )
    for stock_item in stock_items_for_meta:
        reserved_qty = int(reserved_quantities.get(int(stock_item.type_id), 0) or 0)
        stock_item.reserved_quantity = reserved_qty
        stock_item.available_quantity = max(int(stock_item.quantity) - reserved_qty, 0)
        stock_item.buy_location_label = _build_buy_stock_location_label(
            stock_item,
            buy_name_map=buy_name_map,
            fallback_label=buy_locations_label,
        )

    stock_rows: list[dict[str, object]] = []
    stock_meta_by_type: dict[int, dict[str, object]] = {}
    for stock_item in stock_items_for_meta:
        blueprint_variant = str(
            stock_blueprint_variants.get(int(stock_item.type_id), "")
        ).strip().lower()
        if blueprint_variant == "bpc":
            unit_price = Decimal("0")
            default_unit_price = Decimal("0")
            has_override = False
        else:
            (
                unit_price,
                default_unit_price,
                has_override,
            ) = _compute_effective_buy_unit_price(
                stock_item=stock_item,
                buy_override_map=buy_override_map,
                buy_market_group_override_map=buy_market_group_override_map,
                type_market_group_path_map=displayed_type_market_group_path_map,
                buy_container_override=buy_container_override,
                in_container=False,
            )
        stock_meta_by_type[int(stock_item.type_id)] = {
            "type_id": int(stock_item.type_id),
            "base_type_name": str(stock_item.type_name or get_type_name(int(stock_item.type_id))),
            "display_type_name": str(stock_item.display_type_name or stock_item.type_name or ""),
            "blueprint_variant": str(blueprint_variant or ""),
            "display_sell_price_to_member": unit_price,
            "default_sell_price_to_member": default_unit_price,
            "has_buy_price_override": bool(has_override),
            "jita_buy_price": Decimal(stock_item.jita_buy_price or 0),
            "jita_sell_price": Decimal(stock_item.jita_sell_price or 0),
            "quantity": int(stock_item.quantity),
            "reserved_quantity": int(stock_item.reserved_quantity),
            "available_quantity": int(stock_item.available_quantity),
            "source_structure_ids": [
                int(sid)
                for sid in (getattr(stock_item, "source_structure_ids", []) or [])
                if int(sid) > 0
            ],
            "buy_location_label": str(stock_item.buy_location_label or buy_locations_label),
        }

    try:
        scoped_buy_assets = _get_buy_location_scoped_corp_assets(config=config)
    except Exception:
        scoped_buy_assets = []
    if scoped_buy_assets:
        scoped_item_ids: set[int] = set()
        for scoped_asset in scoped_buy_assets:
            try:
                scoped_item_id = int(scoped_asset.get("item_id") or 0)
            except (TypeError, ValueError):
                scoped_item_id = 0
            if scoped_item_id > 0:
                scoped_item_ids.add(scoped_item_id)
        blueprint_details_by_item_id = _get_corp_blueprint_details_by_item_id(
            config=config,
            item_ids=scoped_item_ids,
        )
        blueprint_variant_by_item_id = {
            int(item_id): str(details.get("variant") or "").strip().lower()
            for item_id, details in blueprint_details_by_item_id.items()
            if str(details.get("variant") or "").strip().lower() in {"bpc", "bpo"}
        }
        blueprint_runs_by_item_id = {
            int(item_id): int(details.get("runs") or 0)
            for item_id, details in blueprint_details_by_item_id.items()
            if str(details.get("variant") or "").strip().lower() == "bpc"
            and int(details.get("runs") or 0) > 0
        }
        stock_rows = _build_buy_material_rows(
            scoped_assets=scoped_buy_assets,
            config=config,
            stock_meta_by_type=stock_meta_by_type,
            buy_override_map=buy_override_map,
            buy_market_group_override_map=buy_market_group_override_map,
            buy_container_override=buy_container_override,
            type_market_group_path_map=displayed_type_market_group_path_map,
            type_market_group_label_map=displayed_type_market_group_label_map,
            buy_name_map=buy_name_map,
            fallback_location_label=buy_locations_label,
            blueprint_variant_by_item_id=blueprint_variant_by_item_id,
            blueprint_runs_by_item_id=blueprint_runs_by_item_id,
        )

    if not stock_rows:
        for index, stock_item in enumerate(stock_items):
            variant_token = str(getattr(stock_item, "blueprint_variant", "") or "") or "std"
            stock_rows.append(
                {
                    "row_kind": "item",
                    "row_index": int(index),
                    "type_id": int(stock_item.type_id),
                    "display_type_name": str(
                        getattr(stock_item, "display_type_name", "")
                        or stock_item.type_name
                        or ""
                    ),
                    "blueprint_variant": str(getattr(stock_item, "blueprint_variant", "") or ""),
                    "bpc_runs": None,
                    "quantity": int(stock_item.quantity),
                    "reserved_quantity": int(stock_item.reserved_quantity),
                    "available_quantity": int(stock_item.available_quantity),
                    "display_sell_price_to_member": stock_item.display_sell_price_to_member,
                    "default_sell_price_to_member": stock_item.default_sell_price_to_member,
                    "has_buy_price_override": bool(stock_item.has_buy_price_override),
                    "icon_url": str(getattr(stock_item, "icon_url", "") or ""),
                    "icon_fallback_url": str(
                        getattr(stock_item, "icon_fallback_url", "") or ""
                    ),
                    "source_structure_ids": [
                        int(sid)
                        for sid in (getattr(stock_item, "source_structure_ids", []) or [])
                        if int(sid) > 0
                    ],
                    "buy_location_label": str(
                        getattr(stock_item, "buy_location_label", "") or buy_locations_label
                    ),
                    "market_group_name": str(
                        (
                            displayed_type_market_group_label_map.get(
                                int(stock_item.type_id),
                                {},
                            )
                        ).get("name")
                        or ""
                    ),
                    "market_group_path": str(
                        (
                            displayed_type_market_group_label_map.get(
                                int(stock_item.type_id),
                                {},
                            )
                        ).get("path")
                        or (
                            displayed_type_market_group_label_map.get(
                                int(stock_item.type_id),
                                {},
                            )
                        ).get("name")
                        or ""
                    ),
                    "depth": 0,
                    "container_path": "",
                    "indent_padding_rem": 0,
                    "form_quantity_field_name": (
                        f"qty_{int(stock_item.type_id)}_{variant_token}_root_{int(index)}"
                    ),
                }
            )

    if pre_filter_stock_count > 0 and post_group_filter_count == 0:
        messages.info(
            request,
            _(
                "Stock exists, but none of it matches the allowed Market Groups based on the current configuration."
            ),
        )

    buy_last_update = None
    try:
        candidates = [config.last_stock_sync, config.last_price_sync]
        candidates = [dt for dt in candidates if dt]
        buy_last_update = max(candidates) if candidates else None
    except Exception:
        buy_last_update = None

    try:
        div_map, _div_scope_missing = get_corp_divisions_cached(
            int(config.corporation_id), allow_refresh=False
        )
        hangar_division_label = (
            div_map.get(int(config.hangar_division)) if div_map else None
        )
    except Exception:
        hangar_division_label = None

    hangar_division_label = (
        hangar_division_label or ""
    ).strip() or f"Hangar Division {int(config.hangar_division)}"

    context = {
        "config": config,
        "stock": stock_rows,
        "buy_locations_label": buy_locations_label,
        "stock_refreshing": stock_refreshing,
        "buy_stock_progress": buy_stock_progress,
        "corp_assets_scope_missing": corp_assets_scope_missing,
        "hangar_division_label": hangar_division_label,
        "buy_last_update": buy_last_update,
        "buy_next_refresh_minutes": _minutes_until_refresh(buy_last_update),
        "nav_context": _build_nav_context(request.user),
    }

    context.update(
        build_nav_context(
            request.user,
            active_tab="material_hub",
            can_manage_corp=request.user.has_perm(
                "indy_hub.can_manage_corp_bp_requests"
            ),
        )
    )

    return render(request, "indy_hub/material_exchange/buy.html", context)


@login_required
@indy_hub_permission_required("can_manage_material_hub")
@require_http_methods(["POST"])
@tokens_required(scopes="esi-assets.read_corporation_assets.v1")
def material_exchange_sync_stock(request, tokens):
    """
    Force an immediate sync of stock from ESI corp assets.
    Updates MaterialExchangeStock and redirects back.
    """
    emit_view_analytics_event(view_name="material_exchange.sync_stock", request=request)
    if not _is_material_exchange_enabled():
        messages.warning(request, _("Buyback is disabled."))
        return redirect("indy_hub:material_exchange_index")

    if not _get_material_exchange_config():
        messages.warning(request, _("Buyback is not configured."))
        return redirect("indy_hub:material_exchange_index")

    try:
        sync_material_exchange_stock()
        config = MaterialExchangeConfig.objects.first()
        messages.success(
            request,
            _(
                f"Stock synced successfully. Last sync: {config.last_stock_sync.strftime('%Y-%m-%d %H:%M:%S') if config.last_stock_sync else 'just now'}"
            ),
        )
    except Exception as e:
        messages.error(request, _(f"Stock sync failed: {str(e)}"))

    # Redirect back to buy page or referrer
    referrer = request.headers.get("referer", "")
    if "material-exchange/buy" in referrer:
        return redirect("indy_hub:material_exchange_buy")
    elif "material-exchange/sell" in referrer:
        return redirect("indy_hub:material_exchange_sell")
    else:
        return redirect("indy_hub:material_exchange_index")


@login_required
@indy_hub_permission_required("can_manage_material_hub")
@require_http_methods(["POST"])
def material_exchange_sync_prices(request):
    """
    Force an immediate sync of Jita prices for current stock items.
    Updates MaterialExchangeStock jita_buy_price/jita_sell_price and redirects back.
    """
    emit_view_analytics_event(
        view_name="material_exchange.sync_prices", request=request
    )
    if not _is_material_exchange_enabled():
        messages.warning(request, _("Buyback is disabled."))
        return redirect("indy_hub:material_exchange_index")

    if not _get_material_exchange_config():
        messages.warning(request, _("Buyback is not configured."))
        return redirect("indy_hub:material_exchange_index")

    try:
        sync_material_exchange_prices()
        config = MaterialExchangeConfig.objects.first()
        messages.success(
            request,
            _(
                f"Prices synced successfully. Last sync: {config.last_price_sync.strftime('%Y-%m-%d %H:%M:%S') if getattr(config, 'last_price_sync', None) else 'just now'}"
            ),
        )
    except Exception as e:
        messages.error(request, _(f"Price sync failed: {str(e)}"))

    referrer = request.headers.get("referer", "")
    if "material-exchange/buy" in referrer:
        return redirect("indy_hub:material_exchange_buy")
    elif "material-exchange/sell" in referrer:
        return redirect("indy_hub:material_exchange_sell")
    else:
        return redirect("indy_hub:material_exchange_index")


@login_required
@require_http_methods(["POST"])
@login_required
@require_http_methods(["POST"])
def material_exchange_approve_sell(request, order_id):
    """Approve a sell order (member → hub)."""
    emit_view_analytics_event(
        view_name="material_exchange.approve_sell", request=request
    )
    if not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Permission denied."))
        return redirect("indy_hub:material_exchange_index")

    order = get_object_or_404(
        MaterialExchangeSellOrder,
        id=order_id,
        status=MaterialExchangeSellOrder.Status.DRAFT,
    )

    order.status = MaterialExchangeSellOrder.Status.AWAITING_VALIDATION
    order.approved_by = request.user
    order.approved_at = timezone.now()
    order.save()

    messages.success(
        request,
        _(f"Sell order #{order.id} approved. Awaiting payment verification."),
    )
    return redirect("indy_hub:material_exchange_index")


@login_required
@require_http_methods(["POST"])
def material_exchange_reject_sell(request, order_id):
    emit_view_analytics_event(
        view_name="material_exchange.reject_sell", request=request
    )
    """Reject a sell order."""
    if not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Permission denied."))
        return redirect("indy_hub:material_exchange_index")

    order = get_object_or_404(
        MaterialExchangeSellOrder,
        id=order_id,
        status__in=[
            MaterialExchangeSellOrder.Status.DRAFT,
            MaterialExchangeSellOrder.Status.AWAITING_VALIDATION,
            MaterialExchangeSellOrder.Status.ANOMALY,
            MaterialExchangeSellOrder.Status.ANOMALY_REJECTED,
            MaterialExchangeSellOrder.Status.VALIDATED,
        ],
    )
    order.status = MaterialExchangeSellOrder.Status.REJECTED
    order.save()

    messages.warning(request, _(f"Sell order #{order.id} rejected."))
    return redirect("indy_hub:material_exchange_index")


@login_required
@require_http_methods(["POST"])
def material_exchange_verify_payment_sell(request, order_id):
    emit_view_analytics_event(
        view_name="material_exchange.verify_payment_sell", request=request
    )
    """Mark sell order as completed (contract accepted in-game)."""
    if not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Permission denied."))
        return redirect("indy_hub:material_exchange_index")

    order = get_object_or_404(
        MaterialExchangeSellOrder, id=order_id, status="validated"
    )

    order.status = "completed"
    order.payment_verified_by = request.user
    order.payment_verified_at = timezone.now()
    order.save()

    messages.success(request, _(f"Sell order #{order.id} completed."))
    return redirect("indy_hub:material_exchange_index")


@login_required
@require_http_methods(["POST"])
def material_exchange_complete_sell(request, order_id):
    emit_view_analytics_event(
        view_name="material_exchange.complete_sell", request=request
    )
    """Mark sell order as fully completed and create transaction logs for each item."""
    if not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Permission denied."))
        return redirect("indy_hub:material_exchange_index")

    order = get_object_or_404(
        MaterialExchangeSellOrder, id=order_id, status="completed"
    )

    with transaction.atomic():
        order.status = "completed"
        order.save()

        # Create transaction log for each item and update stock
        added_quantities_by_type: dict[int, int] = {}
        for item in order.items.all():
            snapshot = MaterialExchangeTransaction.build_jita_snapshot(
                config=order.config,
                type_id=item.type_id,
                quantity=item.quantity,
            )
            # Create transaction log
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

            # Update stock (add quantity)
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
        except Exception:
            logger.warning(
                "Failed to add cached corp assets for manually completed sell order %s",
                order.id,
                exc_info=True,
            )

    messages.success(
        request, _(f"Sell order #{order.id} completed and transaction logged.")
    )
    return redirect("indy_hub:material_exchange_index")


@login_required
@require_http_methods(["POST"])
def material_exchange_approve_buy(request, order_id):
    emit_view_analytics_event(
        view_name="material_exchange.approve_buy", request=request
    )
    """Approve a buy order (hub → member) - Creates contract permission."""
    if not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Permission denied."))
        return redirect("indy_hub:material_exchange_index")

    order = get_object_or_404(MaterialExchangeBuyOrder, id=order_id, status="draft")

    # Re-check stock for all items
    errors = []
    for item in order.items.all():
        try:
            stock_item = order.config.stock_items.get(type_id=item.type_id)
            if stock_item.quantity < item.quantity:
                errors.append(
                    _(
                        f"{item.type_name}: insufficient stock. Available: {stock_item.quantity}, required: {item.quantity}"
                    )
                )
        except MaterialExchangeStock.DoesNotExist:
            errors.append(_(f"{item.type_name}: not in stock."))

    if errors:
        messages.error(request, _("Cannot approve: ") + "; ".join(errors))
        return redirect("indy_hub:material_exchange_index")

    order.status = "awaiting_validation"
    order.approved_by = request.user
    order.approved_at = timezone.now()
    order.save()

    messages.success(
        request,
        _(f"Buy order #{order.id} approved. Corporation will now create a contract."),
    )
    return redirect("indy_hub:material_exchange_index")


@login_required
@require_http_methods(["POST"])
def material_exchange_reject_buy(request, order_id):
    emit_view_analytics_event(view_name="material_exchange.reject_buy", request=request)
    """Reject a buy order."""
    if not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Permission denied."))
        return redirect("indy_hub:material_exchange_index")

    order = get_object_or_404(MaterialExchangeBuyOrder, id=order_id, status="draft")

    _reject_buy_order(order)

    messages.warning(request, _(f"Buy order #{order.id} rejected and buyer notified."))
    return redirect("indy_hub:material_exchange_index")


def _reject_buy_order(order: MaterialExchangeBuyOrder) -> None:
    from ..notifications import notify_user

    notify_user(
        order.buyer,
        _("❌ Buy Order Rejected"),
        _(
            f"Your buy order #{order.id} has been rejected.\n\n"
            f"Reason: Admin decision.\n\n"
            f"Contact the admins in Auth if you need details or want to retry."
        ),
        level="error",
        link=f"/indy_hub/material-exchange/my-orders/buy/{order.id}/",
    )

    order.status = "rejected"
    order.save()


@login_required
@require_http_methods(["POST"])
def material_exchange_mark_delivered_buy(request, order_id):
    emit_view_analytics_event(
        view_name="material_exchange.mark_delivered_buy", request=request
    )
    """Mark buy order as delivered."""
    if not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Permission denied."))
        return redirect("indy_hub:material_exchange_index")

    order = get_object_or_404(
        MaterialExchangeBuyOrder,
        id=order_id,
        status=MaterialExchangeBuyOrder.Status.VALIDATED,
    )
    delivery_method = request.POST.get("delivery_method", "contract")

    _complete_buy_order(
        order, delivered_by=request.user, delivery_method=delivery_method
    )

    messages.success(request, _(f"Buy order #{order.id} marked as delivered."))
    return redirect("indy_hub:material_exchange_index")


@login_required
@require_http_methods(["POST"])
def material_exchange_complete_buy(request, order_id):
    emit_view_analytics_event(
        view_name="material_exchange.complete_buy", request=request
    )
    """Mark buy order as completed and create transaction logs for each item."""
    if not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Permission denied."))
        return redirect("indy_hub:material_exchange_index")

    order = get_object_or_404(
        MaterialExchangeBuyOrder,
        id=order_id,
        status=MaterialExchangeBuyOrder.Status.VALIDATED,
    )

    _complete_buy_order(order)

    messages.success(
        request, _(f"Buy order #{order.id} completed and transaction logged.")
    )
    return redirect("indy_hub:material_exchange_index")


def _complete_buy_order(order, *, delivered_by=None, delivery_method=None):
    """Helper to finalize a buy order (auth-side manual completion)."""
    with transaction.atomic():
        if delivered_by:
            order.delivered_by = delivered_by
            order.delivered_at = timezone.now()
            order.delivery_method = delivery_method

        order.status = MaterialExchangeBuyOrder.Status.COMPLETED
        order.save()

        # Create transaction log for each item and update stock
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
        except Exception:
            logger.warning(
                "Failed to consume cached corp assets for manually completed buy order %s",
                order.id,
                exc_info=True,
            )


@login_required
@indy_hub_permission_required("can_manage_material_hub")
def material_exchange_transactions(request):
    emit_view_analytics_event(
        view_name="material_exchange.transactions", request=request
    )
    """
    Transaction history and finance reporting.
    Shows all completed transactions with filters and monthly aggregates.
    """
    if not _is_material_exchange_enabled():
        messages.warning(request, _("Buyback is disabled."))
        return redirect("indy_hub:material_exchange_index")

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, _("Buyback is not configured."))
        return redirect("indy_hub:material_exchange_index")

    # Filters
    transaction_type = str(request.GET.get("type", "") or "").strip().lower()
    if transaction_type not in {"", "sell", "buy"}:
        transaction_type = ""
    user_filter = str(request.GET.get("user", "") or "").strip()

    transactions_base_qs = config.transactions.select_related(
        "user", "sell_order", "buy_order"
    )
    if transaction_type:
        transactions_base_qs = transactions_base_qs.filter(
            transaction_type=transaction_type
        )
    if user_filter:
        transactions_base_qs = transactions_base_qs.filter(
            user__username__icontains=user_filter
        )

    grouped_rows: list[dict] = []

    if transaction_type in {"", "sell"}:
        sell_group_rows = list(
            transactions_base_qs.filter(
                transaction_type=MaterialExchangeTransaction.TransactionType.SELL,
                sell_order_id__isnull=False,
            )
            .values(
                "sell_order_id",
                "user_id",
                "user__username",
                "sell_order__order_reference",
                "sell_order__created_at",
                "sell_order__esi_contract_id",
            )
            .annotate(
                total_price=Sum("total_price", default=0),
                completed_at=Max("completed_at"),
                tx_count=Count("id"),
            )
        )
        for row in sell_group_rows:
            order_id = int(row.get("sell_order_id") or 0)
            grouped_rows.append(
                {
                    "transaction_type": MaterialExchangeTransaction.TransactionType.SELL,
                    "order_reference": str(row.get("sell_order__order_reference") or "")
                    or f"SELL-{order_id}",
                    "user_id": int(row.get("user_id") or 0),
                    "username": str(row.get("user__username") or ""),
                    "created_at": row.get("sell_order__created_at"),
                    "completed_at": row.get("completed_at"),
                    "contract_id": int(row.get("sell_order__esi_contract_id") or 0),
                    "tx_count": int(row.get("tx_count") or 0),
                    "total_price": Decimal(str(row.get("total_price") or 0)),
                }
            )

    if transaction_type in {"", "buy"}:
        buy_group_rows = list(
            transactions_base_qs.filter(
                transaction_type=MaterialExchangeTransaction.TransactionType.BUY,
                buy_order_id__isnull=False,
            )
            .values(
                "buy_order_id",
                "user_id",
                "user__username",
                "buy_order__order_reference",
                "buy_order__created_at",
                "buy_order__esi_contract_id",
            )
            .annotate(
                total_price=Sum("total_price", default=0),
                completed_at=Max("completed_at"),
                tx_count=Count("id"),
            )
        )
        for row in buy_group_rows:
            order_id = int(row.get("buy_order_id") or 0)
            grouped_rows.append(
                {
                    "transaction_type": MaterialExchangeTransaction.TransactionType.BUY,
                    "order_reference": str(row.get("buy_order__order_reference") or "")
                    or f"BUY-{order_id}",
                    "user_id": int(row.get("user_id") or 0),
                    "username": str(row.get("user__username") or ""),
                    "created_at": row.get("buy_order__created_at"),
                    "completed_at": row.get("completed_at"),
                    "contract_id": int(row.get("buy_order__esi_contract_id") or 0),
                    "tx_count": int(row.get("tx_count") or 0),
                    "total_price": Decimal(str(row.get("total_price") or 0)),
                }
            )

    contract_ids = {
        int(row.get("contract_id") or 0)
        for row in grouped_rows
        if int(row.get("contract_id") or 0) > 0
    }
    contract_accepted_map = {
        int(contract_id): accepted_at
        for contract_id, accepted_at in ESIContract.objects.filter(
            contract_id__in=list(contract_ids)
        ).values_list("contract_id", "date_accepted")
    }

    grouped_user_ids = {
        int(row.get("user_id") or 0)
        for row in grouped_rows
        if int(row.get("user_id") or 0) > 0
    }
    users_by_id = {
        int(user.id): user for user in User.objects.filter(id__in=list(grouped_user_ids))
    }

    for row in grouped_rows:
        user = users_by_id.get(int(row.get("user_id") or 0))
        main_character = (
            _resolve_main_character_name(user) if user else str(row.get("username") or "")
        )
        row["who"] = main_character or str(row.get("username") or "")
        if row["transaction_type"] == MaterialExchangeTransaction.TransactionType.SELL:
            row["party_from"] = row["who"]
            row["party_to"] = "Hub"
        else:
            row["party_from"] = "Hub"
            row["party_to"] = row["who"]
        accepted_at = contract_accepted_map.get(int(row.get("contract_id") or 0))
        row["accepted_at"] = accepted_at
        created_at = row.get("created_at")
        if created_at and accepted_at and accepted_at >= created_at:
            row["acceptance_duration_display"] = _format_duration_short(
                accepted_at - created_at
            )
        else:
            row["acceptance_duration_display"] = "-"

    grouped_rows.sort(
        key=lambda item: item.get("completed_at")
        or item.get("created_at")
        or timezone.now(),
        reverse=True,
    )

    # Pagination (order-level rows)
    paginator = Paginator(grouped_rows, 50)
    page_number = request.GET.get("page", 1)
    page_obj = paginator.get_page(page_number)
    transactions = list(page_obj.object_list)

    top_user_rollup: dict[int, dict[str, object]] = {}
    for row in grouped_rows:
        user_id = int(row.get("user_id") or 0)
        if user_id <= 0:
            continue
        bucket = top_user_rollup.setdefault(
            user_id,
            {
                "username": str(row.get("username") or f"User {user_id}"),
                "who": str(row.get("who") or row.get("username") or f"User {user_id}"),
                "orders": 0,
                "buy_total": Decimal("0"),
                "sell_total": Decimal("0"),
            },
        )
        bucket["orders"] = int(bucket.get("orders") or 0) + 1
        total_price = Decimal(str(row.get("total_price") or 0))
        if row.get("transaction_type") == MaterialExchangeTransaction.TransactionType.BUY:
            bucket["buy_total"] = Decimal(str(bucket.get("buy_total") or 0)) + total_price
        else:
            bucket["sell_total"] = Decimal(str(bucket.get("sell_total") or 0)) + total_price

    top_users = []
    for _user_id, bucket in top_user_rollup.items():
        buy_total = Decimal(str(bucket.get("buy_total") or 0))
        sell_total = Decimal(str(bucket.get("sell_total") or 0))
        top_users.append(
            {
                "who": str(bucket.get("who") or bucket.get("username") or ""),
                "orders": int(bucket.get("orders") or 0),
                "buy_total": buy_total,
                "sell_total": sell_total,
                "net_flow": buy_total - sell_total,
                "total_volume": buy_total + sell_total,
            }
        )
    top_users.sort(key=lambda row: Decimal(str(row.get("total_volume") or 0)), reverse=True)
    top_users = top_users[:10]

    # Aggregates for current month
    now = timezone.now()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    month_stats = config.transactions.filter(completed_at__gte=month_start).aggregate(
        total_sell_volume=Sum(
            "total_price", filter=Q(transaction_type="sell"), default=0
        ),
        total_buy_volume=Sum(
            "total_price", filter=Q(transaction_type="buy"), default=0
        ),
        sell_count=Count("id", filter=Q(transaction_type="sell")),
        buy_count=Count("id", filter=Q(transaction_type="buy")),
    )

    context = {
        "config": config,
        "page_obj": page_obj,
        "transactions": transactions,
        "top_users": top_users,
        "is_paginated": page_obj.has_other_pages(),
        "transaction_type": transaction_type,
        "user_filter": user_filter,
        "month_stats": month_stats,
        "nav_context": _build_nav_context(request.user),
    }

    context.update(
        build_nav_context(
            request.user,
            active_tab="material_hub",
            can_manage_corp=request.user.has_perm(
                "indy_hub.can_manage_corp_bp_requests"
            ),
        )
    )

    return render(request, "indy_hub/material_exchange/transactions.html", context)


@login_required
@indy_hub_permission_required("can_manage_material_hub")
def material_exchange_stats_history(request):
    emit_view_analytics_event(
        view_name="material_exchange.stats_history", request=request
    )
    """Buyback stats based on all non-capital orders."""
    if not _is_material_exchange_enabled():
        messages.warning(request, _("Buyback is disabled."))
        return redirect("indy_hub:material_exchange_index")

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, _("Buyback is not configured."))
        return redirect("indy_hub:material_exchange_index")

    settings_obj = MaterialExchangeSettings.get_solo()

    period_options = [
        ("1m", _("This month")),
        ("3m", _("Last 3 months")),
        ("6m", _("Last 6 months")),
        ("12m", _("Last 12 months")),
        ("24m", _("Last 24 months")),
        ("all", _("All time")),
    ]
    period_months_map = {"1m": 1, "3m": 3, "6m": 6, "12m": 12, "24m": 24}
    selected_period = request.GET.get("period", "all")
    if selected_period not in {key for key, _ in period_options}:
        selected_period = "all"

    start_date_raw = str(request.GET.get("start_date") or "").strip()
    end_date_raw = str(request.GET.get("end_date") or "").strip()

    def _parse_date(raw_value: str):
        try:
            return datetime.strptime(raw_value, "%Y-%m-%d").date()
        except ValueError:
            return None

    custom_start_date = _parse_date(start_date_raw) if start_date_raw else None
    custom_end_date = _parse_date(end_date_raw) if end_date_raw else None
    if start_date_raw and custom_start_date is None:
        messages.warning(request, _("Invalid start date; expected YYYY-MM-DD."))
    if end_date_raw and custom_end_date is None:
        messages.warning(request, _("Invalid end date; expected YYYY-MM-DD."))
    if custom_start_date and custom_end_date and custom_start_date > custom_end_date:
        custom_start_date, custom_end_date = custom_end_date, custom_start_date

    def _to_decimal(raw_value) -> Decimal:
        try:
            return Decimal(str(raw_value or 0))
        except Exception:
            return Decimal("0")

    def _to_datetime(raw_value):
        if isinstance(raw_value, datetime):
            parsed = raw_value
        elif isinstance(raw_value, str):
            candidate = raw_value.strip()
            if not candidate:
                return None
            if candidate.endswith("Z"):
                candidate = f"{candidate[:-1]}+00:00"
            try:
                parsed = datetime.fromisoformat(candidate)
            except ValueError:
                return None
        else:
            return None

        if timezone.is_naive(parsed):
            try:
                parsed = timezone.make_aware(parsed)
            except Exception:
                return None
        return parsed

    available_corp_ids = sorted(
        {
            int(corp_id)
            for corp_id in MaterialExchangeConfig.objects.values_list(
                "corporation_id", flat=True
            )
            if int(corp_id or 0) > 0
        }
    )
    if int(config.corporation_id or 0) > 0:
        available_corp_ids.append(int(config.corporation_id))
    if int(getattr(settings_obj, "stats_selected_corporation_id", 0) or 0) > 0:
        available_corp_ids.append(int(settings_obj.stats_selected_corporation_id))
    available_corp_ids = sorted(set(available_corp_ids))

    chosen_corporation_id = int(
        getattr(settings_obj, "stats_selected_corporation_id", 0)
        or int(config.corporation_id)
        or 0
    )
    if chosen_corporation_id <= 0 and available_corp_ids:
        chosen_corporation_id = int(available_corp_ids[0])

    corp_config_divisions = sorted(
        {
            int(div)
            for div in MaterialExchangeConfig.objects.filter(
                corporation_id=int(chosen_corporation_id or 0)
            ).values_list("hangar_division", flat=True)
            if int(div or 0) in range(1, 8)
        }
    )
    saved_wallet_division = int(
        getattr(settings_obj, "stats_selected_wallet_division", 0) or 0
    )
    if saved_wallet_division in range(1, 8):
        chosen_wallet_division = int(saved_wallet_division)
    elif corp_config_divisions:
        chosen_wallet_division = int(corp_config_divisions[0])
    else:
        chosen_wallet_division = 1

    if request.method == "POST" and request.POST.get("action") == "save_stats_preferences":
        chosen_corporation_raw = str(
            request.POST.get("chosen_corporation_id") or ""
        ).strip()
        chosen_wallet_raw = str(
            request.POST.get("chosen_wallet_division") or ""
        ).strip()

        try:
            post_corp_id = int(chosen_corporation_raw)
        except (TypeError, ValueError):
            post_corp_id = 0
        try:
            post_wallet_division = int(chosen_wallet_raw)
        except (TypeError, ValueError):
            post_wallet_division = 0

        if post_corp_id <= 0:
            messages.error(request, _("Choose a corporation for Buyback stats."))
        elif post_wallet_division not in range(1, 8):
            messages.error(request, _("Choose wallet division 1-7."))
        else:
            settings_obj.stats_selected_corporation_id = int(post_corp_id)
            settings_obj.stats_selected_wallet_division = int(post_wallet_division)
            settings_obj.save(
                update_fields=[
                    "stats_selected_corporation_id",
                    "stats_selected_wallet_division",
                    "updated_at",
                ]
            )
            messages.success(request, _("Buyback stats preferences saved."))
        return redirect("indy_hub:material_exchange_stats_history")

    corp_options = [
        {
            "id": int(corp_id),
            "name": get_corporation_name(int(corp_id)) or str(corp_id),
        }
        for corp_id in available_corp_ids
    ]

    division_scope_missing = False
    try:
        wallet_division_names, division_scope_missing = get_corp_wallet_divisions_cached(
            int(chosen_corporation_id),
            allow_refresh=False,
        )
    except Exception:
        wallet_division_names = {}

    corptools_division_balance = Decimal("0")
    if (
        CorporationWalletDivision is not None
        and int(chosen_corporation_id or 0) > 0
    ):
        try:
            corptools_division_rows = list(
                CorporationWalletDivision.objects.filter(
                    corporation__corporation__corporation_id=int(chosen_corporation_id),
                ).values("division", "name", "balance")
            )
            if corptools_division_rows:
                division_scope_missing = False
                for row in corptools_division_rows:
                    try:
                        division_idx = int(row.get("division") or 0)
                    except (TypeError, ValueError):
                        division_idx = 0
                    if division_idx not in range(1, 8):
                        continue
                    division_name = str(row.get("name") or "").strip()
                    if division_name:
                        wallet_division_names[division_idx] = division_name
                    if division_idx == int(chosen_wallet_division):
                        corptools_division_balance = _to_decimal(row.get("balance"))
        except Exception as exc:
            logger.warning(
                "Failed to read Corptools wallet divisions for corp %s: %s",
                chosen_corporation_id,
                exc,
            )

    wallet_division_options = [
        {
            "id": idx,
            "name": str(wallet_division_names.get(idx) or f"Wallet Division {idx}"),
        }
        for idx in range(1, 8)
    ]

    period_start = None
    filter_start = None
    filter_end = None
    if custom_start_date or custom_end_date:
        if custom_start_date:
            filter_start = timezone.make_aware(
                datetime.combine(custom_start_date, time.min)
            )
        if custom_end_date:
            filter_end = timezone.make_aware(
                datetime.combine(custom_end_date, time.max)
            )
    elif selected_period in period_months_map:
        months = period_months_map[selected_period]
        month_anchor = timezone.now().replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        month_index = (month_anchor.year * 12 + month_anchor.month - 1) - (months - 1)
        start_year = month_index // 12
        start_month = (month_index % 12) + 1
        period_start = month_anchor.replace(year=start_year, month=start_month)
        filter_start = period_start

    corp_config_ids = list(
        MaterialExchangeConfig.objects.filter(
            corporation_id=int(chosen_corporation_id or 0),
        ).values_list("id", flat=True)
    )
    selected_config_ids = list(corp_config_ids)
    stats_scope_mode = "corp"
    stats_scope_note = ""
    if not selected_config_ids:
        stats_scope_mode = "empty"
        stats_scope_note = _(
            "No Buyback configs found for this corporation yet."
        )
    buy_orders_qs = MaterialExchangeBuyOrder.objects.filter(
        config_id__in=selected_config_ids
    )
    sell_orders_qs = MaterialExchangeSellOrder.objects.filter(
        config_id__in=selected_config_ids
    )
    if filter_start:
        buy_orders_qs = buy_orders_qs.filter(created_at__gte=filter_start)
        sell_orders_qs = sell_orders_qs.filter(created_at__gte=filter_start)
    if filter_end:
        buy_orders_qs = buy_orders_qs.filter(created_at__lte=filter_end)
        sell_orders_qs = sell_orders_qs.filter(created_at__lte=filter_end)

    sell_rows = list(
        sell_orders_qs.annotate(
            expected_total=Sum("items__total_price", default=0),
            expected_qty=Sum("items__quantity", default=0),
        ).values(
            "id",
            "seller_id",
            "status",
            "esi_contract_id",
            "order_reference",
            "created_at",
            "expected_total",
            "expected_qty",
        )
    )
    buy_rows = list(
        buy_orders_qs.annotate(
            expected_total=Sum("items__total_price", default=0),
            expected_qty=Sum("items__quantity", default=0),
        ).values(
            "id",
            "buyer_id",
            "status",
            "esi_contract_id",
            "order_reference",
            "created_at",
            "expected_total",
            "expected_qty",
        )
    )

    for row in sell_rows:
        row["expected_total"] = _to_decimal(row.get("expected_total"))
        row["expected_qty"] = int(row.get("expected_qty") or 0)
    for row in buy_rows:
        row["expected_total"] = _to_decimal(row.get("expected_total"))
        row["expected_qty"] = int(row.get("expected_qty") or 0)

    sell_order_ids = [int(row["id"]) for row in sell_rows]
    buy_order_ids = [int(row["id"]) for row in buy_rows]

    all_contract_ids = {
        int(row["esi_contract_id"])
        for row in (sell_rows + buy_rows)
        if int(row.get("esi_contract_id") or 0) > 0
    }

    wallet_scope_missing = False
    market_scope_missing = False
    wallet_activity_error = ""
    wallet_journal_rows: list[dict] = []
    wallet_market_orders_rows: list[dict] = []
    wallet_market_monthly_map: dict[str, int] = {}
    wallet_data_source = "esi"

    wallet_market_ref_types = {
        "market_transaction",
        "market_escrow",
        "broker_fee",
        "brokers_fee",
        "transaction_tax",
        "market_provider_tax",
        "market_fine_paid",
    }
    wallet_donation_ref_types = {
        "player_donation",
        "corporation_donation",
    }
    wallet_withdrawal_ref_types = {
        "corporation_account_withdrawal",
    }

    if int(chosen_corporation_id or 0) > 0:
        if (
            CorporationWalletJournalEntry is not None
            and CorporationMarketOrder is not None
        ):
            wallet_data_source = "corptools"
            try:
                wallet_journal_rows = list(
                    CorporationWalletJournalEntry.objects.filter(
                        division__corporation__corporation__corporation_id=int(
                            chosen_corporation_id
                        ),
                        division__division=int(chosen_wallet_division),
                    ).values(
                        "date",
                        "ref_type",
                        "amount",
                        "context_id",
                    )
                )
            except Exception as exc:
                wallet_activity_error = str(exc)
                logger.warning(
                    "Failed to read Corptools wallet journal for corp %s division %s: %s",
                    chosen_corporation_id,
                    chosen_wallet_division,
                    exc,
                )

            try:
                wallet_market_orders_rows = [
                    {
                        "wallet_division": int(chosen_wallet_division),
                        "is_buy_order": row.get("is_buy_order"),
                        "volume_remain": row.get("volume_remain"),
                        "volume_total": row.get("volume_total"),
                        "price": row.get("price"),
                    }
                    for row in CorporationMarketOrder.objects.filter(
                        wallet_division__corporation__corporation__corporation_id=int(
                            chosen_corporation_id
                        ),
                        wallet_division__division=int(chosen_wallet_division),
                        state="active",
                    ).values(
                        "is_buy_order",
                        "volume_remain",
                        "volume_total",
                        "price",
                    )
                ]
            except Exception as exc:
                wallet_activity_error = wallet_activity_error or str(exc)
                logger.warning(
                    "Failed to read Corptools market orders for corp %s division %s: %s",
                    chosen_corporation_id,
                    chosen_wallet_division,
                    exc,
                )
        else:
            wallet_data_source = "esi"
            try:
                wallet_character_id = _get_character_for_scope(
                    int(chosen_corporation_id),
                    "esi-wallet.read_corporation_wallets.v1",
                )
                wallet_journal_rows = shared_client.fetch_corporation_wallet_journal(
                    int(chosen_corporation_id),
                    division=int(chosen_wallet_division),
                    character_id=int(wallet_character_id),
                )
            except ESITokenError:
                wallet_scope_missing = True
            except (ESIClientError, Exception) as exc:
                wallet_activity_error = str(exc)
                logger.warning(
                    "Failed to fetch wallet journal for corp %s division %s: %s",
                    chosen_corporation_id,
                    chosen_wallet_division,
                    exc,
                )

            try:
                market_character_id = _get_character_for_scope(
                    int(chosen_corporation_id),
                    "esi-markets.read_corporation_orders.v1",
                )
                wallet_market_orders_rows = shared_client.fetch_corporation_orders(
                    int(chosen_corporation_id),
                    character_id=int(market_character_id),
                )
            except ESITokenError:
                market_scope_missing = True
            except (ESIClientError, Exception) as exc:
                wallet_activity_error = wallet_activity_error or str(exc)
                logger.warning(
                    "Failed to fetch market orders for corp %s: %s",
                    chosen_corporation_id,
                    exc,
                )

    wallet_activity_count = 0
    wallet_inflow_total = Decimal("0")
    wallet_outflow_total = Decimal("0")
    wallet_market_activity_count = 0
    wallet_market_activity_total = Decimal("0")
    wallet_market_transaction_count = 0
    wallet_market_transaction_total = Decimal("0")
    wallet_market_escrow_count = 0
    wallet_market_escrow_total = Decimal("0")
    wallet_fee_count = 0
    wallet_fee_total = Decimal("0")
    wallet_donation_count = 0
    wallet_donation_total = Decimal("0")
    wallet_withdrawal_count = 0
    wallet_withdrawal_total = Decimal("0")
    me_contract_wallet_entry_count = 0
    me_contract_wallet_amount_total = Decimal("0")
    wallet_first_posted_at = None
    wallet_last_posted_at = None
    wallet_ref_type_rollup: dict[str, dict[str, Decimal | int]] = {}

    for row in wallet_journal_rows:
        posted_at = _to_datetime(row.get("date"))
        if filter_start and (not posted_at or posted_at < filter_start):
            continue
        if filter_end and (not posted_at or posted_at > filter_end):
            continue

        ref_type = str(row.get("ref_type") or "").strip().lower() or "unknown"
        amount = _to_decimal(row.get("amount"))
        wallet_activity_count += 1
        if posted_at:
            if wallet_first_posted_at is None or posted_at < wallet_first_posted_at:
                wallet_first_posted_at = posted_at
            if wallet_last_posted_at is None or posted_at > wallet_last_posted_at:
                wallet_last_posted_at = posted_at
        if amount >= 0:
            wallet_inflow_total += amount
        else:
            wallet_outflow_total += abs(amount)

        rollup = wallet_ref_type_rollup.setdefault(
            ref_type,
            {"count": 0, "amount": Decimal("0")},
        )
        rollup["count"] = int(rollup["count"]) + 1
        rollup["amount"] = _to_decimal(rollup["amount"]) + amount

        if ref_type in wallet_market_ref_types:
            wallet_market_activity_count += 1
            wallet_market_activity_total += amount
            if posted_at:
                month_key = posted_at.strftime("%Y-%m")
                wallet_market_monthly_map[month_key] = (
                    int(wallet_market_monthly_map.get(month_key, 0)) + 1
                )
            if ref_type == "market_transaction":
                wallet_market_transaction_count += 1
                wallet_market_transaction_total += amount
            elif ref_type == "market_escrow":
                wallet_market_escrow_count += 1
                wallet_market_escrow_total += amount
            elif ref_type in {
                "broker_fee",
                "brokers_fee",
                "transaction_tax",
                "market_provider_tax",
                "market_fine_paid",
            }:
                wallet_fee_count += 1
                wallet_fee_total += amount

        if ref_type in wallet_donation_ref_types:
            wallet_donation_count += 1
            wallet_donation_total += amount

        if ref_type in wallet_withdrawal_ref_types:
            wallet_withdrawal_count += 1
            wallet_withdrawal_total += amount

        try:
            context_id = int(row.get("context_id") or 0)
        except (TypeError, ValueError):
            context_id = 0
        if context_id > 0 and context_id in all_contract_ids:
            me_contract_wallet_entry_count += 1
            me_contract_wallet_amount_total += amount

    wallet_ref_type_rows = [
        {
            "ref_type": str(ref_type),
            "count": int(data.get("count") or 0),
            "amount": _to_decimal(data.get("amount")),
        }
        for ref_type, data in wallet_ref_type_rollup.items()
    ]
    wallet_ref_type_rows.sort(
        key=lambda item: (
            int(item["count"]),
            abs(_to_decimal(item["amount"])),
        ),
        reverse=True,
    )
    wallet_ref_type_rows = wallet_ref_type_rows[:10]

    wallet_open_buy_orders = 0
    wallet_open_sell_orders = 0
    wallet_open_order_value = Decimal("0")
    for row in wallet_market_orders_rows:
        try:
            order_division = int(row.get("wallet_division") or 0)
        except (TypeError, ValueError):
            order_division = 0
        if order_division != int(chosen_wallet_division):
            continue

        is_buy_raw = row.get("is_buy_order")
        is_buy = bool(is_buy_raw) if isinstance(is_buy_raw, bool) else str(
            is_buy_raw
        ).strip().lower() in {"1", "true", "yes"}
        try:
            remaining_qty = int(row.get("volume_remain") or row.get("volume_total") or 0)
        except (TypeError, ValueError):
            remaining_qty = 0
        remaining_qty = max(remaining_qty, 0)
        open_value = (_to_decimal(row.get("price")) * Decimal(str(remaining_qty))).quantize(
            Decimal("0.01")
        )
        wallet_open_order_value += open_value
        if is_buy:
            wallet_open_buy_orders += 1
        else:
            wallet_open_sell_orders += 1

    wallet_supplemental_total = (
        wallet_donation_total
        + wallet_withdrawal_total
        + wallet_fee_total
        + wallet_market_transaction_total
        + wallet_market_escrow_total
    )
    wallet_net_total = wallet_inflow_total - wallet_outflow_total
    wallet_analysis_window_start = filter_start or wallet_first_posted_at
    wallet_analysis_window_end = filter_end or timezone.now()
    if (
        wallet_analysis_window_start is not None
        and wallet_analysis_window_end is not None
        and wallet_analysis_window_end < wallet_analysis_window_start
    ):
        wallet_analysis_window_end = wallet_analysis_window_start
    wallet_analysis_window_days = (
        max((wallet_analysis_window_end - wallet_analysis_window_start).days + 1, 1)
        if wallet_analysis_window_start and wallet_analysis_window_end
        else 0
    )
    wallet_daily_net = (
        (wallet_net_total / Decimal(str(wallet_analysis_window_days))).quantize(
            Decimal("0.01")
        )
        if wallet_analysis_window_days > 0
        else Decimal("0")
    )
    wallet_net_projected_30d = (wallet_daily_net * Decimal("30")).quantize(
        Decimal("0.01")
    )

    market_trend_source_label = _("Wallet Market Activity")
    if not wallet_market_monthly_map:
        market_trend_source_label = _("Market Orders (Buyback Proxy)")

    contract_meta_map = {
        int(contract_id): {
            "price": _to_decimal(price),
            "date_accepted": date_accepted,
        }
        for contract_id, price, date_accepted in ESIContract.objects.filter(
            contract_id__in=list(all_contract_ids),
        ).values_list("contract_id", "price", "date_accepted")
    }
    contract_price_map = {
        int(contract_id): _to_decimal(meta.get("price"))
        for contract_id, meta in contract_meta_map.items()
    }
    contract_accepted_at_map = {
        int(contract_id): meta.get("date_accepted")
        for contract_id, meta in contract_meta_map.items()
    }

    sell_expected_cost_total = sum(
        (row["expected_total"] for row in sell_rows),
        Decimal("0"),
    )
    buy_expected_total = sum(
        (row["expected_total"] for row in buy_rows),
        Decimal("0"),
    )

    sell_actual_cost_total = Decimal("0")
    sell_actual_count = 0
    for row in sell_rows:
        cid = int(row.get("esi_contract_id") or 0)
        if cid <= 0 or cid not in contract_price_map:
            continue
        sell_actual_cost_total += contract_price_map[cid]
        sell_actual_count += 1

    buy_actual_revenue_total = Decimal("0")
    buy_actual_count = 0
    for row in buy_rows:
        cid = int(row.get("esi_contract_id") or 0)
        if cid <= 0 or cid not in contract_price_map:
            continue
        buy_actual_revenue_total += contract_price_map[cid]
        buy_actual_count += 1

    sell_contract_coverage_pct = round(
        (sell_actual_count / len(sell_rows)) * 100,
        1,
    ) if sell_rows else 0
    buy_contract_coverage_pct = round(
        (buy_actual_count / len(buy_rows)) * 100,
        1,
    ) if buy_rows else 0

    sell_tx_qs = MaterialExchangeTransaction.objects.filter(
        config_id__in=selected_config_ids,
        sell_order_id__in=sell_order_ids,
    )
    buy_tx_qs = MaterialExchangeTransaction.objects.filter(
        config_id__in=selected_config_ids,
        buy_order_id__in=buy_order_ids,
    )

    sell_snapshot_rollup = sell_tx_qs.aggregate(
        expected_jita_buy=Sum("jita_buy_total_value_snapshot", default=0),
        expected_jita_sell=Sum("jita_sell_total_value_snapshot", default=0),
        expected_jita_split=Sum("jita_split_total_value_snapshot", default=0),
        snapshot_count=Count("id", filter=Q(jita_sell_total_value_snapshot__isnull=False)),
        total_count=Count("id"),
    )
    buy_snapshot_rollup = buy_tx_qs.aggregate(
        expected_jita_buy=Sum("jita_buy_total_value_snapshot", default=0),
        expected_jita_sell=Sum("jita_sell_total_value_snapshot", default=0),
        expected_jita_split=Sum("jita_split_total_value_snapshot", default=0),
        snapshot_count=Count("id", filter=Q(jita_sell_total_value_snapshot__isnull=False)),
        total_count=Count("id"),
    )

    sell_expected_jita_buy_total = _to_decimal(sell_snapshot_rollup["expected_jita_buy"])
    sell_expected_jita_sell_total = _to_decimal(
        sell_snapshot_rollup["expected_jita_sell"]
    )
    sell_expected_jita_split_total = _to_decimal(
        sell_snapshot_rollup["expected_jita_split"]
    )
    buy_expected_jita_buy_total = _to_decimal(buy_snapshot_rollup["expected_jita_buy"])
    buy_expected_jita_sell_total = _to_decimal(
        buy_snapshot_rollup["expected_jita_sell"]
    )
    buy_expected_jita_split_total = _to_decimal(
        buy_snapshot_rollup["expected_jita_split"]
    )

    buy_snapshot_count = int(buy_snapshot_rollup["snapshot_count"] or 0)
    buy_snapshot_total_count = int(buy_snapshot_rollup["total_count"] or 0)
    snapshot_coverage_pct = (
        round((buy_snapshot_count / buy_snapshot_total_count) * 100, 1)
        if buy_snapshot_total_count
        else 0
    )

    tx_rows = MaterialExchangeTransaction.objects.filter(
        config_id__in=selected_config_ids
    ).filter(
        Q(sell_order_id__in=sell_order_ids) | Q(buy_order_id__in=buy_order_ids)
    )
    if filter_start:
        tx_rows = tx_rows.filter(completed_at__gte=filter_start)
    if filter_end:
        tx_rows = tx_rows.filter(completed_at__lte=filter_end)

    tx_time_rollup = tx_rows.aggregate(
        first_completed=Min("completed_at"),
        last_completed=Max("completed_at"),
    )

    tx_buy_total = _to_decimal(
        tx_rows.filter(
            transaction_type=MaterialExchangeTransaction.TransactionType.BUY
        ).aggregate(total=Sum("total_price", default=0))["total"]
    )
    tx_sell_total = _to_decimal(
        tx_rows.filter(
            transaction_type=MaterialExchangeTransaction.TransactionType.SELL
        ).aggregate(total=Sum("total_price", default=0))["total"]
    )
    contract_transaction_count = int(tx_rows.count())

    sell_tx_rollup_by_order = {
        int(row["sell_order_id"]): {
            "total_value": _to_decimal(row.get("total_value")),
            "completed_at": row.get("completed_at"),
        }
        for row in tx_rows.filter(sell_order_id__isnull=False)
        .values("sell_order_id")
        .annotate(
            total_value=Sum("total_price", default=0),
            completed_at=Max("completed_at"),
        )
        if int(row.get("sell_order_id") or 0) > 0
    }
    buy_tx_rollup_by_order = {
        int(row["buy_order_id"]): {
            "total_value": _to_decimal(row.get("total_value")),
            "completed_at": row.get("completed_at"),
        }
        for row in tx_rows.filter(buy_order_id__isnull=False)
        .values("buy_order_id")
        .annotate(
            total_value=Sum("total_price", default=0),
            completed_at=Max("completed_at"),
        )
        if int(row.get("buy_order_id") or 0) > 0
    }
    sell_tx_totals_by_order = {
        int(order_id): _to_decimal((rollup or {}).get("total_value"))
        for order_id, rollup in sell_tx_rollup_by_order.items()
    }
    buy_tx_totals_by_order = {
        int(order_id): _to_decimal((rollup or {}).get("total_value"))
        for order_id, rollup in buy_tx_rollup_by_order.items()
    }

    # Use real contract prices first, and fallback to transaction totals for rows
    # missing a matched contract price.
    total_sell_volume = Decimal("0")
    sell_orders_with_actual = 0
    for row in sell_rows:
        order_id = int(row.get("id") or 0)
        cid = int(row.get("esi_contract_id") or 0)
        if cid > 0 and cid in contract_price_map:
            total_sell_volume += _to_decimal(contract_price_map.get(cid))
            sell_orders_with_actual += 1
            continue
        fallback_total = _to_decimal(sell_tx_totals_by_order.get(order_id))
        if fallback_total != 0:
            total_sell_volume += fallback_total
            sell_orders_with_actual += 1

    total_buy_volume = Decimal("0")
    buy_orders_with_actual = 0
    for row in buy_rows:
        order_id = int(row.get("id") or 0)
        cid = int(row.get("esi_contract_id") or 0)
        if cid > 0 and cid in contract_price_map:
            total_buy_volume += _to_decimal(contract_price_map.get(cid))
            buy_orders_with_actual += 1
            continue
        fallback_total = _to_decimal(buy_tx_totals_by_order.get(order_id))
        if fallback_total != 0:
            total_buy_volume += fallback_total
            buy_orders_with_actual += 1

    total_transactions = max(
        contract_transaction_count,
        sell_orders_with_actual + buy_orders_with_actual,
    )
    actual_exchange_profit = total_buy_volume - total_sell_volume
    has_actual_contract_activity = (
        total_buy_volume > 0
        or total_sell_volume > 0
        or tx_buy_total > 0
        or tx_sell_total > 0
    )
    wallet_supplemental_applied = (
        wallet_supplemental_total if has_actual_contract_activity else Decimal("0")
    )
    actual_exchange_profit_with_wallet = (
        actual_exchange_profit + wallet_supplemental_applied
    )
    sell_cost_delta = total_sell_volume - sell_expected_cost_total
    buy_revenue_delta_jita_sell = total_buy_volume - buy_expected_jita_sell_total
    buy_revenue_delta_jita_buy = total_buy_volume - buy_expected_jita_buy_total

    member_sales_volume = total_buy_volume
    jita_buy_value = buy_expected_jita_buy_total
    jita_sell_value = buy_expected_jita_sell_total
    jita_split_value = buy_expected_jita_split_total

    type_rollup: dict[int, dict[str, Decimal]] = {}
    for row in tx_rows.values(
        "transaction_type",
        "type_id",
        "quantity",
        "total_price",
        "jita_buy_total_value_snapshot",
        "jita_sell_total_value_snapshot",
        "jita_split_total_value_snapshot",
    ):
        try:
            type_id = int(row.get("type_id") or 0)
        except (TypeError, ValueError):
            continue
        if type_id <= 0:
            continue

        data = type_rollup.setdefault(
            type_id,
            {
                "acquired_qty": Decimal("0"),
                "acquired_cost": Decimal("0"),
                "sold_qty": Decimal("0"),
                "sold_revenue": Decimal("0"),
                "sold_jita_buy": Decimal("0"),
                "sold_jita_sell": Decimal("0"),
                "sold_jita_split": Decimal("0"),
            },
        )

        qty = Decimal(str(max(int(row.get("quantity") or 0), 0)))
        total_value = Decimal(str(row.get("total_price") or 0))
        tx_type = str(row.get("transaction_type") or "")

        if tx_type == MaterialExchangeTransaction.TransactionType.SELL:
            data["acquired_qty"] += qty
            data["acquired_cost"] += total_value
        elif tx_type == MaterialExchangeTransaction.TransactionType.BUY:
            data["sold_qty"] += qty
            data["sold_revenue"] += total_value
            data["sold_jita_buy"] += Decimal(str(row.get("jita_buy_total_value_snapshot") or 0))
            data["sold_jita_sell"] += Decimal(str(row.get("jita_sell_total_value_snapshot") or 0))
            data["sold_jita_split"] += Decimal(str(row.get("jita_split_total_value_snapshot") or 0))

    unrealized_inventory_value = Decimal("0")
    unrealized_inventory_cost_basis = Decimal("0")
    unrealized_earnings_potential = Decimal("0")
    if type_rollup:
        stock_price_rows = (
            MaterialExchangeStock.objects.filter(
                config_id__in=selected_config_ids,
                type_id__in=list(type_rollup.keys()),
            )
            .values("type_id")
            .annotate(jita_sell_price=Max("jita_sell_price"))
        )
        stock_prices = {
            int(row["type_id"]): _to_decimal(row.get("jita_sell_price"))
            for row in stock_price_rows
        }
        for type_id, rollup in type_rollup.items():
            acquired_qty = rollup["acquired_qty"]
            sold_qty = rollup["sold_qty"]
            if acquired_qty <= 0:
                continue

            remaining_qty = acquired_qty - sold_qty
            if remaining_qty <= 0:
                continue

            avg_cost = (rollup["acquired_cost"] / acquired_qty).quantize(Decimal("0.0001"))
            remaining_cost = (avg_cost * remaining_qty).quantize(Decimal("0.01"))
            unrealized_inventory_cost_basis += remaining_cost

            jita_sell_price = _to_decimal(stock_prices.get(int(type_id)))
            if jita_sell_price <= 0:
                continue

            remaining_value = (jita_sell_price * remaining_qty).quantize(Decimal("0.01"))
            unrealized_inventory_value += remaining_value
            unrealized_earnings_potential += remaining_value - remaining_cost

    member_sales_vs_jita_buy_delta = member_sales_volume - jita_buy_value
    member_sales_vs_jita_sell_delta = member_sales_volume - jita_sell_value
    member_sales_vs_jita_split_delta = member_sales_volume - jita_split_value

    realized_member_sale_profit = Decimal("0")
    potential_profit_jita_buy = Decimal("0")
    potential_profit_jita_sell = Decimal("0")
    potential_profit_jita_split = Decimal("0")
    potential_priced_type_count = 0
    for rollup in type_rollup.values():
        acquired_qty = rollup["acquired_qty"]
        sold_qty = rollup["sold_qty"]
        if acquired_qty <= 0 or sold_qty <= 0:
            continue

        avg_cost = (rollup["acquired_cost"] / acquired_qty).quantize(Decimal("0.0001"))
        cogs_for_sold = (avg_cost * sold_qty).quantize(Decimal("0.01"))
        realized_member_sale_profit += rollup["sold_revenue"] - cogs_for_sold
        potential_profit_jita_buy += rollup["sold_jita_buy"] - cogs_for_sold
        potential_profit_jita_sell += rollup["sold_jita_sell"] - cogs_for_sold
        potential_profit_jita_split += rollup["sold_jita_split"] - cogs_for_sold
        potential_priced_type_count += 1

    expected_profit_jita_buy_with_wallet = (
        potential_profit_jita_buy + wallet_supplemental_applied
    )
    expected_profit_jita_sell_with_wallet = (
        potential_profit_jita_sell + wallet_supplemental_applied
    )
    expected_profit_jita_split_with_wallet = (
        potential_profit_jita_split + wallet_supplemental_applied
    )
    projected_profit = actual_exchange_profit_with_wallet + unrealized_earnings_potential
    projected_revenue = total_buy_volume + unrealized_inventory_value

    contract_profit_margin_pct = (
        round((actual_exchange_profit / total_buy_volume) * 100, 2)
        if total_buy_volume > 0
        else 0
    )
    net_profit_margin_pct = (
        round((actual_exchange_profit_with_wallet / total_buy_volume) * 100, 2)
        if total_buy_volume > 0
        else 0
    )
    expected_margin_jita_split_pct = (
        round((expected_profit_jita_split_with_wallet / jita_split_value) * 100, 2)
        if jita_split_value > 0
        else 0
    )
    expected_margin_jita_buy_pct = (
        round((expected_profit_jita_buy_with_wallet / jita_buy_value) * 100, 2)
        if jita_buy_value > 0
        else 0
    )
    expected_margin_jita_sell_pct = (
        round((expected_profit_jita_sell_with_wallet / jita_sell_value) * 100, 2)
        if jita_sell_value > 0
        else 0
    )
    projected_margin_pct = (
        round((projected_profit / projected_revenue) * 100, 2)
        if projected_revenue > 0
        else 0
    )
    realized_vs_jita_buy_pct = (
        round((member_sales_volume / jita_buy_value) * 100, 2) if jita_buy_value > 0 else 0
    )
    realized_vs_jita_sell_pct = (
        round((member_sales_volume / jita_sell_value) * 100, 2)
        if jita_sell_value > 0
        else 0
    )
    realized_vs_jita_split_pct = (
        round((member_sales_volume / jita_split_value) * 100, 2)
        if jita_split_value > 0
        else 0
    )
    wallet_adjustment_pct_of_revenue = (
        round((wallet_supplemental_applied / total_buy_volume) * 100, 2)
        if total_buy_volume > 0
        else 0
    )
    unrealized_roi_pct = (
        round((unrealized_earnings_potential / unrealized_inventory_cost_basis) * 100, 2)
        if unrealized_inventory_cost_basis > 0
        else 0
    )

    analysis_window_start = filter_start or period_start or tx_time_rollup.get(
        "first_completed"
    )
    analysis_window_end = filter_end or timezone.now()
    if not analysis_window_start:
        analysis_window_start = tx_time_rollup.get("first_completed")
    if (
        analysis_window_start is not None
        and analysis_window_end is not None
        and analysis_window_end < analysis_window_start
    ):
        analysis_window_end = analysis_window_start
    analysis_window_days = (
        max((analysis_window_end - analysis_window_start).days + 1, 1)
        if analysis_window_start and analysis_window_end
        else 0
    )

    average_daily_revenue = (
        (total_buy_volume / Decimal(str(analysis_window_days))).quantize(Decimal("0.01"))
        if analysis_window_days > 0
        else Decimal("0")
    )
    average_daily_cost = (
        (total_sell_volume / Decimal(str(analysis_window_days))).quantize(Decimal("0.01"))
        if analysis_window_days > 0
        else Decimal("0")
    )
    average_daily_net_profit = (
        (actual_exchange_profit_with_wallet / Decimal(str(analysis_window_days))).quantize(
            Decimal("0.01")
        )
        if analysis_window_days > 0
        else Decimal("0")
    )
    forecast_30d_revenue = (average_daily_revenue * Decimal("30")).quantize(
        Decimal("0.01")
    )
    forecast_30d_profit = (average_daily_net_profit * Decimal("30")).quantize(
        Decimal("0.01")
    )
    forecast_90d_profit = (average_daily_net_profit * Decimal("90")).quantize(
        Decimal("0.01")
    )
    forecast_30d_margin_pct = (
        round((forecast_30d_profit / forecast_30d_revenue) * 100, 2)
        if forecast_30d_revenue > 0
        else 0
    )

    contracts_qs = ESIContract.objects.filter(contract_type="item_exchange")
    if int(chosen_corporation_id or 0) > 0:
        contracts_qs = contracts_qs.filter(corporation_id=int(chosen_corporation_id))
    if filter_start:
        contracts_qs = contracts_qs.filter(date_issued__gte=filter_start)
    if filter_end:
        contracts_qs = contracts_qs.filter(date_issued__lte=filter_end)

    contract_counts_raw = {
        str(row["status"]): int(row["count"])
        for row in contracts_qs.values("status")
        .annotate(count=Count("contract_id"))
        .order_by("status")
    }
    contract_stats = {
        "total": int(sum(contract_counts_raw.values())),
        "completed": int(
            contract_counts_raw.get("finished", 0)
            + contract_counts_raw.get("finished_issuer", 0)
            + contract_counts_raw.get("finished_contractor", 0)
        ),
        "outstanding": int(contract_counts_raw.get("outstanding", 0)),
        "in_progress": int(contract_counts_raw.get("in_progress", 0)),
        "cancelled": int(contract_counts_raw.get("cancelled", 0)),
        "rejected": int(contract_counts_raw.get("rejected", 0)),
        "failed": int(contract_counts_raw.get("failed", 0)),
        "expired": int(contract_counts_raw.get("expired", 0)),
        "deleted": int(contract_counts_raw.get("deleted", 0)),
        "reversed": int(contract_counts_raw.get("reversed", 0)),
        "deleted_before_acceptance": int(
            contracts_qs.filter(status="deleted", date_accepted__isnull=True).count()
        ),
        "deleted_after_acceptance": int(
            contracts_qs.filter(status="deleted", date_accepted__isnull=False).count()
        ),
    }

    buy_order_status_counts = {
        str(row["status"]): int(row["count"])
        for row in buy_orders_qs.values("status")
        .annotate(count=Count("id"))
        .order_by("status")
    }
    sell_order_status_counts = {
        str(row["status"]): int(row["count"])
        for row in sell_orders_qs.values("status")
        .annotate(count=Count("id"))
        .order_by("status")
    }

    buy_order_status_display_counts = {
        (
            _("Current Validated")
            if status == "validated"
            else _("Completed")
            if status == "completed"
            else _("Cancelled")
            if status == "cancelled"
            else _("Rejected")
            if status == "rejected"
            else _("Awaiting Auth Validation")
            if status == "awaiting_validation"
            else _("Order Created - Awaiting Contract")
            if status == "draft"
            else status
        ): count
        for status, count in buy_order_status_counts.items()
    }
    sell_order_status_display_counts = {
        (
            _("Current Validated")
            if status == "validated"
            else _("Current Anomaly")
            if status == "anomaly"
            else _("Current Anomaly Rejected")
            if status == "anomaly_rejected"
            else _("Completed")
            if status == "completed"
            else _("Cancelled")
            if status == "cancelled"
            else _("Rejected")
            if status == "rejected"
            else _("Awaiting Auth Validation")
            if status == "awaiting_validation"
            else _("Order Created - Awaiting Contract")
            if status == "draft"
            else status
        ): count
        for status, count in sell_order_status_counts.items()
    }

    contract_progress_stats = {
        "made": len(sell_rows) + len(buy_rows),
        "completed": int(
            buy_order_status_counts.get("completed", 0)
            + sell_order_status_counts.get("completed", 0)
        ),
        "cancelled": int(
            buy_order_status_counts.get("cancelled", 0)
            + sell_order_status_counts.get("cancelled", 0)
        ),
        "rejected": int(
            buy_order_status_counts.get("rejected", 0)
            + sell_order_status_counts.get("rejected", 0)
        ),
        "current_validated": int(
            buy_order_status_counts.get("validated", 0)
            + sell_order_status_counts.get("validated", 0)
        ),
        "current_awaiting_validation": int(
            buy_order_status_counts.get("awaiting_validation", 0)
            + sell_order_status_counts.get("awaiting_validation", 0)
        ),
        "current_anomaly": int(
            sell_order_status_counts.get("anomaly", 0)
            + sell_order_status_counts.get("anomaly_rejected", 0)
        ),
    }

    tx_monthly_rows = (
        tx_rows.annotate(month=TruncMonth("completed_at"))
        .values("month", "transaction_type")
        .annotate(
            order_count=Count("id"),
            total_value=Sum("total_price", default=0),
        )
        .order_by("month", "transaction_type")
    )

    if all_contract_ids:
        buyback_contracts_qs = contracts_qs.filter(contract_id__in=list(all_contract_ids))
    else:
        buyback_contracts_qs = contracts_qs.none()

    buy_monthly_map: dict[str, dict[str, Decimal | int]] = {}
    sell_monthly_map: dict[str, dict[str, Decimal | int]] = {}
    market_monthly_map: dict[str, int] = dict(wallet_market_monthly_map)
    month_keys: set[str] = set()

    for row in tx_monthly_rows:
        month = row.get("month")
        if not month:
            continue
        key = month.strftime("%Y-%m")
        tx_type = str(row.get("transaction_type") or "").strip().lower()
        target = None
        if tx_type == MaterialExchangeTransaction.TransactionType.BUY:
            target = buy_monthly_map
        elif tx_type == MaterialExchangeTransaction.TransactionType.SELL:
            target = sell_monthly_map
        if target is None:
            continue
        existing = target.get(
            key,
            {
                "count": 0,
                "value": Decimal("0"),
            },
        )
        target[key] = {
            "count": int(existing.get("count") or 0) + int(row.get("order_count") or 0),
            "value": _to_decimal(existing.get("value")) + _to_decimal(row.get("total_value")),
        }
        month_keys.add(key)

    if not market_monthly_map:
        market_orders_monthly_rows = (
            buyback_contracts_qs.annotate(month=TruncMonth("date_issued"))
            .values("month")
            .annotate(order_count=Count("contract_id"))
            .order_by("month")
        )
        for row in market_orders_monthly_rows:
            month = row.get("month")
            if not month:
                continue
            key = month.strftime("%Y-%m")
            market_monthly_map[key] = int(row.get("order_count") or 0)
            month_keys.add(key)
    else:
        month_keys.update(market_monthly_map.keys())

    sorted_month_keys = sorted(month_keys)
    trend_rows = []
    chart_labels: list[str] = []
    buy_volumes: list[float] = []
    sell_volumes: list[float] = []
    transaction_counts: list[int] = []
    for month_key in sorted_month_keys:
        buy_row = buy_monthly_map.get(month_key, {})
        sell_row = sell_monthly_map.get(month_key, {})
        market_count = int(market_monthly_map.get(month_key, 0))
        buy_count = int(buy_row.get("count") or 0)
        sell_count = int(sell_row.get("count") or 0)
        buy_value = _to_decimal(buy_row.get("value"))
        sell_value = _to_decimal(sell_row.get("value"))

        trend_rows.append(
            {
                "month": month_key,
                "buy_order_count": buy_count,
                "sell_order_count": sell_count,
                "market_order_count": market_count,
                "buy_order_value": buy_value,
                "sell_order_value": sell_value,
            }
        )
        chart_labels.append(month_key)
        buy_volumes.append(float(buy_value))
        sell_volumes.append(float(sell_value))
        transaction_counts.append(buy_count + sell_count + market_count)

    donation_contracts_qs = buyback_contracts_qs.filter(
        status__in=["finished", "finished_issuer", "finished_contractor"],
        price=0,
        reward=0,
    )
    donation_contract_ids = [
        int(contract_id)
        for contract_id in donation_contracts_qs.values_list("contract_id", flat=True)
    ]
    donation_item_rows = []
    if donation_contract_ids:
        donation_item_rows = list(
            ESIContractItem.objects.filter(
                contract_id__in=donation_contract_ids,
                is_included=True,
            )
            .values("type_id")
            .annotate(total_qty=Sum("quantity", default=0))
        )

    donation_estimated_jita_sell = Decimal("0")
    if donation_item_rows:
        donation_type_ids = [int(row["type_id"]) for row in donation_item_rows]
        donation_stock_rows = (
            MaterialExchangeStock.objects.filter(
                config_id__in=selected_config_ids,
                type_id__in=donation_type_ids,
            )
            .values("type_id")
            .annotate(jita_sell_price=Max("jita_sell_price"))
        )
        donation_price_map = {
            int(row["type_id"]): _to_decimal(row.get("jita_sell_price"))
            for row in donation_stock_rows
        }
        for row in donation_item_rows:
            type_id = int(row.get("type_id") or 0)
            qty = _to_decimal(row.get("total_qty"))
            price = donation_price_map.get(type_id, Decimal("0"))
            if qty > 0 and price > 0:
                donation_estimated_jita_sell += (qty * price).quantize(Decimal("0.01"))

    donation_stats = {
        "contracts": int(len(donation_contract_ids)),
        "estimated_jita_sell": donation_estimated_jita_sell,
        "market_orders_total": int(
            wallet_open_buy_orders + wallet_open_sell_orders
        )
        if wallet_market_orders_rows
        else int(buyback_contracts_qs.count()),
        "contract_buy_total": total_buy_volume,
        "contract_sell_total": total_sell_volume,
        "contract_transaction_count": contract_transaction_count,
        "wallet_activity_count": int(wallet_activity_count),
        "wallet_division_balance": corptools_division_balance,
        "wallet_inflow_total": wallet_inflow_total,
        "wallet_outflow_total": wallet_outflow_total,
        "wallet_net_total": wallet_net_total,
        "wallet_net_projected_30d": wallet_net_projected_30d,
        "wallet_analysis_window_days": int(wallet_analysis_window_days),
        "wallet_market_activity_count": int(wallet_market_activity_count),
        "wallet_market_activity_total": wallet_market_activity_total,
        "wallet_market_transaction_count": int(wallet_market_transaction_count),
        "wallet_market_transaction_total": wallet_market_transaction_total,
        "wallet_market_escrow_count": int(wallet_market_escrow_count),
        "wallet_market_escrow_total": wallet_market_escrow_total,
        "wallet_fee_count": int(wallet_fee_count),
        "wallet_fee_total": wallet_fee_total,
        "wallet_donation_count": int(wallet_donation_count),
        "wallet_donation_total": wallet_donation_total,
        "wallet_withdrawal_count": int(wallet_withdrawal_count),
        "wallet_withdrawal_total": wallet_withdrawal_total,
        "wallet_supplemental_total": wallet_supplemental_total,
        "me_contract_wallet_entry_count": int(me_contract_wallet_entry_count),
        "me_contract_wallet_amount_total": me_contract_wallet_amount_total,
        "open_buy_orders": int(wallet_open_buy_orders),
        "open_sell_orders": int(wallet_open_sell_orders),
        "open_order_value": wallet_open_order_value,
    }

    user_ids = {
        int(row["seller_id"])
        for row in sell_rows
        if int(row.get("seller_id") or 0) > 0
    } | {
        int(row["buyer_id"])
        for row in buy_rows
        if int(row.get("buyer_id") or 0) > 0
    }
    user_map = {int(user.id): user for user in User.objects.filter(id__in=list(user_ids))}

    sold_rollup: dict[int, dict[str, Decimal | int]] = {}
    bought_rollup: dict[int, dict[str, Decimal | int]] = {}

    for row in sell_rows:
        user_id = int(row.get("seller_id") or 0)
        if user_id <= 0:
            continue
        bucket = sold_rollup.setdefault(
            user_id,
            {"orders": 0, "value": Decimal("0"), "quantity": 0},
        )
        bucket["orders"] = int(bucket["orders"]) + 1
        bucket["value"] = _to_decimal(bucket["value"]) + _to_decimal(
            row.get("expected_total")
        )
        bucket["quantity"] = int(bucket["quantity"]) + int(row.get("expected_qty") or 0)

    for row in buy_rows:
        user_id = int(row.get("buyer_id") or 0)
        if user_id <= 0:
            continue
        bucket = bought_rollup.setdefault(
            user_id,
            {"orders": 0, "value": Decimal("0"), "quantity": 0},
        )
        bucket["orders"] = int(bucket["orders"]) + 1
        bucket["value"] = _to_decimal(bucket["value"]) + _to_decimal(
            row.get("expected_total")
        )
        bucket["quantity"] = int(bucket["quantity"]) + int(row.get("expected_qty") or 0)

    def _ranked_users(rollup: dict[int, dict[str, Decimal | int]], *, top_n: int = 10):
        ranked = []
        for user_id, bucket in rollup.items():
            user = user_map.get(int(user_id))
            username = str(getattr(user, "username", "") or f"User {user_id}")
            ranked.append(
                {
                    "user_id": int(user_id),
                    "username": username,
                    "main_character": _resolve_main_character_name(user) if user else username,
                    "orders": int(bucket.get("orders") or 0),
                    "total_value": _to_decimal(bucket.get("value")),
                    "quantity": int(bucket.get("quantity") or 0),
                }
            )
        ranked.sort(key=lambda item: item["total_value"], reverse=True)
        return ranked[:top_n]

    most_sold_users = _ranked_users(sold_rollup)
    most_bought_users = _ranked_users(bought_rollup)

    top_user_stats = []
    merged_user_ids = sorted(set(sold_rollup.keys()) | set(bought_rollup.keys()))
    for user_id in merged_user_ids:
        user = user_map.get(int(user_id))
        sold = sold_rollup.get(int(user_id), {})
        bought = bought_rollup.get(int(user_id), {})
        top_user_stats.append(
            {
                "username": str(getattr(user, "username", "") or f"User {user_id}"),
                "main_character": _resolve_main_character_name(user)
                if user
                else str(getattr(user, "username", "") or f"User {user_id}"),
                "buy_volume": _to_decimal(bought.get("value")),
                "sell_volume": _to_decimal(sold.get("value")),
                "buy_orders": int(bought.get("orders") or 0),
                "sell_orders": int(sold.get("orders") or 0),
                "total_orders": int(bought.get("orders") or 0)
                + int(sold.get("orders") or 0),
                "net_flow": _to_decimal(bought.get("value"))
                - _to_decimal(sold.get("value")),
            }
        )
    top_user_stats.sort(
        key=lambda item: _to_decimal(item["buy_volume"]) + _to_decimal(item["sell_volume"]),
        reverse=True,
    )
    top_user_stats = top_user_stats[:10]

    sell_rows_by_id = {int(row.get("id") or 0): row for row in sell_rows}
    buy_rows_by_id = {int(row.get("id") or 0): row for row in buy_rows}
    recent_transactions: list[dict[str, object]] = []

    for order_id, rollup in sell_tx_rollup_by_order.items():
        order_row = sell_rows_by_id.get(int(order_id))
        if not order_row:
            continue
        user_id = int(order_row.get("seller_id") or 0)
        user = user_map.get(user_id)
        username = str(getattr(user, "username", "") or f"User {user_id}")
        who = _resolve_main_character_name(user) if user else username
        contract_id = int(order_row.get("esi_contract_id") or 0)
        created_at = order_row.get("created_at")
        accepted_at = contract_accepted_at_map.get(contract_id)
        duration_display = "-"
        if created_at and accepted_at and accepted_at >= created_at:
            duration_display = _format_duration_short(accepted_at - created_at)
        recent_transactions.append(
            {
                "order_reference": str(order_row.get("order_reference") or "")
                or f"SELL-{int(order_id)}",
                "transaction_type": MaterialExchangeTransaction.TransactionType.SELL,
                "who": who,
                "party_from": who,
                "party_to": "Hub",
                "total_price": _to_decimal((rollup or {}).get("total_value")),
                "created_at": created_at,
                "accepted_at": accepted_at,
                "completed_at": (rollup or {}).get("completed_at"),
                "acceptance_duration_display": duration_display,
            }
        )

    for order_id, rollup in buy_tx_rollup_by_order.items():
        order_row = buy_rows_by_id.get(int(order_id))
        if not order_row:
            continue
        user_id = int(order_row.get("buyer_id") or 0)
        user = user_map.get(user_id)
        username = str(getattr(user, "username", "") or f"User {user_id}")
        who = _resolve_main_character_name(user) if user else username
        contract_id = int(order_row.get("esi_contract_id") or 0)
        created_at = order_row.get("created_at")
        accepted_at = contract_accepted_at_map.get(contract_id)
        duration_display = "-"
        if created_at and accepted_at and accepted_at >= created_at:
            duration_display = _format_duration_short(accepted_at - created_at)
        recent_transactions.append(
            {
                "order_reference": str(order_row.get("order_reference") or "")
                or f"BUY-{int(order_id)}",
                "transaction_type": MaterialExchangeTransaction.TransactionType.BUY,
                "who": who,
                "party_from": "Hub",
                "party_to": who,
                "total_price": _to_decimal((rollup or {}).get("total_value")),
                "created_at": created_at,
                "accepted_at": accepted_at,
                "completed_at": (rollup or {}).get("completed_at"),
                "acceptance_duration_display": duration_display,
            }
        )

    recent_transactions.sort(
        key=lambda row: row.get("completed_at") or row.get("created_at") or timezone.now(),
        reverse=True,
    )
    recent_transactions = recent_transactions[:25]

    context = {
        "config": config,
        "chosen_corporation_id": chosen_corporation_id,
        "chosen_wallet_division": chosen_wallet_division,
        "corp_options": corp_options,
        "wallet_division_options": wallet_division_options,
        "division_scope_missing": bool(division_scope_missing),
        "wallet_scope_missing": bool(wallet_scope_missing),
        "market_scope_missing": bool(market_scope_missing),
        "wallet_activity_error": wallet_activity_error,
        "wallet_data_source": wallet_data_source,
        "stats_scope_mode": stats_scope_mode,
        "stats_scope_note": stats_scope_note,
        "corp_config_count": len(corp_config_ids),
        "selected_config_count": len(selected_config_ids),
        "chart_labels": chart_labels,
        "buy_volumes": buy_volumes,
        "sell_volumes": sell_volumes,
        "transaction_counts": transaction_counts,
        "months_count": len(chart_labels),
        "total_buy_volume": total_buy_volume,
        "total_sell_volume": total_sell_volume,
        "total_transactions": total_transactions,
        "top_user_stats": top_user_stats,
        "recent_transactions": recent_transactions,
        "period_options": period_options,
        "selected_period": selected_period,
        "period_start": period_start,
        "start_date": custom_start_date.isoformat() if custom_start_date else "",
        "end_date": custom_end_date.isoformat() if custom_end_date else "",
        "sell_expected_cost_total": sell_expected_cost_total,
        "sell_actual_cost_total": sell_actual_cost_total,
        "sell_cost_delta": sell_cost_delta,
        "sell_expected_jita_sell_total": sell_expected_jita_sell_total,
        "sell_expected_jita_buy_total": sell_expected_jita_buy_total,
        "sell_expected_jita_split_total": sell_expected_jita_split_total,
        "sell_contract_coverage_pct": sell_contract_coverage_pct,
        "buy_expected_jita_sell_total": buy_expected_jita_sell_total,
        "buy_expected_jita_buy_total": buy_expected_jita_buy_total,
        "buy_expected_jita_split_total": buy_expected_jita_split_total,
        "buy_actual_revenue_total": buy_actual_revenue_total,
        "buy_revenue_delta_jita_sell": buy_revenue_delta_jita_sell,
        "buy_revenue_delta_jita_buy": buy_revenue_delta_jita_buy,
        "buy_contract_coverage_pct": buy_contract_coverage_pct,
        "actual_exchange_profit": actual_exchange_profit,
        "actual_exchange_profit_with_wallet": actual_exchange_profit_with_wallet,
        "wallet_supplemental_applied": wallet_supplemental_applied,
        "member_sales_volume": member_sales_volume,
        "jita_buy_value": jita_buy_value,
        "jita_sell_value": jita_sell_value,
        "jita_split_value": jita_split_value,
        "snapshot_coverage_pct": snapshot_coverage_pct,
        "member_sales_vs_jita_buy_delta": member_sales_vs_jita_buy_delta,
        "member_sales_vs_jita_sell_delta": member_sales_vs_jita_sell_delta,
        "member_sales_vs_jita_split_delta": member_sales_vs_jita_split_delta,
        "realized_member_sale_profit": realized_member_sale_profit,
        "potential_profit_jita_buy": potential_profit_jita_buy,
        "potential_profit_jita_sell": potential_profit_jita_sell,
        "potential_profit_jita_split": potential_profit_jita_split,
        "expected_profit_jita_buy_with_wallet": expected_profit_jita_buy_with_wallet,
        "expected_profit_jita_sell_with_wallet": expected_profit_jita_sell_with_wallet,
        "expected_profit_jita_split_with_wallet": expected_profit_jita_split_with_wallet,
        "potential_priced_type_count": potential_priced_type_count,
        "unrealized_inventory_value": unrealized_inventory_value,
        "unrealized_inventory_cost_basis": unrealized_inventory_cost_basis,
        "unrealized_earnings_potential": unrealized_earnings_potential,
        "projected_profit": projected_profit,
        "projected_revenue": projected_revenue,
        "contract_profit_margin_pct": contract_profit_margin_pct,
        "net_profit_margin_pct": net_profit_margin_pct,
        "expected_margin_jita_buy_pct": expected_margin_jita_buy_pct,
        "expected_margin_jita_sell_pct": expected_margin_jita_sell_pct,
        "expected_margin_jita_split_pct": expected_margin_jita_split_pct,
        "projected_margin_pct": projected_margin_pct,
        "realized_vs_jita_buy_pct": realized_vs_jita_buy_pct,
        "realized_vs_jita_sell_pct": realized_vs_jita_sell_pct,
        "realized_vs_jita_split_pct": realized_vs_jita_split_pct,
        "wallet_adjustment_pct_of_revenue": wallet_adjustment_pct_of_revenue,
        "unrealized_roi_pct": unrealized_roi_pct,
        "analysis_window_days": analysis_window_days,
        "average_daily_revenue": average_daily_revenue,
        "average_daily_cost": average_daily_cost,
        "average_daily_net_profit": average_daily_net_profit,
        "forecast_30d_revenue": forecast_30d_revenue,
        "forecast_30d_profit": forecast_30d_profit,
        "forecast_90d_profit": forecast_90d_profit,
        "forecast_30d_margin_pct": forecast_30d_margin_pct,
        "contract_progress_stats": contract_progress_stats,
        "contract_stats": contract_stats,
        "contract_counts_raw": contract_counts_raw,
        "buy_order_status_counts": buy_order_status_counts,
        "sell_order_status_counts": sell_order_status_counts,
        "buy_order_status_display_counts": buy_order_status_display_counts,
        "sell_order_status_display_counts": sell_order_status_display_counts,
        "trend_rows": trend_rows,
        "market_trend_source_label": market_trend_source_label,
        "wallet_ref_type_rows": wallet_ref_type_rows,
        "most_sold_users": most_sold_users,
        "most_bought_users": most_bought_users,
        "donation_stats": donation_stats,
        "data_limitations": _(
            "Actual bought/sold totals prefer matched ESI contract prices and fall back to Buyback transaction totals when a contract price is missing. Wallet activity (donations, withdrawals, fees, market refs) is supplemental. Forecast fields are run-rate estimates from the selected date window. Wallet supplemental adjustments are only applied to net profit when contract-backed activity exists in the selected range."
        ),
        "nav_context": _build_nav_context(request.user),
    }

    context.update(
        build_nav_context(
            request.user,
            active_tab="stats",
            can_manage_corp=request.user.has_perm(
                "indy_hub.can_manage_corp_bp_requests"
            ),
        )
    )

    return render(request, "indy_hub/material_exchange/stats_history.html", context)


@login_required
@require_http_methods(["POST"])
def material_exchange_assign_contract(request, order_id):
    emit_view_analytics_event(
        view_name="material_exchange.assign_contract", request=request
    )
    """Assign ESI contract ID to a sell or buy order."""
    if not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Permission denied."))
        return redirect("indy_hub:material_exchange_index")

    order_type = request.POST.get("order_type")  # 'sell' or 'buy'
    contract_id = request.POST.get("contract_id", "").strip()

    if not contract_id or not contract_id.isdigit():
        messages.error(request, _("Invalid contract ID. Must be a number."))
        return redirect("indy_hub:material_exchange_index")

    contract_id_int = int(contract_id)

    try:
        if order_type == "sell":
            order = get_object_or_404(MaterialExchangeSellOrder, id=order_id)
            # Assign contract ID to all items in this order
            order.items.update(
                esi_contract_id=contract_id_int,
                esi_validation_checked_at=None,  # Reset to trigger re-validation
            )
            messages.success(
                request,
                _(
                    f"Contract ID {contract_id_int} assigned to sell order #{order.id}. Validation will run automatically."
                ),
            )
        elif order_type == "buy":
            order = get_object_or_404(MaterialExchangeBuyOrder, id=order_id)
            order.items.update(
                esi_contract_id=contract_id_int,
                esi_validation_checked_at=None,
            )
            messages.success(
                request,
                _(
                    f"Contract ID {contract_id_int} assigned to buy order #{order.id}. Validation will run automatically."
                ),
            )
        else:
            messages.error(request, _("Invalid order type."))

    except Exception as exc:
        logger.error(f"Error assigning contract ID: {exc}", exc_info=True)
        messages.error(request, _(f"Error assigning contract ID: {exc}"))

    return redirect("indy_hub:material_exchange_index")


def _build_nav_context(user):
    """Helper to build navigation context for Buyback."""
    return {
        "can_manage": user.has_perm("indy_hub.can_manage_material_hub"),
    }


def _get_corp_name_for_hub(corporation_id: int) -> str:
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

