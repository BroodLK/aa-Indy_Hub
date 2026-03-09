"""Material Exchange views for Indy Hub."""

# Standard Library
import hashlib
from decimal import ROUND_CEILING, Decimal

# Django
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import Permission
from django.core.cache import cache
from django.core.paginator import Paginator
from django.db import transaction
from django.db.models import Count, Q, Sum
from django.db.models.functions import TruncMonth
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from django.views.decorators.http import require_http_methods

# Alliance Auth
from allianceauth.authentication.models import UserProfile
from allianceauth.services.hooks import get_extension_logger

from ..decorators import indy_hub_permission_required, tokens_required
from ..models import (
    CachedCharacterAsset,
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
    asset_chain_has_context,
    build_asset_index_by_item_id,
    get_corp_assets_cached,
    get_corp_divisions_cached,
    get_office_folder_item_id_from_assets,
    get_user_assets_cached,
    make_managed_hangar_location_id,
    resolve_structure_names,
)
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
from ..utils.eve import get_type_name
from ..utils.material_exchange_pricing import (
    apply_markup_with_jita_bounds,
    compute_buy_price_from_member,
)
from .navigation import build_nav_context

logger = get_extension_logger(__name__)
User = get_user_model()

_PRODUCTION_IDS_CACHE: set[int] | None = None
_INDUSTRY_MARKET_GROUP_IDS_CACHE: set[int] | None = None

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
        if step.get("completed"):
            current_step_index = idx

    if order.timeline_breadcrumb:
        order.progress_current_label = order.timeline_breadcrumb[current_step_index][
            "status"
        ]
        current_step_position = order.timeline_breadcrumb[current_step_index].get(
            "position_percent", 0
        )
        if current_step_index < order.progress_total_steps - 1:
            next_step_position = order.timeline_breadcrumb[current_step_index + 1].get(
                "position_percent", current_step_position
            )
            order.progress_active_start = current_step_position
            order.progress_active_width = max(
                0, round(next_step_position - current_step_position, 2)
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
                material_eve_type__market_group_id_raw__isnull=True
            )
            .values_list("material_eve_type__market_group_id_raw", flat=True)
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
                market_group_id_raw__in=expanded_group_ids
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
    """Return active admins for Material Exchange (explicit permission holders only)."""

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


def _fetch_fuzzwork_prices(type_ids: list[int]) -> dict[int, dict[str, Decimal]]:
    """Batch fetch Jita buy/sell prices from Fuzzwork for given type IDs."""
    # Local
    from ..services.fuzzwork import FuzzworkError, fetch_fuzzwork_prices

    if not type_ids:
        return {}

    try:
        return fetch_fuzzwork_prices(type_ids, timeout=15)
    except FuzzworkError as exc:  # pragma: no cover - defensive
        logger.warning("material_exchange: failed to fetch fuzzwork prices: %s", exc)
        return {}


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
    """

    grouped: dict[tuple[int, str], int] = {}
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
        if len(parts) >= 3:
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
            key_tuple = (type_id, variant)
            grouped[key_tuple] = grouped.get(key_tuple, 0) + qty

    entries: list[dict[str, object]] = []
    for (type_id, variant), quantity in grouped.items():
        entries.append(
            {
                "type_id": int(type_id),
                "blueprint_variant": str(variant or ""),
                "quantity": int(quantity),
            }
        )
    return entries


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
        if sell_markup_percent_override is not None:
            sell_overrides[type_id_int] = {
                "kind": "markup",
                "percent": Decimal(sell_markup_percent_override),
                "base": str(sell_markup_base_override or "buy"),
            }
        elif sell_override is not None:
            # Legacy fallback: keep supporting existing fixed-price rows.
            sell_overrides[type_id_int] = {
                "kind": "fixed",
                "price": Decimal(sell_override),
            }
        if buy_markup_percent_override is not None:
            buy_overrides[type_id_int] = {
                "kind": "markup",
                "percent": Decimal(buy_markup_percent_override),
                "base": str(buy_markup_base_override or "buy"),
            }
        elif buy_override is not None:
            # Legacy fallback: keep supporting existing fixed-price rows.
            buy_overrides[type_id_int] = {
                "kind": "fixed",
                "price": Decimal(buy_override),
            }

    return sell_overrides, buy_overrides


def _compute_effective_sell_unit_price(
    *,
    config: MaterialExchangeConfig,
    type_id: int,
    jita_buy: Decimal,
    jita_sell: Decimal,
    sell_override_map: dict[int, dict[str, object]],
) -> tuple[Decimal, Decimal, bool]:
    """Return (effective, default, has_override) for sell-page pricing."""

    default_unit_price = compute_buy_price_from_member(
        config=config,
        jita_buy=jita_buy,
        jita_sell=jita_sell,
    )
    override_value = sell_override_map.get(int(type_id))
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
    stock_item: MaterialExchangeStock,
    buy_override_map: dict[int, dict[str, object]],
) -> tuple[Decimal, Decimal, bool]:
    """Return (effective, default, has_override) for buy-page pricing."""

    default_unit_price = Decimal(stock_item.sell_price_to_member)
    override_value = buy_override_map.get(int(stock_item.type_id))
    if override_value is None:
        return default_unit_price, default_unit_price, False
    if str(override_value.get("kind") or "") == "markup":
        override_base = str(override_value.get("base") or "buy").strip().lower()
        if override_base not in {"buy", "sell"}:
            override_base = "buy"
        effective_price = apply_markup_with_jita_bounds(
            jita_buy=Decimal(stock_item.jita_buy_price or 0),
            jita_sell=Decimal(stock_item.jita_sell_price or 0),
            base_choice=override_base,
            percent=Decimal(override_value.get("percent") or 0),
            enforce_bounds=bool(
                getattr(stock_item.config, "enforce_jita_price_bounds", False)
            ),
        )
    else:
        effective_price = Decimal(override_value.get("price") or 0)
    return effective_price, default_unit_price, effective_price != default_unit_price


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
    try:
        return int(asset.get("quantity", 0) or 0) < 0
    except (TypeError, ValueError):
        return False


def _asset_blueprint_variant(asset: dict) -> str:
    """Return blueprint variant token for an asset row: 'bpo', 'bpc', or ''."""
    if not _asset_is_blueprint(asset):
        return ""
    try:
        raw_quantity = int(asset.get("quantity", 0) or 0)
    except (TypeError, ValueError):
        raw_quantity = 0
    if raw_quantity == -2:
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
) -> list[dict]:
    """Build sell rows, grouping assets by containers where possible."""

    if not assets:
        return []

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
    price_meta_cache: dict[tuple[int, str], dict[str, object] | None] = {}

    def get_price_meta(type_id: int, blueprint_variant: str = "") -> dict[str, object] | None:
        type_id_int = int(type_id)
        meta_key = (type_id_int, str(blueprint_variant or ""))
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
        if not has_market_price and type_id_int not in sell_override_map:
            price_meta_cache[meta_key] = None
            return None

        unit_price, default_unit_price, has_override = _compute_effective_sell_unit_price(
            config=config,
            type_id=type_id_int,
            jita_buy=jita_buy,
            jita_sell=jita_sell,
            sell_override_map=sell_override_map,
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
        if get_price_meta(type_id, blueprint_variant) is None:
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
    ) -> dict[str, object] | None:
        if quantity <= 0:
            return None
        meta = get_price_meta(type_id, blueprint_variant)
        if meta is None:
            return None

        available_for_type = remaining_by_type.get(int(type_id), 0)
        available_qty = min(int(quantity), int(available_for_type))
        remaining_by_type[int(type_id)] = max(int(available_for_type) - available_qty, 0)
        reserved_qty = max(int(quantity) - available_qty, 0)
        row_idx = next_row_index()
        variant_token = str(meta.get("blueprint_variant") or "") or "std"

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
            "form_quantity_field_name": f"qty_{int(type_id)}_{variant_token}_{row_idx}",
            "user_quantity": int(quantity),
            "reserved_quantity": int(reserved_qty),
            "available_quantity": int(available_qty),
            "depth": int(depth),
            "container_path": ",".join(ancestors),
            "indent_padding_rem": round(max(0, depth) * 1.15, 2),
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

        grouped_child_items: dict[tuple[int, str], int] = {}
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
            child_key = (child_type_id, child_blueprint_variant)
            grouped_child_items[child_key] = grouped_child_items.get(child_key, 0) + child_qty

        child_rows: list[dict] = []
        grouped_items_sorted = sorted(
            grouped_child_items.items(),
            key=lambda pair: (
                str(
                    (
                        get_price_meta(pair[0][0], pair[0][1]) or {}
                    ).get("type_name")
                    or get_type_name(pair[0][0])
                ).lower(),
                int(pair[0][0]),
                str(pair[0][1] or ""),
            ),
        )
        for child_key, child_qty in grouped_items_sorted:
            child_type_id, child_blueprint_variant = child_key
            row = build_item_row(
                type_id=child_type_id,
                quantity=child_qty,
                blueprint_variant=child_blueprint_variant,
                ancestors=next_ancestors,
                depth=depth + 1,
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
        }

        return [container_row, *child_rows]

    root_container_assets: list[dict] = []
    root_items_by_key: dict[tuple[int, str], int] = {}
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
        root_key = (type_id, blueprint_variant)
        root_items_by_key[root_key] = root_items_by_key.get(root_key, 0) + qty

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
        ),
    )
    for root_key, qty in root_items_sorted:
        type_id, blueprint_variant = root_key
        row = build_item_row(
            type_id=type_id,
            quantity=qty,
            blueprint_variant=blueprint_variant,
            ancestors=[],
            depth=0,
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
                allow_refresh=False,
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

    variants_by_type: dict[int, set[str]] = {}
    for asset in scoped_assets:
        if not _asset_is_blueprint(asset):
            continue

        try:
            type_id = int(asset.get("type_id") or 0)
        except (TypeError, ValueError):
            continue
        if type_id <= 0:
            continue
        if wanted_type_ids is not None and type_id not in wanted_type_ids:
            continue

        try:
            raw_quantity = int(asset.get("quantity", 0) or 0)
        except (TypeError, ValueError):
            raw_quantity = 0
        variant = "bpc" if raw_quantity == -2 else "bpo"
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
    stock_meta_by_type: dict[int, dict[str, object]],
    buy_name_map: dict[int, str],
    fallback_location_label: str,
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

    def _resolve_item_meta(type_id: int, blueprint_variant: str = "") -> dict[str, object] | None:
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
            unit_price = Decimal(base_meta.get("display_sell_price_to_member") or 0)
            default_unit_price = Decimal(base_meta.get("default_sell_price_to_member") or 0)
            has_override = bool(base_meta.get("has_buy_price_override", False))

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

    def build_item_row(
        *,
        type_id: int,
        quantity: int,
        blueprint_variant: str,
        source_structure_ids: list[int],
        ancestors: list[str],
        depth: int,
    ) -> dict[str, object] | None:
        if int(quantity) <= 0:
            return None

        item_meta = _resolve_item_meta(type_id, blueprint_variant)
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

        return {
            "row_kind": "item",
            "row_index": row_idx,
            "type_id": int(type_id),
            "display_type_name": str(item_meta.get("display_type_name") or ""),
            "blueprint_variant": str(item_meta.get("blueprint_variant") or ""),
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
            "depth": int(depth),
            "container_path": ",".join(ancestors),
            "indent_padding_rem": round(max(0, depth) * 1.15, 2),
            "form_quantity_field_name": f"qty_{int(type_id)}_{variant_token}_{row_idx}",
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
        nested_container_assets: list[dict] = []
        grouped_child_items: dict[tuple[int, str, tuple[int, ...]], int] = {}

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
            child_variant = _asset_blueprint_variant(child)
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
                tuple(sorted(child_source_ids)),
            )
            grouped_child_items[group_key] = grouped_child_items.get(group_key, 0) + int(child_qty)

        child_rows: list[dict] = []
        grouped_items_sorted = sorted(
            grouped_child_items.items(),
            key=lambda pair: (
                str(
                    (_resolve_item_meta(pair[0][0], pair[0][1]) or {}).get("display_type_name")
                    or get_type_name(pair[0][0])
                ).lower(),
                int(pair[0][0]),
                str(pair[0][1] or ""),
                ",".join(str(x) for x in pair[0][2]),
            ),
        )
        for grouped_key, grouped_qty in grouped_items_sorted:
            child_type_id, child_variant, source_ids_tuple = grouped_key
            child_row = build_item_row(
                type_id=child_type_id,
                quantity=int(grouped_qty),
                blueprint_variant=child_variant,
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
            "container_name": _container_display_name(asset),
            "container_type_id": container_type_id if container_type_id > 0 else None,
            "container_icon_url": container_icon_url,
            "container_icon_fallback_url": container_icon_fallback_url,
            "depth": int(depth),
            "container_path": ",".join(ancestors),
            "indent_padding_rem": round(max(0, depth) * 1.15, 2),
        }

        return [container_row, *child_rows]

    root_container_assets: list[dict] = []
    root_items_by_key: dict[tuple[int, str, tuple[int, ...]], int] = {}
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
        blueprint_variant = _asset_blueprint_variant(asset)

        source_ids: list[int] = []
        for raw_source_id in asset.get("source_structure_ids", []) or []:
            try:
                source_id = int(raw_source_id)
            except (TypeError, ValueError):
                continue
            if source_id > 0 and source_id not in source_ids:
                source_ids.append(source_id)
        key = (int(type_id), str(blueprint_variant or ""), tuple(sorted(source_ids)))
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
            ",".join(str(x) for x in pair[0][2]),
        ),
    )
    for root_key, qty in root_items_sorted:
        type_id, blueprint_variant, source_ids_tuple = root_key
        row = build_item_row(
            type_id=type_id,
            quantity=int(qty),
            blueprint_variant=blueprint_variant,
            source_structure_ids=list(source_ids_tuple),
            ancestors=[],
            depth=0,
        )
        if row is not None:
            rows.append(row)

    return rows


def _selected_buy_stock_items_share_source_location(
    stock_items: list[MaterialExchangeStock],
) -> bool:
    """Return True when selected stock rows can be sourced from one common location."""

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
            return False

    return True


@login_required
@indy_hub_permission_required("can_access_indy_hub")
def material_exchange_index(request):
    """
    Material Exchange hub landing page.
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
def material_exchange_history(request):
    """Admin-only history page showing closed (completed/rejected/cancelled) orders."""
    emit_view_analytics_event(view_name="material_exchange.history", request=request)
    if not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("You are not allowed to view this page."))
        return redirect("indy_hub:material_exchange_index")

    if not _is_material_exchange_enabled():
        messages.warning(request, _("Material Exchange is disabled."))
        return redirect("indy_hub:material_exchange_index")

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, _("Material Exchange is not configured."))
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
        messages.warning(request, _("Material Exchange is disabled."))
        return redirect("indy_hub:material_exchange_index")

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, _("Material Exchange is not configured."))
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

        try:
            all_cached_assets_for_pricing, _scope_missing_for_pricing = get_user_assets_cached(
                request.user,
                allow_refresh=False,
            )
        except Exception:
            all_cached_assets_for_pricing = []
        variant_quantities = _build_sell_variant_quantities(
            assets=all_cached_assets_for_pricing,
            location_id=selected_location_id,
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
                )
            )
            return buy_price > 0

        sell_page_base_url = reverse("indy_hub:material_exchange_sell")
        location_tabs = []
        character_tabs = []
        active_character_tab = ""
        selected_character_id: int | None = None

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
            character_names_map = _resolve_user_character_names_map(request.user)
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
        messages.warning(request, _("Material Exchange is disabled."))
        return redirect("indy_hub:material_exchange_index")

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, _("Material Exchange is not configured."))
        return redirect("indy_hub:material_exchange_index")
    if not bool(getattr(config, "buy_enabled", True)):
        messages.info(request, _("Buy orders are currently disabled for this hub."))
        return redirect("indy_hub:material_exchange_index")
    stock_refreshing = False
    _sell_override_map, buy_override_map = _get_item_price_override_maps(config)

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
        submitted_blueprint_variants = _get_buy_stock_blueprint_variant_map(
            config=config,
            type_ids=submitted_type_ids,
        )

        with transaction.atomic():
            override_type_ids = set(buy_override_map.keys())
            stock_items = list(
                config.stock_items.select_for_update()
                .filter(type_id__in=submitted_type_ids, quantity__gt=0)
                .filter(Q(jita_buy_price__gt=0) | Q(type_id__in=override_type_ids))
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

                if blueprint_variant == "bpc":
                    unit_price = Decimal("0")
                else:
                    unit_price, _default_unit_price, _has_override = (
                        _compute_effective_buy_unit_price(
                            stock_item=stock_item,
                            buy_override_map=buy_override_map,
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

            # Get order reference from client (generated in JavaScript)
            client_order_ref = request.POST.get("order_reference", "").strip()

            # Create ONE order with ALL items
            order = MaterialExchangeBuyOrder.objects.create(
                config=config,
                buyer=request.user,
                status=MaterialExchangeBuyOrder.Status.DRAFT,
                order_reference=client_order_ref if client_order_ref else None,
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

    stock_blueprint_variants = _get_buy_stock_blueprint_variant_map(
        config=config,
        type_ids={int(item.type_id) for item in stock_items},
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
        type_ids={int(item.type_id) for item in stock_items},
    )
    for stock_item in stock_items:
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
    for stock_item in stock_items:
        stock_meta_by_type[int(stock_item.type_id)] = {
            "type_id": int(stock_item.type_id),
            "base_type_name": str(stock_item.type_name or get_type_name(int(stock_item.type_id))),
            "display_type_name": str(stock_item.display_type_name or stock_item.type_name or ""),
            "blueprint_variant": str(stock_item.blueprint_variant or ""),
            "display_sell_price_to_member": stock_item.display_sell_price_to_member,
            "default_sell_price_to_member": stock_item.default_sell_price_to_member,
            "has_buy_price_override": bool(stock_item.has_buy_price_override),
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
        stock_rows = _build_buy_material_rows(
            scoped_assets=scoped_buy_assets,
            stock_meta_by_type=stock_meta_by_type,
            buy_name_map=buy_name_map,
            fallback_location_label=buy_locations_label,
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
                    "depth": 0,
                    "container_path": "",
                    "indent_padding_rem": 0,
                    "form_quantity_field_name": f"qty_{int(stock_item.type_id)}_{variant_token}_{int(index)}",
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
        messages.warning(request, _("Material Exchange is disabled."))
        return redirect("indy_hub:material_exchange_index")

    if not _get_material_exchange_config():
        messages.warning(request, _("Material Exchange is not configured."))
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
        messages.warning(request, _("Material Exchange is disabled."))
        return redirect("indy_hub:material_exchange_index")

    if not _get_material_exchange_config():
        messages.warning(request, _("Material Exchange is not configured."))
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

    # Redirect back to buy page or referrer
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
        for item in order.items.all():
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
            )

            # Update stock (add quantity)
            stock_item, _created = MaterialExchangeStock.objects.get_or_create(
                config=order.config,
                type_id=item.type_id,
                defaults={"type_name": item.type_name},
            )
            stock_item.quantity += item.quantity
            stock_item.save()

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
        for item in order.items.all():
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
            )

            try:
                stock_item = order.config.stock_items.get(type_id=item.type_id)
                stock_item.quantity = max(stock_item.quantity - item.quantity, 0)
                stock_item.save()
            except MaterialExchangeStock.DoesNotExist:
                continue


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
        messages.warning(request, _("Material Exchange is disabled."))
        return redirect("indy_hub:material_exchange_index")

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, _("Material Exchange is not configured."))
        return redirect("indy_hub:material_exchange_index")

    # Filters
    transaction_type = request.GET.get("type", "")  # 'sell', 'buy', or ''
    user_filter = request.GET.get("user", "")

    transactions_qs = config.transactions.select_related(
        "user", "sell_order", "buy_order"
    ).prefetch_related("sell_order__items", "buy_order__items")

    if transaction_type:
        transactions_qs = transactions_qs.filter(transaction_type=transaction_type)
    if user_filter:
        transactions_qs = transactions_qs.filter(user__username__icontains=user_filter)

    transactions_qs = transactions_qs.order_by("-completed_at")

    # Pagination
    paginator = Paginator(transactions_qs, 50)
    page_number = request.GET.get("page", 1)
    page_obj = paginator.get_page(page_number)

    transactions = list(page_obj.object_list)

    for tx in transactions:
        if tx.sell_order_id:
            order = tx.sell_order
            tx.has_linked_order = True
            tx.order_reference = order.order_reference or f"SELL-{tx.sell_order_id}"
            tx.order_items = list(order.items.all())
        elif tx.buy_order_id:
            order = tx.buy_order
            tx.has_linked_order = True
            tx.order_reference = order.order_reference or f"BUY-{tx.buy_order_id}"
            tx.order_items = list(order.items.all())
        else:
            tx.has_linked_order = False
            tx.order_reference = ""
            tx.order_items = []

        if not tx.order_items:
            tx.order_items = [tx]

        tx.order_item_count = len(tx.order_items)
        tx.order_total_price = sum(
            (item.total_price for item in tx.order_items),
            Decimal("0"),
        )

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
    """Monthly statistics history for Material Exchange transactions."""
    if not _is_material_exchange_enabled():
        messages.warning(request, _("Material Exchange is disabled."))
        return redirect("indy_hub:material_exchange_index")

    config = _get_material_exchange_config()
    if not config:
        messages.warning(request, _("Material Exchange is not configured."))
        return redirect("indy_hub:material_exchange_index")

    period_options = [
        ("1m", _("This month")),
        ("3m", _("Last 3 months")),
        ("6m", _("Last 6 months")),
        ("12m", _("Last 12 months")),
        ("24m", _("Last 24 months")),
        ("all", _("All time")),
    ]
    period_months_map = {
        "1m": 1,
        "3m": 3,
        "6m": 6,
        "12m": 12,
        "24m": 24,
    }
    selected_period = request.GET.get("period", "all")
    if selected_period not in {key for key, _ in period_options}:
        selected_period = "all"

    filtered_transactions = config.transactions.all()
    period_start = None
    if selected_period in period_months_map:
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
        filtered_transactions = filtered_transactions.filter(
            completed_at__gte=period_start
        )

    monthly_rows = (
        filtered_transactions.annotate(month=TruncMonth("completed_at"))
        .values("month")
        .annotate(
            total_sell_volume=Sum(
                "total_price", filter=Q(transaction_type="sell"), default=0
            ),
            total_buy_volume=Sum(
                "total_price", filter=Q(transaction_type="buy"), default=0
            ),
            sell_orders=Count("id", filter=Q(transaction_type="sell")),
            buy_orders=Count("id", filter=Q(transaction_type="buy")),
        )
        .order_by("month")
    )

    chart_labels = []
    buy_volumes = []
    sell_volumes = []
    transaction_counts = []

    total_buy_volume = Decimal("0")
    total_sell_volume = Decimal("0")
    total_transactions = 0

    for row in monthly_rows:
        month = row.get("month")
        if not month:
            continue
        buy_volume = row.get("total_buy_volume") or Decimal("0")
        sell_volume = row.get("total_sell_volume") or Decimal("0")
        buy_count = row.get("buy_orders") or 0
        sell_count = row.get("sell_orders") or 0

        chart_labels.append(month.strftime("%Y-%m"))
        buy_volumes.append(float(buy_volume))
        sell_volumes.append(float(sell_volume))
        transaction_counts.append(buy_count + sell_count)

        total_buy_volume += buy_volume
        total_sell_volume += sell_volume
        total_transactions += buy_count + sell_count

    user_stats = (
        filtered_transactions.values("user__username")
        .annotate(
            buy_volume=Sum("total_price", filter=Q(transaction_type="buy"), default=0),
            sell_volume=Sum(
                "total_price", filter=Q(transaction_type="sell"), default=0
            ),
            buy_orders=Count("id", filter=Q(transaction_type="buy")),
            sell_orders=Count("id", filter=Q(transaction_type="sell")),
        )
        .order_by("user__username")
    )

    user_rows = []
    for row in user_stats:
        buy_volume = row.get("buy_volume") or Decimal("0")
        sell_volume = row.get("sell_volume") or Decimal("0")
        buy_orders = row.get("buy_orders") or 0
        sell_orders = row.get("sell_orders") or 0

        user_rows.append(
            {
                "username": row.get("user__username") or "-",
                "buy_volume": buy_volume,
                "sell_volume": sell_volume,
                "buy_orders": buy_orders,
                "sell_orders": sell_orders,
                "total_orders": buy_orders + sell_orders,
                "net_flow": buy_volume - sell_volume,
            }
        )

    top_user_stats = sorted(
        user_rows,
        key=lambda item: item["buy_volume"] + item["sell_volume"],
        reverse=True,
    )[:10]

    context = {
        "config": config,
        "chart_labels": chart_labels,
        "buy_volumes": buy_volumes,
        "sell_volumes": sell_volumes,
        "transaction_counts": transaction_counts,
        "months_count": len(chart_labels),
        "total_buy_volume": total_buy_volume,
        "total_sell_volume": total_sell_volume,
        "total_transactions": total_transactions,
        "top_user_stats": top_user_stats,
        "period_options": period_options,
        "selected_period": selected_period,
        "period_start": period_start,
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
    """Helper to build navigation context for Material Exchange."""
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
