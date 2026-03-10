"""Material Exchange Configuration views."""

# Standard Library
import hashlib
import json
from datetime import timedelta
from decimal import Decimal, InvalidOperation

# Django
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.cache import cache
from django.db import transaction
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger
from esi.views import sso_redirect

# AA Example App
from indy_hub.services.providers import esi_provider

from ..app_settings import ROLE_SNAPSHOT_STALE_HOURS
from ..decorators import indy_hub_permission_required, tokens_required
from ..models import (
    CharacterRoles,
    MaterialExchangeConfig,
    MaterialExchangeItemPriceOverride,
    MaterialExchangeSettings,
)
from ..services.asset_cache import (
    get_corp_assets_cached,
    get_corp_divisions_cached,
    resolve_structure_names,
)
from ..services.esi_client import ESIUnmodifiedError
from ..utils.analytics import emit_view_analytics_event
from ..utils.eve import PLACEHOLDER_PREFIX, get_type_name

esi = esi_provider
logger = get_extension_logger(__name__)
MARKET_GROUP_CHOICE_DEPTH = 1
MARKET_GROUP_SEARCH_ITEMS_PER_GROUP = 100
NON_EXPANDABLE_GRANULAR_GROUPS = {
    "Skills",
    "Special Edition Assets",
    "Structure Equipment",
}
MARKET_GROUP_MAJOR_ORDER = [
    "Ammunition & Charges",
    "Apparel",
    "Blueprints & Reactions",
    "Drones",
    "Implants & Boosters",
    "Manufacture & Research",
    "Personalization",
    "Pilot's Services",
    "Planetary Infrastructure",
    "Ship and Module Modifications",
    "Ship Equipment",
    "Ship SKINs",
    "Ships",
    "Skills",
    "Special Edition Assets",
    "Structure Equipment",
    "Structure Modifications",
    "Structures",
    "Trade Goods",
]
MARKET_GROUP_GRANULAR_CHILDREN: dict[str, list[str]] = {
    "Ammunition & Charges": [
        "Bombs",
        "Breacher Pods",
        "Cap Booster Charges",
        "Command Burst Charges",
        "Condenser Packs",
        "Exotic Plasma Charges",
        "Frequency Crystals",
        "Hybrid Charges",
        "Mining Crystals",
        "Missiles",
        "Nanite Repair Paste",
        "Probes",
        "Projectile Ammo",
        "Scripts",
        "Structure Area Denial Ammunition",
        "Structure Guided Bombs",
    ],
    "Apparel": ["Accessories", "Men's Clothing", "Women's Clothing"],
    "Blueprints & Reactions": [
        "Ammunition & Charges",
        "Drones",
        "Manufacture & Research",
        "Reaction Formulas",
        "Ship Equipment",
        "Ship Modifications",
        "Ships",
        "Structure Equipment",
        "Structure Modifications",
        "Structures",
    ],
    "Drones": [
        "Combat Drones",
        "Combat Utility Drones",
        "Electronic Warfare Drones",
        "Fighters",
        "Logistic Drones",
        "Mining Drones",
        "Salvage Drones",
    ],
    "Implants & Boosters": ["Booster", "Cerebral Accelerators", "Implants"],
    "Manufacture & Research": ["Components", "Materials", "Research Equipment"],
    "Personalization": ["Design Elements", "Sequencing Binders"],
    "Pilot's Services": [
        "Expert Systems",
        "HyperNet Relay",
        "Pilot's Services",
        "PLEX",
        "Skill Trading",
    ],
    "Planetary Infrastructure": ["Command Centers", "Orbital Infrastructure"],
    "Ship and Module Modifications": ["Mutaplasmids", "Rigs", "Subsystems"],
    "Ship Equipment": [
        "Compressors",
        "Drone Upgrades",
        "Electronic Warfare",
        "Electronics and Sensor Upgrades",
        "Engineering Equipment",
        "Fleet Assistance Modules",
        "Harvest Equipment",
        "Hull & Armor",
        "Propulsion",
        "Scanning Equipment",
        "Shield",
        "Smartbombs",
        "Turrets & Launchers",
    ],
    "Ship SKINs": [
        "Battlecruisers",
        "Battleships",
        "Capital Ships",
        "Capsules",
        "Corvettes",
        "Cruisers",
        "Destroyers",
        "Frigates",
        "Haulers and Industrial Ships",
        "Mining Barges",
        "Multiple Hull SKINs",
        "Shuttles",
    ],
    "Ships": [
        "Battlecruisers",
        "Battleships",
        "Capital Ships",
        "Corvettes",
        "Cruisers",
        "Destroyers",
        "Frigates",
        "Haulers and Industrial Ships",
        "Mining Barges",
        "Shuttles",
        "Special Edition Ships",
    ],
    "Structure Modifications": [
        "Structure Combat Rigs",
        "Structure Engineering Rigs",
        "Structure Resource Processing Rigs",
    ],
    "Structures": [
        "Citadels",
        "Deployable Structures",
        "Engineering Complexes",
        "FLEX Structures",
        "Refineries",
        "Sovereignty Structures",
        "Starbase Structures",
    ],
    "Trade Goods": [
        "Acceleration Gate Keys",
        "AEGIS Databases",
        "Aurum Tokens",
        "Bounty Encrypted Bonds",
        "Consumer Products",
        "Covert Research Tools",
        "Criminal Dog Tags",
        "Criminal Evidence",
        "Filaments",
        "Industrial Goods",
        "Insignias",
        "Limited Rarities",
        "Narcotics",
        "Nexus Chips",
        "Passengers",
        "Political Paraphernalia",
        "Radioactive Goods",
        "Rogue Drone Data",
        "Security Tags",
        "Sleeper Components",
        "Starbase Charters",
        "Strong Boxes",
        "Triglavian Data",
        "Unknown Components",
    ],
}
MARKET_GROUP_GRANULAR_GRANDCHILDREN: dict[str, dict[str, list[str]]] = {
    "Manufacture & Research": {
        "Components": [
            "Advanced Capital Components",
            "Advanced Components",
            "Fuel Blocks",
            "Protective Components",
            "R.A.M.",
            "Standard Capital Ship Components",
            "Structure Components",
            "Subsystem Components",
        ],
        "Materials": [
            "Advanced Protective Technology",
            "Atavum",
            "Colony Reagents",
            "Faction Materials",
            "Gas Clouds Materials",
            "Ice Products",
            "Infomorph Systems",
            "Minerals",
            "Molecular-Forging Tools",
            "Named Components",
            "Planetary Materials",
            "Raw Materials",
            "Reaction Materials",
            "Salvage Materials",
        ],
        "Research Equipment": [
            "Ancient Relics",
            "Datacores",
            "Decryptors",
            "R.Db",
        ],
    }
}


def _normalize_market_group_name(raw_value: str) -> str:
    return " ".join(str(raw_value or "").replace("&amp;", "&").split()).strip().casefold()


def _find_market_group_id_by_name(
    *,
    all_groups: dict[int, dict[str, str | int | None]],
    group_name: str,
    parent_id: int | None = None,
) -> int | None:
    target_name = _normalize_market_group_name(group_name)
    if not target_name:
        return None

    parent_id_value = int(parent_id) if isinstance(parent_id, int) else None
    matches = [
        int(group_id)
        for group_id, payload in all_groups.items()
        if _normalize_market_group_name(payload.get("name", "")) == target_name
    ]
    if not matches:
        return None

    if parent_id is None:
        root_matches = [
            gid
            for gid in matches
            if payload_parent_id_is_none(
                all_groups.get(gid, {}).get("parent_market_group_id", None)
            )
        ]
        if root_matches:
            return int(sorted(root_matches)[0])
        return int(sorted(matches)[0])

    direct_matches = [
        gid
        for gid in matches
        if int(all_groups.get(gid, {}).get("parent_market_group_id") or 0)
        == int(parent_id_value)
    ]
    if direct_matches:
        return int(sorted(direct_matches)[0])

    # Fallback: nearest descendant under the requested parent branch.
    for gid in sorted(matches):
        path_ids = _get_market_group_path_ids(gid, all_groups)
        if int(parent_id_value) in path_ids:
            return int(gid)
    return int(sorted(matches)[0])


def payload_parent_id_is_none(parent_id) -> bool:
    return parent_id in (None, "", 0, "0")


def _get_market_group_tree() -> list[dict[str, object]]:
    """Return curated market-group tree for config UI."""

    cache_key = "indy_hub:material_exchange:market_group_tree:v2"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    all_groups = _build_market_group_index()
    tree: list[dict[str, object]] = []
    if not all_groups:
        cache.set(cache_key, tree, 3600)
        return tree

    for major_label in MARKET_GROUP_MAJOR_ORDER:
        major_group_id = _find_market_group_id_by_name(
            all_groups=all_groups,
            group_name=major_label,
            parent_id=None,
        )
        if not major_group_id:
            continue

        children: list[dict[str, object]] = []
        if major_label not in NON_EXPANDABLE_GRANULAR_GROUPS:
            major_grandchild_map = MARKET_GROUP_GRANULAR_GRANDCHILDREN.get(
                major_label, {}
            )
            for child_label in MARKET_GROUP_GRANULAR_CHILDREN.get(major_label, []):
                child_group_id = _find_market_group_id_by_name(
                    all_groups=all_groups,
                    group_name=child_label,
                    parent_id=int(major_group_id),
                )
                if not child_group_id:
                    continue

                grand_children: list[dict[str, object]] = []
                for grandchild_label in major_grandchild_map.get(child_label, []):
                    grandchild_group_id = _find_market_group_id_by_name(
                        all_groups=all_groups,
                        group_name=grandchild_label,
                        parent_id=int(child_group_id),
                    )
                    if not grandchild_group_id:
                        continue
                    grand_children.append(
                        {
                            "id": int(grandchild_group_id),
                            "label": str(grandchild_label),
                            "expandable": False,
                            "children": [],
                        }
                    )

                children.append(
                    {
                        "id": int(child_group_id),
                        "label": str(child_label),
                        "expandable": bool(grand_children),
                        "children": grand_children,
                    }
                )

        tree.append(
            {
                "id": int(major_group_id),
                "label": str(major_label),
                "expandable": bool(
                    major_label not in NON_EXPANDABLE_GRANULAR_GROUPS and children
                ),
                "children": children,
            }
        )

    cache.set(cache_key, tree, 3600)
    return tree


def _collect_market_group_tree_ids(
    market_group_tree: list[dict[str, object]],
) -> set[int]:
    """Return all IDs included in the curated tree (all depths)."""

    ids: set[int] = set()
    stack = list(market_group_tree or [])
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        try:
            node_id = int(node.get("id"))
        except (TypeError, ValueError):
            node_id = 0
        if node_id > 0:
            ids.add(node_id)

        raw_children = node.get("children", []) or []
        if isinstance(raw_children, list):
            for child in raw_children:
                if isinstance(child, dict):
                    stack.append(child)
    return ids


@login_required
@indy_hub_permission_required("can_manage_material_hub")
def material_exchange_request_divisions_token(request):
    """Request ESI token with divisions scope, then redirect back to config."""
    emit_view_analytics_event(
        view_name="material_exchange_config.request_divisions_token",
        request=request,
    )
    return sso_redirect(
        request,
        scopes="esi-corporations.read_divisions.v1",
        return_to="indy_hub:material_exchange_config",
    )


@login_required
@indy_hub_permission_required("can_manage_material_hub")
def material_exchange_request_all_scopes(request):
    """
    emit_view_analytics_event(
        view_name="material_exchange_config.request_all_scopes", request=request
    )
    Request all Material Exchange required ESI scopes at once.

    Required scopes:
    - esi-assets.read_corporation_assets.v1 (for structures)
    - esi-corporations.read_divisions.v1 (for hangar divisions)
    - esi-contracts.read_corporation_contracts.v1 (for contract validation)
    - esi-universe.read_structures.v1 (for structure names)
    """
    scopes = " ".join(
        [
            "esi-assets.read_corporation_assets.v1",
            "esi-corporations.read_divisions.v1",
            "esi-contracts.read_corporation_contracts.v1",
            "esi-universe.read_structures.v1",
        ]
    )
    return sso_redirect(
        request,
        scopes=scopes,
        return_to="indy_hub:material_exchange_config",
    )


@login_required
@indy_hub_permission_required("can_manage_material_hub")
def material_exchange_request_contracts_scope(request):
    """Request ESI token with contracts scope, then redirect back to config."""
    emit_view_analytics_event(
        view_name="material_exchange_config.request_contracts_scope", request=request
    )
    return sso_redirect(
        request,
        scopes="esi-contracts.read_corporation_contracts.v1",
        return_to="indy_hub:material_exchange_config",
    )


def _get_token_for_corp(user, corp_id, scope, require_corporation_token: bool = False):
    """Return a valid token for the given corp that has the scope.

    If require_corporation_token is True, only return corporation-type tokens
    that belong to the selected corporation. Otherwise, prefer those and
    fall back to a character token that belongs to the corp.
    """
    # Alliance Auth
    from allianceauth.eveonline.models import EveCharacter
    from esi.models import Token

    # Important: require_scopes expects an iterable of scopes
    tokens = Token.objects.filter(user=user).require_scopes([scope]).require_valid()
    tokens = list(tokens)
    if not tokens:
        logger.debug(
            f"_get_token_for_corp: user={user.username}, corp_id={corp_id}, scope={scope} -> no valid tokens with scope"
        )
    else:
        logger.debug(
            f"_get_token_for_corp: user={user.username}, corp_id={corp_id}, "
            f"scope={scope}, require_corp={require_corporation_token}, "
            f"found {len(tokens)} valid tokens with scope"
        )

    def _character_matches(token) -> bool:
        char_id = getattr(token, "character_id", None)
        if not char_id:
            return False
        # Prefer cached character relation if available to avoid ESI calls
        try:
            char_obj = getattr(token, "character", None)
            if char_obj and getattr(char_obj, "corporation_id", None) is not None:
                return int(char_obj.corporation_id) == int(corp_id)
        except Exception:
            pass
        try:
            stored = EveCharacter.objects.get_character_by_id(int(char_id))
            if stored is None:
                stored = EveCharacter.objects.create_character(int(char_id))
            if stored and getattr(stored, "corporation_id", None) is not None:
                return int(stored.corporation_id) == int(corp_id)
        except Exception:
            return False

    # Prefer corporation tokens that belong to the selected corp
    for token in tokens:
        if getattr(token, "token_type", "") != Token.TOKEN_TYPE_CORPORATION:
            continue
        corp_attr = getattr(token, "corporation_id", None)
        logger.debug(
            f"  Checking corp token id={token.id}: corp_attr={corp_attr}, "
            f"type={getattr(token, 'token_type', '')}, char_id={token.character_id}"
        )
        if corp_attr is not None and int(corp_attr) == int(corp_id):
            logger.info(
                f"Found matching corp token id={token.id} for corp_id={corp_id}"
            )
            return token
        # For corp tokens missing corp_attr, accept if backing character belongs to corp
        if corp_attr is None and _character_matches(token):
            return token

    # If a corporation token is required, still try character tokens as fallback
    # (character tokens from the corp can still access corp endpoints if the character has roles)
    for token in tokens:
        if _character_matches(token):
            logger.info(
                f"Using character token id={token.id} (char_id={token.character_id}) for corp_id={corp_id}"
            )
            return token

    # No suitable token for this corporation
    logger.warning(
        f"No token found (corp or character): user={user.username}, corp_id={corp_id}, "
        f"scope={scope}, checked {len(tokens)} tokens"
    )
    return None


@login_required
@indy_hub_permission_required("can_manage_material_hub")
@tokens_required(scopes="esi-characters.read_corporation_roles.v1")
def material_exchange_config(request, tokens):
    emit_view_analytics_event(
        view_name="material_exchange_config.page", request=request
    )
    """
    Material Exchange configuration page.
    Allows admins to configure corp, structure, and pricing.
    """
    config = MaterialExchangeConfig.objects.first()

    # Get available corporations from user's ESI tokens
    available_corps = _get_user_corporations(request.user)

    # Do NOT load structures on initial page load - wait for AJAX after corp selection
    available_structures = []
    hangar_divisions = {}
    division_scope_missing = False
    assets_scope_missing = False
    current_corp_ticker = ""
    current_hangar_name = ""

    if config and getattr(config, "corporation_id", None):
        try:
            hangar_divisions, division_scope_missing = _get_corp_hangar_divisions(
                request.user, config.corporation_id
            )
            current_hangar_name = hangar_divisions.get(
                int(config.hangar_division),
                f"Hangar Division {config.hangar_division}",
            )
        except Exception:
            current_hangar_name = f"Hangar Division {config.hangar_division}"

        for corp in available_corps:
            if corp.get("id") == config.corporation_id:
                current_corp_ticker = corp.get("ticker", "")
                break

    if request.method == "POST":
        if request.POST.get("delete_config") == "1":
            if config:
                config.delete()
                messages.success(request, _("Configuration deleted."))
            else:
                messages.info(request, _("No configuration to delete."))
            return redirect("indy_hub:material_exchange_config")
        return _handle_config_save(request, config)

    market_group_choices: list[dict[str, str | int]] = []
    market_group_tree: list[dict[str, object]] = []
    allowed_choice_ids: set[int] = set()
    try:
        market_group_tree = _get_market_group_tree()
        allowed_choice_ids = _collect_market_group_tree_ids(market_group_tree)
        market_group_choices = [
            {"id": int(node.get("id")), "label": str(node.get("label"))}
            for node in market_group_tree
            if node.get("id")
        ]
    except Exception as exc:
        logger.warning("Failed to build market group tree: %s", exc)

    all_groups = _build_market_group_index()
    market_group_override_choices: list[dict[str, object]] = []
    if all_groups and allowed_choice_ids:
        market_group_override_choices = [
            {
                "id": int(group_id),
                "path": _build_market_group_path_label(int(group_id), all_groups),
            }
            for group_id in sorted(
                allowed_choice_ids,
                key=lambda gid: _build_market_group_path_label(
                    int(gid), all_groups
                ).lower(),
            )
            if int(group_id) in all_groups
        ]

    def _normalize_selected_group_ids_for_ui(raw_group_ids) -> list[int]:
        normalized: set[int] = set()
        for raw_group_id in raw_group_ids or []:
            try:
                group_id = int(raw_group_id)
            except (TypeError, ValueError):
                continue
            if group_id <= 0:
                continue
            if not allowed_choice_ids:
                normalized.add(group_id)
                continue
            if group_id in allowed_choice_ids:
                normalized.add(group_id)
                continue
            path_ids = _get_market_group_path_ids(group_id, all_groups)
            for path_id in reversed(path_ids):
                if int(path_id) in allowed_choice_ids:
                    normalized.add(int(path_id))
                    break
        return sorted(normalized)
    selected_market_groups_buy = (
        _normalize_selected_group_ids_for_ui(
            list(getattr(config, "allowed_market_groups_buy", []) or [])
        )
        if config
        else []
    )
    selected_market_groups_sell = (
        _normalize_selected_group_ids_for_ui(
            list(getattr(config, "allowed_market_groups_sell", []) or [])
        )
        if config
        else []
    )
    selected_sell_market_groups_by_structure: dict[str, list[int] | None] = {}

    selected_sell_structures: list[dict[str, object]] = []
    selected_buy_structures: list[dict[str, object]] = []
    buy_enabled = True
    allow_fitted_ships = False
    location_match_mode = "name_or_id"
    if config:
        buy_enabled = bool(getattr(config, "buy_enabled", True))
        allow_fitted_ships = bool(getattr(config, "allow_fitted_ships", False))
        location_match_mode = getattr(config, "location_match_mode", None) or "name_or_id"
        sell_ids = config.get_sell_structure_ids(include_primary=False)
        buy_ids = config.get_buy_structure_ids(include_primary=False)
        sell_name_map = config.get_sell_structure_name_map()
        buy_name_map = config.get_buy_structure_name_map()
        selected_sell_structures = [
            {"id": int(sid), "name": sell_name_map.get(int(sid), "")}
            for sid in sell_ids
        ]
        selected_buy_structures = [
            {"id": int(sid), "name": buy_name_map.get(int(sid), "")}
            for sid in buy_ids
        ]
        sell_group_map = config.get_sell_market_group_map()
        if sell_group_map:
            for sid in sell_ids:
                sid_int = int(sid)
                if sid_int not in sell_group_map:
                    continue
                groups = sell_group_map[sid_int]
                if groups is None:
                    selected_sell_market_groups_by_structure[str(sid_int)] = None
                    continue
                selected_sell_market_groups_by_structure[str(sid_int)] = (
                    _normalize_selected_group_ids_for_ui(groups)
                )
        elif selected_market_groups_sell:
            fallback_sell_groups = _normalize_selected_group_ids_for_ui(
                selected_market_groups_sell
            )
            for sid in sell_ids:
                selected_sell_market_groups_by_structure[str(int(sid))] = list(
                    fallback_sell_groups
                )

    market_group_search_index = {}
    try:
        market_group_search_index = _get_market_group_search_index_for_ids(
            allowed_choice_ids
        )
    except Exception as exc:
        logger.warning("Failed to build market group search index: %s", exc)

    item_price_overrides: list[dict[str, object]] = []
    item_override_type_choices: list[dict[str, object]] = []
    market_group_price_overrides: list[dict[str, object]] = []
    if config:
        override_rows = list(
            config.item_price_overrides.values(
                "type_id",
                "type_name",
                "sell_markup_percent_override",
                "sell_markup_base_override",
                "buy_markup_percent_override",
                "buy_markup_base_override",
                "sell_price_override",
                "buy_price_override",
            )
        )
        for row in override_rows:
            type_id = int(row.get("type_id") or 0)
            if type_id <= 0:
                continue
            type_name = str(row.get("type_name") or "").strip() or get_type_name(type_id)
            item_price_overrides.append(
                {
                    "type_id": type_id,
                    "type_name": type_name,
                    "sell_markup_percent_override": row.get(
                        "sell_markup_percent_override"
                    ),
                    "sell_markup_base_override": row.get("sell_markup_base_override"),
                    "buy_markup_percent_override": row.get(
                        "buy_markup_percent_override"
                    ),
                    "buy_markup_base_override": row.get("buy_markup_base_override"),
                    # Legacy fixed-price values are still surfaced so existing rows can be
                    # migrated by admins during normal config edits.
                    "sell_price_override": row.get("sell_price_override"),
                    "buy_price_override": row.get("buy_price_override"),
                }
            )

        choice_map: dict[int, str] = {}
        for stock_row in config.stock_items.values("type_id", "type_name"):
            type_id = int(stock_row.get("type_id") or 0)
            if type_id <= 0:
                continue
            type_name = str(stock_row.get("type_name") or "").strip() or get_type_name(type_id)
            choice_map[type_id] = type_name
        for row in item_price_overrides:
            type_id = int(row["type_id"])
            if type_id not in choice_map:
                choice_map[type_id] = str(row.get("type_name") or "").strip() or get_type_name(type_id)

        item_override_type_choices = [
            {"id": int(type_id), "name": str(type_name)}
            for type_id, type_name in sorted(
                choice_map.items(), key=lambda pair: pair[1].lower()
            )
        ]

        raw_group_override_rows = list(
            getattr(config, "market_group_price_overrides", []) or []
        )
        parsed_group_overrides: dict[int, dict[str, object]] = {}
        for row in raw_group_override_rows:
            if not isinstance(row, dict):
                continue
            try:
                market_group_id = int(
                    row.get("market_group_id") or row.get("group_id") or 0
                )
            except (TypeError, ValueError):
                continue
            if market_group_id <= 0:
                continue

            market_group_path = (
                str(
                    row.get("market_group_path")
                    or row.get("group_path")
                    or _build_market_group_path_label(market_group_id, all_groups)
                ).strip()
                or f"Group {market_group_id}"
            )
            parsed_group_overrides[market_group_id] = {
                "market_group_id": market_group_id,
                "market_group_path": market_group_path,
                "sell_markup_percent_override": row.get("sell_markup_percent_override"),
                "sell_markup_base_override": row.get("sell_markup_base_override"),
                "buy_markup_percent_override": row.get("buy_markup_percent_override"),
                "buy_markup_base_override": row.get("buy_markup_base_override"),
                "sell_price_override": row.get("sell_price_override"),
                "buy_price_override": row.get("buy_price_override"),
            }
        market_group_price_overrides = sorted(
            parsed_group_overrides.values(),
            key=lambda payload: str(payload.get("market_group_path") or "").lower(),
        )

    context = {
        "config": config,
        "available_corps": available_corps,
        "available_structures": available_structures,
        "assets_scope_missing": assets_scope_missing,
        "current_corp_ticker": current_corp_ticker,
        "current_hangar_name": current_hangar_name,
        "hangar_divisions": (
            hangar_divisions
            if (hangar_divisions or division_scope_missing)
            else {i: f"Hangar Division {i}" for i in range(1, 8)}
        ),
        "division_scope_missing": division_scope_missing,
        "market_group_choices": market_group_choices,
        "market_group_tree": market_group_tree,
        "selected_market_groups_buy": selected_market_groups_buy,
        "selected_market_groups_sell": selected_market_groups_sell,
        "selected_sell_market_groups_by_structure": selected_sell_market_groups_by_structure,
        "market_group_search_index": market_group_search_index,
        "selected_sell_structures": selected_sell_structures,
        "selected_buy_structures": selected_buy_structures,
        "buy_enabled": buy_enabled,
        "allow_fitted_ships": allow_fitted_ships,
        "location_match_mode": location_match_mode,
        "item_price_overrides": item_price_overrides,
        "item_override_type_choices": item_override_type_choices,
        "market_group_override_choices": market_group_override_choices,
        "market_group_price_overrides": market_group_price_overrides,
    }

    from .navigation import build_nav_context

    context.update(
        build_nav_context(
            request.user,
            active_tab="material_hub",
            can_manage_corp=request.user.has_perm(
                "indy_hub.can_manage_corp_bp_requests"
            ),
        )
    )
    context["back_to_overview_url"] = reverse("indy_hub:index")
    context["material_exchange_enabled"] = (
        MaterialExchangeSettings.get_solo().is_enabled
    )

    return render(request, "indy_hub/material_exchange/config.html", context)


@login_required
@indy_hub_permission_required("can_manage_material_hub")
def material_exchange_toggle_active(request):
    emit_view_analytics_event(
        view_name="material_exchange_config.toggle_active", request=request
    )
    """Toggle Material Exchange availability from settings page."""

    if request.method != "POST":
        return redirect("indy_hub:settings_hub")

    next_url = request.POST.get("next") or reverse("indy_hub:settings_hub")
    settings_obj = MaterialExchangeSettings.get_solo()

    desired_active = request.POST.get("is_active") == "on"
    if settings_obj.is_enabled == desired_active:
        messages.info(
            request,
            _("No change: Material Exchange is already {state}.").format(
                state=_("enabled") if settings_obj.is_enabled else _("disabled")
            ),
        )
        return redirect(next_url)

    settings_obj.is_enabled = desired_active
    settings_obj.save(update_fields=["is_enabled", "updated_at"])
    try:
        config = MaterialExchangeConfig.objects.first()
        if config and config.is_active != desired_active:
            config.is_active = desired_active
            config.save(update_fields=["is_active", "updated_at"])
    except Exception:
        pass
    try:
        # Third Party
        from django_celery_beat.models import PeriodicTask

        PeriodicTask.objects.filter(name="indy-hub-material-exchange-cycle").update(
            enabled=desired_active
        )
    except Exception:
        # Beat not installed or table missing; ignore
        pass
    if desired_active:
        messages.success(request, _("Material Exchange enabled."))
    else:
        messages.success(request, _("Material Exchange disabled."))

    return redirect(next_url)


@login_required
@indy_hub_permission_required("can_manage_material_hub")
@tokens_required(
    scopes="esi-assets.read_corporation_assets.v1 esi-corporations.read_divisions.v1"
)
def material_exchange_get_structures(request, tokens, corp_id):
    emit_view_analytics_event(
        view_name="material_exchange_config.get_structures", request=request
    )
    """
    AJAX endpoint to get structures for a given corporation.
    Returns JSON list of structures.
    """
    # Django
    from django.http import JsonResponse

    structures, assets_scope_missing = _get_corp_structures(request.user, corp_id)
    hangar_divisions, division_scope_missing = _get_corp_hangar_divisions(
        request.user, corp_id
    )

    return JsonResponse(
        {
            "structures": [
                {"id": s["id"], "name": s["name"], "flags": s.get("flags", [])}
                for s in structures
            ],
            "hangar_divisions": hangar_divisions,
            "division_scope_missing": division_scope_missing,
            "assets_scope_missing": assets_scope_missing,
        }
    )


def _find_director_character(user, corp_id):
    """Find a character with DIRECTOR role in the given corporation.

    Returns the character_id or None if not found.
    """
    # Alliance Auth
    from allianceauth.eveonline.models import EveCharacter
    from esi.models import Token

    # AA Example App
    from indy_hub.services.esi_client import shared_client

    logger.warning(
        "Looking for DIRECTOR character in corp %s for user %s", corp_id, user.username
    )

    # Get ALL character tokens for the user first
    try:
        all_tokens = Token.objects.filter(user=user).require_valid()
        all_tokens_list = list(all_tokens)
        logger.warning(
            "Found %s valid tokens for user %s: %s",
            len(all_tokens_list),
            user.username,
            [t.character_id for t in all_tokens_list],
        )
    except Exception as exc:
        logger.warning("Failed to get tokens for user %s: %s", user.username, exc)
        return None

    # Try tokens with the role-checking scope
    try:
        scoped_tokens = (
            Token.objects.filter(user=user)
            .require_scopes(["esi-characters.read_corporation_roles.v1"])
            .require_valid()
        )
        scoped_tokens_list = list(scoped_tokens)
        logger.warning(
            "Found %s tokens with role-checking scope for user %s: %s",
            len(scoped_tokens_list),
            user.username,
            [t.character_id for t in scoped_tokens_list],
        )
    except Exception as exc:
        logger.warning(
            "Failed to filter tokens by scope for user %s: %s", user.username, exc
        )
        scoped_tokens_list = []

    def _coerce_list(value: object) -> list[str]:
        if isinstance(value, (list, tuple)):
            return [str(item) for item in value if item]
        return []

    def _load_roles(character_id: int) -> list[str]:
        snapshot = CharacterRoles.objects.filter(character_id=character_id).first()
        snapshot_stale = bool(
            snapshot
            and (timezone.now() - snapshot.last_updated)
            >= timedelta(hours=ROLE_SNAPSHOT_STALE_HOURS)
        )
        if snapshot and not snapshot_stale:
            return [str(role).upper() for role in (snapshot.roles or []) if role]

        force_refresh = snapshot is None
        try:
            payload = shared_client.fetch_character_corporation_roles(
                character_id,
                force_refresh=force_refresh,
            )
        except ESIUnmodifiedError:
            if snapshot:
                return [str(role).upper() for role in (snapshot.roles or []) if role]
            return []
        except Exception as exc:
            logger.warning(
                "Failed to fetch roles for character %s: %s",
                character_id,
                exc,
            )
            return []

        if not isinstance(payload, dict):
            logger.warning(
                "Unexpected roles payload for character %s: %s",
                character_id,
                type(payload),
            )
            return []

        role_payload = {
            "roles": _coerce_list(payload.get("roles")),
            "roles_at_hq": _coerce_list(payload.get("roles_at_hq")),
            "roles_at_base": _coerce_list(payload.get("roles_at_base")),
            "roles_at_other": _coerce_list(payload.get("roles_at_other")),
        }
        CharacterRoles.objects.update_or_create(
            character_id=character_id,
            defaults={
                "owner_user": user,
                "corporation_id": int(corp_id) if corp_id else None,
                **role_payload,
            },
        )
        return [str(role).upper() for role in role_payload["roles"] if role]

    # Check scoped tokens first
    for token in scoped_tokens_list:
        try:
            character_id = token.character_id
            logger.warning(
                "Checking character %s from scoped token",
                character_id,
            )

            # Get the character from the database
            try:
                char = EveCharacter.objects.get(character_id=character_id)
                char_corp_id = int(char.corporation_id) if char.corporation_id else None
                logger.warning(
                    "Character %s is in corp %s (looking for %s)",
                    character_id,
                    char_corp_id,
                    corp_id,
                )
                if char_corp_id != int(corp_id):
                    logger.warning(
                        "Character %s is in corp %s, not %s - SKIPPING",
                        character_id,
                        char_corp_id,
                        corp_id,
                    )
                    continue
            except EveCharacter.DoesNotExist:
                logger.warning(
                    "Character %s not found in database",
                    character_id,
                )
                continue

            logger.warning(
                "Checking DIRECTOR role for character %s in corp %s",
                character_id,
                corp_id,
            )

            corp_roles = _load_roles(character_id)
            logger.warning("Character %s roles: %s", character_id, corp_roles)

            if "DIRECTOR" in corp_roles:
                logger.warning(
                    "Found DIRECTOR character %s for corporation %s",
                    character_id,
                    corp_id,
                )
                return character_id
            else:
                logger.warning(
                    "Character %s does NOT have Director role (has: %s)",
                    character_id,
                    corp_roles,
                )
        except Exception as exc:
            logger.warning(
                "Failed to check director role for character %s: %s",
                getattr(token, "character_id", "?"),
                exc,
            )
            continue

    # If no scoped tokens worked, try ALL tokens (they might have the scope but not filtered correctly)
    logger.warning(
        "No DIRECTOR found in scoped tokens, trying all tokens for user %s",
        user.username,
    )

    all_tokens = Token.objects.filter(user=user).require_valid()
    for token in all_tokens:
        try:
            character_id = token.character_id
            logger.warning(
                "Checking character %s from all tokens",
                character_id,
            )

            # Get the character from the database
            try:
                char = EveCharacter.objects.get(character_id=character_id)
                char_corp_id = int(char.corporation_id) if char.corporation_id else None
                if char_corp_id != int(corp_id):
                    continue
            except EveCharacter.DoesNotExist:
                continue

            logger.warning(
                "Checking DIRECTOR role for character %s (second pass)",
                character_id,
            )

            corp_roles = _load_roles(character_id)
            logger.warning(
                "Character %s roles (second pass): %s", character_id, corp_roles
            )

            if "DIRECTOR" in corp_roles:
                logger.warning(
                    "Found DIRECTOR character %s for corporation %s (second pass)",
                    character_id,
                    corp_id,
                )
                return character_id
            else:
                logger.warning(
                    "Character %s does NOT have Director role in second pass (has: %s)",
                    character_id,
                    corp_roles,
                )
        except Exception as exc:
            logger.warning(
                "Unexpected error checking character %s: %s",
                getattr(token, "character_id", "?"),
                exc,
            )
            continue

    logger.warning(
        "No DIRECTOR character found for user %s in corporation %s",
        user.username,
        corp_id,
    )
    return None


@login_required
@indy_hub_permission_required("can_manage_material_hub")
@tokens_required(
    scopes="esi-characters.read_corporation_roles.v1 esi-assets.read_corporation_assets.v1"
)
def material_exchange_refresh_corp_assets(request, tokens):
    emit_view_analytics_event(
        view_name="material_exchange_config.refresh_corp_assets", request=request
    )
    """
    AJAX endpoint to refresh corporation assets and structures.
    Triggers background task to fetch latest ESI data.
    """
    # Standard Library
    import json

    # Django
    from django.http import JsonResponse

    if request.method != "POST":
        return JsonResponse(
            {"success": False, "error": "Method not allowed"}, status=405
        )

    try:
        data = json.loads(request.body)
        corp_id = int(data.get("corporation_id"))
    except (json.JSONDecodeError, ValueError, TypeError):
        return JsonResponse(
            {"success": False, "error": "Invalid corporation_id"}, status=400
        )

    try:
        # Find a DIRECTOR character for this corporation
        director_char_id = _find_director_character(request.user, corp_id)
        if not director_char_id:
            return JsonResponse(
                {
                    "success": False,
                    "error": "No character with DIRECTOR role found in this corporation",
                },
                status=400,
            )

        # Trigger task to refresh corp assets using the director character
        # AA Example App
        from indy_hub.tasks.material_exchange import refresh_corp_assets_cached

        task = refresh_corp_assets_cached.delay(corp_id, director_char_id)

        return JsonResponse(
            {
                "success": True,
                "task_id": task.id,
                "message": "Asset refresh task started. Structures will be updated shortly.",
            }
        )
    except Exception as exc:
        logger.exception(
            "Failed to trigger asset refresh for corp %s: %s", corp_id, exc
        )
        return JsonResponse(
            {"success": False, "error": f"Failed to refresh assets: {str(exc)}"},
            status=500,
        )


@login_required
@indy_hub_permission_required("can_manage_material_hub")
def material_exchange_check_refresh_status(request, task_id):
    emit_view_analytics_event(
        view_name="material_exchange_config.check_refresh_status", request=request
    )
    """
    AJAX endpoint to check the status of a refresh task.
    Returns the task status: pending, success, or failure, plus progress info.

    Can also accept corp_id query parameter to check actual database updates
    instead of relying on Celery backend state tracking.
    """
    # Standard Library
    from datetime import timedelta

    # Third Party
    from celery.result import AsyncResult

    # Django
    from django.http import JsonResponse
    from django.utils import timezone

    # AA Example App
    from indy_hub.models import CachedStructureName

    try:
        # Get optional corp_id from query params for database-based status check
        corp_id = request.GET.get("corp_id")

        task_result = AsyncResult(task_id)

        # Try to get the task state from Celery
        try:
            state = task_result.state
        except AttributeError:
            # DisabledBackend or other backend that doesn't support task tracking
            state = None

        # Get progress info from task metadata
        progress_info = {}
        # Check state safely, handling DisabledBackend which may not support state access
        if state and state in ["PROGRESS", "SUCCESS"]:
            try:
                progress_data = task_result.info
                if isinstance(progress_data, dict) and "current" in progress_data:
                    progress_info = {
                        "current": progress_data.get("current", 0),
                        "total": progress_data.get("total", 0),
                        "percent": (
                            int(
                                (
                                    progress_data.get("current", 0)
                                    / progress_data.get("total", 1)
                                )
                                * 100
                            )
                            if progress_data.get("total", 0) > 0
                            else 0
                        ),
                        "status": progress_data.get("status", ""),
                    }
            except Exception as exc:
                logger.debug("Failed to extract progress info: %s", exc)

        # If we have a corp_id, verify by checking database updates
        if corp_id and state != "SUCCESS":
            try:
                # Check if any structures were cached in the last 30 seconds
                # This indicates the task has completed or is completing
                recent_structures = CachedStructureName.objects.filter(
                    last_resolved__gte=timezone.now() - timedelta(seconds=30)
                ).exists()

                if recent_structures:
                    logger.info(
                        "Task %s appears complete (found recent structure caches)",
                        task_id,
                    )
                    return JsonResponse(
                        {
                            "status": "success",
                            "progress": {
                                "percent": 100,
                                "status": "Complete!",
                            },
                        }
                    )
            except Exception as exc:
                logger.debug("Failed to check structure cache status: %s", exc)

        # Use Celery state if available
        if state == "PENDING":
            return JsonResponse(
                {
                    "status": "pending",
                    "progress": progress_info
                    or {"percent": 0, "status": "Initializing..."},
                }
            )
        elif state == "SUCCESS":
            return JsonResponse(
                {
                    "status": "success",
                    "progress": {"percent": 100, "status": "Complete!"},
                }
            )
        elif state == "FAILURE":
            return JsonResponse(
                {
                    "status": "failure",
                    "error": str(task_result.info),
                    "progress": progress_info,
                },
                status=400,
            )
        elif state == "PROGRESS":
            return JsonResponse(
                {
                    "status": "pending",
                    "progress": progress_info
                    or {"percent": 0, "status": "Processing..."},
                }
            )
        elif state is None:
            # Backend doesn't support state tracking and no db verification
            # Wait a bit longer before declaring success (give task time to run)
            return JsonResponse(
                {
                    "status": "pending",
                    "progress": {
                        "percent": 50,
                        "status": "Processing (no backend tracking)...",
                    },
                }
            )
        else:
            # RETRY, STARTED, etc.
            return JsonResponse(
                {
                    "status": "pending",
                    "progress": progress_info
                    or {"percent": 0, "status": "In progress..."},
                }
            )
    except Exception as exc:
        logger.exception("Failed to check refresh status for task %s: %s", task_id, exc)
        return JsonResponse(
            {
                "status": "failure",
                "error": f"Failed to check status: {str(exc)}",
                "progress": {"percent": 0},
            },
            status=500,
        )


def _get_user_corporations(user):
    """
    Get list of corporations the user has ESI access to.
    Returns list of dicts with corp_id and corp_name.
    """
    # Alliance Auth
    from allianceauth.eveonline.models import EveCharacter, EveCorporationInfo
    from esi.models import Token

    corporations = []
    seen_corps = set()

    character_ids = set()
    try:
        tokens = Token.objects.filter(user=user)
        for token in tokens:
            if token.character_id:
                character_ids.add(int(token.character_id))
    except Exception:
        logger.warning("Failed to list tokens for user %s", user.username)
        return corporations

    for char_id in character_ids:
        try:
            char_obj = EveCharacter.objects.get_character_by_id(int(char_id))
            if char_obj is None:
                char_obj = EveCharacter.objects.create_character(int(char_id))
        except Exception as exc:
            logger.debug("Skip char %s (character lookup failed: %s)", char_id, exc)
            continue

        corp_id = getattr(char_obj, "corporation_id", None)
        if not corp_id or corp_id in seen_corps:
            continue

        try:
            corp_obj = EveCorporationInfo.objects.filter(corporation_id=corp_id).first()
            if corp_obj is None:
                corp_obj = EveCorporationInfo.objects.create_corporation(int(corp_id))
        except Exception as exc:
            logger.debug("Skip corp %s (lookup failed: %s)", corp_id, exc)
            continue

        corporations.append(
            {
                "id": corp_id,
                "name": getattr(corp_obj, "corporation_name", f"Corp {corp_id}"),
                "ticker": getattr(corp_obj, "corporation_ticker", ""),
            }
        )
        seen_corps.add(corp_id)

    return corporations


def _get_corp_structures(user, corp_id):
    """Get list of player structures using lazy queryset and resolve names for user's DIRECTOR characters."""

    cache_key = f"indy_hub:material_exchange:corp_structures:v3:{int(corp_id)}"
    cached = cache.get(cache_key)
    if cached is not None:
        try:
            cached_structures, cached_scope_missing = cached
            cached_structures = list(cached_structures or [])
            cached_structures.sort(key=lambda row: str(row.get("name", "")).lower())
            return cached_structures, cached_scope_missing
        except Exception:
            return cached

    # Get structure IDs from corp assets
    assets_qs, assets_scope_missing = get_corp_assets_cached(
        int(corp_id),
        allow_refresh=True,
        as_queryset=True,
    )

    # Get unique structure IDs from corp assets.
    # - OfficeFolder assets carry the structure_id directly in location_id.
    # - CorpSAG* assets store the office folder item_id in location_id, so we
    #   must map that to the OfficeFolder's location_id (structure_id).
    resolvable_structure_flags = {
        "OfficeFolder",
        "StructureFuel",
        "MoonMaterialBay",
        "QuantumCoreRoom",
        "ServiceSlot0",
        "CorpDeliveries",
    }

    office_folder_map = {
        int(item_id): int(location_id)
        for item_id, location_id in assets_qs.filter(location_flag="OfficeFolder")
        .exclude(item_id__isnull=True)
        .values_list("item_id", "location_id")
        .distinct()
    }

    loc_ids_set: set[int] = set()
    structure_flags: dict[int, set[str]] = {}

    for loc_id in assets_qs.filter(
        location_flag__in=resolvable_structure_flags
    ).values_list("location_id", flat=True):
        if loc_id:
            loc_ids_set.add(int(loc_id))

    for office_folder_item_id, flag in assets_qs.filter(
        location_flag__startswith="CorpSAG"
    ).values_list("location_id", "location_flag"):
        if not office_folder_item_id:
            continue
        structure_id = office_folder_map.get(int(office_folder_item_id))
        if not structure_id:
            # Some corp asset payloads report CorpSAG location_id directly as
            # structure/station ID instead of office folder item_id.
            structure_id = int(office_folder_item_id)
        if structure_id:
            loc_ids_set.add(int(structure_id))
            structure_flags.setdefault(int(structure_id), set()).add(str(flag))

    loc_ids = list(loc_ids_set)

    if not loc_ids:
        result = (
            [
                {
                    "id": 0,
                    "name": _("⚠ No corporation assets available (ESI scope missing)"),
                }
            ],
            assets_scope_missing,
        )
        cache.set(cache_key, result, 300)
        return result

    # Resolve structure names using user's DIRECTOR characters
    # This will use /universe/structures/{structure_id} for each structure
    structure_names = resolve_structure_names(
        sorted(loc_ids), character_id=None, corporation_id=int(corp_id), user=user
    )

    structures: list[dict] = []
    for loc_id in sorted(loc_ids):
        resolved_name = structure_names.get(loc_id)
        if not resolved_name or str(resolved_name).startswith(PLACEHOLDER_PREFIX):
            continue
        structures.append(
            {
                "id": loc_id,
                "name": resolved_name,
                "flags": sorted(structure_flags.get(int(loc_id), set())),
            }
        )

    structures.sort(key=lambda row: str(row.get("name", "")).lower())

    result = (structures, assets_scope_missing)
    cache.set(cache_key, result, 300)
    return result


@login_required
@indy_hub_permission_required("can_manage_material_hub")
def material_exchange_request_assets_token(request):
    emit_view_analytics_event(
        view_name="material_exchange_config.request_assets_token", request=request
    )
    """Request ESI token with corp assets scope, then redirect back to config."""
    return sso_redirect(
        request,
        scopes="esi-assets.read_corporation_assets.v1",
        return_to="indy_hub:material_exchange_config",
    )


def _get_corp_hangar_divisions(user, corp_id):
    """Get hangar division names from cached ESI data."""

    default_divisions = {
        1: _("Hangar Division 1"),
        2: _("Hangar Division 2"),
        3: _("Hangar Division 3"),
        4: _("Hangar Division 4"),
        5: _("Hangar Division 5"),
        6: _("Hangar Division 6"),
        7: _("Hangar Division 7"),
    }

    divisions, scope_missing = get_corp_divisions_cached(int(corp_id))
    if divisions:
        default_divisions.update(divisions)
    return default_divisions, scope_missing


def _get_industry_market_group_ids() -> set[int]:
    """Return market group IDs used by all item types (cached)."""

    cache_key = "indy_hub:material_exchange:all_market_group_ids:v1"
    cached = cache.get(cache_key)
    if cached is not None:
        try:
            return {int(x) for x in cached}
        except Exception:
            return set()

    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import ItemType

        ids = {
            int(group_id)
            for group_id in ItemType.objects.exclude(
                market_group_id_raw__isnull=True
            ).values_list("market_group_id_raw", flat=True)
            if group_id is not None
        }
    except Exception:
        # Fallback to internal SDE table if eve_sde models are unavailable.
        try:
            # AA Example App
            from indy_hub.models import SdeMarketGroup

            ids = set(SdeMarketGroup.objects.values_list("id", flat=True))
        except Exception as exc:
            logger.warning("Failed to load market group IDs: %s", exc)
            ids = set()

    cache.set(cache_key, list(ids), 3600)
    return ids


def _get_itemtype_market_group_name_rows():
    """Return (market_group_id, item_name) rows for all item types."""

    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import ItemType

        return ItemType.objects.exclude(market_group_id_raw__isnull=True).values_list(
            "market_group_id_raw",
            "name",
        )
    except Exception:
        return []


def _build_market_group_index() -> dict[int, dict[str, str | int | None]]:
    """Return a dict of market group metadata keyed by id."""

    try:
        # AA Example App
        from indy_hub.models import SdeMarketGroup

        return {
            g["id"]: {
                "id": g["id"],
                "name": g["name"],
                "parent_market_group_id": g["parent_id"],
            }
            for g in SdeMarketGroup.objects.values("id", "name", "parent_id")
        }
    except Exception as exc:
        logger.warning("Failed to load market group choices: %s", exc)
        return {}


def _get_market_group_path_ids(
    group_id: int, all_groups: dict[int, dict[str, str | int | None]]
) -> list[int]:
    """Return path of IDs from root to the group (inclusive)."""

    path: list[int] = []
    seen: set[int] = set()
    current_id = group_id
    while current_id and current_id in all_groups and current_id not in seen:
        seen.add(current_id)
        path.append(current_id)
        current_id = all_groups[current_id]["parent_market_group_id"]
    return list(reversed(path))


def _build_market_group_path_label(
    group_id: int,
    all_groups: dict[int, dict[str, str | int | None]],
    *,
    separator: str = " -> ",
) -> str:
    """Return readable market-group path label for a group id."""

    try:
        group_id_int = int(group_id)
    except (TypeError, ValueError):
        return ""
    if group_id_int <= 0:
        return ""

    path_ids = _get_market_group_path_ids(group_id_int, all_groups)
    if not path_ids:
        group_name = str(all_groups.get(group_id_int, {}).get("name") or "").strip()
        return group_name or f"Group {group_id_int}"

    labels: list[str] = []
    for path_id in path_ids:
        label = str(all_groups.get(int(path_id), {}).get("name") or "").strip()
        if label:
            labels.append(label)
    if not labels:
        return f"Group {group_id_int}"
    return separator.join(labels)


def _normalize_market_group_ids_for_choice_depth(
    raw_group_ids, *, depth_from_root: int
) -> list[int]:
    """Normalize market group ids to the configured UI grouping depth."""

    normalized: set[int] = set()
    all_groups = _build_market_group_index()

    for raw_group_id in raw_group_ids or []:
        try:
            group_id = int(raw_group_id)
        except (TypeError, ValueError):
            continue
        if group_id <= 0:
            continue

        if not all_groups:
            normalized.add(group_id)
            continue

        path_ids = _get_market_group_path_ids(group_id, all_groups)
        if not path_ids:
            normalized.add(group_id)
            continue

        if len(path_ids) <= depth_from_root:
            normalized.add(int(path_ids[-1]))
        else:
            normalized.add(int(path_ids[depth_from_root]))

    return sorted(normalized)


def _get_industry_market_group_choice_ids(
    depth_from_root: int = MARKET_GROUP_CHOICE_DEPTH,
) -> set[int]:
    """Return grouped market group IDs at the given depth for all item types."""

    cache_key = (
        "indy_hub:material_exchange:market_group_choice_ids:v3:"
        f"depth:{depth_from_root}"
    )
    cached = cache.get(cache_key)
    if cached is not None:
        try:
            return {int(x) for x in cached}
        except Exception:
            return set()

    used_ids = _get_industry_market_group_ids()
    if not used_ids:
        return set()

    all_groups = _build_market_group_index()
    if not all_groups:
        return set()

    grouped_ids: set[int] = set()
    for group_id in used_ids:
        path_ids = _get_market_group_path_ids(int(group_id), all_groups)
        if not path_ids:
            continue
        if len(path_ids) <= depth_from_root:
            grouped_ids.add(path_ids[-1])
        else:
            grouped_ids.add(path_ids[depth_from_root])

    cache.set(cache_key, list(grouped_ids), 3600)
    return grouped_ids


def _get_industry_market_group_choices(
    depth_from_root: int = MARKET_GROUP_CHOICE_DEPTH,
) -> list[dict[str, str | int]]:
    """Return sorted market group choices (id + label) for all item types."""

    cache_key = (
        "indy_hub:material_exchange:market_group_choices:v3:"
        f"depth:{depth_from_root}"
    )
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    grouped_ids = _get_industry_market_group_choice_ids(depth_from_root)
    if not grouped_ids:
        return []

    all_groups = _build_market_group_index()
    if not all_groups:
        return []

    choices = [
        {"id": int(group_id), "label": all_groups[int(group_id)]["name"]}
        for group_id in grouped_ids
        if int(group_id) in all_groups
    ]
    choices.sort(key=lambda x: (str(x["label"]).lower()))

    cache.set(cache_key, choices, 3600)
    return choices


def _get_industry_market_group_search_index(
    depth_from_root: int = MARKET_GROUP_CHOICE_DEPTH,
) -> dict[int, dict[str, object]]:
    """Return market group labels and item names for search."""

    cache_key = (
        "indy_hub:material_exchange:market_group_search_index:v3:"
        f"depth:{depth_from_root}"
    )
    cached = cache.get(cache_key)
    if cached is not None:
        try:
            return {int(k): v for k, v in cached.items()}
        except Exception:
            return {}

    grouped_ids = _get_industry_market_group_choice_ids(depth_from_root)
    if not grouped_ids:
        return {}

    all_groups = _build_market_group_index()
    if not all_groups:
        return {}

    rows = _get_itemtype_market_group_name_rows()
    if rows is None:
        rows = []

    index: dict[int, dict[str, object]] = {
        int(group_id): {
            "label": str(all_groups[int(group_id)]["name"]),
            "items": set(),
        }
        for group_id in grouped_ids
        if int(group_id) in all_groups
    }

    max_items_per_group = int(MARKET_GROUP_SEARCH_ITEMS_PER_GROUP)
    for market_group_id, type_name in rows:
        if not market_group_id:
            continue
        path_ids = _get_market_group_path_ids(int(market_group_id), all_groups)
        if not path_ids:
            continue
        if len(path_ids) <= depth_from_root:
            grouped_id = path_ids[-1]
        else:
            grouped_id = path_ids[depth_from_root]
        if grouped_id in index and type_name:
            if len(index[grouped_id]["items"]) >= max_items_per_group:
                continue
            index[grouped_id]["items"].add(str(type_name))

    for group_id, payload in index.items():
        items = sorted(payload["items"], key=lambda x: x.lower())
        payload["items"] = items

    cache.set(
        cache_key,
        {str(k): v for k, v in index.items()},
        3600,
    )
    return index


def _get_market_group_search_index_for_ids(
    allowed_group_ids: set[int],
) -> dict[int, dict[str, object]]:
    """Return search index for a specific set of allowed market-group ids."""

    allowed_ids = {int(group_id) for group_id in allowed_group_ids if int(group_id) > 0}
    if not allowed_ids:
        return {}

    key_raw = ",".join(str(group_id) for group_id in sorted(allowed_ids))
    key_hash = hashlib.md5(key_raw.encode("utf-8"), usedforsecurity=False).hexdigest()
    cache_key = f"indy_hub:material_exchange:market_group_search_index:allowed:v1:{key_hash}"
    cached = cache.get(cache_key)
    if cached is not None:
        try:
            return {int(group_id): payload for group_id, payload in cached.items()}
        except Exception:
            return {}

    all_groups = _build_market_group_index()
    if not all_groups:
        return {}

    index: dict[int, dict[str, object]] = {
        int(group_id): {
            "label": str(all_groups.get(int(group_id), {}).get("name") or f"Group {group_id}"),
            "items": set(),
        }
        for group_id in allowed_ids
    }

    rows = _get_itemtype_market_group_name_rows() or []
    max_items_per_group = int(MARKET_GROUP_SEARCH_ITEMS_PER_GROUP)
    for market_group_id, type_name in rows:
        if not market_group_id or not type_name:
            continue
        path_ids = _get_market_group_path_ids(int(market_group_id), all_groups)
        if not path_ids:
            continue
        selected_group_id = None
        for path_group_id in reversed(path_ids):
            if int(path_group_id) in allowed_ids:
                selected_group_id = int(path_group_id)
                break
        if selected_group_id is None:
            continue
        if len(index[selected_group_id]["items"]) >= max_items_per_group:
            continue
        index[selected_group_id]["items"].add(str(type_name))

    serialized = {}
    for group_id, payload in index.items():
        serialized[str(group_id)] = {
            "label": payload["label"],
            "items": sorted(payload["items"], key=lambda name: str(name).lower()),
        }
    cache.set(cache_key, serialized, 3600)

    return {
        int(group_id): payload for group_id, payload in serialized.items()
    }


def _handle_config_save(request, existing_config):
    """Handle POST request to save Material Exchange configuration."""

    corporation_id = request.POST.get("corporation_id")
    sell_structure_ids_raw = request.POST.getlist("sell_structure_ids")
    buy_structure_ids_raw = request.POST.getlist("buy_structure_ids")
    hangar_division = request.POST.get("hangar_division")
    sell_markup_percent = request.POST.get("sell_markup_percent", "0")
    sell_markup_base = request.POST.get("sell_markup_base", "buy")
    buy_markup_percent = request.POST.get("buy_markup_percent", "5")
    buy_markup_base = request.POST.get("buy_markup_base", "buy")
    allowed_market_groups_buy_raw = request.POST.getlist("allowed_market_groups_buy")
    allowed_market_groups_sell_raw = request.POST.getlist("allowed_market_groups_sell")
    allowed_market_groups_buy_json_raw = (
        request.POST.get("allowed_market_groups_buy_json", "") or ""
    ).strip()
    allowed_market_groups_sell_json_raw = (
        request.POST.get("allowed_market_groups_sell_json", "") or ""
    ).strip()
    allowed_market_groups_sell_by_structure_raw = (
        request.POST.get("allowed_market_groups_sell_by_structure_json", "") or ""
    ).strip()
    item_price_overrides_raw = (
        request.POST.get("item_price_overrides_json", "") or ""
    ).strip()
    market_group_price_overrides_raw = (
        request.POST.get("market_group_price_overrides_json", "") or ""
    ).strip()

    enforce_jita_price_bounds = request.POST.get("enforce_jita_price_bounds") == "on"

    notify_admins_on_sell_anomaly = (
        request.POST.get("notify_admins_on_sell_anomaly") == "on"
    )
    buy_enabled = request.POST.get("buy_enabled") == "on"
    allow_fitted_ships = request.POST.get("allow_fitted_ships") == "on"
    location_match_mode = request.POST.get("location_match_mode") or "name_or_id"

    raw_is_active = request.POST.get("is_active")
    if raw_is_active is None and existing_config is not None:
        is_active = existing_config.is_active
    else:
        is_active = raw_is_active == "on"

    def _parse_decimal(raw_value: str, fallback: str) -> Decimal:
        normalized = (raw_value or "").strip().replace(",", ".")
        if not normalized:
            normalized = fallback
        return Decimal(normalized)

    def _parse_group_ids_json(raw_value: str) -> list[str]:
        if not raw_value:
            return []
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            return []
        if not isinstance(payload, (list, tuple, set)):
            return []
        parsed: list[str] = []
        for raw_group_id in payload:
            try:
                parsed.append(str(int(raw_group_id)))
            except (TypeError, ValueError):
                continue
        return parsed

    def _parse_optional_price(raw_value) -> Decimal | None:
        if raw_value is None:
            return None
        normalized = str(raw_value).strip().replace(",", ".")
        if not normalized:
            return None
        parsed = Decimal(normalized)
        if parsed < Decimal("0"):
            raise ValueError("Override prices must be positive numbers or empty.")
        return parsed.quantize(Decimal("0.01"))

    def _parse_optional_markup_percent(
        raw_value, *, minimum: Decimal, maximum: Decimal, label: str
    ) -> Decimal | None:
        if raw_value is None:
            return None
        normalized = str(raw_value).strip().replace(",", ".")
        if not normalized:
            return None
        parsed = Decimal(normalized).quantize(Decimal("0.01"))
        if parsed < minimum or parsed > maximum:
            raise ValueError(
                f"{label} must be between {minimum} and {maximum}."
            )
        return parsed

    def _parse_optional_markup_base(raw_value, *, fallback: str) -> str:
        base_value = str(raw_value or "").strip().lower()
        if base_value not in {"buy", "sell"}:
            return fallback
        return base_value

    def _parse_item_price_overrides(raw_value: str) -> list[dict[str, object]]:
        if not raw_value:
            return []
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            return []
        if not isinstance(payload, list):
            return []

        parsed_by_type: dict[int, dict[str, object]] = {}
        for row in payload:
            if not isinstance(row, dict):
                continue

            try:
                type_id = int(row.get("type_id") or 0)
            except (TypeError, ValueError):
                continue
            if type_id <= 0:
                continue

            type_name = str(row.get("type_name") or "").strip()
            sell_markup_percent_override = _parse_optional_markup_percent(
                row.get("sell_markup_percent_override"),
                minimum=Decimal("-100"),
                maximum=Decimal("100"),
                label="Sell override %",
            )
            buy_markup_percent_override = _parse_optional_markup_percent(
                row.get("buy_markup_percent_override"),
                minimum=Decimal("-100"),
                maximum=Decimal("1000"),
                label="Buy override %",
            )
            sell_markup_base_override = (
                _parse_optional_markup_base(
                    row.get("sell_markup_base_override"),
                    fallback=str(sell_markup_base or "buy"),
                )
                if sell_markup_percent_override is not None
                else None
            )
            buy_markup_base_override = (
                _parse_optional_markup_base(
                    row.get("buy_markup_base_override"),
                    fallback=str(buy_markup_base or "buy"),
                )
                if buy_markup_percent_override is not None
                else None
            )

            # Legacy fixed-price fields are preserved if the row still carries them.
            sell_price_override = _parse_optional_price(row.get("sell_price_override"))
            buy_price_override = _parse_optional_price(row.get("buy_price_override"))

            has_markup_override = (
                sell_markup_percent_override is not None
                or buy_markup_percent_override is not None
            )
            has_legacy_override = (
                sell_price_override is not None or buy_price_override is not None
            )

            if not has_markup_override and not has_legacy_override:
                continue

            parsed_by_type[type_id] = {
                "type_id": type_id,
                "type_name": type_name,
                "sell_markup_percent_override": sell_markup_percent_override,
                "sell_markup_base_override": sell_markup_base_override,
                "buy_markup_percent_override": buy_markup_percent_override,
                "buy_markup_base_override": buy_markup_base_override,
                "sell_price_override": sell_price_override,
                "buy_price_override": buy_price_override,
            }

        return list(parsed_by_type.values())

    def _normalize_market_group_id_for_config(
        raw_group_id,
        *,
        allowed_ids: set[int],
        all_groups: dict[int, dict[str, str | int | None]],
    ) -> int | None:
        try:
            group_id = int(raw_group_id or 0)
        except (TypeError, ValueError):
            return None
        if group_id <= 0:
            return None
        if not allowed_ids or group_id in allowed_ids:
            return int(group_id)

        path_ids = _get_market_group_path_ids(group_id, all_groups)
        for path_id in reversed(path_ids):
            path_id_int = int(path_id)
            if path_id_int in allowed_ids:
                return path_id_int
        return None

    def _parse_market_group_price_overrides(
        raw_value: str,
        *,
        allowed_ids: set[int],
        all_groups: dict[int, dict[str, str | int | None]],
    ) -> list[dict[str, object]]:
        if not raw_value:
            return []
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            return []
        if not isinstance(payload, list):
            return []

        parsed_by_group: dict[int, dict[str, object]] = {}
        for row in payload:
            if not isinstance(row, dict):
                continue

            market_group_id = _normalize_market_group_id_for_config(
                row.get("market_group_id") or row.get("group_id"),
                allowed_ids=allowed_ids,
                all_groups=all_groups,
            )
            if market_group_id is None:
                continue

            sell_markup_percent_override = _parse_optional_markup_percent(
                row.get("sell_markup_percent_override"),
                minimum=Decimal("-100"),
                maximum=Decimal("100"),
                label="Sell group override %",
            )
            buy_markup_percent_override = _parse_optional_markup_percent(
                row.get("buy_markup_percent_override"),
                minimum=Decimal("-100"),
                maximum=Decimal("1000"),
                label="Buy group override %",
            )
            sell_markup_base_override = (
                _parse_optional_markup_base(
                    row.get("sell_markup_base_override"),
                    fallback=str(sell_markup_base or "buy"),
                )
                if sell_markup_percent_override is not None
                else None
            )
            buy_markup_base_override = (
                _parse_optional_markup_base(
                    row.get("buy_markup_base_override"),
                    fallback=str(buy_markup_base or "buy"),
                )
                if buy_markup_percent_override is not None
                else None
            )

            sell_price_override = _parse_optional_price(row.get("sell_price_override"))
            buy_price_override = _parse_optional_price(row.get("buy_price_override"))

            has_markup_override = (
                sell_markup_percent_override is not None
                or buy_markup_percent_override is not None
            )
            has_fixed_override = (
                sell_price_override is not None or buy_price_override is not None
            )
            if not has_markup_override and not has_fixed_override:
                continue

            parsed_by_group[int(market_group_id)] = {
                "market_group_id": int(market_group_id),
                "market_group_path": str(
                    row.get("market_group_path")
                    or row.get("group_path")
                    or _build_market_group_path_label(int(market_group_id), all_groups)
                ).strip()
                or f"Group {int(market_group_id)}",
                "sell_markup_percent_override": sell_markup_percent_override,
                "sell_markup_base_override": sell_markup_base_override,
                "buy_markup_percent_override": buy_markup_percent_override,
                "buy_markup_base_override": buy_markup_base_override,
                "sell_price_override": sell_price_override,
                "buy_price_override": buy_price_override,
            }

        return sorted(
            parsed_by_group.values(),
            key=lambda payload: str(payload.get("market_group_path") or "").lower(),
        )

    # Validation
    try:
        if allowed_market_groups_buy_json_raw:
            parsed_buy_json = _parse_group_ids_json(allowed_market_groups_buy_json_raw)
            if parsed_buy_json:
                allowed_market_groups_buy_raw = parsed_buy_json
        if allowed_market_groups_sell_json_raw:
            parsed_sell_json = _parse_group_ids_json(allowed_market_groups_sell_json_raw)
            if parsed_sell_json:
                allowed_market_groups_sell_raw = parsed_sell_json

        if not corporation_id:
            raise ValueError("Corporation ID is required")
        if not sell_structure_ids_raw:
            raise ValueError("At least one sell structure is required")
        if not hangar_division:
            raise ValueError(
                "Hangar division is required. Please ensure the divisions scope token is added and a division is selected."
            )

        corporation_id = int(corporation_id)
        hangar_division = int(hangar_division)
        sell_markup_percent = _parse_decimal(sell_markup_percent, "0")
        buy_markup_percent = _parse_decimal(buy_markup_percent, "5")
        item_price_overrides = _parse_item_price_overrides(item_price_overrides_raw)

        market_group_tree = _get_market_group_tree()
        allowed_ids: set[int] = _collect_market_group_tree_ids(market_group_tree)
        all_groups = _build_market_group_index()
        market_group_price_overrides = _parse_market_group_price_overrides(
            market_group_price_overrides_raw,
            allowed_ids=allowed_ids,
            all_groups=all_groups,
        )

        def _parse_group_ids(raw_list: list[str]) -> list[int]:
            parsed: set[int] = set()
            for raw_group_id in raw_list or []:
                try:
                    group_id = int(raw_group_id)
                except (TypeError, ValueError):
                    continue
                if group_id <= 0:
                    continue
                if not allowed_ids:
                    parsed.add(group_id)
                    continue
                if group_id in allowed_ids:
                    parsed.add(group_id)
                    continue
                path_ids = _get_market_group_path_ids(group_id, all_groups)
                for path_id in reversed(path_ids):
                    if int(path_id) in allowed_ids:
                        parsed.add(int(path_id))
                        break
            return sorted(parsed)

        allowed_market_groups_buy = _parse_group_ids(allowed_market_groups_buy_raw)
        allowed_market_groups_sell = _parse_group_ids(allowed_market_groups_sell_raw)

        if not (1 <= hangar_division <= 7):
            raise ValueError("Hangar division must be between 1 and 7")

        valid_location_match_modes = {"name_or_id", "strict_id"}
        if location_match_mode not in valid_location_match_modes:
            location_match_mode = "name_or_id"

    except (ValueError, TypeError, InvalidOperation) as e:
        messages.error(request, _("Invalid configuration values: {}").format(e))
        return redirect("indy_hub:material_exchange_config")

    def _parse_structure_ids(raw_list: list[str]) -> list[int]:
        parsed: list[int] = []
        for raw in raw_list:
            try:
                value = int(raw)
            except (TypeError, ValueError):
                continue
            if value <= 0:
                continue
            if value not in parsed:
                parsed.append(value)
        return parsed

    sell_structure_ids = _parse_structure_ids(sell_structure_ids_raw)
    buy_structure_ids = _parse_structure_ids(buy_structure_ids_raw)

    if not sell_structure_ids:
        messages.error(
            request,
            _("At least one valid sell location is required."),
        )
        return redirect("indy_hub:material_exchange_config")

    if buy_enabled and not buy_structure_ids:
        messages.error(
            request,
            _("Buy locations are required when buy orders are enabled."),
        )
        return redirect("indy_hub:material_exchange_config")

    allowed_market_groups_sell_by_structure: dict[str, list[int] | None] = {}
    sell_group_payload_was_submitted = bool(allowed_market_groups_sell_by_structure_raw)
    if sell_group_payload_was_submitted:
        try:
            payload = json.loads(allowed_market_groups_sell_by_structure_raw)
        except json.JSONDecodeError:
            payload = {}
        if isinstance(payload, dict):
            for raw_sid, raw_groups in payload.items():
                try:
                    sid = int(raw_sid)
                except (TypeError, ValueError):
                    continue
                if sid not in sell_structure_ids:
                    continue

                if raw_groups is None:
                    allowed_market_groups_sell_by_structure[str(sid)] = None
                    continue

                if not isinstance(raw_groups, (list, tuple, set)):
                    continue

                parsed_groups = _parse_group_ids([str(gid) for gid in raw_groups])
                allowed_market_groups_sell_by_structure[str(sid)] = parsed_groups

        # Ensure every selected sell structure has an explicit rule.
        # Missing entries default to "all groups allowed".
        for sid in sell_structure_ids:
            sid_key = str(int(sid))
            if sid_key not in allowed_market_groups_sell_by_structure:
                allowed_market_groups_sell_by_structure[sid_key] = None
    elif allowed_market_groups_sell:
        for sid in sell_structure_ids:
            allowed_market_groups_sell_by_structure[str(int(sid))] = list(
                allowed_market_groups_sell
            )

    structure_flags_by_id: dict[int, set[str]] = {}
    allowed_structure_ids: set[int] = set()
    corp_structure_names_by_id: dict[int, str] = {}
    try:
        corp_structures, assets_scope_missing = _get_corp_structures(
            request.user, corporation_id
        )
        for entry in corp_structures or []:
            try:
                sid = int(entry.get("id"))
            except (TypeError, ValueError):
                continue
            if sid <= 0:
                continue
            allowed_structure_ids.add(sid)
            corp_name = str(entry.get("name") or "").strip()
            if corp_name:
                corp_structure_names_by_id[sid] = corp_name
            flags = entry.get("flags") or []
            structure_flags_by_id[sid] = {str(flag) for flag in flags if flag}
    except Exception:
        assets_scope_missing = False

    if allowed_structure_ids:
        invalid_sell = [sid for sid in sell_structure_ids if sid not in allowed_structure_ids]
        if invalid_sell:
            messages.error(
                request,
                _("One or more sell locations are not available for this corporation."),
            )
            return redirect("indy_hub:material_exchange_config")

        invalid_buy = [sid for sid in buy_structure_ids if sid not in allowed_structure_ids]
        if invalid_buy:
            messages.error(
                request,
                _("One or more buy locations are not available for this corporation."),
            )
            return redirect("indy_hub:material_exchange_config")

    if not assets_scope_missing and buy_structure_ids:
        required_flag = f"CorpSAG{hangar_division}"
        invalid_hangar: list[int] = []
        unknown_hangar: list[int] = []
        for sid in buy_structure_ids:
            flags = structure_flags_by_id.get(int(sid), set())
            if not flags:
                # If we cannot infer CorpSAG flags for a location, do not
                # hard-fail configuration save on unknown data.
                unknown_hangar.append(int(sid))
                continue
            if required_flag not in flags:
                invalid_hangar.append(int(sid))
        if invalid_hangar:
            invalid_hangar_names = [
                corp_structure_names_by_id.get(int(sid), "") or f"Structure {sid}"
                for sid in invalid_hangar
            ]
            messages.warning(
                request,
                _(
                    "Selected buy locations are missing the chosen hangar division. "
                    f"Missing {required_flag} on: {', '.join(invalid_hangar_names)}. "
                    "Configuration was saved, but buy stock may remain unavailable until this is corrected."
                ),
            )
        if unknown_hangar:
            unknown_hangar_names = [
                corp_structure_names_by_id.get(int(sid), "") or f"Structure {sid}"
                for sid in unknown_hangar
            ]
            messages.warning(
                request,
                _(
                    "Could not verify hangar divisions for: "
                    f"{', '.join(unknown_hangar_names)}. Configuration was saved, but buy stock may remain unavailable until corp assets are refreshed."
                ),
            )

    # Save or update config
    with transaction.atomic():
        resolved_names: dict[int, str] = {}
        all_structure_ids = list({*sell_structure_ids, *buy_structure_ids})
        if corporation_id and all_structure_ids:
            try:
                token_for_names = _get_token_for_corp(
                    request.user, corporation_id, "esi-universe.read_structures.v1"
                )
                character_id_for_names = (
                    getattr(token_for_names, "character_id", None)
                    if token_for_names
                    else None
                )
                resolved_names = resolve_structure_names(
                    [int(sid) for sid in all_structure_ids],
                    character_id_for_names,
                    int(corporation_id),
                    user=request.user,
                )
            except Exception:
                resolved_names = {}

        def _resolve_name(sid: int) -> str:
            name = resolved_names.get(int(sid), "") or ""
            if name and not str(name).startswith("Structure "):
                return str(name)
            fallback = corp_structure_names_by_id.get(int(sid), "") or ""
            return str(fallback)

        sell_structure_names = [_resolve_name(sid) for sid in sell_structure_ids]
        buy_structure_names = [_resolve_name(sid) for sid in buy_structure_ids]

        primary_structure_id = sell_structure_ids[0] if sell_structure_ids else 0
        if not primary_structure_id and buy_structure_ids:
            primary_structure_id = buy_structure_ids[0]
        primary_structure_name = _resolve_name(primary_structure_id) if primary_structure_id else ""
        if primary_structure_id and not primary_structure_name:
            primary_structure_name = f"Structure {primary_structure_id}"

        target_config: MaterialExchangeConfig
        if existing_config:
            existing_config.corporation_id = corporation_id
            existing_config.structure_id = primary_structure_id
            existing_config.structure_name = primary_structure_name
            existing_config.hangar_division = hangar_division
            existing_config.sell_markup_percent = sell_markup_percent
            existing_config.sell_markup_base = sell_markup_base
            existing_config.buy_markup_percent = buy_markup_percent
            existing_config.buy_markup_base = buy_markup_base
            existing_config.enforce_jita_price_bounds = enforce_jita_price_bounds
            existing_config.notify_admins_on_sell_anomaly = (
                notify_admins_on_sell_anomaly
            )
            existing_config.sell_structure_ids = sell_structure_ids
            existing_config.sell_structure_names = sell_structure_names
            existing_config.buy_structure_ids = buy_structure_ids
            existing_config.buy_structure_names = buy_structure_names
            existing_config.buy_enabled = buy_enabled
            existing_config.allow_fitted_ships = allow_fitted_ships
            existing_config.location_match_mode = location_match_mode
            existing_config.allowed_market_groups_buy = allowed_market_groups_buy
            existing_config.allowed_market_groups_sell = allowed_market_groups_sell
            existing_config.allowed_market_groups_sell_by_structure = (
                allowed_market_groups_sell_by_structure
            )
            existing_config.market_group_price_overrides = market_group_price_overrides
            existing_config.is_active = is_active
            existing_config.save()
            target_config = existing_config
            messages.success(
                request, _("Material Exchange configuration updated successfully.")
            )
        else:
            target_config = MaterialExchangeConfig.objects.create(
                corporation_id=corporation_id,
                structure_id=primary_structure_id,
                structure_name=primary_structure_name,
                hangar_division=hangar_division,
                sell_markup_percent=sell_markup_percent,
                sell_markup_base=sell_markup_base,
                buy_markup_percent=buy_markup_percent,
                buy_markup_base=buy_markup_base,
                enforce_jita_price_bounds=enforce_jita_price_bounds,
                notify_admins_on_sell_anomaly=notify_admins_on_sell_anomaly,
                sell_structure_ids=sell_structure_ids,
                sell_structure_names=sell_structure_names,
                buy_structure_ids=buy_structure_ids,
                buy_structure_names=buy_structure_names,
                buy_enabled=buy_enabled,
                allow_fitted_ships=allow_fitted_ships,
                location_match_mode=location_match_mode,
                allowed_market_groups_buy=allowed_market_groups_buy,
                allowed_market_groups_sell=allowed_market_groups_sell,
                allowed_market_groups_sell_by_structure=allowed_market_groups_sell_by_structure,
                market_group_price_overrides=market_group_price_overrides,
                is_active=is_active,
            )
            messages.success(
                request, _("Material Exchange configuration created successfully.")
            )

        desired_type_ids = {
            int(row["type_id"]) for row in item_price_overrides if row.get("type_id")
        }
        MaterialExchangeItemPriceOverride.objects.filter(config=target_config).exclude(
            type_id__in=desired_type_ids
        ).delete()
        for row in item_price_overrides:
            type_id = int(row["type_id"])
            type_name = str(row.get("type_name") or "").strip()
            if not type_name:
                type_name = get_type_name(type_id)
            MaterialExchangeItemPriceOverride.objects.update_or_create(
                config=target_config,
                type_id=type_id,
                defaults={
                    "type_name": type_name,
                    "sell_markup_percent_override": row.get(
                        "sell_markup_percent_override"
                    ),
                    "sell_markup_base_override": row.get("sell_markup_base_override"),
                    "buy_markup_percent_override": row.get(
                        "buy_markup_percent_override"
                    ),
                    "buy_markup_base_override": row.get("buy_markup_base_override"),
                    "sell_price_override": row.get("sell_price_override"),
                    "buy_price_override": row.get("buy_price_override"),
                },
            )

    return redirect("indy_hub:material_exchange_index")


@login_required
@indy_hub_permission_required("can_manage_material_hub")
@tokens_required(scopes="esi-characters.read_corporation_roles.v1")
def material_exchange_debug_tokens(request, corp_id, tokens):
    emit_view_analytics_event(
        view_name="material_exchange_config.debug_tokens", request=request
    )
    """Debug endpoint: list user's tokens and scopes relevant to a corporation.

    Query params:
    - scope: optional scope name to filter tokens (e.g., "esi-assets.read_corporation_assets.v1")
    """
    # Django
    from django.http import JsonResponse

    # Alliance Auth
    from esi.models import Token

    scope = request.GET.get("scope")
    qs = Token.objects.filter(user=request.user)
    if scope:
        qs = qs.require_scopes([scope])
    qs = qs.require_valid()

    results = []

    # Reuse character corp check
    def _character_matches(token) -> bool:
        char_id = getattr(token, "character_id", None)
        if not char_id:
            return False
        try:
            char_obj = getattr(token, "character", None)
            if char_obj and getattr(char_obj, "corporation_id", None) is not None:
                return int(char_obj.corporation_id) == int(corp_id)
        except Exception:
            pass
        try:
            character_resource = esi.client.Character
            operation = getattr(
                character_resource, "get_characters_character_id", None
            ) or getattr(character_resource, "GetCharactersCharacterId")
            result_obj = operation(character_id=char_id)
            char_info = result_obj.results()
            if isinstance(char_info, dict):
                corp_value = char_info.get("corporation_id")
            else:
                corp_value = None
                for attr in ("model_dump", "dict", "to_dict"):
                    converter = getattr(char_info, attr, None)
                    if callable(converter):
                        try:
                            result = converter()
                        except Exception:
                            result = None
                        if isinstance(result, dict):
                            corp_value = result.get("corporation_id")
                            break
                if corp_value is None:
                    corp_value = getattr(char_info, "corporation_id", None)
            return int(corp_value or 0) == int(corp_id)
        except Exception:
            return False

    for t in qs:
        try:
            scope_names = list(t.scopes.values_list("name", flat=True))
        except Exception:
            scope_names = []
        results.append(
            {
                "id": t.id,
                "type": getattr(t, "token_type", ""),
                "corporation_id": getattr(t, "corporation_id", None),
                "character_id": getattr(t, "character_id", None),
                "belongs_to_corp": (
                    (
                        getattr(t, "corporation_id", None) is not None
                        and int(getattr(t, "corporation_id")) == int(corp_id)
                    )
                    or _character_matches(t)
                ),
                "scopes": scope_names,
            }
        )

    return JsonResponse(
        {"corp_id": int(corp_id), "scope_filter": scope or None, "tokens": results}
    )
