"""Cached, read-only asset source resolution for the production simulator.

Everything here is a pure cache read. Deliberately does *not* call
``resolve_structure_names()``: that helper makes blocking ESI calls in every
mode (corp structure fetches, per-character token/role lookups, a 0.3s sleep
per structure) and writes CachedStructureName rows even with
``schedule_async=True``, none of which belongs on a GET handler. The asset
refresh task already warms the name cache for exactly these locations, so a
cache read is sufficient.

Scope is personal assets only. ``CachedCharacterAsset`` carries the container
parent chain but personal assets have no hangar divisions; corp divisions live
in a different cache and would need director rights.
"""

# Future
from __future__ import annotations

# Standard Library
from collections.abc import Iterable
from datetime import timedelta
from typing import Any

# Django
from django.db.models import Max
from django.utils import timezone

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

from ..app_settings import CHAR_ASSET_CACHE_MAX_AGE_MINUTES
from ..models import CachedCharacterAsset, CachedStructureName
from ..utils.eve import get_type_name
from .asset_cache import (
    PLACEHOLDER_PREFIX,
    asset_chain_has_context,
    build_asset_index_by_item_id,
)

logger = get_extension_logger(__name__)

# Guard against a pathological asset graph; mirrors the helpers in asset_cache.
MAX_CONTAINER_DEPTH = 25

# Cap the type filter so a crafted query cannot fan out unboundedly.
MAX_TYPE_FILTER = 500

PLAYER_STRUCTURE_ID_MIN = 2_147_483_648

NON_HANGAR_FLAGS = {
    "Cargo",
    "DroneBay",
    "FighterBay",
    "FighterTube0",
    "FighterTube1",
    "FighterTube2",
    "FighterTube3",
    "FighterTube4",
    "FleetHangar",
    "SpecializedOreHold",
    "SpecializedGasHold",
    "SpecializedMineralHold",
    "SpecializedSalvageHold",
    "SpecializedShipHold",
    "SpecializedSmallShipHold",
    "SpecializedMediumShipHold",
    "SpecializedLargeShipHold",
    "SpecializedIndustrialShipHold",
    "SpecializedAmmoHold",
    "SpecializedCommandCenterHold",
    "SpecializedPlanetaryCommoditiesHold",
    "SpecializedMaterialBay",
    "SpecializedQuafeHold",
    "SpecializedFuelBay",
    "ShipHangar",
    "ShipMaintenanceBay",
    "AutoFit",
    "HiSlot0",
    "HiSlot1",
    "HiSlot2",
    "HiSlot3",
    "HiSlot4",
    "HiSlot5",
    "HiSlot6",
    "HiSlot7",
    "MedSlot0",
    "MedSlot1",
    "MedSlot2",
    "MedSlot3",
    "MedSlot4",
    "MedSlot5",
    "MedSlot6",
    "MedSlot7",
    "LoSlot0",
    "LoSlot1",
    "LoSlot2",
    "LoSlot3",
    "LoSlot4",
    "LoSlot5",
    "LoSlot6",
    "LoSlot7",
    "RigSlot0",
    "RigSlot1",
    "RigSlot2",
    "RigSlot3",
    "RigSlot4",
    "RigSlot5",
    "RigSlot6",
    "RigSlot7",
    "SubSlot0",
    "SubSlot1",
    "SubSlot2",
    "SubSlot3",
    "SubSlot4",
    "SubSlot5",
    "SubSlot6",
    "SubSlot7",
    "ServiceSlot0",
    "ServiceSlot1",
    "ServiceSlot2",
    "ServiceSlot3",
    "ServiceSlot4",
    "ServiceSlot5",
    "ServiceSlot6",
    "ServiceSlot7",
    "Deliveries",
    "AssetSafety",
    "Skill",
    "Booster",
    "Implant",
    "Capsule",
}


def _get_ship_type_ids(type_ids: Iterable[int]) -> set[int]:
    """Return type IDs that are ships (category 6)."""
    cleaned = {int(tid) for tid in type_ids if int(tid or 0) > 0}
    if not cleaned:
        return set()
    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import ItemType

        return set(
            ItemType.objects.filter(
                id__in=cleaned,
                group__category_id=6,
            ).values_list("id", flat=True)
        )
    except Exception:
        return set()


def _is_personal_hangar_asset(
    asset: dict,
    index: dict[int, dict],
    *,
    ship_type_ids: set[int],
    location_id: int,
    max_depth: int = MAX_CONTAINER_DEPTH,
) -> bool:
    """Return True if the asset resides in the personal character item hangar (or container in hangar).

    Excludes ships, ship contents (cargo, drone bay, fitted modules), and non-hangar slots.
    """
    current = asset
    seen: set[int] = set()

    for _ in range(max_depth):
        try:
            current_type_id = int(
                current.get("_row", {}).get("type_id") or current.get("type_id", 0) or 0
            )
        except (TypeError, ValueError):
            current_type_id = 0

        # Any ship in the ancestry or as the asset itself (if not a blueprint) is excluded
        is_bp = bool(
            current.get("_row", {}).get("is_blueprint", False)
            or current.get("is_blueprint", False)
        )
        if not is_bp and current_type_id in ship_type_ids:
            return False

        current_flag = str(current.get("location_flag", "") or "")
        if current_flag in NON_HANGAR_FLAGS:
            return False

        try:
            parent_id = int(current.get("location_id", 0) or 0)
        except (TypeError, ValueError):
            parent_id = 0

        # When the parent reaches the top-level location (structure/station)
        if parent_id == int(location_id):
            return current_flag == "Hangar" or not current_flag

        if parent_id in seen or parent_id <= 0:
            return False
        seen.add(parent_id)

        parent = index.get(parent_id)
        if not parent:
            return False
        current = parent

    return False


def asset_cache_max_age() -> timedelta:
    """Project-wide freshness budget for personal asset data.

    The client previously hardcoded 24h, which is far more permissive than the
    rest of the codebase considers fresh.
    """
    return timedelta(minutes=max(1, int(CHAR_ASSET_CACHE_MAX_AGE_MINUTES or 60)))


def is_placeholder_name(name: Any) -> bool:
    """True for the synthetic 'Structure <id>' name the cache writes on failure."""
    return str(name or "").startswith(PLACEHOLDER_PREFIX)


def _npc_station_names(location_ids: Iterable[int]) -> dict[int, str]:
    """Offline NPC station names from the SDE.

    Free and ESI-free. Lazily imported (as build_scheduler does) so a missing
    or unmigrated SDE degrades to the placeholder instead of erroring.
    """
    ids = [int(value) for value in location_ids if int(value) > 0]
    if not ids:
        return {}
    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import NPCStation

        rows = NPCStation.objects.filter(id__in=ids).values_list("id", "name")
        return {
            int(station_id): str(name).strip()
            for station_id, name in rows
            if station_id and str(name or "").strip()
        }
    except Exception:
        logger.debug("SDE station names unavailable", exc_info=True)
        return {}


def _get_region_name(region_id: int) -> str:
    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import Region

        row = Region.objects.filter(id=region_id).values_list("name", flat=True).first()
        return str(row or "").strip()
    except Exception:
        return ""


def _get_system_name(system_id: int) -> str:
    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import SolarSystem

        row = (
            SolarSystem.objects.filter(id=system_id)
            .values_list("name", flat=True)
            .first()
        )
        return str(row or "").strip()
    except Exception:
        return ""


def resolve_location_system_and_region(
    location_ids: Iterable[int],
) -> dict[int, dict[str, Any]]:
    """Map location IDs (NPC stations and structures) to system and region metadata.

    Returns dict mapping location_id -> {
        "station_name": str,
        "system_id": int | None,
        "system_name": str,
        "region_id": int | None,
        "region_name": str,
    }
    """
    ids = sorted({int(value) for value in location_ids if int(value) > 0})
    if not ids:
        return {}

    results: dict[int, dict[str, Any]] = {}
    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import NPCStation

        stations = NPCStation.objects.filter(id__in=ids).select_related(
            "solar_system__constellation__region"
        )
        for station in stations:
            system = getattr(station, "solar_system", None)
            constellation = getattr(system, "constellation", None) if system else None
            region = getattr(constellation, "region", None) if constellation else None
            results[int(station.id)] = {
                "station_name": str(getattr(station, "name", "") or "").strip(),
                "system_id": int(system.id) if system else None,
                "system_name": str(getattr(system, "name", "") or "").strip(),
                "region_id": int(region.id) if region else None,
                "region_name": str(getattr(region, "name", "") or "").strip(),
            }
    except Exception:
        logger.debug("Failed resolving NPC stations for location IDs", exc_info=True)

    unresolved_ids = [loc_id for loc_id in ids if loc_id not in results]
    if unresolved_ids:
        try:
            # Alliance Auth (External Libs)
            from eve_sde.models import SolarSystem

            structures = CachedStructureName.objects.filter(
                structure_id__in=unresolved_ids
            ).values_list("structure_id", "name")
            for struct_id, struct_name in structures:
                s_id = int(struct_id)
                s_name = str(struct_name or "").strip()
                system_obj = None
                if " - " in s_name:
                    candidate_sys = s_name.split(" - ")[0].strip()
                    try:
                        system_obj = (
                            SolarSystem.objects.filter(name__iexact=candidate_sys)
                            .select_related("constellation__region")
                            .first()
                        )
                    except Exception:
                        system_obj = None
                system = system_obj
                constellation = (
                    getattr(system, "constellation", None) if system else None
                )
                region = (
                    getattr(constellation, "region", None) if constellation else None
                )
                results[s_id] = {
                    "station_name": s_name,
                    "system_id": int(system.id) if system else None,
                    "system_name": str(getattr(system, "name", "") or "").strip(),
                    "region_id": int(region.id) if region else None,
                    "region_name": str(getattr(region, "name", "") or "").strip(),
                }
        except Exception:
            logger.debug("Failed resolving structure systems", exc_info=True)

    for loc_id in ids:
        if loc_id not in results:
            results[loc_id] = {
                "station_name": f"{PLACEHOLDER_PREFIX}{loc_id}",
                "system_id": None,
                "system_name": "",
                "region_id": None,
                "region_name": "",
            }

    return results


def resolve_cached_location_names(
    location_ids: Iterable[int],
) -> dict[int, dict[str, Any]]:
    """Map location IDs to cached display names. No ESI, no writes.

    CachedStructureName is keyed by ID rather than by kind, so it holds NPC
    stations as well as player structures; one query covers both.
    """
    ids = sorted({int(value) for value in location_ids if int(value) > 0})
    if not ids:
        return {}

    resolved: dict[int, dict[str, Any]] = {}
    try:
        rows = CachedStructureName.objects.filter(structure_id__in=ids).values_list(
            "structure_id", "name", "last_resolved"
        )
    except Exception:
        logger.exception("Unable to read cached structure names")
        rows = []

    for structure_id, name, last_resolved in rows:
        cleaned = str(name or "").strip()
        if not cleaned:
            continue
        resolved[int(structure_id)] = {
            "name": cleaned,
            "is_placeholder": is_placeholder_name(cleaned),
            "last_resolved": last_resolved,
        }

    # Fill gaps, and upgrade placeholders, from the offline station table.
    needs_station_lookup = [
        location_id
        for location_id in ids
        if location_id not in resolved or resolved[location_id]["is_placeholder"]
    ]
    for location_id, station_name in _npc_station_names(needs_station_lookup).items():
        resolved[location_id] = {
            "name": station_name,
            "is_placeholder": False,
            "last_resolved": None,
        }

    return resolved


def _describe_location(location_id: int, names: dict[int, dict[str, Any]]) -> dict:
    entry = names.get(location_id)
    if entry:
        return {
            "location_name": entry["name"],
            "name_is_placeholder": bool(entry["is_placeholder"]),
            "name_last_resolved": (
                entry["last_resolved"].isoformat() if entry["last_resolved"] else None
            ),
        }
    # No cached name yet. Say so rather than presenting an ID as a name.
    return {
        "location_name": f"{PLACEHOLDER_PREFIX}{location_id}",
        "name_is_placeholder": True,
        "name_last_resolved": None,
    }


def list_asset_sources(user, *, blueprints: bool = False) -> dict[str, Any]:
    """Locations where the user has cached personal item hangar assets, with names and freshness.

    ``blueprints=True`` lists BPC/BPO locations instead of materials.
    Excludes non-hangar locations, fitted ships, and ship contents.
    Containers include only genuine containers, excluding ships.
    """
    all_user_assets = list(
        CachedCharacterAsset.objects.filter(user=user).values(
            "item_id",
            "character_id",
            "raw_location_id",
            "location_id",
            "location_flag",
            "type_id",
            "set_name",
            "quantity",
            "is_blueprint",
            "synced_at",
        )
    )

    if blueprints:
        try:
            # Alliance Auth
            from allianceauth.authentication.models import CharacterOwnership
            from django.db.models import Q
            from indy_hub.models import Blueprint, CachedStructureName

            user_char_ids = list(
                CharacterOwnership.objects.filter(user=user).values_list(
                    "character__character_id", flat=True
                )
            )
            bp_rows = list(
                Blueprint.objects.filter(
                    Q(owner_user=user) | Q(character_id__in=user_char_ids)
                ).values(
                    "item_id",
                    "character_id",
                    "location_id",
                    "location_name",
                    "location_flag",
                    "type_id",
                    "quantity",
                    "last_updated",
                )
            )
            for bp in bp_rows:
                lid = bp.get("location_id")
                if not lid:
                    continue
                loc_name = str(bp.get("location_name") or "").strip()
                if loc_name and int(lid) >= PLAYER_STRUCTURE_ID_MIN:
                    CachedStructureName.objects.get_or_create(
                        structure_id=int(lid),
                        defaults={"name": loc_name},
                    )
                all_user_assets.append(
                    {
                        "item_id": bp["item_id"],
                        "character_id": bp["character_id"] or 0,
                        "raw_location_id": bp["location_id"],
                        "location_id": bp["location_id"],
                        "location_flag": bp["location_flag"] or "Hangar",
                        "type_id": bp["type_id"],
                        "set_name": bp["location_name"] or "",
                        "quantity": bp["quantity"] or 1,
                        "is_blueprint": True,
                        "synced_at": bp["last_updated"],
                    }
                )
        except Exception:
            pass

    if not all_user_assets:
        return {
            "sources": [],
            "max_age_seconds": int(asset_cache_max_age().total_seconds()),
            "supports_divisions": False,
        }

    # Resolve character names for the user's characters
    character_names: dict[int, str] = {}
    try:
        # Alliance Auth
        from allianceauth.authentication.models import CharacterOwnership

        for ownership in CharacterOwnership.objects.filter(user=user).select_related(
            "character"
        ):
            if ownership.character:
                cid = int(ownership.character.character_id)
                character_names[cid] = str(ownership.character.character_name or cid)
    except Exception:
        pass

    all_char_ids = {
        int(r["character_id"]) for r in all_user_assets if r.get("character_id")
    }
    missing_cids = all_char_ids - set(character_names.keys())
    if missing_cids:
        try:
            # Alliance Auth
            from allianceauth.eveonline.models import EveCharacter

            for ec in EveCharacter.objects.filter(character_id__in=missing_cids):
                character_names[int(ec.character_id)] = str(
                    ec.character_name or ec.character_id
                )
        except Exception:
            pass

    all_type_ids = {
        int(row["type_id"]) for row in all_user_assets if row.get("type_id")
    }
    ship_type_ids = _get_ship_type_ids(all_type_ids)

    # Group assets by (character_id, root location_id)
    assets_by_char_and_loc: dict[tuple[int, int], list[dict]] = {}
    for row in all_user_assets:
        loc_id = row.get("location_id")
        char_id = int(row.get("character_id") or 0)
        if loc_id:
            assets_by_char_and_loc.setdefault((char_id, int(loc_id)), []).append(row)

    max_age = asset_cache_max_age()
    now = timezone.now()
    unique_location_ids = list({loc_id for _, loc_id in assets_by_char_and_loc.keys()})
    resolved_names = resolve_cached_location_names(unique_location_ids)
    loc_meta_map = resolve_location_system_and_region(unique_location_ids)

    sources = []
    for char_id, location_id in sorted(
        assets_by_char_and_loc.keys(),
        key=lambda k: (character_names.get(k[0], ""), k[0], k[1]),
    ):
        location_rows = assets_by_char_and_loc[(char_id, location_id)]
        esi_rows = _rows_as_esi_shape(location_rows)
        index = build_asset_index_by_item_id(esi_rows)

        # Identify all valid personal item hangar assets at this location for this character
        valid_hangar_assets = []
        for asset in esi_rows:
            raw_row = asset["_row"]
            if bool(raw_row.get("is_blueprint", False)) != blueprints:
                continue
            if _is_personal_hangar_asset(
                asset,
                index,
                ship_type_ids=ship_type_ids,
                location_id=location_id,
            ):
                valid_hangar_assets.append(raw_row)

        # If no valid hangar assets for this mode at this location, skip
        if not valid_hangar_assets:
            continue

        # Find all containers for this location:
        # Parent items that reside in the hangar, are NOT ships, and are referenced by raw_location_id of hangar assets
        container_item_ids = {
            int(row["raw_location_id"])
            for row in valid_hangar_assets
            if row.get("raw_location_id") and int(row["raw_location_id"]) != location_id
        }

        # Validate that container items themselves are non-ship hangar containers
        valid_containers: dict[int, str] = {}
        for container_id in sorted(container_item_ids):
            parent_asset = index.get(container_id)
            if not parent_asset:
                continue
            parent_type_id = int(parent_asset.get("_row", {}).get("type_id", 0) or 0)
            if parent_type_id in ship_type_ids:
                continue
            if not _is_personal_hangar_asset(
                parent_asset,
                index,
                ship_type_ids=ship_type_ids,
                location_id=location_id,
            ):
                continue
            parent_row = parent_asset.get("_row", {})
            label = str(parent_row.get("set_name") or "").strip() or get_type_name(
                parent_type_id
            )
            valid_containers[container_id] = label or f"Container {container_id}"

        last_synced = max(
            (row["synced_at"] for row in valid_hangar_assets if row.get("synced_at")),
            default=None,
        )

        char_name = character_names.get(char_id) or (
            f"Character {char_id}" if char_id else "Personal Assets"
        )

        meta = loc_meta_map.get(location_id, {})
        sources.append(
            {
                "character_id": char_id,
                "character_name": char_name,
                "location_id": location_id,
                "system_id": meta.get("system_id"),
                "system_name": meta.get("system_name", ""),
                "region_id": meta.get("region_id"),
                "region_name": meta.get("region_name", ""),
                **_describe_location(location_id, resolved_names),
                "last_synced": last_synced.isoformat() if last_synced else None,
                "is_stale": bool(last_synced and (now - last_synced) > max_age),
                "location_flags": ["Hangar"],
                "containers": [
                    {"item_id": item_id, "name": name}
                    for item_id, name in valid_containers.items()
                ],
            }
        )

    regions_map: dict[int, dict[str, Any]] = {}
    systems_map: dict[int, dict[str, Any]] = {}
    for s in sources:
        rid = s.get("region_id")
        rname = s.get("region_name")
        if rid and rname:
            if rid not in regions_map:
                regions_map[rid] = {
                    "region_id": rid,
                    "region_name": rname,
                    "source_count": 0,
                }
            regions_map[rid]["source_count"] += 1
        sid = s.get("system_id")
        sname = s.get("system_name")
        if sid and sname:
            if sid not in systems_map:
                systems_map[sid] = {
                    "system_id": sid,
                    "system_name": sname,
                    "region_id": rid,
                    "region_name": rname,
                    "source_count": 0,
                }
            systems_map[sid]["source_count"] += 1

    return {
        "sources": sources,
        "regions": sorted(
            regions_map.values(), key=lambda r: str(r["region_name"]).lower()
        ),
        "systems": sorted(
            systems_map.values(), key=lambda s: str(s["system_name"]).lower()
        ),
        "max_age_seconds": int(max_age.total_seconds()),
        # Personal assets have no corporation hangar divisions; the UI should
        # say so rather than render an empty division selector.
        "supports_divisions": False,
    }


def _rows_as_esi_shape(rows: list[dict]) -> list[dict]:
    """Adapt cached rows into the dict shape the asset_cache helpers expect.

    Those helpers were written for the ESI sync path, where ``location_id`` is
    the *immediate* parent. Cached rows store the already-flattened root in
    ``location_id`` and the immediate parent in ``raw_location_id``.
    """
    adapted = []
    for row in rows:
        raw_location_id = row.get("raw_location_id") or row.get("location_id") or 0
        adapted.append(
            {
                "item_id": row.get("item_id"),
                "location_id": int(raw_location_id or 0),
                "location_flag": str(row.get("location_flag") or ""),
                "_row": row,
            }
        )
    return adapted


def _has_container_ancestor(asset, index, container_item_id: int) -> bool:
    """Whether container_item_id appears in the asset's parent chain.

    Complements asset_chain_has_context, which answers the different question
    of whether the chain sits at a given (location, flag) pair.
    """
    current = asset
    seen: set[int] = set()
    for _ in range(MAX_CONTAINER_DEPTH):
        parent_id = int(current.get("location_id") or 0)
        if parent_id == int(container_item_id):
            return True
        if parent_id in seen:
            return False
        seen.add(parent_id)
        parent = index.get(parent_id)
        if not parent:
            return False
        current = parent
    return False


def get_source_assets(
    user,
    *,
    location_id: int,
    character_id: int = 0,
    type_ids: list[int] | None = None,
    location_flag: str = "",
    container_item_id: int = 0,
    blueprints: bool = False,
) -> dict[str, Any] | None:
    """Cached quantities at one user-authorized location for personal item hangars.

    Returns None when the user has no hangar assets at that location at all.
    Excludes ships, ship cargo, and fitted modules.
    """
    filter_kwargs: dict[str, Any] = {
        "user": user,
        "location_id": location_id,
    }
    if int(character_id or 0) > 0:
        filter_kwargs["character_id"] = int(character_id)

    location_assets = list(
        CachedCharacterAsset.objects.filter(**filter_kwargs).values(
            "item_id",
            "character_id",
            "raw_location_id",
            "location_id",
            "location_flag",
            "type_id",
            "quantity",
            "is_blueprint",
            "synced_at",
            "set_name",
        )
    )
    if not location_assets:
        return None

    all_type_ids = {
        int(row["type_id"]) for row in location_assets if row.get("type_id")
    }
    ship_type_ids = _get_ship_type_ids(all_type_ids)
    esi_rows = _rows_as_esi_shape(location_assets)
    index = build_asset_index_by_item_id(esi_rows)

    # If container_item_id is specified, ensure it is a valid non-ship container
    if container_item_id:
        container_asset = index.get(int(container_item_id))
        if not container_asset:
            return {
                "location_id": location_id,
                **_describe_location(
                    location_id, resolve_cached_location_names([location_id])
                ),
                "location_flag": location_flag,
                "container_item_id": container_item_id,
                "max_age_seconds": int(asset_cache_max_age().total_seconds()),
                "last_synced": None,
                "is_stale": False,
                "assets": [],
            }
        c_type_id = int(container_asset.get("_row", {}).get("type_id", 0) or 0)
        if c_type_id in ship_type_ids or not _is_personal_hangar_asset(
            container_asset,
            index,
            ship_type_ids=ship_type_ids,
            location_id=location_id,
        ):
            return {
                "location_id": location_id,
                **_describe_location(
                    location_id, resolve_cached_location_names([location_id])
                ),
                "location_flag": location_flag,
                "container_item_id": container_item_id,
                "max_age_seconds": int(asset_cache_max_age().total_seconds()),
                "last_synced": None,
                "is_stale": False,
                "assets": [],
            }

    matched = []
    for asset in esi_rows:
        raw_row = asset["_row"]
        if bool(raw_row.get("is_blueprint", False)) != blueprints:
            continue
        if not _is_personal_hangar_asset(
            asset,
            index,
            ship_type_ids=ship_type_ids,
            location_id=location_id,
        ):
            continue
        if location_flag and not asset_chain_has_context(
            asset,
            index,
            location_id=location_id,
            location_flag=location_flag,
            max_depth=MAX_CONTAINER_DEPTH,
        ):
            continue
        if container_item_id and not _has_container_ancestor(
            asset, index, container_item_id
        ):
            continue
        matched.append(raw_row)

    if not matched and not container_item_id and not location_flag:
        # Check if user had any personal hangar asset at this location at all
        has_any_hangar = any(
            _is_personal_hangar_asset(
                a, index, ship_type_ids=ship_type_ids, location_id=location_id
            )
            for a in esi_rows
        )
        if not has_any_hangar:
            return None

    wanted = set(type_ids or [])
    grouped: dict[int, dict[str, Any]] = {}
    for row in matched:
        type_id = int(row["type_id"])
        if wanted and type_id not in wanted:
            continue
        entry = grouped.setdefault(
            type_id, {"quantity": 0, "last_synced": row["synced_at"]}
        )
        entry["quantity"] += int(row["quantity"] or 0)
        if row["synced_at"] and (
            not entry["last_synced"] or row["synced_at"] > entry["last_synced"]
        ):
            entry["last_synced"] = row["synced_at"]

    aggregated = [
        {
            "type_id": type_id,
            "quantity": data["quantity"],
            "last_synced": data["last_synced"],
        }
        for type_id, data in grouped.items()
    ]

    max_age = asset_cache_max_age()
    now = timezone.now()
    names = resolve_cached_location_names([location_id])
    newest = max(
        (row["last_synced"] for row in aggregated if row["last_synced"]), default=None
    )

    return {
        "location_id": location_id,
        **_describe_location(location_id, names),
        "location_flag": location_flag,
        "container_item_id": container_item_id or None,
        "max_age_seconds": int(max_age.total_seconds()),
        "last_synced": newest.isoformat() if newest else None,
        "is_stale": bool(newest and (now - newest) > max_age),
        "assets": [
            {
                "type_id": row["type_id"],
                "type_name": get_type_name(row["type_id"]),
                "quantity": row["quantity"],
                "last_synced": (
                    row["last_synced"].isoformat() if row["last_synced"] else None
                ),
            }
            for row in sorted(aggregated, key=lambda item: item["type_id"])
            if row["quantity"] > 0
        ],
    }


def parse_bpc_source_scope(value: Any) -> tuple[str, int, int]:
    """Parse a token into (scope, target_id, container_item_id).

    Scopes: 'all', 'region', 'system', 'station', 'container'.
    """
    text = str(value or "").strip()
    if not text or text == "all":
        return "all", 0, 0

    if text.startswith("region:"):
        try:
            rid = int(text.split(":", 1)[1])
            return ("region", rid, 0) if rid > 0 else ("all", 0, 0)
        except (ValueError, IndexError):
            return "all", 0, 0

    if text.startswith("system:"):
        try:
            sid = int(text.split(":", 1)[1])
            return ("system", sid, 0) if sid > 0 else ("all", 0, 0)
        except (ValueError, IndexError):
            return "all", 0, 0

    if text.startswith("station:"):
        try:
            lid = int(text.split(":", 1)[1])
            return ("station", lid, 0) if lid > 0 else ("all", 0, 0)
        except (ValueError, IndexError):
            return "all", 0, 0

    if text.startswith("container:"):
        parts = text.split(":")
        try:
            lid = int(parts[1])
            cid = int(parts[2]) if len(parts) > 2 else 0
            return ("container", lid, cid) if lid > 0 and cid > 0 else ("all", 0, 0)
        except (ValueError, IndexError):
            return "all", 0, 0

    # Legacy "<location_id>" or "<location_id>:<container_id>"
    location_part, _sep, container_part = text.partition(":")
    try:
        location_id = int(location_part)
        container_item_id = int(container_part) if container_part else 0
    except ValueError:
        return "all", 0, 0
    if location_id <= 0 or container_item_id < 0:
        return "all", 0, 0
    if container_item_id > 0:
        return "container", location_id, container_item_id
    return "station", location_id, 0


def parse_bpc_source(value: Any) -> tuple[int, int]:
    """Parse a ``"<location_id>"`` or ``"<location_id>:<container_id>"`` token.

    Returns (0, 0) for "all eligible locations", regional/system scopes, or anything malformed.
    """
    scope, target_id, container_id = parse_bpc_source_scope(value)
    if scope in ("station", "container"):
        return target_id, container_id
    return 0, 0


def _extract_blueprint_ids_for_locations(
    user, location_ids: Iterable[int], *, character_id: int = 0
) -> set[int]:
    loc_list = sorted({int(lid) for lid in location_ids if int(lid) > 0})
    if not loc_list:
        return set()

    matched_ids: set[int] = set()

    # Match from Blueprint model
    try:
        # Alliance Auth
        from allianceauth.authentication.models import CharacterOwnership
        from django.db.models import Q
        from indy_hub.models import Blueprint

        user_char_ids = list(
            CharacterOwnership.objects.filter(user=user).values_list(
                "character__character_id", flat=True
            )
        )
        bp_filter = Q(location_id__in=loc_list) & (
            Q(owner_user=user) | Q(character_id__in=user_char_ids)
        )
        if int(character_id or 0) > 0:
            bp_filter &= Q(character_id=int(character_id))

        for item_id in Blueprint.objects.filter(bp_filter).values_list(
            "item_id", flat=True
        ):
            if item_id:
                matched_ids.add(int(item_id))
    except Exception:
        pass

    filter_kwargs: dict[str, Any] = {
        "user": user,
        "location_id__in": loc_list,
    }
    if int(character_id or 0) > 0:
        filter_kwargs["character_id"] = int(character_id)

    location_assets = list(
        CachedCharacterAsset.objects.filter(**filter_kwargs).values(
            "item_id",
            "character_id",
            "raw_location_id",
            "location_id",
            "location_flag",
            "type_id",
            "is_blueprint",
            "synced_at",
            "set_name",
        )
    )
    if location_assets:
        all_type_ids = {
            int(row["type_id"]) for row in location_assets if row.get("type_id")
        }
        ship_type_ids = _get_ship_type_ids(all_type_ids)
        esi_rows = _rows_as_esi_shape(location_assets)
        index = build_asset_index_by_item_id(esi_rows)

        for asset in esi_rows:
            raw_row = asset["_row"]
            if not bool(raw_row.get("is_blueprint", False)):
                continue
            if not _is_personal_hangar_asset(
                asset,
                index,
                ship_type_ids=ship_type_ids,
                location_id=raw_row.get("location_id"),
            ):
                continue
            item_id = raw_row.get("item_id")
            if item_id:
                matched_ids.add(int(item_id))

    return matched_ids


def blueprint_item_ids_at_source(
    user,
    *,
    location_id: int = 0,
    character_id: int = 0,
    container_item_id: int = 0,
    system_id: int = 0,
    region_id: int = 0,
) -> set[int] | None:
    """Item IDs of the user's cached blueprints at one source (personal item hangar).

    Supports filtering by specific region, solar system, station, or container.
    Returns None when the user has no cached blueprints there.
    Excludes ships and blueprints inside ships.
    """
    loc_id_set: set[int] = set()
    try:
        # Alliance Auth
        from allianceauth.authentication.models import CharacterOwnership
        from django.db.models import Q
        from indy_hub.models import Blueprint

        user_char_ids = list(
            CharacterOwnership.objects.filter(user=user).values_list(
                "character__character_id", flat=True
            )
        )
        bp_locs = (
            Blueprint.objects.filter(
                Q(owner_user=user) | Q(character_id__in=user_char_ids)
            )
            .values_list("location_id", flat=True)
            .distinct()
        )
        for lid in bp_locs:
            if lid and int(lid) > 0:
                loc_id_set.add(int(lid))
    except Exception:
        pass

    for lid in (
        CachedCharacterAsset.objects.filter(user=user, is_blueprint=True)
        .values_list("location_id", flat=True)
        .distinct()
    ):
        if lid and int(lid) > 0:
            loc_id_set.add(int(lid))

    loc_ids = sorted(loc_id_set)

    if int(region_id or 0) > 0:
        meta_map = resolve_location_system_and_region(loc_ids)
        target_loc_ids = [
            loc_id
            for loc_id in loc_ids
            if meta_map.get(loc_id, {}).get("region_id") == int(region_id)
        ]
        if not target_loc_ids:
            return None
        matched_ids = _extract_blueprint_ids_for_locations(
            user, target_loc_ids, character_id=character_id
        )
        return matched_ids if matched_ids else None

    if int(system_id or 0) > 0:
        meta_map = resolve_location_system_and_region(loc_ids)
        target_loc_ids = [
            loc_id
            for loc_id in loc_ids
            if meta_map.get(loc_id, {}).get("system_id") == int(system_id)
        ]
        if not target_loc_ids:
            return None
        matched_ids = _extract_blueprint_ids_for_locations(
            user, target_loc_ids, character_id=character_id
        )
        return matched_ids if matched_ids else None

    if int(location_id or 0) <= 0:
        return None

    filter_kwargs: dict[str, Any] = {
        "user": user,
        "location_id": int(location_id),
    }
    if int(character_id or 0) > 0:
        filter_kwargs["character_id"] = int(character_id)

    location_assets = list(
        CachedCharacterAsset.objects.filter(**filter_kwargs).values(
            "item_id",
            "character_id",
            "raw_location_id",
            "location_id",
            "location_flag",
            "type_id",
            "is_blueprint",
            "synced_at",
            "set_name",
        )
    )

    matched_ids = _extract_blueprint_ids_for_locations(
        user, [int(location_id)], character_id=character_id
    )

    if location_assets:
        all_type_ids = {
            int(row["type_id"]) for row in location_assets if row.get("type_id")
        }
        ship_type_ids = _get_ship_type_ids(all_type_ids)
        esi_rows = _rows_as_esi_shape(location_assets)
        index = build_asset_index_by_item_id(esi_rows)

        if container_item_id:
            container_asset = index.get(int(container_item_id))
            if not container_asset:
                return set()
            c_type_id = int(container_asset.get("_row", {}).get("type_id", 0) or 0)
            if c_type_id in ship_type_ids or not _is_personal_hangar_asset(
                container_asset,
                index,
                ship_type_ids=ship_type_ids,
                location_id=location_id,
            ):
                return set()

        for asset in esi_rows:
            raw_row = asset["_row"]
            if not bool(raw_row.get("is_blueprint", False)):
                continue
            if not _is_personal_hangar_asset(
                asset,
                index,
                ship_type_ids=ship_type_ids,
                location_id=location_id,
            ):
                continue
            if container_item_id and not _has_container_ancestor(
                asset, index, container_item_id
            ):
                continue
            item_id = raw_row.get("item_id")
            if item_id:
                matched_ids.add(int(item_id))

    if not matched_ids and not container_item_id:
        return None

    return matched_ids


def describe_bpc_source(user, token: Any) -> dict[str, Any]:
    """Resolve a BPC source token to a validated, named description.

    ``item_ids`` is None for "all eligible locations" (no narrowing) and a set
    otherwise; an invalid or foreign source is reported as rejected and falls
    back to no narrowing rather than to an empty set.
    """
    raw_token_str = str(token or "").strip()
    scope, target_id, container_item_id = parse_bpc_source_scope(raw_token_str)
    if scope == "all" or not raw_token_str or raw_token_str == "all":
        return {
            "token": "all",
            "scope": "all",
            "item_ids": None,
            "rejected": bool(raw_token_str not in ("", "all")),
            "location_name": "All eligible locations",
        }

    def _get_bpc_loc_ids_and_last_synced(
        target_locs: list[int],
    ) -> tuple[list[int], Any]:
        last_dt = (
            CachedCharacterAsset.objects.filter(
                user=user, location_id__in=target_locs, is_blueprint=True
            )
            .aggregate(last=Max("synced_at"))
            .get("last")
        )
        try:
            # Alliance Auth
            from allianceauth.authentication.models import CharacterOwnership
            from django.db.models import Q
            from indy_hub.models import Blueprint

            user_char_ids = list(
                CharacterOwnership.objects.filter(user=user).values_list(
                    "character__character_id", flat=True
                )
            )
            bp_last = (
                Blueprint.objects.filter(
                    Q(location_id__in=target_locs)
                    & (Q(owner_user=user) | Q(character_id__in=user_char_ids))
                )
                .aggregate(last=Max("last_updated"))
                .get("last")
            )
            if bp_last and (not last_dt or bp_last > last_dt):
                last_dt = bp_last
        except Exception:
            pass
        return target_locs, last_dt

    all_bpc_loc_ids_set: set[int] = set(
        CachedCharacterAsset.objects.filter(user=user, is_blueprint=True)
        .values_list("location_id", flat=True)
        .distinct()
    )
    try:
        # Alliance Auth
        from allianceauth.authentication.models import CharacterOwnership
        from django.db.models import Q
        from indy_hub.models import Blueprint

        user_char_ids = list(
            CharacterOwnership.objects.filter(user=user).values_list(
                "character__character_id", flat=True
            )
        )
        for lid in (
            Blueprint.objects.filter(
                Q(owner_user=user) | Q(character_id__in=user_char_ids)
            )
            .values_list("location_id", flat=True)
            .distinct()
        ):
            if lid and int(lid) > 0:
                all_bpc_loc_ids_set.add(int(lid))
    except Exception:
        pass

    all_bpc_loc_ids = sorted(all_bpc_loc_ids_set)

    if scope == "region":
        item_ids = blueprint_item_ids_at_source(user, region_id=target_id)
        if item_ids is None:
            return {
                "token": "all",
                "scope": "all",
                "item_ids": None,
                "rejected": True,
                "location_name": "All eligible locations",
            }
        region_name = _get_region_name(target_id)
        meta_map = resolve_location_system_and_region(all_bpc_loc_ids)
        target_loc_ids = [
            loc_id
            for loc_id in all_bpc_loc_ids
            if meta_map.get(loc_id, {}).get("region_id") == int(target_id)
        ]
        _, last_synced = _get_bpc_loc_ids_and_last_synced(target_loc_ids)
        return {
            "token": f"region:{target_id}",
            "scope": "region",
            "region_id": target_id,
            "region_name": region_name,
            "location_name": (
                f"Region: {region_name}" if region_name else f"Region {target_id}"
            ),
            "item_ids": item_ids,
            "rejected": False,
            "last_synced": last_synced.isoformat() if last_synced else None,
        }

    if scope == "system":
        item_ids = blueprint_item_ids_at_source(user, system_id=target_id)
        if item_ids is None:
            return {
                "token": "all",
                "scope": "all",
                "item_ids": None,
                "rejected": True,
                "location_name": "All eligible locations",
            }
        system_name = _get_system_name(target_id)
        meta_map = resolve_location_system_and_region(all_bpc_loc_ids)
        target_loc_ids = [
            loc_id
            for loc_id in all_bpc_loc_ids
            if meta_map.get(loc_id, {}).get("system_id") == int(target_id)
        ]
        _, last_synced = _get_bpc_loc_ids_and_last_synced(target_loc_ids)
        return {
            "token": f"system:{target_id}",
            "scope": "system",
            "system_id": target_id,
            "system_name": system_name,
            "location_name": (
                f"Solar System: {system_name}" if system_name else f"System {target_id}"
            ),
            "item_ids": item_ids,
            "rejected": False,
            "last_synced": last_synced.isoformat() if last_synced else None,
        }

    location_id = target_id
    item_ids = blueprint_item_ids_at_source(
        user, location_id=location_id, container_item_id=container_item_id
    )
    if item_ids is None:
        return {
            "token": "all",
            "scope": "all",
            "item_ids": None,
            "rejected": True,
            "location_name": "All eligible locations",
        }
    names = resolve_cached_location_names([location_id])
    loc_meta = resolve_location_system_and_region([location_id]).get(location_id, {})
    _, last_synced = _get_bpc_loc_ids_and_last_synced([location_id])
    if raw_token_str.startswith("container:"):
        token_out = f"container:{location_id}:{container_item_id}"
    elif raw_token_str.startswith("station:"):
        token_out = f"station:{location_id}"
    elif container_item_id:
        token_out = f"{location_id}:{container_item_id}"
    else:
        token_out = str(location_id)

    return {
        "token": token_out,
        "scope": "container" if container_item_id else "station",
        "item_ids": item_ids,
        "rejected": False,
        "location_id": location_id,
        "container_item_id": container_item_id or None,
        "system_id": loc_meta.get("system_id"),
        "system_name": loc_meta.get("system_name"),
        "region_id": loc_meta.get("region_id"),
        "region_name": loc_meta.get("region_name"),
        **_describe_location(location_id, names),
        "last_synced": last_synced.isoformat() if last_synced else None,
    }
