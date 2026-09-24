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
from datetime import timedelta
from typing import Any, Iterable

# Django
from django.db.models import Max, Sum
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
            int(station_id): str(name).strip() for station_id, name in rows if station_id and str(name or "").strip()
        }
    except Exception:
        logger.debug("SDE station names unavailable", exc_info=True)
        return {}


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
        location_id for location_id in ids if location_id not in resolved or resolved[location_id]["is_placeholder"]
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
            "name_last_resolved": (entry["last_resolved"].isoformat() if entry["last_resolved"] else None),
        }
    # No cached name yet. Say so rather than presenting an ID as a name.
    return {
        "location_name": f"{PLACEHOLDER_PREFIX}{location_id}",
        "name_is_placeholder": True,
        "name_last_resolved": None,
    }


def list_asset_sources(user, *, blueprints: bool = False) -> dict[str, Any]:
    """Locations where the user has cached assets, with names and freshness.

    ``blueprints=True`` lists BPC/BPO locations instead of materials.
    """
    base = CachedCharacterAsset.objects.filter(user=user, is_blueprint=blueprints)
    rows = list(base.values("location_id").annotate(last_synced=Max("synced_at")).order_by("location_id"))
    location_ids = [int(row["location_id"]) for row in rows if row.get("location_id")]
    if not location_ids:
        return {
            "sources": [],
            "max_age_seconds": int(asset_cache_max_age().total_seconds()),
        }

    flags_by_location: dict[int, set[str]] = {}
    containers_by_location: dict[int, dict[int, str]] = {}
    for row in base.filter(location_id__in=location_ids).values(
        "location_id", "location_flag", "raw_location_id", "item_id", "set_name"
    ):
        location_id = int(row["location_id"])
        flag = str(row.get("location_flag") or "").strip()
        if flag:
            flags_by_location.setdefault(location_id, set()).add(flag)
        # A raw_location_id that differs from the resolved root is a container.
        raw_location_id = row.get("raw_location_id")
        if raw_location_id and int(raw_location_id) != location_id:
            containers_by_location.setdefault(location_id, {}).setdefault(int(raw_location_id), "")

    # Name the containers from their own asset rows.
    container_item_ids = {item_id for containers in containers_by_location.values() for item_id in containers}
    if container_item_ids:
        for row in base.model.objects.filter(user=user, item_id__in=sorted(container_item_ids)).values(
            "item_id", "set_name", "type_id"
        ):
            label = str(row.get("set_name") or "").strip() or get_type_name(int(row["type_id"]))
            for containers in containers_by_location.values():
                if int(row["item_id"]) in containers:
                    containers[int(row["item_id"])] = label

    max_age = asset_cache_max_age()
    now = timezone.now()
    names = resolve_cached_location_names(location_ids)

    sources = []
    for row in rows:
        if not row.get("location_id"):
            continue
        location_id = int(row["location_id"])
        last_synced = row["last_synced"]
        sources.append(
            {
                "location_id": location_id,
                **_describe_location(location_id, names),
                "last_synced": last_synced.isoformat() if last_synced else None,
                "is_stale": bool(last_synced and (now - last_synced) > max_age),
                "location_flags": sorted(flags_by_location.get(location_id, set())),
                "containers": [
                    {"item_id": item_id, "name": label or f"Container {item_id}"}
                    for item_id, label in sorted(containers_by_location.get(location_id, {}).items())
                ],
            }
        )

    return {
        "sources": sources,
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
    type_ids: list[int] | None = None,
    location_flag: str = "",
    container_item_id: int = 0,
    blueprints: bool = False,
) -> dict[str, Any] | None:
    """Cached quantities at one user-authorized location.

    Returns None when the user has no assets at that location at all, which the
    caller should treat as "not available to you".
    """
    owns_location = CachedCharacterAsset.objects.filter(user=user, location_id=location_id).exists()
    if not owns_location:
        return None

    scoped = CachedCharacterAsset.objects.filter(user=user, location_id=location_id, is_blueprint=blueprints)

    # Narrowing by flag or container needs the parent chain, so pull the rows
    # and let the shared helpers walk them.
    if location_flag or container_item_id:
        rows = list(
            scoped.values(
                "item_id",
                "raw_location_id",
                "location_id",
                "location_flag",
                "type_id",
                "quantity",
                "synced_at",
            )
        )
        # The index must span every asset at the location, not just the scoped
        # subset, or container parents would be missing from the chain.
        chain_rows = list(
            CachedCharacterAsset.objects.filter(user=user, location_id=location_id).values(
                "item_id", "raw_location_id", "location_id", "location_flag"
            )
        )
        index = build_asset_index_by_item_id(_rows_as_esi_shape(chain_rows))

        matched = []
        for asset in _rows_as_esi_shape(rows):
            if location_flag and not asset_chain_has_context(
                asset,
                index,
                location_id=location_id,
                location_flag=location_flag,
                max_depth=MAX_CONTAINER_DEPTH,
            ):
                continue
            if container_item_id and not _has_container_ancestor(asset, index, container_item_id):
                continue
            matched.append(asset["_row"])

        wanted = set(type_ids or [])
        grouped: dict[int, dict[str, Any]] = {}
        for row in matched:
            type_id = int(row["type_id"])
            if wanted and type_id not in wanted:
                continue
            entry = grouped.setdefault(type_id, {"quantity": 0, "last_synced": row["synced_at"]})
            entry["quantity"] += int(row["quantity"] or 0)
            if row["synced_at"] and row["synced_at"] > entry["last_synced"]:
                entry["last_synced"] = row["synced_at"]
        aggregated = [
            {
                "type_id": type_id,
                "quantity": data["quantity"],
                "last_synced": data["last_synced"],
            }
            for type_id, data in grouped.items()
        ]
    else:
        if type_ids:
            scoped = scoped.filter(type_id__in=type_ids[:MAX_TYPE_FILTER])
        aggregated = [
            {
                "type_id": int(row["type_id"]),
                "quantity": int(row["quantity"] or 0),
                "last_synced": row["last_synced"],
            }
            for row in scoped.values("type_id").annotate(quantity=Sum("quantity"), last_synced=Max("synced_at"))
        ]

    max_age = asset_cache_max_age()
    now = timezone.now()
    names = resolve_cached_location_names([location_id])
    newest = max((row["last_synced"] for row in aggregated if row["last_synced"]), default=None)

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
                "last_synced": (row["last_synced"].isoformat() if row["last_synced"] else None),
            }
            for row in sorted(aggregated, key=lambda item: item["type_id"])
            if row["quantity"] > 0
        ],
    }


def parse_bpc_source(value: Any) -> tuple[int, int]:
    """Parse a ``"<location_id>"`` or ``"<location_id>:<container_id>"`` token.

    Returns (0, 0) for "all eligible locations" or anything malformed.
    """
    text = str(value or "").strip()
    if not text or text == "all":
        return 0, 0
    location_part, _sep, container_part = text.partition(":")
    try:
        location_id = int(location_part)
        container_item_id = int(container_part) if container_part else 0
    except ValueError:
        return 0, 0
    if location_id <= 0 or container_item_id < 0:
        return 0, 0
    return location_id, container_item_id


def blueprint_item_ids_at_source(user, *, location_id: int, container_item_id: int = 0) -> set[int] | None:
    """Item IDs of the user's cached blueprints at one source.

    Returns None when the user has no cached blueprints there, so a location
    ID from a browser or preference is never proof of access: the caller only
    ever narrows the user's own blueprints to these IDs. A container narrows
    further to blueprints anywhere inside it (nested containers included).
    """
    scoped = CachedCharacterAsset.objects.filter(user=user, location_id=location_id, is_blueprint=True)
    if not scoped.exists():
        return None
    if not container_item_id:
        return {int(item_id) for item_id in scoped.values_list("item_id", flat=True) if item_id}

    chain_rows = list(
        CachedCharacterAsset.objects.filter(user=user, location_id=location_id).values(
            "item_id", "raw_location_id", "location_id", "location_flag"
        )
    )
    index = build_asset_index_by_item_id(_rows_as_esi_shape(chain_rows))
    rows = list(scoped.values("item_id", "raw_location_id", "location_id", "location_flag"))
    return {
        int(asset["item_id"])
        for asset in _rows_as_esi_shape(rows)
        if asset["item_id"] and _has_container_ancestor(asset, index, container_item_id)
    }


def describe_bpc_source(user, token: Any) -> dict[str, Any]:
    """Resolve a BPC source token to a validated, named description.

    ``item_ids`` is None for "all eligible locations" (no narrowing) and a set
    otherwise; an invalid or foreign source is reported as rejected and falls
    back to no narrowing rather than to an empty set.
    """
    location_id, container_item_id = parse_bpc_source(token)
    if not location_id:
        return {"token": "all", "item_ids": None, "rejected": bool(str(token or "").strip() not in ("", "all"))}
    item_ids = blueprint_item_ids_at_source(user, location_id=location_id, container_item_id=container_item_id)
    if item_ids is None:
        return {"token": "all", "item_ids": None, "rejected": True}
    names = resolve_cached_location_names([location_id])
    last_synced = (
        CachedCharacterAsset.objects.filter(user=user, location_id=location_id, is_blueprint=True)
        .aggregate(last=Max("synced_at"))
        .get("last")
    )
    return {
        "token": f"{location_id}:{container_item_id}" if container_item_id else str(location_id),
        "item_ids": item_ids,
        "rejected": False,
        "location_id": location_id,
        "container_item_id": container_item_id or None,
        **_describe_location(location_id, names),
        "last_synced": last_synced.isoformat() if last_synced else None,
    }
