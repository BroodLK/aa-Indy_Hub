import json
import os
import urllib.request
from decimal import Decimal

from django.apps import apps
from allianceauth.eveonline.models import EveCharacter

from indy_hub.services.public_contracts import (
    _coerce_openapi_value,
    _normalize_openapi_rows,
    _resolve_operation,
    _run_openapi_operation,
)
from indy_hub.services.esi_client import shared_client

SYSTEMS = {
    "F9-FUV": (30002320, 10000027),
    "LXQ2-T": (30002355, 10000027),
    "9WVY-F": (30005137, 10000066),
}
CAPITAL_GROUP_IDS = {30, 485, 513, 547, 659, 883}


def _value(row, *keys):
    for key in keys:
        if key in row:
            return row[key]
        camel = key.split("_")[0] + "".join(part.title() for part in key.split("_")[1:])
        if camel in row:
            return row[camel]
    return None


_JANICE_CACHE = {}


def _janice_batch(names: list[str]) -> None:
    missing = [n for n in names if n not in _JANICE_CACHE]
    if not missing:
        return
    for i in range(0, len(missing), 500):
        chunk = missing[i : i + 500]
        request = urllib.request.Request(
            "https://janice.e-351.com/api/rest/v2/appraisal",
            data="\n".join(chunk).encode(),
            headers={
                "Content-Type": "text/plain",
                "X-ApiKey": os.getenv("JANICE_API_KEY", ""),
                "User-Agent": "AA-IndyHub-PublicContractScanner/1.0",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                payload = json.load(response)
            for item in payload.get("items", []):
                name = item.get("itemType", {}).get("name")
                if name:
                    value = item.get("adjustedPrice") or item.get("sell", {}).get("min")
                    _JANICE_CACHE[name] = Decimal(str(value)) if value else None
        except Exception:
            pass


def _janice(name: str) -> Decimal | None:
    if name in _JANICE_CACHE:
        return _JANICE_CACHE[name]
    _janice_batch([name])
    return _JANICE_CACHE.get(name)


def scan_public_contracts(character_id: int = 0, max_pages: int = 2000, progress_callback=None):
    results = []
    diagnostics = {
        "target_matches": 0,
        "total_contracts": 0,
        "structure_failures": 0,
        "unresolved_locations": 0,
        "item_rows": 0,
        "ship_hulls": 0,
    }

    def log(msg):
        if progress_callback:
            try:
                progress_callback(msg, diagnostics)
            except TypeError:
                progress_callback(msg)

    esi_token = None
    if character_id:
        try:
            esi_token = shared_client._get_token(character_id, "esi-universe.read_structures.v1")
            char = EveCharacter.objects.get(character_id=character_id)
            log(f"Character: {char.character_name}")
        except Exception:
            esi_token = None
            log(f"Character ID: {character_id} (Token failed)")
    else:
        log("Character: Public (No token)")

    contracts_op = _resolve_operation("Contracts", "get_contracts_public_region_id")
    items_op = _resolve_operation("Contracts", "get_contracts_public_items_contract_id")
    structure_op = _resolve_operation("Universe", "get_universe_structures_structure_id")
    station_op = _resolve_operation("Universe", "get_universe_stations_station_id")
    type_op = _resolve_operation("Universe", "get_universe_types_type_id")
    group_op = _resolve_operation("Universe", "get_universe_groups_group_id")

    if not all((contracts_op, items_op, type_op, group_op)):
        raise RuntimeError("AA ESI client is missing a required public-contract operation.")

    type_cache, group_cache, location_cache, seen = {}, {}, {}, set()

    def call(operation, **kwargs):
        if esi_token and "structure_id" in kwargs:
            kwargs["token"] = esi_token
        payload = _run_openapi_operation(operation, prefer_disable_etag=True, **kwargs)
        return _coerce_openapi_value(payload)

    def location_system(location_id):
        if location_id in location_cache:
            return location_cache[location_id]
        operation = structure_op if location_id >= 1_000_000_000 else station_op
        if not operation:
            return 0
        try:
            row = call(operation, **({"structure_id": location_id} if location_id >= 1_000_000_000 else {"station_id": location_id}))
            location_cache[location_id] = int(_value(row, "solar_system_id", "system_id") or 0)
        except Exception:
            if location_id >= 1_000_000_000:
                diagnostics["structure_failures"] += 1
            location_cache[location_id] = 0
        if not location_cache[location_id]:
            diagnostics["unresolved_locations"] += 1
        return location_cache[location_id]

    region_targets = {}
    for system_name, (system_id, region_id) in SYSTEMS.items():
        if region_id not in region_targets:
            region_targets[region_id] = {}
        region_targets[region_id][system_id] = system_name

    ItemType = None
    if apps.is_installed("eve_sde"):
        try:
            from eve_sde.models import ItemType
        except ImportError:
            pass

    log(f"Scanning public contracts with AA's {'authenticated' if esi_token else 'public'} ESI client...")

    for region_id, target_systems in region_targets.items():
        log(f"Scanning region {region_id} for {', '.join(target_systems.values())}...")
        previous_page_signature = None
        for page in range(1, max_pages + 1):
            try:
                rows = call(contracts_op, region_id=region_id, page=page)
            except Exception as exc:
                log(f"  ESI page {page} failed: {type(exc).__name__}: {exc}")
                break
            if not isinstance(rows, list) or not rows:
                break
            rows = _normalize_openapi_rows(rows)
            page_signature = tuple(int(_value(row, "contract_id") or 0) for row in rows)
            if page_signature == previous_page_signature:
                log("  ESI repeated the previous page; scan complete.")
                break
            previous_page_signature = page_signature
            log(f"  ESI page {page}: {len(rows)} contracts")

            page_items = {}
            all_type_ids = set()
            for contract in rows:
                cid = int(_value(contract, "contract_id") or 0)
                ctype = _value(contract, "type")
                if cid in seen or ctype not in ("item_exchange", "auction"):
                    continue

                locs = [int(_value(contract, "start_location_id") or 0), int(_value(contract, "end_location_id") or 0)]
                res_sys_ids = {location_system(x) for x in locs if x}
                is_target = any(sid in target_systems for sid in res_sys_ids)
                is_potential = is_target or any(sid == 0 for sid in res_sys_ids)

                if not is_potential:
                    continue

                try:
                    items_payload = call(items_op, contract_id=cid)
                    page_items[cid] = _normalize_openapi_rows(items_payload)
                    for item in page_items[cid]:
                        all_type_ids.add(int(_value(item, "type_id") or 0))
                except Exception:
                    continue

            missing_tids = [tid for tid in all_type_ids if tid not in type_cache]
            if ItemType and missing_tids:
                for it in ItemType.objects.filter(id__in=missing_tids).select_related("group"):
                    type_cache[it.id] = {"name": it.name, "group_id": it.group_id}
                    group_cache[it.group_id] = {"category_id": it.group.category_id if it.group else 0}

            for tid in all_type_ids:
                if tid not in type_cache:
                    try:
                        type_cache[tid] = call(type_op, type_id=tid)
                        gid = int(_value(type_cache[tid], "group_id") or 0)
                        if gid not in group_cache:
                            group_cache[gid] = call(group_op, group_id=gid)
                    except Exception:
                        continue

            all_names = {str(_value(type_cache[tid], "name")) for tid in all_type_ids if tid in type_cache}
            _janice_batch(list(all_names))

            for contract in rows:
                cid = int(_value(contract, "contract_id") or 0)
                ctype = _value(contract, "type")
                if cid in seen or ctype not in ("item_exchange", "auction"):
                    continue
                if cid not in page_items:
                    continue

                seen.add(cid)
                diagnostics["total_contracts"] += 1

                locations = [int(_value(contract, "start_location_id") or 0), int(_value(contract, "end_location_id") or 0)]
                resolved_system_ids = {location_system(x) for x in locations if x}
                # A zero system ID means the location could not be resolved.
                # Do not present those contracts as matches for the target
                # systems; they were only fetched so diagnostics can account
                # for them when a structure token is unavailable.
                if not any(sid in target_systems for sid in resolved_system_ids):
                    continue
                matching_system_name = None
                label_system = None
                for loc_sys_id in resolved_system_ids:
                    if loc_sys_id in target_systems:
                        matching_system_name = target_systems[loc_sys_id]
                        diagnostics["target_matches"] += 1
                        label_system = matching_system_name
                        break
                    if loc_sys_id > 0:
                        label_system = f"System {loc_sys_id}"

                if not label_system:
                    label_system = f"Location {locations[0]}" if locations and locations[0] else "Unknown"

                bundle_value = Decimal("0")
                ships = []
                for item in page_items[cid]:
                    diagnostics["item_rows"] += 1
                    # ESI marks bundle components with is_included=false.
                    # Older/mocked responses may omit the field; treat those
                    # rows as included rather than silently dropping them.
                    if _value(item, "is_included") is False:
                        continue
                    type_id = int(_value(item, "type_id") or 0)
                    if type_id not in type_cache:
                        continue
                    info = type_cache[type_id]
                    name = str(_value(info, "name") or type_id)
                    quantity = max(1, int(_value(item, "quantity") or 1))
                    price = _janice(name)
                    if price:
                        bundle_value += price * quantity
                    group_id = int(_value(info, "group_id") or 0)
                    if group_id not in group_cache:
                        continue
                    if _value(group_cache[group_id], "category_id") == 6 or group_id in CAPITAL_GROUP_IDS:
                        ships.append(name)
                        diagnostics["ship_hulls"] += 1
                if ships:
                    contract_price = Decimal(str(_value(contract, "price") or 0)) + Decimal(str(_value(contract, "reward") or 0))
                    verdict = "No reference price"
                    if bundle_value:
                        verdict = "Fair Price" if abs(contract_price - bundle_value) <= bundle_value * Decimal(".05") else ("Above Average Price" if contract_price > bundle_value else "Below Average Price")
                    for ship in ships:
                        res_str = f"{ship} - {contract_price:,.0f} ISK - {label_system} - {verdict}"
                        results.append(res_str)
                        log(f"FOUND: {res_str}")

    return {
        "results": results,
        "diagnostics": diagnostics,
        "esi_token_used": bool(esi_token),
    }
