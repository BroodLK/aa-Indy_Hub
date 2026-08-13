from decimal import Decimal
from datetime import timedelta

from django.apps import apps
from django.core.cache import cache
from django.utils import timezone
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
# Authoritative structure IDs supplied by the operator. Public-contract
# locations can be structure IDs, and matching them directly avoids depending
# on structure/station resolution before deciding whether a contract belongs
# to one of the target locations.
TARGET_LOCATIONS = {
    1036732971380: "F9-FUV",
    1051373249547: "LXQ2-T",
    1051373639126: "9WVY-F",
}
CAPITAL_GROUP_IDS = {30, 485, 513, 547, 659, 883}
MIN_CONTRACT_PRICE = Decimal("500000000")
SCAN_STATE_TIMEOUT = 7 * 24 * 60 * 60
GONE_MARK_WINDOW = timedelta(hours=36)


def _format_isk(value: Decimal) -> str:
    if value >= Decimal("1000000000"):
        number = value / Decimal("1000000000")
        suffix = "bil"
    else:
        number = value / Decimal("1000000")
        suffix = "mil"
    formatted = f"{number:.2f}".rstrip("0").rstrip(".")
    return f"{formatted}{suffix}"


def _value(row, *keys):
    for key in keys:
        if key in row:
            return row[key]
        camel = key.split("_")[0] + "".join(part.title() for part in key.split("_")[1:])
        if camel in row:
            return row[camel]
    return None


def scan_public_contracts(character_id: int = 0, max_pages: int = 2000, progress_callback=None):
    result_rows = []
    state_key = f"indy-hub:public-contract-scan-results:{int(character_id or 0)}"
    previous_rows = cache.get(state_key) or []
    previous_by_id = {int(row["contract_id"]): row for row in previous_rows if row.get("contract_id")}
    terminal_by_id = {}
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
                status = str(_value(contract, "status") or "").lower()
                if status in {"completed", "deleted"}:
                    # Terminal rows are consulted only to mark a previously
                    # displayed outstanding contract as gone. They never get
                    # item details or enter the current result set.
                    if cid in previous_by_id:
                        terminal_by_id[cid] = contract
                    continue
                # The public-contract endpoint commonly omits status. In
                # that response shape, the endpoint itself represents the
                # outstanding listing. Reject only explicit terminal states.
                if status and status != "outstanding":
                    continue
                if cid in seen or ctype not in ("item_exchange", "auction"):
                    continue

                locs = [int(_value(contract, "start_location_id") or 0), int(_value(contract, "end_location_id") or 0)]
                res_sys_ids = {location_system(x) for x in locs if x}
                is_target = any(location_id in TARGET_LOCATIONS for location_id in locs) or any(
                    sid in target_systems for sid in res_sys_ids
                )
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

            for contract in rows:
                cid = int(_value(contract, "contract_id") or 0)
                ctype = _value(contract, "type")
                status = str(_value(contract, "status") or "").lower()
                if status and status != "outstanding":
                    continue
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
                direct_target_name = next(
                    (TARGET_LOCATIONS[location_id] for location_id in locations if location_id in TARGET_LOCATIONS),
                    None,
                )
                if not direct_target_name and not any(sid in target_systems for sid in resolved_system_ids):
                    continue
                matching_system_name = None
                label_system = None
                if direct_target_name:
                    matching_system_name = direct_target_name
                    diagnostics["target_matches"] += 1
                    label_system = direct_target_name
                for loc_sys_id in resolved_system_ids:
                    if matching_system_name:
                        break
                    if loc_sys_id in target_systems:
                        matching_system_name = target_systems[loc_sys_id]
                        diagnostics["target_matches"] += 1
                        label_system = matching_system_name
                        break
                    if loc_sys_id > 0:
                        label_system = f"System {loc_sys_id}"

                if not label_system:
                    label_system = f"Location {locations[0]}" if locations and locations[0] else "Unknown"

                ships = []
                capital_ships = []
                non_ship_item_count = 0
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
                    group_id = int(_value(info, "group_id") or 0)
                    if group_id not in group_cache:
                        continue
                    if _value(group_cache[group_id], "category_id") == 6 or group_id in CAPITAL_GROUP_IDS:
                        ships.append(name)
                        if group_id in CAPITAL_GROUP_IDS:
                            capital_ships.append(name)
                        diagnostics["ship_hulls"] += 1
                    else:
                        non_ship_item_count += 1
                if ships:
                    contract_price = Decimal(str(_value(contract, "price") or 0)) + Decimal(str(_value(contract, "reward") or 0))
                    if contract_price < MIN_CONTRACT_PRICE:
                        continue
                    ships = list(dict.fromkeys(ships))
                    capital_ships = list(dict.fromkeys(capital_ships))
                    primary_ship = capital_ships[0] if capital_ships else ships[0]
                    ship_label = primary_ship
                    nested_ships = [ship for ship in ships if ship != primary_ship]
                    fit_label = "Likely Fit" if non_ship_item_count > 5 else "Likely Unfit"
                    result_rows.append({
                        "contract_id": cid,
                        "location": label_system,
                        "location_order": {"F9-FUV": 0, "LXQ2-T": 1, "9WVY-F": 2}.get(label_system, 99),
                        "price": contract_price,
                        "text": f"{ship_label} - {_format_isk(contract_price)} - {label_system} - {fit_label}",
                        "contents": nested_ships,
                        "is_new": cid not in previous_by_id,
                        "is_gone": False,
                    })
                    res_str = result_rows[-1]["text"]
                    log(f"FOUND: {res_str}")

    current_ids = {row["contract_id"] for row in result_rows}
    now = timezone.now()
    for row in result_rows:
        row["is_new"] = row["contract_id"] not in previous_by_id

    # A missing outstanding row is gone unless its known completion is older
    # than 36 hours. Deleted/completed ESI rows are used only for this check.
    for old_id, old_row in previous_by_id.items():
        if old_id in current_ids:
            continue
        contract = terminal_by_id.get(old_id)
        completed_at = _value(contract, "date_completed") if contract else old_row.get("date_completed")
        completed_dt = None
        if completed_at:
            try:
                completed_dt = timezone.datetime.fromisoformat(str(completed_at).replace("Z", "+00:00"))
                if timezone.is_naive(completed_dt):
                    completed_dt = timezone.make_aware(completed_dt)
            except (TypeError, ValueError):
                completed_dt = None
        if completed_dt is None or now - completed_dt <= GONE_MARK_WINDOW:
            gone_row = dict(old_row)
            gone_row["is_gone"] = True
            gone_row["is_new"] = False
            result_rows.append(gone_row)

    result_rows.sort(key=lambda row: (row["location_order"], -row["price"]))
    results = []
    previous_location = None
    for row in result_rows:
        if row["location"] != previous_location:
            results.append(f"### Contracts Available in {row['location']}")
            previous_location = row["location"]
        prefix = "**NEW** " if row.get("is_new") else ""
        text = f"~~{row['text']}~~" if row.get("is_gone") else row["text"]
        results.append(f"- {prefix}{text}")
        if row["contents"]:
            contents = ", ".join(row["contents"])
            results.append(f"   - {contents}")

    cache.set(state_key, result_rows, timeout=SCAN_STATE_TIMEOUT)

    return {
        "results": results,
        "diagnostics": diagnostics,
        "esi_token_used": bool(esi_token),
    }
