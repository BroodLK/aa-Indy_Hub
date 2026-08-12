"""Print ship public-contract prices in F9-FUV and 9W using AA's ESI client."""

import json
import os
import urllib.request
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError

from indy_hub.services.public_contracts import _resolve_operation, _run_openapi_operation, _coerce_openapi_value
from indy_hub.services.esi_client import shared_client

SYSTEMS = {
    "F9-FUV": (30002320, 10000027),
    "LXQ2-T": (30002355, 10000027),
    "9W": (30005137, 10000066),
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


def _janice(name: str) -> Decimal | None:
    request = urllib.request.Request(
        "https://janice.e-351.com/api/rest/v2/appraisal",
        data=(name + " 1\n").encode(),
        headers={"Content-Type": "text/plain", "X-ApiKey": os.getenv("JANICE_API_KEY", "")},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
        value = payload.get("adjustedPrice") or payload.get("sell", {}).get("min")
        return Decimal(str(value)) if value else None
    except Exception:
        return None


class Command(BaseCommand):
    help = "Print ship prices from public contracts in F9-FUV and 9W."

    def add_arguments(self, parser):
        parser.add_argument("--max-pages", type=int, default=2000)

    def handle(self, *args, **options):
        max_pages = max(1, options["max_pages"])
        contracts_op = _resolve_operation("Contracts", "get_contracts_public_region_id")
        items_op = _resolve_operation("Contracts", "get_contracts_public_items_contract_id")
        structure_op = _resolve_operation("Universe", "get_universe_structures_structure_id")
        station_op = _resolve_operation("Universe", "get_universe_stations_station_id")
        type_op = _resolve_operation("Universe", "get_universe_types_type_id")
        group_op = _resolve_operation("Universe", "get_universe_groups_group_id")
        if not all((contracts_op, items_op, type_op, group_op)):
            raise CommandError("AA ESI client is missing a required public-contract operation.")

        type_cache, group_cache, location_cache, seen = {}, {}, {}, set()
        results = []
        structure_failures = 0
        unresolved_locations = 0
        target_matches = 0
        self.stdout.write("Scanning public contracts with AA's authenticated ESI client...")

        def call(operation, **kwargs):
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
                nonlocal structure_failures
                if location_id >= 1_000_000_000:
                    structure_failures += 1
                location_cache[location_id] = 0
            if not location_cache[location_id]:
                nonlocal unresolved_locations
                unresolved_locations += 1
            return location_cache[location_id]

        for system_name, (system_id, region_id) in SYSTEMS.items():
            self.stdout.write(f"Scanning {system_name} in region {region_id}...")
            previous_page_signature = None
            for page in range(1, max_pages + 1):
                try:
                    rows = call(contracts_op, region_id=region_id, page=page)
                except Exception:
                    rows = []
                if not isinstance(rows, list) or not rows:
                    break
                page_signature = tuple(int(_value(row, "contract_id") or 0) for row in rows)
                if page_signature == previous_page_signature:
                    self.stdout.write("  ESI repeated the previous page; scan complete.")
                    break
                previous_page_signature = page_signature
                self.stdout.write(f"  ESI page {page}: {len(rows)} contracts")
                for contract in rows:
                    cid = int(_value(contract, "contract_id") or 0)
                    if cid in seen or _value(contract, "type") != "item_exchange" or _value(contract, "status") != "outstanding":
                        continue
                    seen.add(cid)
                    locations = [int(_value(contract, "start_location_id") or 0), int(_value(contract, "end_location_id") or 0)]
                    if system_id not in {location_system(x) for x in locations if x}:
                        continue
                    target_matches += 1
                    try:
                        items = call(items_op, contract_id=cid)
                    except Exception:
                        self.stdout.write(f"  Contract {cid}: item details unavailable; skipping.")
                        continue
                    bundle_value = Decimal("0")
                    ships = []
                    for item in items if isinstance(items, list) else []:
                        if not _value(item, "is_included"):
                            continue
                        type_id = int(_value(item, "type_id") or 0)
                        if type_id not in type_cache:
                            type_cache[type_id] = call(type_op, type_id=type_id)
                        info = type_cache[type_id]
                        name = str(_value(info, "name") or type_id)
                        quantity = max(1, int(_value(item, "quantity") or 1))
                        price = _janice(name)
                        if price:
                            bundle_value += price * quantity
                        group_id = int(_value(info, "group_id") or 0)
                        if group_id not in group_cache:
                            group_cache[group_id] = call(group_op, group_id=group_id)
                        if _value(group_cache[group_id], "category_id") == 6 or group_id in CAPITAL_GROUP_IDS:
                            ships.append(name)
                    if ships:
                        contract_price = Decimal(str(_value(contract, "price") or 0)) + Decimal(str(_value(contract, "reward") or 0))
                        verdict = "No reference price"
                        if bundle_value:
                            verdict = "Fair Price" if abs(contract_price - bundle_value) <= bundle_value * Decimal(".05") else ("Above Average Price" if contract_price > bundle_value else "Below Average Price")
                        for ship in ships:
                            results.append(f"{ship} - {contract_price:,.0f} ISK - {system_name} - {verdict}")

        self.stdout.write("\nFinal list:")
        self.stdout.write(
            f"Diagnostics: target contracts={target_matches}, "
            f"structure lookup failures={structure_failures}, "
            f"unresolved locations={unresolved_locations}"
        )
        self.stdout.write("\n".join(results) if results else "No matching ship contracts found.")
