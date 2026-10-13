"""Fleet Loot Splitter and Loss Reimbursement calculation service."""

from __future__ import annotations

# Standard Library
import json
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any
from urllib.parse import urlparse

# Third Party
import requests

# Django
from django.core.cache import cache

# Alliance Auth
from allianceauth.eveonline.models import EveCharacter
from allianceauth.services.hooks import get_extension_logger

from ..utils.eve import get_type_name
from .fuzzwork import fetch_fuzzwork_prices

logger = get_extension_logger(__name__)

CACHE_TTL_KILLMAIL = 86400  # 24 hours
CACHE_TTL_PRICE = 3600  # 1 hour
DEFAULT_USER_AGENT = "AllianceAuth-IndyHub-LootSplitter/2.0.0"


def _to_decimal(val: Any) -> Decimal:
    """Safely convert any value (including 150m, 1.5b, 50k strings) to Decimal with 2 decimal places."""
    if val is None:
        return Decimal("0.00")
    if isinstance(val, Decimal):
        return val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    try:
        cleaned = str(val).strip().replace(",", "").lower()
        if not cleaned:
            return Decimal("0.00")
        multiplier = Decimal("1")
        if cleaned.endswith("b") or cleaned.endswith("bil") or cleaned.endswith("billion"):
            multiplier = Decimal("1000000000")
            cleaned = re.sub(r"[^\d.]", "", cleaned)
        elif cleaned.endswith("m") or cleaned.endswith("mil") or cleaned.endswith("million"):
            multiplier = Decimal("1000000")
            cleaned = re.sub(r"[^\d.]", "", cleaned)
        elif cleaned.endswith("k") or cleaned.endswith("thousand"):
            multiplier = Decimal("1000")
            cleaned = re.sub(r"[^\d.]", "", cleaned)
        else:
            cleaned = re.sub(r"[^\d.-]", "", cleaned)

        if not cleaned:
            return Decimal("0.00")
        return (Decimal(cleaned) * multiplier).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0.00")


def resolve_fleet_pilots(raw_names_text: str) -> list[dict[str, Any]]:
    """
    Parse a multiline text of pilot names and resolve alts to main characters.
    
    Returns a list of participant dictionaries with main character info,
    grouped alts, and registration status.
    """
    lines = [line.strip() for line in (raw_names_text or "").splitlines() if line.strip()]
    if not lines:
        return []

    # Clean character names (remove common prefixes/suffixes like [CORP], tabs, etc.)
    cleaned_input_names: list[str] = []
    seen_raw = set()
    for raw in lines:
        cleaned = re.sub(r"\[.*?\]", "", raw).strip()
        cleaned = re.sub(r"\(.*?\)", "", cleaned).strip()
        cleaned = cleaned.strip()
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key not in seen_raw:
            seen_raw.add(key)
            cleaned_input_names.append(cleaned)

    if not cleaned_input_names:
        return []

    # Query EveCharacter for all matching names
    char_records = (
        EveCharacter.objects.filter(character_name__in=cleaned_input_names)
        .select_related("character_ownership__user__profile__main_character")
    )
    char_map: dict[str, EveCharacter] = {
        char.character_name.casefold(): char for char in char_records
    }

    # Group characters by Main Character
    mains_dict: dict[str, dict[str, Any]] = {}

    for input_name in cleaned_input_names:
        input_key = input_name.casefold()
        char_obj = char_map.get(input_key)

        if char_obj:
            # Check if user has a main character assigned
            main_char = None
            try:
                ownership = getattr(char_obj, "character_ownership", None)
                if ownership and ownership.user and hasattr(ownership.user, "profile"):
                    main_char = ownership.user.profile.main_character
            except Exception:
                main_char = None

            resolved_main = main_char or char_obj
            main_name = resolved_main.character_name
            main_id = resolved_main.character_id
            main_key = main_name.casefold()

            if main_key not in mains_dict:
                mains_dict[main_key] = {
                    "id": f"pilot_{main_id}",
                    "name": main_name,
                    "character_id": main_id,
                    "portrait_url": f"https://images.evetech.net/characters/{main_id}/portrait?size=64",
                    "is_registered": True,
                    "alts": [],
                    "declined": False,
                }

            if input_name.casefold() != main_name.casefold() and input_name not in mains_dict[main_key]["alts"]:
                mains_dict[main_key]["alts"].append(input_name)
        else:
            # Unlinked / External pilot
            ext_key = input_key
            if ext_key not in mains_dict:
                mains_dict[ext_key] = {
                    "id": f"ext_{abs(hash(input_name)) % 10000000}",
                    "name": input_name,
                    "character_id": None,
                    "portrait_url": "https://images.evetech.net/characters/1/portrait?size=64",
                    "is_registered": False,
                    "alts": [],
                    "declined": False,
                }

    return list(mains_dict.values())


def _get_buyback_pricing_for_types(type_ids: list[int]) -> dict[int, Decimal]:
    """Calculate Indy Hub buyback price for type IDs if configured."""
    buyback_prices: dict[int, Decimal] = {}
    if not type_ids:
        return buyback_prices

    try:
        from ..views.material_exchange import (
            _get_material_exchange_config,
            _is_material_exchange_enabled,
            _get_stock_jita_price_map,
            _get_item_price_override_maps,
            _get_market_group_price_override_maps,
            _get_container_price_override_maps,
            _get_type_market_group_path_map,
            _compute_effective_sell_unit_price,
            _build_refined_ore_pricing_context,
        )

        if not _is_material_exchange_enabled():
            return buyback_prices

        config = _get_material_exchange_config()
        if not config:
            return buyback_prices

        price_data = _get_stock_jita_price_map(config=config, type_ids=type_ids)
        sell_override_map, _ = _get_item_price_override_maps(config)
        sell_mg_override_map, _ = _get_market_group_price_override_maps(config)
        sell_container_override, _ = _get_container_price_override_maps(config)
        type_mg_path_map = _get_type_market_group_path_map(type_ids)

        refined_ore_context = _build_refined_ore_pricing_context(
            config=config,
            type_ids=type_ids,
            price_data=price_data,
        )
        ore_reprocessing_map = refined_ore_context["ore_reprocessing_map"]
        ore_portion_size_map = refined_ore_context["ore_portion_size_map"]
        mineral_effective_sell_prices = refined_ore_context["mineral_effective_sell_prices"]

        for type_id in type_ids:
            fuzz_prices = price_data.get(type_id, {})
            jita_buy = Decimal(str(fuzz_prices.get("buy") or 0))
            jita_sell = Decimal(str(fuzz_prices.get("sell") or 0))

            unit_price, _default_unit_price, _has_override = (
                _compute_effective_sell_unit_price(
                    config=config,
                    type_id=type_id,
                    jita_buy=jita_buy,
                    jita_sell=jita_sell,
                    sell_override_map=sell_override_map,
                    sell_market_group_override_map=sell_mg_override_map,
                    type_market_group_path_map=type_mg_path_map,
                    sell_container_override=sell_container_override,
                    in_container=False,
                    ore_reprocessing_map=ore_reprocessing_map,
                    ore_portion_size_map=ore_portion_size_map,
                    mineral_effective_sell_prices=mineral_effective_sell_prices,
                )
            )
            if unit_price is not None and unit_price > 0:
                buyback_prices[type_id] = unit_price

    except Exception as exc:
        logger.warning("Error computing buyback prices in loot splitter: %s", exc)

    return buyback_prices


def parse_and_price_loot(
    raw_loot_text: str,
    valuation_mode: str = "buyback",
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Parse pasted loot items and fetch market and buyback pricing.
    
    Returns (item_rows, invalid_lines).
    """
    from ..views.material_exchange import _parse_sell_estimate_input

    parsed_rows, invalid_lines = _parse_sell_estimate_input(raw_loot_text)
    if not parsed_rows:
        return [], invalid_lines

    type_ids = [int(row["type_id"]) for row in parsed_rows]
    
    # Fetch Market Prices (Fuzzwork Jita)
    fuzz_prices: dict[int, dict[str, Decimal]] = {}
    try:
        fuzz_prices = fetch_fuzzwork_prices(type_ids)
    except Exception as exc:
        logger.warning("Failed to fetch Fuzzwork prices: %s", exc)

    # Fetch Buyback Prices
    buyback_price_map = _get_buyback_pricing_for_types(type_ids)

    items: list[dict[str, Any]] = []
    for idx, row in enumerate(parsed_rows):
        type_id = int(row["type_id"])
        quantity = int(row["quantity"])
        type_name = get_type_name(type_id) or f"Type #{type_id}"

        # Market price calculation (prefer Jita sell/split)
        m_info = fuzz_prices.get(type_id, {})
        buy_p = _to_decimal(m_info.get("buy", 0))
        sell_p = _to_decimal(m_info.get("sell", 0))
        
        if sell_p > 0 and buy_p > 0:
            market_unit_price = ((buy_p + sell_p) / Decimal("2")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        elif sell_p > 0:
            market_unit_price = sell_p
        elif buy_p > 0:
            market_unit_price = buy_p
        else:
            market_unit_price = Decimal("0.00")

        # Buyback price calculation
        buyback_unit_price = buyback_price_map.get(type_id)
        if buyback_unit_price is None or buyback_unit_price <= 0:
            # Fallback to 90% of market price if no explicit buyback config
            buyback_unit_price = (market_unit_price * Decimal("0.90")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Chosen price based on valuation mode
        if valuation_mode == "market":
            chosen_price = market_unit_price
        else:
            chosen_price = buyback_unit_price

        items.append({
            "id": f"item_{idx}_{type_id}",
            "type_id": type_id,
            "name": type_name,
            "quantity": quantity,
            "icon_url": f"https://images.evetech.net/types/{type_id}/icon?size=64",
            "market_unit_price": float(market_unit_price),
            "buyback_unit_price": float(buyback_unit_price),
            "chosen_unit_price": float(chosen_price),
            "total_value": float((chosen_price * quantity).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
            "kept_by_pilot_id": None,
            "kept_quantity": 0,
        })

    return items, invalid_lines


def fetch_zkillboard_killmail(kill_url_or_id: str) -> dict[str, Any]:
    """
    Fetch killmail details from zKillboard API.
    
    Accepts full URL or raw killmail ID.
    """
    cleaned = str(kill_url_or_id or "").strip()
    if not cleaned:
        raise ValueError("Killmail URL or ID cannot be empty.")

    # Extract ID
    match = re.search(r"kill(?:ID)?/(\d+)", cleaned, re.IGNORECASE)
    if match:
        kill_id = int(match.group(1))
    elif cleaned.isdigit():
        kill_id = int(cleaned)
    else:
        raise ValueError(f"Could not parse a valid killmail ID from '{cleaned}'.")

    cache_key = f"indy_hub:zkill:{kill_id}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    url = f"https://zkillboard.com/api/killID/{kill_id}/"
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept-Encoding": "gzip",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=12)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("Failed to fetch zKillboard data for %s: %s", kill_id, exc)
        raise RuntimeError(f"Unable to reach zKillboard for killmail #{kill_id}: {exc}") from exc

    if not data or not isinstance(data, list) or len(data) == 0:
        raise ValueError(f"zKillboard returned no data for killmail #{kill_id}.")

    km = data[0]
    zkb = km.get("zkb", {})
    victim = km.get("victim", {})

    total_value = _to_decimal(zkb.get("totalValue", 0))
    fitted_value = _to_decimal(zkb.get("fittedValue", 0))
    loss_val = total_value if total_value > 0 else fitted_value

    victim_char_id = victim.get("character_id")
    victim_char_name = ""
    if victim_char_id:
        char_obj = EveCharacter.objects.filter(character_id=victim_char_id).first()
        if char_obj:
            victim_char_name = char_obj.character_name

    ship_type_id = victim.get("ship_type_id")
    ship_name = get_type_name(ship_type_id) if ship_type_id else "Unknown Ship"

    result = {
        "id": f"km_{kill_id}",
        "killmail_id": kill_id,
        "url": f"https://zkillboard.com/kill/{kill_id}/",
        "victim_character_id": victim_char_id,
        "victim_name": victim_char_name,
        "ship_type_id": ship_type_id,
        "ship_name": ship_name,
        "ship_icon_url": f"https://images.evetech.net/types/{ship_type_id}/render?size=64" if ship_type_id else "",
        "loss_value": float(loss_val),
    }

    cache.set(cache_key, result, CACHE_TTL_KILLMAIL)
    return result


def calculate_fleet_loot_split(
    pilots: list[dict[str, Any]],
    loot_items: list[dict[str, Any]],
    killmails: list[dict[str, Any]],
    valuation_mode: str = "buyback",
) -> dict[str, Any]:
    """
    Calculate the net settlement distribution for fleet loot and loss reimbursements.
    
    Formula:
      Gross Loot Value = sum(item.quantity * item.chosen_price)
      Total Loss Reimbursements = sum(killmail.loss_value)
      Net Distributable Pot = max(0, Gross Loot Value - Total Loss Reimbursements)
      Eligible Participants = pilots who have not declined repayment
      Base Share per Eligible Pilot = Net Distributable Pot / len(Eligible Participants)
      
      For each Pilot:
        Reimbursement = sum(killmails for this pilot)
        Kept Items Value = sum(items kept by this pilot)
        Base Share = Base Share (if eligible) else 0
        Net Balance = Base Share + Reimbursement - Kept Items Value
    """
    # 1. Calculate Gross Loot Value
    gross_loot_value = Decimal("0.00")
    for item in loot_items:
        unit_price = _to_decimal(item.get("chosen_unit_price", 0))
        qty = int(item.get("quantity", 0))
        gross_loot_value += (unit_price * qty).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # 2. Calculate Total Loss Reimbursements
    total_loss_reimbursements = Decimal("0.00")
    pilot_reimbursements: dict[str, Decimal] = {}
    for km in killmails:
        val = _to_decimal(km.get("loss_value", 0))
        total_loss_reimbursements += val
        assigned_pilot_id = km.get("assigned_pilot_id") or km.get("pilot_id")
        if assigned_pilot_id:
            pilot_reimbursements[str(assigned_pilot_id)] = (
                pilot_reimbursements.get(str(assigned_pilot_id), Decimal("0.00")) + val
            )

    # 3. Calculate Kept Items Value per Pilot
    pilot_kept_values: dict[str, Decimal] = {}
    total_kept_value = Decimal("0.00")
    for item in loot_items:
        kept_by = item.get("kept_by_pilot_id")
        kept_qty = int(item.get("kept_quantity") or 0)
        unit_price = _to_decimal(item.get("chosen_unit_price", 0))
        if kept_by and kept_qty > 0:
            val = (unit_price * kept_qty).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            pilot_kept_values[str(kept_by)] = pilot_kept_values.get(str(kept_by), Decimal("0.00")) + val
            total_kept_value += val

    # 4. Calculate Net Distributable Pot
    net_distributable_pot = max(Decimal("0.00"), gross_loot_value - total_loss_reimbursements)

    # 5. Eligible (Non-declined) Pilots
    eligible_pilots = [p for p in pilots if not p.get("declined", False)]
    eligible_count = len(eligible_pilots)

    if eligible_count > 0:
        base_share_per_pilot = (net_distributable_pot / Decimal(str(eligible_count))).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
    else:
        base_share_per_pilot = Decimal("0.00")

    # 6. Build Breakdown per Pilot
    pilot_settlements: list[dict[str, Any]] = []
    debtors: list[dict[str, Any]] = []   # owe money (net_balance < 0)
    creditors: list[dict[str, Any]] = [] # receive money (net_balance > 0)

    for pilot in pilots:
        p_id = str(pilot.get("id"))
        p_name = pilot.get("name", "Unknown Pilot")
        is_declined = bool(pilot.get("declined", False))

        pilot_base = Decimal("0.00") if is_declined else base_share_per_pilot
        pilot_reimb = pilot_reimbursements.get(p_id, Decimal("0.00"))
        pilot_kept = pilot_kept_values.get(p_id, Decimal("0.00"))

        net_balance = (pilot_base + pilot_reimb - pilot_kept).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        payout_received = max(Decimal("0.00"), net_balance)
        amount_owed = max(Decimal("0.00"), -net_balance)

        settlement_entry = {
            "id": p_id,
            "name": p_name,
            "character_id": pilot.get("character_id"),
            "portrait_url": pilot.get("portrait_url"),
            "alts": pilot.get("alts", []),
            "declined": is_declined,
            "base_share": float(pilot_base),
            "reimbursement": float(pilot_reimb),
            "kept_value": float(pilot_kept),
            "net_balance": float(net_balance),
            "payout_received": float(payout_received),
            "amount_owed": float(amount_owed),
            "status": "declined" if is_declined else ("receives" if net_balance > 0 else ("owes" if net_balance < 0 else "even")),
        }
        pilot_settlements.append(settlement_entry)

        if net_balance < 0:
            debtors.append({"id": p_id, "name": p_name, "amount": -net_balance})
        elif net_balance > 0:
            creditors.append({"id": p_id, "name": p_name, "amount": net_balance})

    # 7. Calculate direct Settlement Transfers (who pays whom)
    transfers: list[dict[str, Any]] = []
    # Work on copies with Decimals
    d_list = [{"id": d["id"], "name": d["name"], "amount": d["amount"]} for d in debtors]
    c_list = [{"id": c["id"], "name": c["name"], "amount": c["amount"]} for c in creditors]

    d_idx = 0
    c_idx = 0
    while d_idx < len(d_list) and c_idx < len(c_list):
        debtor = d_list[d_idx]
        creditor = c_list[c_idx]

        transfer_amount = min(debtor["amount"], creditor["amount"])
        if transfer_amount > Decimal("0.00"):
            transfers.append({
                "from_pilot": debtor["name"],
                "to_pilot": creditor["name"],
                "amount": float(transfer_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
                "formatted_amount": f"{transfer_amount:,.2f} ISK",
            })
            debtor["amount"] -= transfer_amount
            creditor["amount"] -= transfer_amount

        if debtor["amount"] <= Decimal("0.01"):
            d_idx += 1
        if creditor["amount"] <= Decimal("0.01"):
            c_idx += 1

    # 8. Generate Discord and In-game Text Summaries
    discord_summary = _generate_discord_summary(
        gross_loot_value=gross_loot_value,
        total_loss_reimbursements=total_loss_reimbursements,
        net_distributable_pot=net_distributable_pot,
        base_share_per_pilot=base_share_per_pilot,
        pilot_settlements=pilot_settlements,
        transfers=transfers,
    )

    ingame_summary = _generate_ingame_summary(
        gross_loot_value=gross_loot_value,
        total_loss_reimbursements=total_loss_reimbursements,
        net_distributable_pot=net_distributable_pot,
        base_share_per_pilot=base_share_per_pilot,
        pilot_settlements=pilot_settlements,
        transfers=transfers,
    )

    return {
        "summary": {
            "gross_loot_value": float(gross_loot_value),
            "total_loss_reimbursements": float(total_loss_reimbursements),
            "total_kept_value": float(total_kept_value),
            "net_distributable_pot": float(net_distributable_pot),
            "total_pilots_count": len(pilots),
            "eligible_pilots_count": eligible_count,
            "base_share_per_pilot": float(base_share_per_pilot),
            "valuation_mode": valuation_mode,
        },
        "pilots": pilot_settlements,
        "transfers": transfers,
        "exports": {
            "discord": discord_summary,
            "ingame": ingame_summary,
        },
    }


def _generate_discord_summary(
    gross_loot_value: Decimal,
    total_loss_reimbursements: Decimal,
    net_distributable_pot: Decimal,
    base_share_per_pilot: Decimal,
    pilot_settlements: list[dict[str, Any]],
    transfers: list[dict[str, Any]],
) -> str:
    """Generate Markdown export formatted for Discord."""
    lines = [
        "**__FLEET LOOT & REIMBURSEMENT SUMMARY__**",
        f"**Gross Loot Value:** `{gross_loot_value:,.2f} ISK`",
        f"**Ship Losses Reimbursed:** `{total_loss_reimbursements:,.2f} ISK`",
        f"**Net Distributable Pot:** `{net_distributable_pot:,.2f} ISK`",
        f"**Base Share Per Pilot:** `{base_share_per_pilot:,.2f} ISK`",
        "",
        "**__PILOT PAYOUTS__**",
    ]

    for p in pilot_settlements:
        name = p["name"]
        alts_str = f" *(Alts: {', '.join(p['alts'])})*" if p.get("alts") else ""
        if p["declined"]:
            lines.append(f"• **{name}**{alts_str}: `Declined Share` (0.00 ISK)")
        elif p["net_balance"] > 0:
            lines.append(f"• **{name}**{alts_str}: **Receives `{p['payout_received']:,.2f} ISK`**")
        elif p["net_balance"] < 0:
            lines.append(f"• **{name}**{alts_str}: **Kept Item(s) — Owes `{p['amount_owed']:,.2f} ISK`**")
        else:
            lines.append(f"• **{name}**{alts_str}: `Even` (0.00 ISK)")

    if transfers:
        lines.append("")
        lines.append("**__SETTLEMENT TRANSFERS (WHO PAYS WHOM)__**")
        for t in transfers:
            lines.append(f"• **{t['from_pilot']}** ➔ Pay `{t['formatted_amount']}` to **{t['to_pilot']}**")

    return "\n".join(lines)


def _generate_ingame_summary(
    gross_loot_value: Decimal,
    total_loss_reimbursements: Decimal,
    net_distributable_pot: Decimal,
    base_share_per_pilot: Decimal,
    pilot_settlements: list[dict[str, Any]],
    transfers: list[dict[str, Any]],
) -> str:
    """Generate clean text summary for EVE In-Game Mail or Fleet Chat."""
    lines = [
        "=== FLEET LOOT & REIMBURSEMENT SPLIT ===",
        f"Gross Loot: {gross_loot_value:,.2f} ISK",
        f"Ship Loss Reimbursements: {total_loss_reimbursements:,.2f} ISK",
        f"Net Pot: {net_distributable_pot:,.2f} ISK",
        f"Base Share: {base_share_per_pilot:,.2f} ISK",
        "",
        "--- PAYOUTS ---",
    ]

    for p in pilot_settlements:
        name = p["name"]
        if p["declined"]:
            lines.append(f"{name}: [Declined Share]")
        elif p["net_balance"] > 0:
            lines.append(f"{name}: Receives {p['payout_received']:,.2f} ISK")
        elif p["net_balance"] < 0:
            lines.append(f"{name}: Kept Items -> Owes {p['amount_owed']:,.2f} ISK")
        else:
            lines.append(f"{name}: Even (0.00 ISK)")

    if transfers:
        lines.append("")
        lines.append("--- DIRECT TRANSFERS ---")
        for t in transfers:
            lines.append(f"Transfer {t['formatted_amount']} from {t['from_pilot']} to {t['to_pilot']}")

    return "\n".join(lines)
