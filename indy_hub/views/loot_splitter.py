"""Fleet Loot Splitter Views and API Endpoints."""

from __future__ import annotations

# Standard Library
import json
from decimal import Decimal

# Django
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

from ..decorators import indy_hub_permission_required
from ..services.loot_splitter import (
    calculate_fleet_loot_split,
    fetch_zkillboard_killmail,
    parse_and_price_loot,
    resolve_fleet_pilots,
)

logger = get_extension_logger(__name__)


@login_required
@indy_hub_permission_required("can_access_indy_hub")
def loot_splitter_view(request):
    """Render the Fleet Loot Splitter & Reimbursement Wizard page."""
    context = {
        "page_title": "Fleet Loot Splitter",
    }
    return render(request, "indy_hub/loot_split/index.html", context)


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_http_methods(["POST"])
def loot_splitter_resolve_pilots_api(request):
    """API endpoint to parse pilot names and resolve alts to mains."""
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        data = request.POST

    raw_text = str(data.get("names_text") or "").strip()
    if not raw_text:
        return JsonResponse({"ok": False, "error": "No pilot names provided."}, status=400)

    try:
        pilots = resolve_fleet_pilots(raw_text)
        return JsonResponse({"ok": True, "pilots": pilots})
    except Exception as exc:
        logger.exception("Error resolving fleet pilots: %s", exc)
        return JsonResponse({"ok": False, "error": str(exc)}, status=500)


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_http_methods(["POST"])
def loot_splitter_parse_loot_api(request):
    """API endpoint to parse loot items and fetch market/buyback valuations."""
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        data = request.POST

    raw_text = str(data.get("loot_text") or "").strip()
    valuation_mode = str(data.get("valuation_mode") or "buyback").strip().lower()
    if valuation_mode not in {"buyback", "market"}:
        valuation_mode = "buyback"

    if not raw_text:
        return JsonResponse({"ok": False, "error": "No loot items provided."}, status=400)

    try:
        items, invalid_lines = parse_and_price_loot(raw_text, valuation_mode=valuation_mode)
        return JsonResponse({
            "ok": True,
            "items": items,
            "invalid_lines": invalid_lines,
            "valuation_mode": valuation_mode,
        })
    except Exception as exc:
        logger.exception("Error parsing loot items: %s", exc)
        return JsonResponse({"ok": False, "error": str(exc)}, status=500)


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_http_methods(["POST"])
def loot_splitter_resolve_killmail_api(request):
    """API endpoint to resolve zKillboard killmail link or ID."""
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        data = request.POST

    kill_url = str(data.get("killmail_url") or "").strip()
    if not kill_url:
        return JsonResponse({"ok": False, "error": "No killmail URL or ID provided."}, status=400)

    try:
        km_data = fetch_zkillboard_killmail(kill_url)
        return JsonResponse({"ok": True, "killmail": km_data})
    except Exception as exc:
        logger.warning("Error fetching killmail %s: %s", kill_url, exc)
        return JsonResponse({"ok": False, "error": str(exc)}, status=400)


@login_required
@indy_hub_permission_required("can_access_indy_hub")
@require_http_methods(["POST"])
def loot_splitter_calculate_api(request):
    """API endpoint to run settlement calculation given current wizard state."""
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception as exc:
        return JsonResponse({"ok": False, "error": f"Invalid JSON payload: {exc}"}, status=400)

    pilots = data.get("pilots") or []
    loot_items = data.get("loot_items") or []
    killmails = data.get("killmails") or []
    valuation_mode = str(data.get("valuation_mode") or "buyback").strip().lower()

    if not pilots:
        return JsonResponse({"ok": False, "error": "At least one pilot is required."}, status=400)

    try:
        result = calculate_fleet_loot_split(
            pilots=pilots,
            loot_items=loot_items,
            killmails=killmails,
            valuation_mode=valuation_mode,
        )
        return JsonResponse({"ok": True, **result})
    except Exception as exc:
        logger.exception("Error calculating fleet loot split: %s", exc)
        return JsonResponse({"ok": False, "error": str(exc)}, status=500)
