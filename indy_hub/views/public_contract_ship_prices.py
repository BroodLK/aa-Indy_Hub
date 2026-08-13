"""Hidden manager-only public contract price report."""

from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.core.cache import cache
from django.views.decorators.http import require_POST

from indy_hub.decorators import indy_hub_permission_required, token_required
from indy_hub.tasks.public_contracts import run_ship_price_scanner


from allianceauth.eveonline.models import EveCharacter

@indy_hub_permission_required("can_manage_material_hub")
@token_required(["esi-universe.read_structures.v1"])
def public_contract_ship_prices(request, token):
    """Render the React-based dynamic report page."""
    character_id = int(getattr(token, "character_id", 0) or 0)
    character_name = "Unknown Character"
    if character_id:
        char = EveCharacter.objects.filter(character_id=character_id).first()
        if char:
            character_name = char.character_name
            
    return render(
        request,
        "indy_hub/material_exchange/public_contract_ship_prices.html",
        {
            "character_id": character_id,
            "character_name": character_name,
        }
    )


@indy_hub_permission_required("can_manage_material_hub")
@require_POST
def public_contract_ship_prices_api_start(request):
    """API endpoint to start the scan task."""
    try:
        character_id = int(request.POST.get("character_id", 0) or 0)
        if character_id and not EveCharacter.objects.filter(
            character_id=character_id,
            character_ownership__user=request.user,
        ).exists():
            return JsonResponse({"error": "Character is not available to this user."}, status=403)
        # Keep interactive scans bounded.  The page defaults to 10, so the
        # API must not silently fall back to a 2,000-page scan when an older
        # client omits this field.
        max_pages = max(1, min(int(request.POST.get("max_pages", 10)), 2000))
        
        task = run_ship_price_scanner.apply_async(
            kwargs={"character_id": character_id, "max_pages": max_pages},
            priority=7,
        )
        return JsonResponse({"task_id": task.id})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@indy_hub_permission_required("can_manage_material_hub")
def public_contract_ship_prices_api_status(request, task_id):
    """API endpoint to poll for scan task status and results."""
    try:
        state = cache.get(f"indy-hub:public-contract-scan:{task_id}")
        if not isinstance(state, dict):
            return JsonResponse({"status": "PENDING", "task_id": task_id})
        return JsonResponse({"task_id": task_id, **state})
    except Exception as exc:
        return JsonResponse(
            {
                "status": "FAILURE",
                "task_id": task_id,
                "error": f"Unable to read task status: {exc}",
            },
            status=200,
        )
