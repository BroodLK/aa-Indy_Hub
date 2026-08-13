"""Hidden manager-only public contract price report."""

from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.views.decorators.http import require_POST

from indy_hub.decorators import indy_hub_permission_required, token_required
from indy_hub.tasks.public_contracts import run_ship_price_scanner
from celery.result import AsyncResult


@indy_hub_permission_required("can_manage_material_hub")
@token_required(["esi-universe.read_structures.v1"])
def public_contract_ship_prices(request, token):
    """Render the React-based dynamic report page."""
    return render(
        request,
        "indy_hub/material_exchange/public_contract_ship_prices.html",
        {
            "character_id": int(getattr(token, "character_id", 0) or 0),
        }
    )


@indy_hub_permission_required("can_manage_material_hub")
@token_required(["esi-universe.read_structures.v1"])
@require_POST
def public_contract_ship_prices_api_start(request, token):
    """API endpoint to start the scan task."""
    character_id = int(getattr(token, "character_id", 0) or 0)
    max_pages = int(request.POST.get("max_pages", 2000))
    
    task = run_ship_price_scanner.delay(character_id=character_id, max_pages=max_pages)
    return JsonResponse({"task_id": task.id})


@indy_hub_permission_required("can_manage_material_hub")
def public_contract_ship_prices_api_status(request, task_id):
    """API endpoint to poll for scan task status and results."""
    res = AsyncResult(task_id)
    
    response_data = {
        "status": res.status,
        "task_id": task_id,
    }
    
    if res.status == "PROGRESS":
        response_data.update(res.info)
    elif res.successful():
        response_data.update(res.result)
    elif res.failed():
        response_data["error"] = str(res.result)
        
    return JsonResponse(response_data)
