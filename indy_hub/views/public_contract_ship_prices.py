"""Hidden manager-only public contract price report."""

from io import StringIO

from django.core.management import call_command
from django.http import HttpResponse

from indy_hub.decorators import indy_hub_permission_required, token_required


@indy_hub_permission_required("can_manage_material_hub")
@token_required(["esi-universe.read_structures.v1"])
def public_contract_ship_prices(request):
    """Render the report using the requesting manager's AA ESI token."""
    output = StringIO()
    call_command("public_contract_ship_prices", stdout=output, stderr=output)
    return HttpResponse(output.getvalue(), content_type="text/plain; charset=utf-8")
