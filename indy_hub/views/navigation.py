from __future__ import annotations

# Django
from django.urls import NoReverseMatch, reverse


def build_nav_context(
    user,
    *,
    active_tab: str | None = None,
    can_manage_corp: bool | None = None,
    can_manage_material_hub: bool | None = None,
    can_access_indy_hub: bool | None = None,
    material_hub_enabled: bool | None = None,
) -> dict[str, str | None]:
    """Return navbar context entries for templates extending the Indy Hub base."""

    if can_manage_corp is None:
        can_manage_corp = user.has_perm("indy_hub.can_manage_corp_bp_requests")

    if can_access_indy_hub is None:
        can_access_indy_hub = user.has_perm("indy_hub.can_access_indy_hub")

    if can_manage_material_hub is None:
        can_manage_material_hub = user.has_perm("indy_hub.can_manage_material_hub")

    if material_hub_enabled is None:
        try:
            from ..models import MaterialExchangeSettings

            material_hub_enabled = MaterialExchangeSettings.get_solo().is_enabled
        except Exception:
            material_hub_enabled = True

    # Primary sections
    overview_url = reverse("indy_hub:index")
    blueprints_url = reverse("indy_hub:all_bp_list")
    blueprint_sharing_url = reverse("indy_hub:bp_copy_request_page")
    reprocessing_url = reverse("indy_hub:reprocessing_browse")
    material_hub_url = reverse("indy_hub:material_exchange_index")
    try:
        capital_orders_url = reverse("indy_hub:capital_ship_orders")
    except NoReverseMatch:
        capital_orders_url = None
    industry_url = reverse("indy_hub:personnal_job_list")
    esi_url = reverse("indy_hub:esi_hub")
    stats_url = reverse("indy_hub:material_exchange_stats_history")
    settings_url = reverse("indy_hub:settings_hub")

    # Legacy dashboard URLs (still used by some templates for "Back" buttons)
    personal_url = reverse("indy_hub:index")

    active_tab = (active_tab or "").strip() or None

    overview_class = ""
    blueprints_class = ""
    blueprint_sharing_class = ""
    reprocessing_class = ""
    material_hub_class = ""
    capital_orders_class = ""
    industry_class = ""
    esi_class = ""
    stats_class = ""
    settings_class = ""

    if active_tab in {
        "overview",
        "blueprints",
        "blueprint_sharing",
        "reprocessing",
        "material_hub",
        "capital_orders",
        "industry",
        "esi",
        "stats",
        "settings",
    }:
        if active_tab == "overview":
            overview_class = "active fw-semibold"
        elif active_tab == "blueprints":
            blueprints_class = "active fw-semibold"
        elif active_tab == "blueprint_sharing":
            blueprint_sharing_class = "active fw-semibold"
        elif active_tab == "reprocessing":
            reprocessing_class = "active fw-semibold"
        elif active_tab == "material_hub":
            material_hub_class = "active fw-semibold"
        elif active_tab == "capital_orders":
            capital_orders_class = "active fw-semibold"
        elif active_tab == "industry":
            industry_class = "active fw-semibold"
        elif active_tab == "esi":
            esi_class = "active fw-semibold"
        elif active_tab == "stats":
            stats_class = "active fw-semibold"
        elif active_tab == "settings":
            settings_class = "active fw-semibold"

    material_hub_nav_url = material_hub_url if material_hub_enabled else None
    stats_nav_url = stats_url if can_manage_material_hub else None
    # Keep Capital Orders visible in the top bar for all Indy Hub users when the
    # route is available; view-level permissions still control actual access.
    capital_orders_nav_url = capital_orders_url

    context: dict[str, str | None] = {
        # New top-level sections
        "overview_nav_url": overview_url,
        "overview_nav_class": overview_class,
        "blueprints_nav_url": blueprints_url,
        "blueprints_nav_class": blueprints_class,
        "blueprint_sharing_nav_url": blueprint_sharing_url,
        "blueprint_sharing_nav_class": blueprint_sharing_class,
        "reprocessing_nav_url": reprocessing_url,
        "reprocessing_nav_class": reprocessing_class,
        "material_hub_nav_url": material_hub_nav_url,
        "material_hub_nav_class": material_hub_class,
        "capital_orders_nav_url": capital_orders_nav_url,
        "capital_orders_nav_class": capital_orders_class,
        "industry_nav_url": industry_url,
        "industry_nav_class": industry_class,
        "esi_nav_url": esi_url,
        "esi_nav_class": esi_class,
        "stats_nav_url": stats_nav_url,
        "stats_nav_class": stats_class,
        "settings_nav_url": settings_url,
        "settings_nav_class": settings_class,
        # Permission flags for dropdowns
        "can_manage_corp_bp_requests": can_manage_corp,
        "can_access_indy_hub": can_access_indy_hub,
        "material_hub_enabled": material_hub_enabled,
        # Legacy keys (kept so we don't break older templates / buttons)
        "personal_nav_url": personal_url,
        "personal_nav_class": "",
    }

    if active_tab:
        context["active_tab"] = active_tab

    return context
