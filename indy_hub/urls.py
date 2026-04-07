# Django
from django.http import HttpResponse
from django.urls import path

from .views.api import (
    craft_build_environment,
    craft_bpc_contracts,
    craft_sync_owned_bpcs,
    craft_industry_fees,
    craft_bp_payload,
    fuzzwork_price,
    load_production_config,
    menu_badge_count,
    save_production_config,
)
from .views.hubs import (
    settings_hub,
    test_darkly_theme,
)
from .views.industry import (
    all_bp_list,
    bp_accept_copy_request,
    bp_buyer_accept_offer,
    bp_cancel_copy_request,
    bp_chat_decide,
    bp_chat_history,
    bp_chat_send,
    bp_cond_copy_request,
    bp_copy_fulfill_requests,
    bp_copy_history,
    bp_copy_my_requests,
    bp_copy_request_create,
    bp_copy_request_page,
    bp_discord_action,
    bp_mark_copy_delivered,
    bp_offer_copy_request,
    bp_reject_copy_request,
    bp_update_copy_request,
    craft_bp,
)
from .views.industry import (
    delete_production_simulation as delete_production_simulation_view,
)
from .views.industry import (
    edit_simulation_name,
    industry_slot_overview,
    personnal_bp_list,
    personnal_job_list,
    production_simulations_list,
)
try:
    from .views.capital_ship_orders import (
        capital_ship_order_cancel,
        capital_ship_order_uncancel,
        capital_ship_order_chat_decide,
        capital_ship_order_chat_history,
        capital_ship_order_chat_send,
        capital_ship_order_refresh_guideline,
        capital_ship_order_reject,
        capital_ship_order_set_definitive_eta,
        capital_ship_order_set_gathering_materials,
        capital_ship_order_set_in_production,
        capital_ship_order_transfer_manager,
        capital_ship_order_update_offer,
        capital_ship_orders_config,
        capital_ship_orders,
        capital_ship_orders_admin,
    )
except ModuleNotFoundError:
    def _capital_orders_module_missing(*_args, **_kwargs):
        return HttpResponse(
            "Capital Orders module is not deployed on this server node.",
            status=503,
            content_type="text/plain",
        )

    capital_ship_orders = _capital_orders_module_missing
    capital_ship_orders_admin = _capital_orders_module_missing
    capital_ship_order_set_in_production = _capital_orders_module_missing
    capital_ship_order_set_gathering_materials = _capital_orders_module_missing
    capital_ship_order_refresh_guideline = _capital_orders_module_missing
    capital_ship_order_update_offer = _capital_orders_module_missing
    capital_ship_order_transfer_manager = _capital_orders_module_missing
    capital_ship_orders_config = _capital_orders_module_missing
    capital_ship_order_set_definitive_eta = _capital_orders_module_missing
    capital_ship_order_reject = _capital_orders_module_missing
    capital_ship_order_cancel = _capital_orders_module_missing
    capital_ship_order_uncancel = _capital_orders_module_missing
    capital_ship_order_chat_history = _capital_orders_module_missing
    capital_ship_order_chat_send = _capital_orders_module_missing
    capital_ship_order_chat_decide = _capital_orders_module_missing
from .views.material_exchange import (
    material_exchange_approve_buy,
    material_exchange_approve_sell,
    material_exchange_assign_contract,
    material_exchange_buy,
    material_exchange_buy_stock_refresh_status,
    material_exchange_complete_buy,
    material_exchange_complete_sell,
    material_exchange_history,
    material_exchange_index,
    material_exchange_mark_delivered_buy,
    material_exchange_reject_buy,
    material_exchange_reject_sell,
    material_exchange_sell,
    material_exchange_sell_estimate,
    material_exchange_sell_assets_refresh_status,
    material_exchange_stats_history,
    material_exchange_sync_prices,
    material_exchange_sync_stock,
    material_exchange_transactions,
    material_exchange_verify_payment_sell,
)
from .views.material_exchange_config import (
    material_exchange_check_refresh_status,
    material_exchange_config,
    material_exchange_debug_tokens,
    material_exchange_get_structures,
    material_exchange_refresh_corp_assets,
    material_exchange_request_all_scopes,
    material_exchange_request_assets_token,
    material_exchange_request_contracts_scope,
    material_exchange_request_divisions_token,
    material_exchange_toggle_active,
)
from .views.material_exchange_orders import (
    buy_order_check_contract,
    buy_order_delete,
    buy_order_detail,
    my_orders,
    sell_order_check_contract,
    sell_order_delete,
    sell_order_detail,
)
from .views.reprocessing_services import (
    reprocessing_admin_applications,
    reprocessing_admin_review,
    reprocessing_authorize_scopes,
    reprocessing_become,
    reprocessing_browse,
    reprocessing_my_requests,
    reprocessing_request_cancel,
    reprocessing_request_check_inbound_contract,
    reprocessing_request_check_return_contract,
    reprocessing_request_create,
    reprocessing_request_detail,
    reprocessing_request_dispute,
    reprocessing_request_mark_awaiting_return,
    reprocessing_request_mark_processing,
    reprocessing_request_submit_inbound,
    reprocessing_request_submit_return,
    reprocessing_request_verify_inbound,
    reprocessing_request_verify_return,
    reprocessing_services_index,
)
from .views.user import (
    authorize_all,
    authorize_assets,
    authorize_blueprints,
    authorize_corp_all,
    authorize_corp_blueprints,
    authorize_corp_jobs,
    authorize_jobs,
    authorize_material_exchange,
    index,
    legacy_token_management_redirect,
    onboarding_set_visibility,
    onboarding_toggle_task,
    production_simulations,
    rename_production_simulation,
    sync_all_tokens,
    sync_blueprints,
    sync_jobs,
    toggle_copy_sharing,
    toggle_corporation_copy_sharing,
    toggle_corporation_job_notifications,
    toggle_job_notifications,
    token_management,
    token_management_live_refresh,
)

app_name = "indy_hub"
urlpatterns = [
    path("", index, name="index"),
    path("test-darkly/", test_darkly_theme, name="test_darkly_theme"),
    path("esi/", token_management, name="esi_hub"),
    path("settings/", settings_hub, name="settings_hub"),
    path("personnal-bp/", personnal_bp_list, name="personnal_bp_list"),
    path(
        "corporation-bp/",
        personnal_bp_list,
        {"scope": "corporation"},
        name="corporation_bp_list",
    ),
    path("all-bp/", all_bp_list, name="all_bp_list"),
    path("personnal-jobs/", personnal_job_list, name="personnal_job_list"),
    path("industry/slots/", industry_slot_overview, name="industry_slot_overview"),
    path(
        "corporation-jobs/",
        personnal_job_list,
        {"scope": "corporation"},
        name="corporation_job_list",
    ),
    path("tokens/", legacy_token_management_redirect, name="token_management"),
    path(
        "tokens/live-refresh/",
        token_management_live_refresh,
        name="token_management_live_refresh",
    ),
    path("tokens/sync-blueprints/", sync_blueprints, name="sync_blueprints"),
    path("tokens/sync-jobs/", sync_jobs, name="sync_jobs"),
    path("tokens/sync-all/", sync_all_tokens, name="sync_all_tokens"),
    path("authorize/blueprints/", authorize_blueprints, name="authorize_blueprints"),
    path("authorize/jobs/", authorize_jobs, name="authorize_jobs"),
    path("authorize/assets/", authorize_assets, name="authorize_assets"),
    path("authorize/all/", authorize_all, name="authorize_all"),
    path(
        "authorize/corporation/blueprints/",
        authorize_corp_blueprints,
        name="authorize_corp_blueprints",
    ),
    path(
        "authorize/corporation/jobs/",
        authorize_corp_jobs,
        name="authorize_corp_jobs",
    ),
    path("authorize/corporation/all/", authorize_corp_all, name="authorize_corp_all"),
    path(
        "authorize/material-exchange/",
        authorize_material_exchange,
        name="authorize_material_exchange",
    ),
    path("craft/<int:type_id>/", craft_bp, name="craft_bp"),
    path("api/fuzzwork-price/", fuzzwork_price, name="fuzzwork_price"),
    path(
        "api/craft-build-environment/",
        craft_build_environment,
        name="craft_build_environment",
    ),
    path(
        "api/craft-bpc-contracts/",
        craft_bpc_contracts,
        name="craft_bpc_contracts",
    ),
    path(
        "api/craft-sync-owned-bpcs/",
        craft_sync_owned_bpcs,
        name="craft_sync_owned_bpcs",
    ),
    path(
        "api/craft-industry-fees/",
        craft_industry_fees,
        name="craft_industry_fees",
    ),
    path(
        "api/craft-bp-payload/<int:type_id>/", craft_bp_payload, name="craft_bp_payload"
    ),
    path(
        "api/production-config/save/",
        save_production_config,
        name="save_production_config",
    ),
    path(
        "api/production-config/load/",
        load_production_config,
        name="load_production_config",
    ),
    path("api/menu-badge-count/", menu_badge_count, name="menu_badge_count"),
    path(
        "simulations/", production_simulations_list, name="production_simulations_list"
    ),
    path(
        "simulations/<int:simulation_id>/delete/",
        delete_production_simulation_view,
        name="delete_production_simulation",
    ),
    path(
        "simulations/<int:simulation_id>/edit-name/",
        edit_simulation_name,
        name="edit_simulation_name",
    ),
    path(
        "simulations/legacy/",
        production_simulations,
        name="production_simulations",
    ),
    path(
        "simulations/<int:simulation_id>/rename/",
        rename_production_simulation,
        name="rename_production_simulation",
    ),
    path("bp-copy/request/", bp_copy_request_page, name="bp_copy_request_page"),
    path(
        "bp-copy/request/create/", bp_copy_request_create, name="bp_copy_request_create"
    ),
    path("bp-copy/fulfill/", bp_copy_fulfill_requests, name="bp_copy_fulfill_requests"),
    path("bp-copy/history/", bp_copy_history, name="bp_copy_history"),
    path(
        "bp-copy/my-requests/", bp_copy_my_requests, name="bp_copy_my_requests"
    ),  # my requests
    path(
        "bp-copy/my-requests/<int:request_id>/update/",
        bp_update_copy_request,
        name="bp_update_copy_request",
    ),
    path(
        "bp-copy/offer/<int:request_id>/",
        bp_offer_copy_request,
        name="bp_offer_copy_request",
    ),
    path("bp-copy/action/", bp_discord_action, name="bp_discord_action"),
    path(
        "bp-copy/accept-offer/<int:offer_id>/",
        bp_buyer_accept_offer,
        name="bp_buyer_accept_offer",
    ),
    path(
        "bp-copy/accept/<int:request_id>/",
        bp_accept_copy_request,
        name="bp_accept_copy_request",
    ),
    path(
        "bp-copy/condition/<int:request_id>/",
        bp_cond_copy_request,
        name="bp_cond_copy_request",
    ),
    path(
        "bp-copy/reject/<int:request_id>/",
        bp_reject_copy_request,
        name="bp_reject_copy_request",
    ),
    path(
        "bp-copy/cancel/<int:request_id>/",
        bp_cancel_copy_request,
        name="bp_cancel_copy_request",
    ),
    path(
        "bp-copy/chat/<int:chat_id>/",
        bp_chat_history,
        name="bp_chat_history",
    ),
    path(
        "bp-copy/chat/<int:chat_id>/send/",
        bp_chat_send,
        name="bp_chat_send",
    ),
    path(
        "bp-copy/chat/<int:chat_id>/decision/",
        bp_chat_decide,
        name="bp_chat_decide",
    ),
    path(
        "bp-copy/delivered/<int:request_id>/",
        bp_mark_copy_delivered,
        name="bp_mark_copy_delivered",
    ),
    path(
        "toggle-job-notifications/",
        toggle_job_notifications,
        name="toggle_job_notifications",
    ),
    path(
        "toggle-corporation-job-notifications/",
        toggle_corporation_job_notifications,
        name="toggle_corporation_job_notifications",
    ),
    path(
        "toggle-corporation-copy-sharing/",
        toggle_corporation_copy_sharing,
        name="toggle_corporation_copy_sharing",
    ),
    path("toggle-copy-sharing/", toggle_copy_sharing, name="toggle_copy_sharing"),
    path(
        "onboarding/toggle-task/",
        onboarding_toggle_task,
        name="onboarding_toggle_task",
    ),
    path(
        "onboarding/visibility/",
        onboarding_set_visibility,
        name="onboarding_set_visibility",
    ),
    # Buyback
    path(
        "material-exchange/",
        material_exchange_index,
        name="material_exchange_index",
    ),
    path(
        "material-exchange/config/",
        material_exchange_config,
        name="material_exchange_config",
    ),
    path(
        "material-exchange/toggle-active/",
        material_exchange_toggle_active,
        name="material_exchange_toggle_active",
    ),
    path(
        "material-exchange/config/request-assets-token/",
        material_exchange_request_assets_token,
        name="material_exchange_request_assets_token",
    ),
    path(
        "material-exchange/config/request-divisions-token/",
        material_exchange_request_divisions_token,
        name="material_exchange_request_divisions_token",
    ),
    path(
        "material-exchange/config/request-contracts-scope/",
        material_exchange_request_contracts_scope,
        name="material_exchange_request_contracts_scope",
    ),
    path(
        "material-exchange/config/request-all-scopes/",
        material_exchange_request_all_scopes,
        name="material_exchange_request_all_scopes",
    ),
    path(
        "material-exchange/api/structures/<int:corp_id>/",
        material_exchange_get_structures,
        name="material_exchange_get_structures",
    ),
    path(
        "material-exchange/api/refresh-assets/",
        material_exchange_refresh_corp_assets,
        name="material_exchange_refresh_corp_assets",
    ),
    path(
        "material-exchange/api/refresh-status/<str:task_id>/",
        material_exchange_check_refresh_status,
        name="material_exchange_check_refresh_status",
    ),
    path(
        "material-exchange/api/debug-tokens/<int:corp_id>/",
        material_exchange_debug_tokens,
        name="material_exchange_debug_tokens",
    ),
    path(
        "material-exchange/sell/",
        material_exchange_sell,
        name="material_exchange_sell",
    ),
    path(
        "material-exchange/api/sell-assets-refresh-status/",
        material_exchange_sell_assets_refresh_status,
        name="material_exchange_sell_assets_refresh_status",
    ),
    path(
        "material-exchange/api/sell-estimate/",
        material_exchange_sell_estimate,
        name="material_exchange_sell_estimate",
    ),
    path(
        "material-exchange/buy/",
        material_exchange_buy,
        name="material_exchange_buy",
    ),
    path(
        "material-exchange/api/buy-stock-refresh-status/",
        material_exchange_buy_stock_refresh_status,
        name="material_exchange_buy_stock_refresh_status",
    ),
    # User Order Management
    path(
        "material-exchange/my-orders/",
        my_orders,
        name="my_orders",
    ),
    path(
        "material-exchange/my-orders/sell/<int:order_id>/",
        sell_order_detail,
        name="sell_order_detail",
    ),
    path(
        "material-exchange/my-orders/sell/<int:order_id>/check-contract/",
        sell_order_check_contract,
        name="sell_order_check_contract",
    ),
    path(
        "material-exchange/my-orders/sell/<int:order_id>/delete/",
        sell_order_delete,
        name="sell_order_delete",
    ),
    path(
        "material-exchange/my-orders/buy/<int:order_id>/",
        buy_order_detail,
        name="buy_order_detail",
    ),
    path(
        "material-exchange/my-orders/buy/<int:order_id>/check-contract/",
        buy_order_check_contract,
        name="buy_order_check_contract",
    ),
    path(
        "material-exchange/my-orders/buy/<int:order_id>/delete/",
        buy_order_delete,
        name="buy_order_delete",
    ),
    # Backward-compatible aliases for legacy links (singular "my-order" and no trailing slash)
    path(
        "material-exchange/my-orders/buy/<int:order_id>/delete",
        buy_order_delete,
        name="buy_order_delete_legacy_no_slash",
    ),
    path(
        "material-exchange/my-order/buy/<int:order_id>/delete/",
        buy_order_delete,
        name="buy_order_delete_legacy_singular",
    ),
    path(
        "material-exchange/my-order/buy/<int:order_id>/delete",
        buy_order_delete,
        name="buy_order_delete_legacy_singular_no_slash",
    ),
    path(
        "material-exchange/capital-orders/",
        capital_ship_orders,
        name="capital_ship_orders",
    ),
    path(
        "material-exchange/capital-orders/admin/",
        capital_ship_orders_admin,
        name="capital_ship_orders_admin",
    ),
    path(
        "material-exchange/capital-orders/config/",
        capital_ship_orders_config,
        name="capital_ship_orders_config",
    ),
    path(
        "material-exchange/capital-orders/<int:order_id>/refresh-guideline/",
        capital_ship_order_refresh_guideline,
        name="capital_ship_order_refresh_guideline",
    ),
    path(
        "material-exchange/capital-orders/<int:order_id>/set-gathering-materials/",
        capital_ship_order_set_gathering_materials,
        name="capital_ship_order_set_gathering_materials",
    ),
    path(
        "material-exchange/capital-orders/<int:order_id>/set-in-production/",
        capital_ship_order_set_in_production,
        name="capital_ship_order_set_in_production",
    ),
    path(
        "material-exchange/capital-orders/<int:order_id>/update-offer/",
        capital_ship_order_update_offer,
        name="capital_ship_order_update_offer",
    ),
    path(
        "material-exchange/capital-orders/<int:order_id>/transfer-manager/",
        capital_ship_order_transfer_manager,
        name="capital_ship_order_transfer_manager",
    ),
    path(
        "material-exchange/capital-orders/<int:order_id>/set-definitive-eta/",
        capital_ship_order_set_definitive_eta,
        name="capital_ship_order_set_definitive_eta",
    ),
    path(
        "material-exchange/capital-orders/<int:order_id>/reject/",
        capital_ship_order_reject,
        name="capital_ship_order_reject",
    ),
    path(
        "material-exchange/capital-orders/<int:order_id>/cancel/",
        capital_ship_order_cancel,
        name="capital_ship_order_cancel",
    ),
    path(
        "material-exchange/capital-orders/<int:order_id>/uncancel/",
        capital_ship_order_uncancel,
        name="capital_ship_order_uncancel",
    ),
    path(
        "material-exchange/capital-orders/<int:order_id>/chat/",
        capital_ship_order_chat_history,
        name="capital_ship_order_chat_history",
    ),
    path(
        "material-exchange/capital-orders/<int:order_id>/chat/send/",
        capital_ship_order_chat_send,
        name="capital_ship_order_chat_send",
    ),
    path(
        "material-exchange/capital-orders/<int:order_id>/chat/decision/",
        capital_ship_order_chat_decide,
        name="capital_ship_order_chat_decide",
    ),
    # Stock & Prices
    path(
        "material-exchange/sync-stock/",
        material_exchange_sync_stock,
        name="material_exchange_sync_stock",
    ),
    path(
        "material-exchange/sync-prices/",
        material_exchange_sync_prices,
        name="material_exchange_sync_prices",
    ),
    path(
        "material-exchange/transactions/",
        material_exchange_transactions,
        name="material_exchange_transactions",
    ),
    path(
        "material-exchange/transactions/stats-history/",
        material_exchange_stats_history,
        name="material_exchange_stats_history",
    ),
    path(
        "material-exchange/history/",
        material_exchange_history,
        name="material_exchange_history",
    ),
    path(
        "material-exchange/sell/<int:order_id>/approve/",
        material_exchange_approve_sell,
        name="material_exchange_approve_sell",
    ),
    path(
        "material-exchange/sell/<int:order_id>/reject/",
        material_exchange_reject_sell,
        name="material_exchange_reject_sell",
    ),
    path(
        "material-exchange/sell/<int:order_id>/verify-payment/",
        material_exchange_verify_payment_sell,
        name="material_exchange_verify_payment_sell",
    ),
    path(
        "material-exchange/sell/<int:order_id>/complete/",
        material_exchange_complete_sell,
        name="material_exchange_complete_sell",
    ),
    path(
        "material-exchange/buy/<int:order_id>/approve/",
        material_exchange_approve_buy,
        name="material_exchange_approve_buy",
    ),
    path(
        "material-exchange/buy/<int:order_id>/reject/",
        material_exchange_reject_buy,
        name="material_exchange_reject_buy",
    ),
    path(
        "material-exchange/buy/<int:order_id>/delivered/",
        material_exchange_mark_delivered_buy,
        name="material_exchange_mark_delivered_buy",
    ),
    path(
        "material-exchange/buy/<int:order_id>/complete/",
        material_exchange_complete_buy,
        name="material_exchange_complete_buy",
    ),
    path(
        "material-exchange/order/<int:order_id>/assign-contract/",
        material_exchange_assign_contract,
        name="material_exchange_assign_contract",
    ),
    # Reprocessing Services
    path(
        "reprocessing-services/",
        reprocessing_services_index,
        name="reprocessing_services_index",
    ),
    path(
        "reprocessing-services/authorize/",
        reprocessing_authorize_scopes,
        name="reprocessing_authorize_scopes",
    ),
    path(
        "reprocessing-services/become/",
        reprocessing_become,
        name="reprocessing_become",
    ),
    path(
        "reprocessing-services/browse/",
        reprocessing_browse,
        name="reprocessing_browse",
    ),
    path(
        "reprocessing-services/my-requests/",
        reprocessing_my_requests,
        name="reprocessing_my_requests",
    ),
    path(
        "reprocessing-services/admin/applications/",
        reprocessing_admin_applications,
        name="reprocessing_admin_applications",
    ),
    path(
        "reprocessing-services/admin/applications/<int:profile_id>/review/",
        reprocessing_admin_review,
        name="reprocessing_admin_review",
    ),
    path(
        "reprocessing-services/request/<int:profile_id>/",
        reprocessing_request_create,
        name="reprocessing_request_create",
    ),
    path(
        "reprocessing-services/requests/<int:request_id>/",
        reprocessing_request_detail,
        name="reprocessing_request_detail",
    ),
    path(
        "reprocessing-services/requests/<int:request_id>/submit-inbound/",
        reprocessing_request_submit_inbound,
        name="reprocessing_request_submit_inbound",
    ),
    path(
        "reprocessing-services/requests/<int:request_id>/verify-inbound/",
        reprocessing_request_verify_inbound,
        name="reprocessing_request_verify_inbound",
    ),
    path(
        "reprocessing-services/requests/<int:request_id>/check-inbound-contract/",
        reprocessing_request_check_inbound_contract,
        name="reprocessing_request_check_inbound_contract",
    ),
    path(
        "reprocessing-services/requests/<int:request_id>/mark-processing/",
        reprocessing_request_mark_processing,
        name="reprocessing_request_mark_processing",
    ),
    path(
        "reprocessing-services/requests/<int:request_id>/awaiting-return/",
        reprocessing_request_mark_awaiting_return,
        name="reprocessing_request_mark_awaiting_return",
    ),
    path(
        "reprocessing-services/requests/<int:request_id>/submit-return/",
        reprocessing_request_submit_return,
        name="reprocessing_request_submit_return",
    ),
    path(
        "reprocessing-services/requests/<int:request_id>/verify-return/",
        reprocessing_request_verify_return,
        name="reprocessing_request_verify_return",
    ),
    path(
        "reprocessing-services/requests/<int:request_id>/check-return-contract/",
        reprocessing_request_check_return_contract,
        name="reprocessing_request_check_return_contract",
    ),
    path(
        "reprocessing-services/requests/<int:request_id>/cancel/",
        reprocessing_request_cancel,
        name="reprocessing_request_cancel",
    ),
    path(
        "reprocessing-services/requests/<int:request_id>/dispute/",
        reprocessing_request_dispute,
        name="reprocessing_request_dispute",
    ),
]

