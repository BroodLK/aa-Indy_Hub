# API views and external services
"""
API views and external service integrations for the Indy Hub module.
These views handle API calls, external data fetching, and service integrations.
"""

# Standard Library
import json
from decimal import Decimal
from math import ceil
from urllib.parse import parse_qsl

# Django
from django.contrib.auth.decorators import login_required
from django.core.cache import cache
from django.db import connection
from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.http import require_http_methods

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger
from esi.models import Token

from ..decorators import indy_hub_access_required, indy_hub_permission_required

# Local
from ..models import (
    BlueprintEfficiency,
    CustomPrice,
    ProductionConfig,
    ProductionSimulation,
)
from ..utils.analytics import emit_view_analytics_event
from ..utils.eve import get_type_name
from ..utils.menu_badge import compute_menu_badge_count
from ..services.public_contracts_store import (
    get_public_jita_bpc_offers,
    get_public_jita_contract_cache_meta,
)
from ..services.everef import (
    EVERefError,
    fetch_industry_cost,
    summarize_job_fees,
)
from ..services.industry_environment import resolve_craft_system_context
from ..tasks.industry import MANUAL_REFRESH_KIND_BLUEPRINTS, request_manual_refresh

logger = get_extension_logger(__name__)

MENU_BADGE_CACHE_TTL_SECONDS = 45
BLUEPRINT_SCOPE_SET = [
    "esi-characters.read_blueprints.v1",
    "esi-universe.read_structures.v1",
]


def _to_serializable(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _to_serializable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_serializable(item) for item in value]
    return value


def _parse_optional_int(raw_value) -> int | None:
    try:
        parsed = int(str(raw_value or "").strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _parse_optional_float(raw_value) -> float | None:
    try:
        parsed = float(str(raw_value or "").strip())
    except (TypeError, ValueError):
        return None
    return parsed


def _normalize_everef_fee_query_params(
    raw_query: dict[str, object],
) -> dict[str, object]:
    """Normalize/guard optional query params before calling EVERef."""
    normalized = dict(raw_query or {})

    # Guard security to known enum values.
    security = str(normalized.get("security", "") or "").strip().upper()
    valid_security = {"HIGH_SEC", "LOW_SEC", "NULL_SEC"}
    if security in valid_security:
        normalized["security"] = security
    else:
        normalized.pop("security", None)

    # If system_id is present, let EVERef derive security/cost indices from that
    # system to avoid conflicting parameter combinations.
    system_id = _parse_optional_int(normalized.get("system_id"))
    if system_id:
        normalized["system_id"] = system_id
        normalized.pop("security", None)
        for key in (
            "manufacturing_cost",
            "invention_cost",
            "copying_cost",
            "reaction_cost",
            "researching_me_cost",
            "researching_te_cost",
        ):
            normalized.pop(key, None)
    else:
        normalized.pop("system_id", None)

    # Keep only positive rig IDs and remove duplicates while preserving order.
    rig_ids = normalized.get("rig_id")
    if isinstance(rig_ids, list):
        seen: set[int] = set()
        clean_rigs: list[int] = []
        for value in rig_ids:
            parsed = _parse_optional_int(value)
            if not parsed or parsed in seen:
                continue
            seen.add(parsed)
            clean_rigs.append(parsed)
        if clean_rigs:
            normalized["rig_id"] = clean_rigs
        else:
            normalized.pop("rig_id", None)

    # Normalize alpha to literal strings expected by query passthrough.
    alpha_raw = str(normalized.get("alpha", "") or "").strip().lower()
    if alpha_raw in {"1", "true", "yes", "on"}:
        normalized["alpha"] = "true"
    elif alpha_raw in {"0", "false", "no", "off"}:
        normalized["alpha"] = "false"
    else:
        normalized.pop("alpha", None)

    return normalized


@login_required
@require_http_methods(["GET"])
def menu_badge_count(request):
    """Return current Indy Hub menu badge count for live menu update."""
    if not request.user.has_perm("indy_hub.can_access_indy_hub"):
        return JsonResponse({"count": 0}, status=403)

    cache_key = f"indy_hub:menu_badge_count:{request.user.id}"
    refresh_lock_key = f"indy_hub:menu_badge_count_refreshing:{request.user.id}"
    count = cache.get(cache_key)
    if count is None:
        try:
            if cache.add(refresh_lock_key, 1, 30):
                count = compute_menu_badge_count(int(request.user.id))
                cache.set(cache_key, count, MENU_BADGE_CACHE_TTL_SECONDS)
                cache.delete(refresh_lock_key)
            else:
                count = 0
        except Exception:
            count = 0
    return JsonResponse({"count": int(count or 0)})


@indy_hub_access_required
@indy_hub_permission_required("can_access_indy_hub")
@login_required
@require_http_methods(["GET"])
def craft_bp_payload(request, type_id: int):
    """Return the craft blueprint payload as JSON for a given number of runs.

    This is used by the V2 UI to simulate profitability across multiple run counts
    while allowing buy/prod decisions to change with cycle rounding effects.
    """
    emit_view_analytics_event(view_name="api.craft_bp_payload", request=request)

    debug_enabled = str(request.GET.get("indy_debug", "")).strip() in {
        "1",
        "true",
        "yes",
    } or str(request.GET.get("debug", "")).strip() in {"1", "true", "yes"}

    try:
        num_runs = max(1, int(request.GET.get("runs", 1)))
    except (TypeError, ValueError):
        num_runs = 1

    try:
        me = int(request.GET.get("me", 0) or 0)
    except (TypeError, ValueError):
        me = 0
    try:
        te = int(request.GET.get("te", 0) or 0)
    except (TypeError, ValueError):
        te = 0

    structure_bonus_raw = _parse_optional_float(
        request.GET.get("build_structure_material_bonus")
    )
    rig_bonus_raw = _parse_optional_float(request.GET.get("build_rig_material_bonus"))
    effective_bonus_raw = _parse_optional_float(
        request.GET.get("build_effective_material_bonus")
    )
    structure_bonus = max(0.0, min(1.0, float(structure_bonus_raw or 0.0)))
    rig_bonus = max(0.0, min(1.0, float(rig_bonus_raw or 0.0)))
    if effective_bonus_raw is None:
        effective_material_bonus = 1.0 - ((1.0 - structure_bonus) * (1.0 - rig_bonus))
    else:
        effective_material_bonus = max(0.0, min(1.0, float(effective_bonus_raw)))
    environment_material_multiplier = max(0.0, 1.0 - effective_material_bonus)

    # Parse per-blueprint ME/TE overrides: me_<bpTypeId>, te_<bpTypeId>
    me_te_configs: dict[int, dict[str, int]] = {}
    for key, value in request.GET.items():
        if not value:
            continue
        if key.startswith("me_"):
            try:
                bp_type_id = int(key.replace("me_", ""))
                me_value = int(value)
                me_te_configs.setdefault(bp_type_id, {})["me"] = me_value
            except (ValueError, TypeError):
                continue
        elif key.startswith("te_"):
            try:
                bp_type_id = int(key.replace("te_", ""))
                te_value = int(value)
                me_te_configs.setdefault(bp_type_id, {})["te"] = te_value
            except (ValueError, TypeError):
                continue

    # Final product and output qty per run.
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT product_eve_type_id, quantity
            FROM indy_hub_sdeindustryactivityproduct
            WHERE eve_type_id = %s AND activity_id IN (1, 11)
            LIMIT 1
            """,
            [type_id],
        )
        product_row = cursor.fetchone()

    product_type_id = product_row[0] if product_row else None
    output_qty_per_run = product_row[1] if product_row and len(product_row) > 1 else 1
    final_product_qty = (output_qty_per_run or 1) * num_runs

    debug_info: dict[str, object] = {}
    if debug_enabled:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM indy_hub_sdeindustryactivitymaterial
                    WHERE eve_type_id = %s AND activity_id IN (1, 11)
                    """,
                    [type_id],
                )
                mats_count = int(cursor.fetchone()[0])
            debug_info = {
                "db_vendor": connection.vendor,
                "requested_type_id": int(type_id),
                "num_runs": int(num_runs),
                "me": int(me),
                "te": int(te),
                "me_te_configs_count": int(len(me_te_configs)),
                "product_row_found": bool(product_row),
                "product_type_id": int(product_type_id) if product_type_id else None,
                "output_qty_per_run": int(output_qty_per_run or 1),
                "top_level_material_rows": mats_count,
                "build_structure_material_bonus": float(structure_bonus),
                "build_rig_material_bonus": float(rig_bonus),
                "build_effective_material_bonus": float(effective_material_bonus),
            }
        except Exception as e:
            debug_info = {
                "debug_error": f"{type(e).__name__}: {str(e)}",
            }

    # Exact per-cycle recipes for craftable items (keyed by product type_id).
    # This avoids approximating recipes from tree occurrences in the frontend.
    recipe_map: dict[int, dict[str, object]] = {}
    recipe_cache: dict[tuple[int, int], dict[str, object]] = {}

    def get_materials_tree(
        bp_id,
        runs,
        blueprint_me=0,
        depth=0,
        max_depth=10,
        seen=None,
        me_te_map=None,
    ):
        if seen is None:
            seen = set()
        if me_te_map is None:
            me_te_map = {}
        if depth > max_depth or bp_id in seen:
            return []
        seen.add(bp_id)

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT m.material_eve_type_id, t.name, m.quantity
                FROM indy_hub_sdeindustryactivitymaterial m
                JOIN eve_sde_itemtype t ON m.material_eve_type_id = t.id
                WHERE m.eve_type_id = %s AND m.activity_id IN (1, 11)
                """,
                [bp_id],
            )

            mats = []
            for row in cursor.fetchall():
                base_per_run_qty = int(row[2] or 0)
                base_total_qty = int(base_per_run_qty) * int(runs)
                me_multiplier = max(0.0, (100 - blueprint_me) / 100.0)
                total_multiplier = me_multiplier * environment_material_multiplier
                qty = ceil(base_total_qty * total_multiplier)
                mat = {
                    "type_id": row[0],
                    "type_name": get_type_name(row[0]),
                    "quantity": qty,
                    "quantity_default": base_total_qty,
                    "cycles": None,
                    "produced_per_cycle": None,
                    "total_produced": None,
                    "surplus": None,
                }

                # If craftable, compute cycles + recurse.
                with connection.cursor() as sub_cursor:
                    sub_cursor.execute(
                        """
                        SELECT eve_type_id
                        FROM indy_hub_sdeindustryactivityproduct
                        WHERE product_eve_type_id = %s AND activity_id IN (1, 11)
                        LIMIT 1
                        """,
                        [mat["type_id"]],
                    )
                    sub_bp_row = sub_cursor.fetchone()

                    if sub_bp_row:
                        sub_bp_id = sub_bp_row[0]
                        sub_cursor.execute(
                            """
                            SELECT quantity
                            FROM indy_hub_sdeindustryactivityproduct
                            WHERE eve_type_id = %s AND activity_id IN (1, 11)
                            LIMIT 1
                            """,
                            [sub_bp_id],
                        )
                        prod_qty_row = sub_cursor.fetchone()
                        output_qty = prod_qty_row[0] if prod_qty_row else 1
                        cycles = ceil(mat["quantity"] / output_qty)
                        total_produced = cycles * output_qty
                        surplus = total_produced - mat["quantity"]
                        mat["cycles"] = cycles
                        mat["produced_per_cycle"] = output_qty
                        mat["total_produced"] = total_produced
                        mat["surplus"] = surplus

                        sub_bp_config = (me_te_map or {}).get(sub_bp_id, {})
                        sub_bp_me = sub_bp_config.get("me", 0)

                        # Build exact per-cycle recipe for this craftable output (mat["type_id"]).
                        # Cache by (blueprint_id, blueprint_me) because ME changes the rounded per-cycle quantities.
                        cache_key = (int(sub_bp_id), int(sub_bp_me))
                        if cache_key not in recipe_cache:
                            with connection.cursor() as recipe_cursor:
                                recipe_cursor.execute(
                                    """
                                    SELECT material_eve_type_id, quantity
                                    FROM indy_hub_sdeindustryactivitymaterial
                                    WHERE eve_type_id = %s AND activity_id IN (1, 11)
                                    """,
                                    [sub_bp_id],
                                )
                                inputs = []
                                for (
                                    mat_type_id,
                                    base_qty_per_cycle,
                                ) in recipe_cursor.fetchall():
                                    qty_per_cycle = ceil(
                                        (base_qty_per_cycle or 0)
                                        * (100 - sub_bp_me)
                                        / 100
                                        * environment_material_multiplier
                                    )
                                    if qty_per_cycle <= 0:
                                        continue
                                    inputs.append(
                                        {
                                            "type_id": int(mat_type_id),
                                            "quantity": int(qty_per_cycle),
                                        }
                                    )
                            recipe_cache[cache_key] = {
                                "produced_per_cycle": int(output_qty or 1),
                                "inputs_per_cycle": inputs,
                            }

                        # Key recipe map by produced item type_id (not blueprint id)
                        produced_type_id = int(mat["type_id"])
                        if produced_type_id not in recipe_map:
                            recipe_map[produced_type_id] = recipe_cache[cache_key]

                        mat["sub_materials"] = get_materials_tree(
                            sub_bp_id,
                            cycles,
                            sub_bp_me,
                            depth + 1,
                            max_depth,
                            seen.copy(),
                            me_te_map,
                        )
                    else:
                        mat["sub_materials"] = []

                mats.append(mat)
            return mats

    materials_tree = get_materials_tree(type_id, num_runs, me, me_te_map=me_te_configs)

    payload = {
        "type_id": type_id,
        "bp_type_id": type_id,
        "num_runs": num_runs,
        "me": me,
        "te": te,
        "product_type_id": product_type_id,
        "output_qty_per_run": output_qty_per_run,
        "final_product_qty": final_product_qty,
        "materials_tree": _to_serializable(materials_tree),
        "recipe_map": _to_serializable(recipe_map),
    }

    if debug_enabled:
        payload["_debug"] = _to_serializable(debug_info)

    return JsonResponse(payload)


@indy_hub_access_required
@indy_hub_permission_required("can_access_indy_hub")
@login_required
def fuzzwork_price(request):
    emit_view_analytics_event(view_name="api.fuzzwork_price", request=request)
    """
    Get item prices from Fuzzwork API.

    This view fetches current market prices for EVE Online items
    from the Fuzzwork Market API service.
    Supports both single type_id and comma-separated multiple type_ids.
    """
    type_id = request.GET.get("type_id")
    full = str(request.GET.get("full", "")).strip().lower() in {"1", "true", "yes"}
    if not type_id:
        return JsonResponse({"error": "type_id parameter required"}, status=400)

    try:
        # Support multiple type IDs separated by commas
        type_ids = [t.strip() for t in type_id.split(",") if t.strip()]
        if not type_ids:
            return JsonResponse({"error": "Invalid type_id parameter"}, status=400)

        # Remove duplicates and join back
        unique_type_ids = list(set(type_ids))
        # Local
        from ..services.fuzzwork import FuzzworkError, fetch_fuzzwork_aggregates

        # Fetch price data from Fuzzwork API
        data = fetch_fuzzwork_aggregates(unique_type_ids, timeout=10)

        # Optional: return the full Fuzzwork payload for each requested typeId.
        # This is used by the "Calcul" tab for deep inspection.
        if full:
            result = {}
            for tid in unique_type_ids:
                # Fuzzwork keys are strings in the aggregates response.
                result[tid] = data.get(tid, {})
            return JsonResponse(result)

        # Return simplified price data (use sell.min for material costs, sell.min for products)
        result = {}
        for tid in unique_type_ids:
            if tid in data:
                item_data = data[tid]
                # Use sell.min as the default price (what you'd pay to buy)
                sell_min = float(item_data.get("sell", {}).get("min", 0))
                result[tid] = sell_min
            else:
                result[tid] = 0

        return JsonResponse(result)

    except FuzzworkError as e:
        logger.error(f"Error fetching price data from Fuzzwork: {e}")
        return JsonResponse({"error": "Unable to fetch price data"}, status=503)
    except (ValueError, KeyError) as e:
        logger.error(f"Error parsing price data: {e}")
        return JsonResponse({"error": "Invalid data received"}, status=500)


@indy_hub_access_required
@indy_hub_permission_required("can_access_indy_hub")
@login_required
@require_http_methods(["GET"])
def craft_build_environment(request):
    """Resolve craft build environment from system name/id and return structure options."""
    emit_view_analytics_event(view_name="api.craft_build_environment", request=request)

    system_text = str(request.GET.get("system", "") or "").strip()
    explicit_system_id = _parse_optional_int(request.GET.get("system_id"))
    selected_structure_id = _parse_optional_int(request.GET.get("structure_id"))

    if not system_text and explicit_system_id is None:
        return JsonResponse(
            {
                "resolved": False,
                "system": None,
                "structures": [],
                "selected_structure_id": selected_structure_id,
                "error": "system parameter required",
            },
            status=200,
        )

    context = resolve_craft_system_context(
        user=request.user,
        system_text=system_text,
        system_id=explicit_system_id,
        include_structures=True,
    )
    system_payload = context.get("system")
    structures = context.get("structures") or []
    if not isinstance(structures, list):
        structures = []

    for row in structures:
        if not isinstance(row, dict):
            continue
        row.setdefault("structure_type_id", None)
        row.setdefault("structure_type_key", "")
        row.setdefault("structure_type_name", "")
        row.setdefault("material_bonus", 0.0)
        row.setdefault("rig_keys", [])
        row.setdefault("rig_type_ids", [])
        row.setdefault("facility_tax", None)

    resolved = bool(system_payload)
    if not resolved:
        return JsonResponse(
            {
                "resolved": False,
                "system": None,
                "structures": [],
                "selected_structure_id": selected_structure_id,
                "error": "system_not_found",
            },
            status=200,
        )

    if selected_structure_id is not None:
        matching = [
            row
            for row in structures
            if int(row.get("structure_id") or 0) == int(selected_structure_id)
        ]
        if not matching:
            selected_structure_id = None

    return JsonResponse(
        {
            "resolved": True,
            "system": system_payload,
            "structures": structures,
            "selected_structure_id": selected_structure_id,
        },
        status=200,
    )


@indy_hub_access_required
@indy_hub_permission_required("can_access_indy_hub")
@login_required
@require_http_methods(["GET"])
def craft_bpc_contracts(request):
    """Return public Jita BPC contracts for one or more blueprint type IDs."""
    emit_view_analytics_event(view_name="api.craft_bpc_contracts", request=request)

    raw_ids = str(request.GET.get("blueprint_type_ids", "")).strip()
    if not raw_ids:
        return JsonResponse({"contracts_by_blueprint": {}}, status=200)

    parsed_ids: list[int] = []
    for raw in raw_ids.split(","):
        try:
            type_id = int(str(raw).strip())
        except (TypeError, ValueError):
            continue
        if type_id > 0 and type_id not in parsed_ids:
            parsed_ids.append(type_id)

    # Keep the endpoint bounded since every type can trigger multiple ESI requests.
    parsed_ids = parsed_ids[:20]
    if not parsed_ids:
        return JsonResponse({"contracts_by_blueprint": {}}, status=200)
    force_refresh = str(request.GET.get("force", "")).strip().lower() in {
        "1",
        "true",
        "yes",
    }

    cache_meta = get_public_jita_contract_cache_meta()

    contracts_by_blueprint: dict[str, list[dict]] = {}
    failures: dict[str, str] = {}

    for blueprint_type_id in parsed_ids:
        try:
            offers = get_public_jita_bpc_offers(
                blueprint_type_id=blueprint_type_id,
                max_offers=500,
            )
        except Exception as exc:
            logger.warning(
                "Unable to fetch DB-cached public contracts for blueprint %s: %s",
                blueprint_type_id,
                exc,
            )
            offers = []
            failures[str(blueprint_type_id)] = str(exc)[:220]
        contracts_by_blueprint[str(blueprint_type_id)] = offers

    logger.info(
        "craft_bpc_contracts user=%s requested=%s force=%s failures=%s",
        request.user.id,
        parsed_ids,
        force_refresh,
        failures,
    )

    return JsonResponse(
        {
            "contracts_by_blueprint": contracts_by_blueprint,
            "errors": failures,
            "cache_ttl_seconds": int(cache_meta.get("cache_ttl_seconds") or 3600),
            "cached_at": str(cache_meta.get("cached_at") or ""),
            "expires_at": str(cache_meta.get("expires_at") or ""),
            "fetched_at": timezone.now().isoformat(),
            "is_cached": bool(cache_meta.get("is_cached")),
        },
        status=200,
    )


@indy_hub_access_required
@indy_hub_permission_required("can_access_indy_hub")
@login_required
@require_http_methods(["POST"])
def craft_sync_owned_bpcs(request):
    """Queue a refresh of the user's owned blueprints/BPCs from ESI."""
    emit_view_analytics_event(view_name="api.craft_sync_owned_bpcs", request=request)

    try:
        blueprint_tokens = (
            Token.objects.filter(user=request.user)
            .require_scopes(BLUEPRINT_SCOPE_SET)
            .require_valid()
        )
    except Exception:
        blueprint_tokens = Token.objects.none()

    if not blueprint_tokens.exists():
        return JsonResponse(
            {
                "scheduled": False,
                "error": "missing_blueprint_tokens",
                "message": "No valid blueprint token found. Re-authorize blueprint scopes first.",
            },
            status=400,
        )

    try:
        scheduled, remaining = request_manual_refresh(
            MANUAL_REFRESH_KIND_BLUEPRINTS,
            request.user.id,
            priority=5,
        )
    except Exception as exc:
        logger.exception(
            "Failed to queue owned BPC refresh for user %s: %s", request.user.id, exc
        )
        return JsonResponse(
            {
                "scheduled": False,
                "error": "queue_failed",
                "message": "Failed to queue blueprint refresh.",
            },
            status=500,
        )

    if scheduled:
        return JsonResponse(
            {
                "scheduled": True,
                "message": "Owned BPC refresh queued. Reload in about a minute.",
            },
            status=200,
        )

    if remaining is None:
        return JsonResponse(
            {
                "scheduled": False,
                "error": "inactive_or_missing_scope",
                "message": "Refresh skipped because the character is offline or missing required scope.",
            },
            status=200,
        )

    retry_seconds = max(1, int(ceil(remaining.total_seconds())))
    retry_minutes = max(1, int(ceil(retry_seconds / 60)))
    return JsonResponse(
        {
            "scheduled": False,
            "error": "cooldown",
            "retry_seconds": retry_seconds,
            "retry_minutes": retry_minutes,
            "message": f"Blueprint refresh is on cooldown. Retry in about {retry_minutes} minute(s).",
        },
        status=200,
    )


@indy_hub_access_required
@indy_hub_permission_required("can_access_indy_hub")
@login_required
@require_http_methods(["GET"])
def craft_industry_fees(request):
    """Return aggregated EVERef job fees for the provided production jobs."""
    emit_view_analytics_event(view_name="api.craft_industry_fees", request=request)

    raw_jobs = str(request.GET.get("jobs", "")).strip()
    if not raw_jobs:
        return JsonResponse(
            {"jobs": [], "total_job_cost": 0, "total_api_cost": 0, "errors": []}, status=200
        )

    jobs: list[tuple[int, int]] = []
    for raw in raw_jobs.split(","):
        token = str(raw or "").strip()
        if ":" not in token:
            continue
        left, right = token.split(":", 1)
        try:
            product_id = int(left.strip())
            runs = max(1, int(right.strip()))
        except (TypeError, ValueError):
            continue
        if product_id <= 0:
            continue
        jobs.append((product_id, runs))

    if not jobs:
        return JsonResponse(
            {"jobs": [], "total_job_cost": 0, "total_api_cost": 0, "errors": []}, status=200
        )

    # Keep this bounded to avoid bursty fan-out.
    jobs = jobs[:40]

    optional_query: dict[str, object] = {}
    string_fields = [
        "security",
        "material_prices",
    ]
    for field_name in string_fields:
        value = str(request.GET.get(field_name, "")).strip()
        if value:
            optional_query[field_name] = value

    int_fields = [
        "structure_type_id",
        "system_id",
        "decryptor_id",
    ]
    for field_name in int_fields:
        raw_value = str(request.GET.get(field_name, "")).strip()
        if not raw_value:
            continue
        try:
            parsed = int(raw_value)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            optional_query[field_name] = parsed

    float_fields = [
        "manufacturing_cost",
        "invention_cost",
        "copying_cost",
        "reaction_cost",
        "researching_me_cost",
        "researching_te_cost",
        "facility_tax",
        "system_cost_bonus",
    ]
    for field_name in float_fields:
        raw_value = str(request.GET.get(field_name, "")).strip()
        if not raw_value:
            continue
        try:
            parsed = float(raw_value)
        except (TypeError, ValueError):
            continue
        optional_query[field_name] = parsed

    alpha_raw = str(request.GET.get("alpha", "")).strip().lower()
    if alpha_raw in {"1", "true", "yes", "on"}:
        optional_query["alpha"] = "true"
    elif alpha_raw in {"0", "false", "no", "off"}:
        optional_query["alpha"] = "false"

    rig_ids = []
    for raw_rig in request.GET.getlist("rig_id"):
        try:
            rig_id = int(str(raw_rig).strip())
        except (TypeError, ValueError):
            continue
        if rig_id > 0:
            rig_ids.append(rig_id)
    if rig_ids:
        optional_query["rig_id"] = rig_ids

    # Optional passthrough for advanced skill/index params:
    # extra_params=invention_cost=0.02&advanced_industry=5
    extra_params_raw = str(request.GET.get("extra_params", "")).strip()
    if extra_params_raw:
        for key, value in parse_qsl(extra_params_raw, keep_blank_values=False):
            safe_key = str(key or "").strip().lower()
            safe_value = str(value or "").strip()
            if not safe_key or not safe_value:
                continue
            if not safe_key.replace("_", "").isalnum():
                continue
            if safe_key in optional_query:
                continue
            optional_query[safe_key] = safe_value

    optional_query = _normalize_everef_fee_query_params(optional_query)

    logger.warning(
        "craft_industry_fees hit user=%s jobs=%s options=%s",
        request.user.id,
        len(jobs),
        sorted(optional_query.keys()),
    )

    response_jobs = []
    errors = []
    total_job_cost = Decimal("0")
    total_api_cost = Decimal("0")

    for product_id, runs in jobs:
        cache_key = (
            "indy_hub:craft_industry_fee:v1:"
            f"{product_id}:{runs}:"
            f"{json.dumps(optional_query, sort_keys=True, default=str)}"
        )
        cached = cache.get(cache_key)
        if cached is not None:
            response_jobs.append(cached)
            total_job_cost += Decimal(str(cached.get("total_job_cost") or 0))
            total_api_cost += Decimal(str(cached.get("total_api_cost") or 0))
            continue

        try:
            payload = fetch_industry_cost(
                product_id=product_id,
                runs=runs,
                query_params=optional_query,
            )
            summary = summarize_job_fees(payload)
            row = {
                "product_id": product_id,
                "runs": runs,
                "total_job_cost": float(summary["total_job_cost"]),
                "total_api_cost": float(summary["total_api_cost"]),
                "section_job_costs": summary["section_job_costs"],
            }
            cache.set(cache_key, row, 300)
            response_jobs.append(row)
            total_job_cost += Decimal(str(row["total_job_cost"]))
            total_api_cost += Decimal(str(row["total_api_cost"]))
        except EVERefError as exc:
            errors.append(
                {
                    "product_id": product_id,
                    "runs": runs,
                    "error": str(exc),
                }
            )

    return JsonResponse(
        {
            "jobs": response_jobs,
            "total_job_cost": float(total_job_cost),
            "total_api_cost": float(total_api_cost),
            "errors": errors,
        },
        status=200,
    )


@indy_hub_access_required
@indy_hub_permission_required("can_access_indy_hub")
@login_required
@require_http_methods(["POST"])
def save_production_config(request):
    emit_view_analytics_event(view_name="api.save_production_config", request=request)
    """
    Save complete production configuration to database.

    Expected JSON payload:
    {
        "blueprint_type_id": 12345,
        "blueprint_name": "Some Blueprint",
        "runs": 1,
        "simulation_name": "My Config",
        "active_tab": "materials",
        "items": [
            {"type_id": 11111, "mode": "prod", "quantity": 100},
            {"type_id": 22222, "mode": "buy", "quantity": 50}
        ],
        "blueprint_efficiencies": [
            {"blueprint_type_id": 12345, "material_efficiency": 10, "time_efficiency": 20}
        ],
        "custom_prices": [
            {"item_type_id": 11111, "unit_price": 1000.0, "is_sale_price": false},
            {"item_type_id": 99999, "unit_price": 50000.0, "is_sale_price": true}
        ],
        "estimated_cost": 125000.0,
        "estimated_revenue": 175000.0,
        "estimated_profit": 50000.0
    }
    """
    try:
        data = json.loads(request.body)
        blueprint_type_id = data.get("blueprint_type_id")
        runs = int(data.get("runs", 1) or 1)
        if runs < 1:
            runs = 1

        simulation_id_raw = data.get("simulation_id")
        simulation_id = None
        if simulation_id_raw not in (None, "", 0, "0"):
            try:
                simulation_id = int(simulation_id_raw)
            except (TypeError, ValueError):
                return JsonResponse({"error": "simulation_id must be an integer"}, status=400)

        if not blueprint_type_id:
            return JsonResponse({"error": "blueprint_type_id is required"}, status=400)

        # Create or update the simulation
        simulation = None
        created = False
        if simulation_id:
            simulation = ProductionSimulation.objects.filter(
                id=simulation_id,
                user=request.user,
            ).first()
            if simulation is None:
                return JsonResponse({"error": "simulation not found"}, status=404)
            if int(simulation.blueprint_type_id) != int(blueprint_type_id):
                return JsonResponse(
                    {"error": "simulation does not match blueprint_type_id"},
                    status=400,
                )
        else:
            simulation, created = ProductionSimulation.objects.get_or_create(
                user=request.user,
                blueprint_type_id=blueprint_type_id,
                runs=runs,
                defaults={
                    "blueprint_name": data.get(
                        "blueprint_name", f"Blueprint {blueprint_type_id}"
                    ),
                    "simulation_name": data.get("simulation_name", ""),
                    "active_tab": data.get("active_tab", "materials"),
                    "estimated_cost": data.get("estimated_cost", 0),
                    "estimated_revenue": data.get("estimated_revenue", 0),
                    "estimated_profit": data.get("estimated_profit", 0),
                },
            )

        if not created:
            # Update the existing simulation
            simulation.blueprint_name = data.get(
                "blueprint_name", simulation.blueprint_name
            )
            simulation.runs = runs
            simulation.simulation_name = data.get(
                "simulation_name", simulation.simulation_name
            )
            simulation.active_tab = data.get("active_tab", simulation.active_tab)
            simulation.estimated_cost = data.get(
                "estimated_cost", simulation.estimated_cost
            )
            simulation.estimated_revenue = data.get(
                "estimated_revenue", simulation.estimated_revenue
            )
            simulation.estimated_profit = data.get(
                "estimated_profit", simulation.estimated_profit
            )
            simulation.save()

        # 1. Save the Prod/Buy/Useless configurations
        items = data.get("items", [])
        if items:
            # Remove the previous configurations
            ProductionConfig.objects.filter(simulation=simulation).delete()

            # Create the new configurations
            configs = []
            for item in items:
                config = ProductionConfig(
                    user=request.user,
                    simulation=simulation,
                    blueprint_type_id=blueprint_type_id,
                    item_type_id=item["type_id"],
                    production_mode=item["mode"],
                    quantity_needed=item.get("quantity", 0),
                    runs=runs,
                )
                configs.append(config)

            ProductionConfig.objects.bulk_create(configs)

            # Update the simulation statistics
            simulation.total_items = len(items)
            simulation.total_buy_items = len([i for i in items if i["mode"] == "buy"])
            simulation.total_prod_items = len([i for i in items if i["mode"] == "prod"])

        # 2. Save the blueprint ME/TE efficiencies
        blueprint_efficiencies = data.get("blueprint_efficiencies", [])
        if blueprint_efficiencies:
            # Remove previous efficiencies
            BlueprintEfficiency.objects.filter(simulation=simulation).delete()

            # Create the new efficiencies
            efficiencies = []
            for eff in blueprint_efficiencies:
                efficiency = BlueprintEfficiency(
                    user=request.user,
                    simulation=simulation,
                    blueprint_type_id=eff["blueprint_type_id"],
                    material_efficiency=eff.get("material_efficiency", 0),
                    time_efficiency=eff.get("time_efficiency", 0),
                )
                efficiencies.append(efficiency)

            BlueprintEfficiency.objects.bulk_create(efficiencies)

        # 3. Save the custom prices
        custom_prices = data.get("custom_prices", [])
        if custom_prices:
            # Remove previous prices
            CustomPrice.objects.filter(simulation=simulation).delete()

            # Create the new prices
            prices = []
            for price in custom_prices:
                custom_price = CustomPrice(
                    user=request.user,
                    simulation=simulation,
                    item_type_id=price["item_type_id"],
                    unit_price=price.get("unit_price", 0),
                    is_sale_price=price.get("is_sale_price", False),
                )
                prices.append(custom_price)

            CustomPrice.objects.bulk_create(prices)

        simulation.save()

        return JsonResponse(
            {
                "success": True,
                "simulation_id": simulation.id,
                "simulation_created": created,
                "saved_items": len(items),
                "saved_efficiencies": len(blueprint_efficiencies),
                "saved_prices": len(custom_prices),
                "message": "Complete production configuration saved successfully",
            }
        )

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON data"}, status=400)
    except Exception as e:
        logger.error(f"Error saving production config: {e}")
        return JsonResponse({"error": "Internal server error"}, status=500)


@indy_hub_access_required
@indy_hub_permission_required("can_access_indy_hub")
@login_required
def load_production_config(request):
    emit_view_analytics_event(view_name="api.load_production_config", request=request)
    """
    Load complete production configuration from database.

    Parameters:
    - blueprint_type_id: Required
    - runs: Optional (default 1)

    Returns:
    {
        "blueprint_type_id": 12345,
        "blueprint_name": "Some Blueprint",
        "runs": 1,
        "simulation_name": "My Config",
        "active_tab": "materials",
        "items": [
            {"type_id": 11111, "mode": "prod", "quantity": 100},
            {"type_id": 22222, "mode": "buy", "quantity": 50}
        ],
        "blueprint_efficiencies": [
            {"blueprint_type_id": 12345, "material_efficiency": 10, "time_efficiency": 20}
        ],
        "custom_prices": [
            {"item_type_id": 11111, "unit_price": 1000.0, "is_sale_price": false},
            {"item_type_id": 99999, "unit_price": 50000.0, "is_sale_price": true}
        ],
        "estimated_cost": 125000.0,
        "estimated_revenue": 175000.0,
        "estimated_profit": 50000.0
    }
    """
    simulation_id_param = request.GET.get("simulation_id")
    simulation_id = None
    if simulation_id_param not in (None, "", "0"):
        try:
            simulation_id = int(simulation_id_param)
        except (TypeError, ValueError):
            return JsonResponse({"error": "simulation_id must be an integer"}, status=400)

    blueprint_type_id = request.GET.get("blueprint_type_id")
    runs_param = request.GET.get("runs", 1)
    try:
        runs = int(runs_param)
    except (TypeError, ValueError):
        return JsonResponse(
            {"error": "runs must be an integer"},
            status=400,
        )
    if runs < 1:
        return JsonResponse(
            {"error": "runs must be >= 1"},
            status=400,
        )

    if simulation_id is None and not blueprint_type_id:
        return JsonResponse(
            {"error": "blueprint_type_id parameter required"}, status=400
        )

    try:
        simulation = None  # Load the simulation if it exists
        if simulation_id is not None:
            simulation = ProductionSimulation.objects.filter(
                user=request.user,
                id=simulation_id,
            ).first()
            if simulation is None:
                return JsonResponse({"error": "simulation not found"}, status=404)
            blueprint_type_id = int(simulation.blueprint_type_id)
            runs = int(simulation.runs or runs)
        else:
            try:
                simulation = ProductionSimulation.objects.get(
                    user=request.user, blueprint_type_id=blueprint_type_id, runs=runs
                )
            except ProductionSimulation.DoesNotExist:
                pass

        items = []  # Step 1: production/buy/useless configurations
        if simulation:
            configs = ProductionConfig.objects.filter(simulation=simulation)
            for config in configs:
                items.append(
                    {
                        "type_id": config.item_type_id,
                        "mode": config.production_mode,
                        "quantity": config.quantity_needed,
                    }
                )

        blueprint_efficiencies = []  # Step 2: blueprint ME/TE efficiencies
        if simulation:
            efficiencies = BlueprintEfficiency.objects.filter(simulation=simulation)
            for eff in efficiencies:
                blueprint_efficiencies.append(
                    {
                        "blueprint_type_id": eff.blueprint_type_id,
                        "material_efficiency": eff.material_efficiency,
                        "time_efficiency": eff.time_efficiency,
                    }
                )

        custom_prices = []  # Step 3: custom prices
        if simulation:
            prices = CustomPrice.objects.filter(simulation=simulation)
            for price in prices:
                custom_prices.append(
                    {
                        "item_type_id": price.item_type_id,
                        "unit_price": float(price.unit_price),
                        "is_sale_price": price.is_sale_price,
                    }
                )

        response_data = {
            "blueprint_type_id": int(blueprint_type_id),
            "runs": runs,
            "items": items,
            "blueprint_efficiencies": blueprint_efficiencies,
            "custom_prices": custom_prices,
        }

        if simulation:  # Add simulation metadata when it exists
            response_data.update(
                {
                    "simulation_id": simulation.id,
                    "blueprint_name": simulation.blueprint_name,
                    "simulation_name": simulation.simulation_name,
                    "active_tab": simulation.active_tab,
                    "estimated_cost": float(simulation.estimated_cost),
                    "estimated_revenue": float(simulation.estimated_revenue),
                    "estimated_profit": float(simulation.estimated_profit),
                    "total_items": simulation.total_items,
                    "total_buy_items": simulation.total_buy_items,
                    "total_prod_items": simulation.total_prod_items,
                }
            )

        return JsonResponse(response_data)

    except Exception as e:
        logger.error(f"Error loading production config: {e}")
        return JsonResponse({"error": "Internal server error"}, status=500)

