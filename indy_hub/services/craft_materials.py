"""Helpers for craft planner material quantity calculations and multi-product BOM engine."""

from __future__ import annotations

# Standard Library
from collections import deque
from decimal import Decimal
from math import ceil

# Django
from django.db import connection

from ..utils.eve import get_type_name


def calculate_job_material_quantity(
    base_per_run_quantity: int,
    runs: int,
    *,
    material_efficiency: int | float = 0,
    structure_bonus: float = 0.0,
    rig_bonus: float = 0.0,
) -> int:
    """Return the total input quantity for a job using per-run rounding.

    EVE rounds the adjusted material quantity for each run before totaling the
    job. That means a material required ``1`` per run stays ``1`` for every run,
    even when ME/structure/rig reductions would otherwise push the aggregated
    total below the run count.
    """

    per_run_quantity = max(0, int(base_per_run_quantity or 0))
    run_count = max(0, int(runs or 0))
    if per_run_quantity <= 0 or run_count <= 0:
        return 0

    me_multiplier = max(0.0, (100.0 - float(material_efficiency or 0)) / 100.0)
    structure_multiplier = max(0.0, 1.0 - float(structure_bonus or 0.0))
    rig_multiplier = max(0.0, 1.0 - float(rig_bonus or 0.0))

    adjusted_per_run_quantity = ceil(
        per_run_quantity * me_multiplier * structure_multiplier * rig_multiplier
    )
    return adjusted_per_run_quantity * run_count


def get_blueprint_product(bp_type_id: int) -> tuple[int | None, int]:
    """Return (product_eve_type_id, output_quantity_per_run) for a blueprint."""
    if not bp_type_id:
        return None, 1
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT product_eve_type_id, quantity
            FROM indy_hub_sdeindustryactivityproduct
            WHERE eve_type_id = %s AND activity_id IN (1, 11)
            LIMIT 1
            """,
            [bp_type_id],
        )
        row = cursor.fetchone()
    if row:
        return (row[0], int(row[1] or 1))
    return None, 1


def get_product_blueprint(product_type_id: int) -> tuple[int | None, int]:
    """Return (blueprint_eve_type_id, output_quantity_per_run) for a manufactured product."""
    if not product_type_id:
        return None, 1
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT eve_type_id, quantity
            FROM indy_hub_sdeindustryactivityproduct
            WHERE product_eve_type_id = %s AND activity_id IN (1, 11)
            LIMIT 1
            """,
            [product_type_id],
        )
        row = cursor.fetchone()
    if row:
        return (row[0], int(row[1] or 1))
    return None, 1


def get_blueprint_materials(bp_type_id: int) -> list[tuple[int, str, int]]:
    """Return list of (material_eve_type_id, material_name, base_quantity) for a blueprint."""
    if not bp_type_id:
        return []
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT m.material_eve_type_id, t.name, m.quantity
            FROM indy_hub_sdeindustryactivitymaterial m
            JOIN eve_sde_itemtype t ON m.material_eve_type_id = t.id
            WHERE m.eve_type_id = %s AND m.activity_id IN (1, 11)
            """,
            [bp_type_id],
        )
        return [(int(r[0]), str(r[1]), int(r[2] or 0)) for r in cursor.fetchall()]


def build_multi_product_materials_tree(
    products_manifest: list[dict],
    *,
    me_te_map: dict[int, dict[str, int]] | None = None,
    structure_bonus: float = 0.0,
    rig_bonus: float = 0.0,
    effective_material_bonus: float | None = None,
    max_depth: int = 10,
    custom_prices: dict[tuple[int, bool], float] | dict[int, float] | None = None,
    price_estimates: dict[int, dict] | None = None,
) -> dict[str, object]:
    """Calculate consolidated multi-product BOM and individual trees.

    Accepts a manifest of products `[{"blueprint_type_id": int, "runs": int, "me": int, "te": int}, ...]`,
    pools intermediate component demands across all products, and returns both consolidated
    totals (procurement and production cycles) and per-product contribution breakdowns.
    """
    if me_te_map is None:
        me_te_map = {}
    if custom_prices is None:
        custom_prices = {}
    if price_estimates is None:
        price_estimates = {}

    structure_bonus = max(0.0, min(1.0, float(structure_bonus or 0.0)))
    rig_bonus = max(0.0, min(1.0, float(rig_bonus or 0.0)))

    # Caches for SDE queries during tree construction
    blueprint_product_cache: dict[int, tuple[int | None, int]] = {}
    product_blueprint_cache: dict[int, tuple[int | None, int]] = {}
    blueprint_materials_cache: dict[int, list[tuple[int, str, int]]] = {}

    def cached_get_blueprint_product(bp_type_id: int) -> tuple[int | None, int]:
        if bp_type_id not in blueprint_product_cache:
            blueprint_product_cache[bp_type_id] = get_blueprint_product(bp_type_id)
        return blueprint_product_cache[bp_type_id]

    def cached_get_product_blueprint(product_type_id: int) -> tuple[int | None, int]:
        if product_type_id not in product_blueprint_cache:
            product_blueprint_cache[product_type_id] = get_product_blueprint(product_type_id)
        return product_blueprint_cache[product_type_id]

    def cached_get_blueprint_materials(bp_type_id: int) -> list[tuple[int, str, int]]:
        if bp_type_id not in blueprint_materials_cache:
            blueprint_materials_cache[bp_type_id] = get_blueprint_materials(bp_type_id)
        return blueprint_materials_cache[bp_type_id]

    # 1. Normalize manifest items
    normalized_products: list[dict] = []
    for idx, item in enumerate(products_manifest or []):
        bp_id = int(item.get("blueprint_type_id") or item.get("type_id") or 0)
        if bp_id <= 0:
            continue
        runs = max(1, int(item.get("runs", 1) or 1))
        me = int(
            item.get("me")
            if item.get("me") is not None
            else me_te_map.get(bp_id, {}).get("me", 0) or 0
        )
        te = int(
            item.get("te")
            if item.get("te") is not None
            else me_te_map.get(bp_id, {}).get("te", 0) or 0
        )
        prod_type_id, output_qty_per_run = cached_get_blueprint_product(bp_id)
        bp_name = (
            item.get("blueprint_name")
            or get_type_name(bp_id)
            or f"Blueprint {bp_id}"
        )
        prod_name = (
            item.get("product_name")
            or (get_type_name(prod_type_id) if prod_type_id else bp_name)
        )
        final_product_qty = output_qty_per_run * runs

        normalized_products.append(
            {
                "blueprint_type_id": bp_id,
                "blueprint_name": bp_name,
                "runs": runs,
                "me": me,
                "te": te,
                "product_type_id": prod_type_id,
                "product_name": prod_name,
                "output_qty_per_run": output_qty_per_run,
                "final_product_qty": final_product_qty,
                "sort_order": int(item.get("sort_order", idx) or idx),
            }
        )

    if not normalized_products:
        return {
            "products": [],
            "materials_tree": [],
            "consolidated_materials": [],
            "recipe_map": {},
            "per_product_breakdown": [],
            "summary": {
                "total_products": 0,
                "total_runs": 0,
                "total_items": 0,
                "total_raw_materials": 0,
                "total_intermediate_craftables": 0,
                "total_cost": 0.0,
                "total_revenue": 0.0,
                "total_profit": 0.0,
                "profit_margin_pct": 0.0,
            },
            "type_id": None,
            "bp_type_id": None,
            "num_runs": 0,
            "me": 0,
            "te": 0,
            "product_type_id": None,
            "output_qty_per_run": 1,
            "final_product_qty": 0,
        }

    recipe_map: dict[int, dict[str, object]] = {}
    recipe_cache: dict[tuple[int, int], dict[str, object]] = {}

    def get_materials_tree_for_product(
        bp_id: int,
        runs: int,
        blueprint_me: int = 0,
        depth: int = 0,
        seen: set[int] | None = None,
    ) -> list[dict]:
        if seen is None:
            seen = set()
        if depth > max_depth or bp_id in seen:
            return []
        seen_current = seen.copy()
        seen_current.add(bp_id)

        raw_mats = cached_get_blueprint_materials(bp_id)
        mats = []
        for mat_type_id, mat_name, base_per_run_qty in raw_mats:
            base_total_qty = base_per_run_qty * runs
            qty = calculate_job_material_quantity(
                base_per_run_qty,
                runs,
                material_efficiency=blueprint_me,
                structure_bonus=structure_bonus,
                rig_bonus=rig_bonus,
            )
            mat: dict[str, object] = {
                "type_id": mat_type_id,
                "type_name": mat_name or get_type_name(mat_type_id),
                "quantity": qty,
                "quantity_default": base_total_qty,
                "cycles": None,
                "produced_per_cycle": None,
                "total_produced": None,
                "surplus": None,
                "sub_materials": [],
            }

            sub_bp_id, output_qty = cached_get_product_blueprint(mat_type_id)
            if sub_bp_id:
                cycles = ceil(qty / output_qty)
                total_produced = cycles * output_qty
                surplus = total_produced - qty
                mat["cycles"] = cycles
                mat["produced_per_cycle"] = output_qty
                mat["total_produced"] = total_produced
                mat["surplus"] = surplus

                sub_bp_config = (me_te_map or {}).get(sub_bp_id, {})
                sub_bp_me = sub_bp_config.get("me", 0)

                cache_key = (int(sub_bp_id), int(sub_bp_me))
                if cache_key not in recipe_cache:
                    sub_inputs_raw = cached_get_blueprint_materials(sub_bp_id)
                    inputs = []
                    for s_mat_type_id, _, s_base_qty in sub_inputs_raw:
                        qty_per_cycle = calculate_job_material_quantity(
                            s_base_qty,
                            1,
                            material_efficiency=sub_bp_me,
                            structure_bonus=structure_bonus,
                            rig_bonus=rig_bonus,
                        )
                        if qty_per_cycle > 0:
                            inputs.append(
                                {
                                    "type_id": int(s_mat_type_id),
                                    "quantity": int(qty_per_cycle),
                                }
                            )
                    recipe_cache[cache_key] = {
                        "produced_per_cycle": int(output_qty or 1),
                        "inputs_per_cycle": inputs,
                    }

                if mat_type_id not in recipe_map:
                    recipe_map[mat_type_id] = recipe_cache[cache_key]

                mat["sub_materials"] = get_materials_tree_for_product(
                    sub_bp_id,
                    cycles,
                    sub_bp_me,
                    depth + 1,
                    seen_current,
                )
            mats.append(mat)
        return mats

    # 2. Build tree for each product in manifest and accumulate root requirements
    pooled_direct_demands: dict[int, int] = {}
    demand_attribution: dict[int, list[dict]] = {}

    for prod in normalized_products:
        p_tree = get_materials_tree_for_product(
            prod["blueprint_type_id"],
            prod["runs"],
            prod["me"],
            0,
        )
        prod["materials_tree"] = p_tree

        # Collect top-level requirements for this product
        for top_node in p_tree:
            t_id = int(top_node["type_id"])
            t_qty = int(top_node["quantity"])
            pooled_direct_demands[t_id] = pooled_direct_demands.get(t_id, 0) + t_qty
            demand_attribution.setdefault(t_id, []).append(
                {
                    "blueprint_type_id": prod["blueprint_type_id"],
                    "blueprint_name": prod["blueprint_name"],
                    "quantity": t_qty,
                }
            )

    # 3. Consolidated Multi-Product BOM with Topological Demand Propagation
    # Discover all reachable craftable item types and craftable-to-craftable dependencies
    discovered_craftables: set[int] = set()
    craftable_inputs: dict[int, list[tuple[int, str, int]]] = {}
    craftable_bp_info: dict[int, tuple[int, int]] = {}

    stack = list(pooled_direct_demands.keys())
    seen_types: set[int] = set()

    while stack:
        item_type_id = stack.pop()
        if item_type_id in seen_types:
            continue
        seen_types.add(item_type_id)

        sub_bp_id, output_qty = cached_get_product_blueprint(item_type_id)
        if sub_bp_id:
            discovered_craftables.add(item_type_id)
            craftable_bp_info[item_type_id] = (sub_bp_id, output_qty)
            sub_mats = cached_get_blueprint_materials(sub_bp_id)
            craftable_inputs[item_type_id] = sub_mats
            for s_mat_id, _, _ in sub_mats:
                if s_mat_id not in seen_types:
                    stack.append(s_mat_id)

    # Build dependency graph between craftable items: u -> v means u consumes v
    in_degree: dict[int, int] = {c: 0 for c in discovered_craftables}
    craftable_children: dict[int, list[int]] = {c: [] for c in discovered_craftables}

    for u in discovered_craftables:
        for v, _, _ in craftable_inputs.get(u, []):
            if v in discovered_craftables:
                craftable_children[u].append(v)
                in_degree[v] += 1

    # Topological sort (Kahn's algorithm)
    ready = deque([c for c in discovered_craftables if in_degree[c] == 0])
    topo_order: list[int] = []
    while ready:
        u = ready.popleft()
        topo_order.append(u)
        for v in craftable_children[u]:
            in_degree[v] -= 1
            if in_degree[v] == 0:
                ready.append(v)

    # Fallback to include any remaining nodes in case of cycles in data
    if len(topo_order) < len(discovered_craftables):
        for c in discovered_craftables:
            if c not in topo_order:
                topo_order.append(c)

    # Ensure recipe_map is populated for all discovered craftables
    for c_type_id in discovered_craftables:
        if c_type_id not in recipe_map:
            sub_bp_id, output_qty = craftable_bp_info[c_type_id]
            sub_bp_me = (me_te_map or {}).get(sub_bp_id, {}).get("me", 0)
            sub_inputs_raw = craftable_inputs.get(c_type_id, [])
            inputs = []
            for s_mat_type_id, _, s_base_qty in sub_inputs_raw:
                qty_per_cycle = calculate_job_material_quantity(
                    s_base_qty,
                    1,
                    material_efficiency=sub_bp_me,
                    structure_bonus=structure_bonus,
                    rig_bonus=rig_bonus,
                )
                if qty_per_cycle > 0:
                    inputs.append(
                        {
                            "type_id": int(s_mat_type_id),
                            "quantity": int(qty_per_cycle),
                        }
                    )
            recipe_map[c_type_id] = {
                "produced_per_cycle": int(output_qty or 1),
                "inputs_per_cycle": inputs,
            }

    # Propagate demand in topological order
    accumulated_demand: dict[int, int] = dict(pooled_direct_demands)

    for c_type_id in topo_order:
        needed_qty = accumulated_demand.get(c_type_id, 0)
        if needed_qty <= 0:
            continue
        sub_bp_id, output_qty = craftable_bp_info[c_type_id]
        cycles = ceil(needed_qty / output_qty)
        sub_bp_me = (me_te_map or {}).get(sub_bp_id, {}).get("me", 0)

        sub_inputs = craftable_inputs.get(c_type_id, [])
        for s_mat_type_id, _, s_base_qty in sub_inputs:
            input_req = calculate_job_material_quantity(
                s_base_qty,
                cycles,
                material_efficiency=sub_bp_me,
                structure_bonus=structure_bonus,
                rig_bonus=rig_bonus,
            )
            accumulated_demand[s_mat_type_id] = (
                accumulated_demand.get(s_mat_type_id, 0) + input_req
            )

    # 4. Generate consolidated materials list
    consolidated_materials: list[dict] = []
    raw_materials_count = 0
    intermediate_craftables_count = 0

    for mat_type_id, total_needed in sorted(
        accumulated_demand.items(), key=lambda x: (-x[1], x[0])
    ):
        if mat_type_id in craftable_bp_info:
            sub_bp_id, output_qty = craftable_bp_info[mat_type_id]
            is_craftable = True
        else:
            sub_bp_id, output_qty = cached_get_product_blueprint(mat_type_id)
            is_craftable = sub_bp_id is not None

        if is_craftable:
            intermediate_craftables_count += 1
            cycles = ceil(total_needed / output_qty)
            total_produced = cycles * output_qty
            surplus = total_produced - total_needed
        else:
            raw_materials_count += 1
            cycles = None
            output_qty = None
            total_produced = None
            surplus = None

        consolidated_materials.append(
            {
                "type_id": mat_type_id,
                "type_name": get_type_name(mat_type_id),
                "quantity": total_needed,
                "quantity_default": total_needed,
                "cycles": cycles,
                "produced_per_cycle": output_qty,
                "total_produced": total_produced,
                "surplus": surplus,
                "is_craftable": is_craftable,
                "sub_blueprint_type_id": sub_bp_id,
                "used_by_products": demand_attribution.get(mat_type_id, []),
            }
        )

    # 5. Price helper & per-product financial contribution breakdown
    def resolve_item_price(type_id: int, is_sale: bool = False) -> float:
        if isinstance(custom_prices, dict):
            if (type_id, is_sale) in custom_prices:
                return float(custom_prices[(type_id, is_sale)] or 0.0)
            if type_id in custom_prices and not is_sale:
                return float(custom_prices[type_id] or 0.0)
        if price_estimates and type_id in price_estimates:
            est = price_estimates[type_id]
            if isinstance(est, dict):
                return float(est.get("price") or 0.0)
            try:
                return float(est)
            except (TypeError, ValueError):
                return 0.0
        return 0.0

    def calculate_tree_leaf_cost(nodes: list[dict]) -> float:
        leaf_cost = 0.0
        for node in nodes or []:
            sub = node.get("sub_materials")
            if sub and len(sub) > 0:
                leaf_cost += calculate_tree_leaf_cost(sub)
            else:
                qty = float(node.get("quantity") or 0)
                price = resolve_item_price(int(node.get("type_id") or 0), is_sale=False)
                leaf_cost += qty * price
        return leaf_cost

    per_product_breakdown: list[dict] = []
    total_cost_accum = 0.0
    total_revenue_accum = 0.0

    for prod in normalized_products:
        p_tree = prod.get("materials_tree", [])
        prod_type_id = prod["product_type_id"]
        runs = prod["runs"]
        final_qty = prod["final_product_qty"]

        unit_sale_price = resolve_item_price(prod_type_id, is_sale=True) if prod_type_id else 0.0
        p_revenue = float(final_qty * unit_sale_price)
        p_cost = float(calculate_tree_leaf_cost(p_tree))
        p_profit = p_revenue - p_cost
        p_unit_cost = (p_cost / final_qty) if final_qty > 0 else 0.0
        p_margin = (p_profit / p_cost * 100.0) if p_cost > 0 else 0.0

        prod["estimated_cost"] = round(p_cost, 2)
        prod["estimated_revenue"] = round(p_revenue, 2)
        prod["estimated_profit"] = round(p_profit, 2)
        prod["profit_margin_pct"] = round(p_margin, 2)

        per_product_breakdown.append(
            {
                "blueprint_type_id": prod["blueprint_type_id"],
                "blueprint_name": prod["blueprint_name"],
                "product_type_id": prod_type_id,
                "product_name": prod["product_name"],
                "runs": runs,
                "output_qty_per_run": prod["output_qty_per_run"],
                "final_product_qty": final_qty,
                "estimated_unit_cost": round(p_unit_cost, 2),
                "estimated_cost": round(p_cost, 2),
                "estimated_revenue": round(p_revenue, 2),
                "estimated_profit": round(p_profit, 2),
                "profit_margin_pct": round(p_margin, 2),
            }
        )

        total_cost_accum += p_cost
        total_revenue_accum += p_revenue

    total_profit_accum = total_revenue_accum - total_cost_accum
    total_margin_pct = (
        (total_profit_accum / total_cost_accum * 100.0) if total_cost_accum > 0 else 0.0
    )

    primary_product = normalized_products[0]
    total_runs_count = sum(p["runs"] for p in normalized_products)

    # For materials_tree in payload:
    # Build a consolidated unified tree merging shared component demands across all basket products
    def build_consolidated_tree_nodes(
        direct_demands: dict[int, int],
        depth: int = 0,
        seen: set[int] | None = None,
    ) -> list[dict]:
        if seen is None:
            seen = set()
        if depth > max_depth:
            return []

        tree_nodes = []
        for mat_type_id, qty in direct_demands.items():
            if qty <= 0:
                continue
            mat_name = get_type_name(mat_type_id)
            mat: dict[str, object] = {
                "type_id": mat_type_id,
                "type_name": mat_name,
                "quantity": qty,
                "quantity_default": qty,
                "cycles": None,
                "produced_per_cycle": None,
                "total_produced": None,
                "surplus": None,
                "sub_materials": [],
            }
            sub_bp_id, output_qty = cached_get_product_blueprint(mat_type_id)
            if sub_bp_id and mat_type_id not in seen:
                seen_current = seen.copy()
                seen_current.add(mat_type_id)
                cycles = ceil(qty / output_qty)
                total_produced = cycles * output_qty
                surplus = total_produced - qty
                mat["cycles"] = cycles
                mat["produced_per_cycle"] = output_qty
                mat["total_produced"] = total_produced
                mat["surplus"] = surplus

                sub_bp_config = (me_te_map or {}).get(sub_bp_id, {})
                sub_bp_me = sub_bp_config.get("me", 0)

                sub_inputs_raw = cached_get_blueprint_materials(sub_bp_id)
                sub_demands: dict[int, int] = {}
                for s_mat_type_id, _, s_base_qty in sub_inputs_raw:
                    s_req = calculate_job_material_quantity(
                        s_base_qty,
                        cycles,
                        material_efficiency=sub_bp_me,
                        structure_bonus=structure_bonus,
                        rig_bonus=rig_bonus,
                    )
                    sub_demands[s_mat_type_id] = sub_demands.get(s_mat_type_id, 0) + s_req

                mat["sub_materials"] = build_consolidated_tree_nodes(
                    sub_demands,
                    depth + 1,
                    seen_current,
                )
            tree_nodes.append(mat)
        return tree_nodes

    if len(normalized_products) == 1 and normalized_products[0].get("materials_tree"):
        payload_tree = normalized_products[0]["materials_tree"]
    else:
        payload_tree = build_consolidated_tree_nodes(pooled_direct_demands)

    return {
        "products": normalized_products,
        "materials_tree": payload_tree,
        "consolidated_materials": consolidated_materials,
        "recipe_map": recipe_map,
        "per_product_breakdown": per_product_breakdown,
        "summary": {
            "total_products": len(normalized_products),
            "total_runs": total_runs_count,
            "total_items": len(consolidated_materials),
            "total_raw_materials": raw_materials_count,
            "total_intermediate_craftables": intermediate_craftables_count,
            "total_cost": round(total_cost_accum, 2),
            "total_revenue": round(total_revenue_accum, 2),
            "total_profit": round(total_profit_accum, 2),
            "profit_margin_pct": round(total_margin_pct, 2),
        },
        "type_id": primary_product["blueprint_type_id"],
        "bp_type_id": primary_product["blueprint_type_id"],
        "num_runs": total_runs_count if len(normalized_products) > 1 else primary_product["runs"],
        "me": primary_product["me"],
        "te": primary_product["te"],
        "product_type_id": primary_product["product_type_id"],
        "output_qty_per_run": primary_product["output_qty_per_run"],
        "final_product_qty": (
            sum(p["final_product_qty"] for p in normalized_products)
            if len(normalized_products) > 1 and all(p["product_type_id"] == primary_product["product_type_id"] for p in normalized_products)
            else primary_product["final_product_qty"]
        ),
    }
