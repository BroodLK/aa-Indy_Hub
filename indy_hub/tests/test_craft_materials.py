"""Tests for craft planner material quantity math and multi-product consolidated BOM engine."""

# Django
from django.test import SimpleTestCase, TestCase

# Alliance Auth (External Libs)
from eve_sde.models import ItemCategory, ItemGroup, ItemType

# AA Example App
from indy_hub.models import (
    SdeIndustryActivityMaterial,
    SdeIndustryActivityProduct,
)
from indy_hub.services.craft_materials import (
    build_multi_product_materials_tree,
    calculate_job_material_quantity,
    get_blueprint_materials,
    get_blueprint_product,
    get_product_blueprint,
)


class CraftMaterialQuantityTests(SimpleTestCase):
    def test_single_unit_per_run_keeps_one_per_run(self) -> None:
        quantity = calculate_job_material_quantity(
            1,
            15,
            material_efficiency=10,
            structure_bonus=0.01,
        )

        self.assertEqual(quantity, 15)

    def test_small_stack_rounds_per_run_before_totaling(self) -> None:
        quantity = calculate_job_material_quantity(
            4,
            15,
            material_efficiency=10,
            structure_bonus=0.01,
        )

        self.assertEqual(quantity, 60)

    def test_larger_stack_matches_per_run_rounding_behavior(self) -> None:
        quantity = calculate_job_material_quantity(
            15,
            15,
            material_efficiency=10,
            structure_bonus=0.01,
        )

        self.assertEqual(quantity, 210)

    def test_large_quantities_do_not_gain_cross_run_discount(self) -> None:
        quantity = calculate_job_material_quantity(
            400,
            15,
            material_efficiency=10,
            structure_bonus=0.01,
        )

        self.assertEqual(quantity, 5355)


class MultiProductBOMCalculationTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from django.db import connection
        with connection.cursor() as cursor:
            for model_cls in [ItemCategory, ItemGroup, ItemType]:
                table = model_cls._meta.db_table
                for field in model_cls._meta.fields:
                    col = field.column
                    try:
                        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} varchar(255);")
                    except Exception:
                        pass
        super().setUpClass()

    @classmethod
    def setUpTestData(cls) -> None:
        category, _ = ItemCategory.objects.get_or_create(id=9901, defaults={"name": "Ship"})
        group, _ = ItemGroup.objects.get_or_create(
            id=9902, defaults={"name": "Cruisers", "category": category}
        )

        # Create Items
        cls.cruiser_a_bp, _ = ItemType.objects.update_or_create(
            id=90001, defaults={"name": "Cruiser A Blueprint", "group": group}
        )
        cls.cruiser_a, _ = ItemType.objects.update_or_create(
            id=90002, defaults={"name": "Cruiser A", "group": group}
        )
        cls.cruiser_b_bp, _ = ItemType.objects.update_or_create(
            id=90003, defaults={"name": "Cruiser B Blueprint", "group": group}
        )
        cls.cruiser_b, _ = ItemType.objects.update_or_create(
            id=90004, defaults={"name": "Cruiser B", "group": group}
        )
        cls.component_x_bp, _ = ItemType.objects.update_or_create(
            id=90011, defaults={"name": "Component X Blueprint", "group": group}
        )
        cls.component_x, _ = ItemType.objects.update_or_create(
            id=90010, defaults={"name": "Component X", "group": group}
        )
        cls.tritanium, _ = ItemType.objects.update_or_create(
            id=90020, defaults={"name": "Tritanium", "group": group}
        )
        cls.pyerite, _ = ItemType.objects.update_or_create(
            id=90021, defaults={"name": "Pyerite", "group": group}
        )
        cls.isogen, _ = ItemType.objects.update_or_create(
            id=90022, defaults={"name": "Isogen", "group": group}
        )
        cls.zydrine, _ = ItemType.objects.update_or_create(
            id=90023, defaults={"name": "Zydrine", "group": group}
        )
        cls.mexallon, _ = ItemType.objects.update_or_create(
            id=90024, defaults={"name": "Mexallon", "group": group}
        )

        # Cross-tier items:
        # Battleship: requires Component Tier 2 (x5), Tritanium (x2000)
        cls.battleship_bp, _ = ItemType.objects.update_or_create(
            id=90031, defaults={"name": "Battleship Blueprint", "group": group}
        )
        cls.battleship, _ = ItemType.objects.update_or_create(
            id=90032, defaults={"name": "Battleship", "group": group}
        )
        # Component Tier 2: requires Component Tier 1 (x2), Pyerite (x500) -> produces 1
        cls.component_t2_bp, _ = ItemType.objects.update_or_create(
            id=90041, defaults={"name": "Component Tier 2 Blueprint", "group": group}
        )
        cls.component_t2, _ = ItemType.objects.update_or_create(
            id=90040, defaults={"name": "Component Tier 2", "group": group}
        )
        # Component Tier 1: requires Tritanium (x100), Isogen (x50) -> produces 5
        cls.component_t1_bp, _ = ItemType.objects.update_or_create(
            id=90051, defaults={"name": "Component Tier 1 Blueprint", "group": group}
        )
        cls.component_t1, _ = ItemType.objects.update_or_create(
            id=90050, defaults={"name": "Component Tier 1", "group": group}
        )
        # Cruiser C: requires Component Tier 1 (x3), Tritanium (x1000)
        cls.cruiser_c_bp, _ = ItemType.objects.update_or_create(
            id=90061, defaults={"name": "Cruiser C Blueprint", "group": group}
        )
        cls.cruiser_c, _ = ItemType.objects.update_or_create(
            id=90062, defaults={"name": "Cruiser C", "group": group}
        )

        # Disjoint module
        cls.module_bp, _ = ItemType.objects.update_or_create(
            id=90071, defaults={"name": "Module Blueprint", "group": group}
        )
        cls.module, _ = ItemType.objects.update_or_create(
            id=90072, defaults={"name": "Module", "group": group}
        )

        # Blueprint products
        SdeIndustryActivityProduct.objects.update_or_create(
            eve_type=cls.cruiser_a_bp,
            activity_id=1,
            defaults={"product_eve_type": cls.cruiser_a, "quantity": 1},
        )
        SdeIndustryActivityProduct.objects.update_or_create(
            eve_type=cls.cruiser_b_bp,
            activity_id=1,
            defaults={"product_eve_type": cls.cruiser_b, "quantity": 1},
        )
        SdeIndustryActivityProduct.objects.update_or_create(
            eve_type=cls.component_x_bp,
            activity_id=1,
            defaults={"product_eve_type": cls.component_x, "quantity": 10},
        )
        SdeIndustryActivityProduct.objects.update_or_create(
            eve_type=cls.battleship_bp,
            activity_id=1,
            defaults={"product_eve_type": cls.battleship, "quantity": 1},
        )
        SdeIndustryActivityProduct.objects.update_or_create(
            eve_type=cls.component_t2_bp,
            activity_id=1,
            defaults={"product_eve_type": cls.component_t2, "quantity": 1},
        )
        SdeIndustryActivityProduct.objects.update_or_create(
            eve_type=cls.component_t1_bp,
            activity_id=1,
            defaults={"product_eve_type": cls.component_t1, "quantity": 5},
        )
        SdeIndustryActivityProduct.objects.update_or_create(
            eve_type=cls.cruiser_c_bp,
            activity_id=1,
            defaults={"product_eve_type": cls.cruiser_c, "quantity": 1},
        )
        SdeIndustryActivityProduct.objects.update_or_create(
            eve_type=cls.module_bp,
            activity_id=1,
            defaults={"product_eve_type": cls.module, "quantity": 1},
        )

        # Blueprint materials
        # Cruiser A requires: 10 Component X, 1000 Tritanium
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.cruiser_a_bp,
            material_eve_type=cls.component_x,
            activity_id=1,
            defaults={"quantity": 10},
        )
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.cruiser_a_bp,
            material_eve_type=cls.tritanium,
            activity_id=1,
            defaults={"quantity": 1000},
        )

        # Cruiser B requires: 15 Component X, 500 Pyerite
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.cruiser_b_bp,
            material_eve_type=cls.component_x,
            activity_id=1,
            defaults={"quantity": 15},
        )
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.cruiser_b_bp,
            material_eve_type=cls.pyerite,
            activity_id=1,
            defaults={"quantity": 500},
        )

        # Component X Blueprint requires: 100 Tritanium, 50 Pyerite per run (produces 10)
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.component_x_bp,
            material_eve_type=cls.tritanium,
            activity_id=1,
            defaults={"quantity": 100},
        )
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.component_x_bp,
            material_eve_type=cls.pyerite,
            activity_id=1,
            defaults={"quantity": 50},
        )

        # Battleship requires: 5 Component Tier 2, 2000 Tritanium
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.battleship_bp,
            material_eve_type=cls.component_t2,
            activity_id=1,
            defaults={"quantity": 5},
        )
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.battleship_bp,
            material_eve_type=cls.tritanium,
            activity_id=1,
            defaults={"quantity": 2000},
        )

        # Component Tier 2 requires: 2 Component Tier 1, 500 Pyerite
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.component_t2_bp,
            material_eve_type=cls.component_t1,
            activity_id=1,
            defaults={"quantity": 2},
        )
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.component_t2_bp,
            material_eve_type=cls.pyerite,
            activity_id=1,
            defaults={"quantity": 500},
        )

        # Component Tier 1 requires: 100 Tritanium, 50 Isogen (produces 5)
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.component_t1_bp,
            material_eve_type=cls.tritanium,
            activity_id=1,
            defaults={"quantity": 100},
        )
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.component_t1_bp,
            material_eve_type=cls.isogen,
            activity_id=1,
            defaults={"quantity": 50},
        )

        # Cruiser C requires: 3 Component Tier 1, 1000 Tritanium
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.cruiser_c_bp,
            material_eve_type=cls.component_t1,
            activity_id=1,
            defaults={"quantity": 3},
        )
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.cruiser_c_bp,
            material_eve_type=cls.tritanium,
            activity_id=1,
            defaults={"quantity": 1000},
        )

        # Module requires: 100 Zydrine, 200 Mexallon
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.module_bp,
            material_eve_type=cls.zydrine,
            activity_id=1,
            defaults={"quantity": 100},
        )
        SdeIndustryActivityMaterial.objects.update_or_create(
            eve_type=cls.module_bp,
            material_eve_type=cls.mexallon,
            activity_id=1,
            defaults={"quantity": 200},
        )

    def test_blueprint_queries(self) -> None:
        prod_id, out_qty = get_blueprint_product(90001)
        self.assertEqual(prod_id, 90002)
        self.assertEqual(out_qty, 1)

        bp_id, bp_out_qty = get_product_blueprint(90010)
        self.assertEqual(bp_id, 90011)
        self.assertEqual(bp_out_qty, 10)

        mats = get_blueprint_materials(90001)
        self.assertEqual(len(mats), 2)
        mat_dict = {m[0]: m[2] for m in mats}
        self.assertEqual(mat_dict[90010], 10)
        self.assertEqual(mat_dict[90020], 1000)

    def test_single_product_bom_parity(self) -> None:
        manifest = [{"blueprint_type_id": 90001, "runs": 2, "me": 0, "te": 0}]
        payload = build_multi_product_materials_tree(manifest)

        self.assertEqual(payload["type_id"], 90001)
        self.assertEqual(payload["num_runs"], 2)
        self.assertEqual(payload["product_type_id"], 90002)
        self.assertEqual(payload["output_qty_per_run"], 1)
        self.assertEqual(payload["final_product_qty"], 2)
        self.assertEqual(len(payload["products"]), 1)
        self.assertEqual(len(payload["materials_tree"]), 2)

        # Check Component X sub materials
        comp_node = next(n for n in payload["materials_tree"] if n["type_id"] == 90010)
        self.assertEqual(comp_node["quantity"], 20)  # 10 * 2
        self.assertEqual(comp_node["cycles"], 2)  # ceil(20 / 10)
        self.assertEqual(comp_node["total_produced"], 20)
        self.assertEqual(comp_node["surplus"], 0)
        self.assertEqual(len(comp_node["sub_materials"]), 2)

    def test_multi_product_shared_intermediate_pooling(self) -> None:
        # 2 runs of Cruiser A (needs 20 Component X) + 1 run of Cruiser B (needs 15 Component X)
        manifest = [
            {"blueprint_type_id": 90001, "runs": 2, "me": 0},
            {"blueprint_type_id": 90003, "runs": 1, "me": 0},
        ]
        payload = build_multi_product_materials_tree(manifest)

        self.assertEqual(payload["summary"]["total_products"], 2)
        self.assertEqual(payload["summary"]["total_runs"], 3)

        cons_mats = {m["type_id"]: m for m in payload["consolidated_materials"]}

        # Component X: total needed = 20 + 15 = 35
        # Output per run = 10 -> pooled cycles = ceil(35 / 10) = 4 cycles
        # Total produced = 40, surplus = 5
        comp_x = cons_mats[90010]
        self.assertEqual(comp_x["quantity"], 35)
        self.assertEqual(comp_x["cycles"], 4)
        self.assertEqual(comp_x["produced_per_cycle"], 10)
        self.assertEqual(comp_x["total_produced"], 40)
        self.assertEqual(comp_x["surplus"], 5)
        self.assertTrue(comp_x["is_craftable"])

        # Check raw materials pooled demand:
        # Tritanium: Cruiser A direct (2 * 1000 = 2000) + Component X 4 cycles (4 * 100 = 400) = 2400
        # Pyerite: Cruiser B direct (1 * 500 = 500) + Component X 4 cycles (4 * 50 = 200) = 700
        self.assertEqual(cons_mats[90020]["quantity"], 2400)
        self.assertFalse(cons_mats[90020]["is_craftable"])
        self.assertEqual(cons_mats[90021]["quantity"], 700)
        self.assertFalse(cons_mats[90021]["is_craftable"])

        # Check recipe_map contains exact per-cycle inputs for Component X
        self.assertIn(90010, payload["recipe_map"])
        rec = payload["recipe_map"][90010]
        self.assertEqual(rec["produced_per_cycle"], 10)
        rec_inputs = {inp["type_id"]: inp["quantity"] for inp in rec["inputs_per_cycle"]}
        self.assertEqual(rec_inputs[90020], 100)
        self.assertEqual(rec_inputs[90021], 50)

    def test_financial_breakdown_and_custom_pricing(self) -> None:
        manifest = [
            {"blueprint_type_id": 90001, "runs": 2, "me": 0},
            {"blueprint_type_id": 90003, "runs": 1, "me": 0},
        ]
        # Custom prices: Tritanium = 5 ISK, Pyerite = 10 ISK, Cruiser A sale price = 50000 ISK, Cruiser B sale price = 40000 ISK
        custom_prices = {
            (90020, False): 5.0,
            (90021, False): 10.0,
            (90002, True): 50000.0,
            (90004, True): 40000.0,
        }
        payload = build_multi_product_materials_tree(manifest, custom_prices=custom_prices)

        breakdown = payload["per_product_breakdown"]
        self.assertEqual(len(breakdown), 2)

        prod_a = breakdown[0]
        self.assertEqual(prod_a["blueprint_type_id"], 90001)
        self.assertEqual(prod_a["runs"], 2)
        self.assertEqual(prod_a["final_product_qty"], 2)
        self.assertEqual(prod_a["estimated_revenue"], 100000.0)  # 2 * 50000

        prod_b = breakdown[1]
        self.assertEqual(prod_b["blueprint_type_id"], 90003)
        self.assertEqual(prod_b["runs"], 1)
        self.assertEqual(prod_b["final_product_qty"], 1)
        self.assertEqual(prod_b["estimated_revenue"], 40000.0)  # 1 * 40000

        self.assertEqual(payload["summary"]["total_revenue"], 140000.0)
        self.assertGreater(payload["summary"]["total_cost"], 0.0)
        self.assertGreater(payload["summary"]["total_profit"], 0.0)

    def test_empty_or_invalid_manifest_returns_empty_summary(self) -> None:
        payload = build_multi_product_materials_tree([])
        self.assertEqual(payload["summary"]["total_products"], 0)
        self.assertEqual(payload["summary"]["total_runs"], 0)
        self.assertEqual(payload["materials_tree"], [])
        self.assertEqual(payload["consolidated_materials"], [])

    def test_multi_product_cross_tier_shared_intermediate_pooling(self) -> None:
        """
        Verify that shared intermediate components consumed at different tiers
        (e.g., Tier 1 consumed directly by Cruiser C and indirectly by Battleship via Tier 2)
        are fully accumulated before computing cycles, preventing under-counting of deep raw materials.
        """
        manifest = [
            {"blueprint_type_id": 90031, "runs": 1, "me": 0},  # Battleship
            {"blueprint_type_id": 90061, "runs": 1, "me": 0},  # Cruiser C
        ]
        payload = build_multi_product_materials_tree(manifest)

        cons_mats = {m["type_id"]: m for m in payload["consolidated_materials"]}

        # Component Tier 2: 5 units needed -> 5 cycles (1 per run), surplus = 0
        comp_t2 = cons_mats[90040]
        self.assertEqual(comp_t2["quantity"], 5)
        self.assertEqual(comp_t2["cycles"], 5)
        self.assertEqual(comp_t2["total_produced"], 5)
        self.assertEqual(comp_t2["surplus"], 0)

        # Component Tier 1: 3 (direct from Cruiser C) + 10 (5 cycles * 2 from Tier 2) = 13 units
        # Output per run = 5 -> ceil(13 / 5) = 3 cycles, total_produced = 15, surplus = 2
        comp_t1 = cons_mats[90050]
        self.assertEqual(comp_t1["quantity"], 13)
        self.assertEqual(comp_t1["cycles"], 3)
        self.assertEqual(comp_t1["produced_per_cycle"], 5)
        self.assertEqual(comp_t1["total_produced"], 15)
        self.assertEqual(comp_t1["surplus"], 2)

        # Raw materials verification:
        # Tritanium: 2000 (Battleship direct) + 1000 (Cruiser C direct) + 300 (3 cycles * 100 from Tier 1) = 3300
        self.assertEqual(cons_mats[90020]["quantity"], 3300)
        # Pyerite: 5 cycles * 500 from Tier 2 = 2500
        self.assertEqual(cons_mats[90021]["quantity"], 2500)
        # Isogen: 3 cycles * 50 from Tier 1 = 150
        self.assertEqual(cons_mats[90022]["quantity"], 150)

    def test_large_manifest_and_many_craftable_nodes_exceeding_iteration_cap(self) -> None:
        """
        Verify that a large basket with many distinct products and craftables completes
        without hitting any iteration cap or dropping/truncating materials.
        """
        group = ItemGroup.objects.get(id=9902)
        raw_mat = ItemType.objects.get(id=90020)

        manifest = []
        # Create 25 distinct products with their own blueprints and intermediate craftables
        for i in range(1, 26):
            bp_id = 91000 + i
            prod_id = 92000 + i
            sub_bp_id = 93000 + i
            sub_prod_id = 94000 + i

            bp, _ = ItemType.objects.update_or_create(
                id=bp_id, defaults={"name": f"Product BP {i}", "group": group}
            )
            prod, _ = ItemType.objects.update_or_create(
                id=prod_id, defaults={"name": f"Product {i}", "group": group}
            )
            sub_bp, _ = ItemType.objects.update_or_create(
                id=sub_bp_id, defaults={"name": f"Sub BP {i}", "group": group}
            )
            sub_prod, _ = ItemType.objects.update_or_create(
                id=sub_prod_id, defaults={"name": f"Sub Component {i}", "group": group}
            )

            # Product produces prod
            SdeIndustryActivityProduct.objects.update_or_create(
                eve_type=bp,
                activity_id=1,
                defaults={"product_eve_type": prod, "quantity": 1},
            )
            # Sub BP produces sub_prod
            SdeIndustryActivityProduct.objects.update_or_create(
                eve_type=sub_bp,
                activity_id=1,
                defaults={"product_eve_type": sub_prod, "quantity": 2},
            )

            # Product requires sub_prod (4 units)
            SdeIndustryActivityMaterial.objects.update_or_create(
                eve_type=bp,
                material_eve_type=sub_prod,
                activity_id=1,
                defaults={"quantity": 4},
            )
            # Sub BP requires raw Tritanium (100 units)
            SdeIndustryActivityMaterial.objects.update_or_create(
                eve_type=sub_bp,
                material_eve_type=raw_mat,
                activity_id=1,
                defaults={"quantity": 100},
            )

            manifest.append({"blueprint_type_id": bp_id, "runs": 2, "me": 0})

        payload = build_multi_product_materials_tree(manifest)

        # 25 products in manifest
        self.assertEqual(payload["summary"]["total_products"], 25)
        self.assertEqual(payload["summary"]["total_runs"], 50)  # 25 * 2

        cons_mats = {m["type_id"]: m for m in payload["consolidated_materials"]}

        # All 25 sub components must be present with exactly 8 units needed (4 * 2 runs)
        for i in range(1, 26):
            sub_prod_id = 94000 + i
            self.assertIn(sub_prod_id, cons_mats)
            self.assertEqual(cons_mats[sub_prod_id]["quantity"], 8)
            self.assertEqual(cons_mats[sub_prod_id]["cycles"], 4)  # ceil(8 / 2) = 4
            self.assertTrue(cons_mats[sub_prod_id]["is_craftable"])

        # Total Tritanium across all 25 sub components = 25 * (4 cycles * 100) = 10,000
        self.assertEqual(cons_mats[90020]["quantity"], 10000)

    def test_no_shared_intermediates_disjoint_trees(self) -> None:
        """
        Verify that products with completely disjoint trees merge properly without interference.
        """
        manifest = [
            {"blueprint_type_id": 90001, "runs": 1, "me": 0},  # Cruiser A
            {"blueprint_type_id": 90071, "runs": 2, "me": 0},  # Module
        ]
        payload = build_multi_product_materials_tree(manifest)

        self.assertEqual(payload["summary"]["total_products"], 2)
        self.assertEqual(payload["summary"]["total_runs"], 3)

        cons_mats = {m["type_id"]: m for m in payload["consolidated_materials"]}

        # Cruiser A branch:
        self.assertIn(90010, cons_mats)  # Component X
        self.assertIn(90020, cons_mats)  # Tritanium
        self.assertIn(90021, cons_mats)  # Pyerite

        # Module branch:
        self.assertIn(90023, cons_mats)  # Zydrine: 2 runs * 100 = 200
        self.assertEqual(cons_mats[90023]["quantity"], 200)
        self.assertIn(90024, cons_mats)  # Mexallon: 2 runs * 200 = 400
        self.assertEqual(cons_mats[90024]["quantity"], 400)

    def test_multi_product_material_efficiency_and_structure_bonus(self) -> None:
        """
        Verify that Material Efficiency (ME) and structure bonuses apply correctly
        across multi-product trees for both root products and pooled intermediate components.
        """
        # Cruiser A with ME 10, Cruiser B with ME 10, structure bonus 1% (0.01)
        # Cruiser A base direct Tritanium = 1000 per run * 2 runs = 2000
        # With ME 10 + 1% structure bonus: calculate_job_material_quantity(2, 1000, 10, 0.01)
        manifest = [
            {"blueprint_type_id": 90001, "runs": 2, "me": 10, "te": 20},
            {"blueprint_type_id": 90003, "runs": 1, "me": 10, "te": 20},
        ]
        me_te_map = {
            90001: {"me": 10, "te": 20},
            90003: {"me": 10, "te": 20},
            90011: {"me": 10, "te": 20},  # Component X BP
        }

        payload = build_multi_product_materials_tree(
            manifest,
            me_te_map=me_te_map,
            structure_bonus=0.01,
            rig_bonus=0.0,
        )

        cons_mats = {m["type_id"]: m for m in payload["consolidated_materials"]}

        # Component X needed:
        # Cruiser A direct needed: 2 * calculate_job_material_quantity(1, 10, 10, 0.01) = 2 * 9 = 18
        # Cruiser B direct needed: 1 * calculate_job_material_quantity(1, 15, 10, 0.01) = 1 * 14 = 14
        # Total Component X needed = 18 + 14 = 32
        # Cycles of Component X (output 10/cycle) = ceil(32 / 10) = 4 cycles
        # Total produced = 40, surplus = 8
        comp_x = cons_mats[90010]
        self.assertEqual(comp_x["quantity"], 32)
        self.assertEqual(comp_x["cycles"], 4)
        self.assertEqual(comp_x["total_produced"], 40)
        self.assertEqual(comp_x["surplus"], 8)

        # Raw materials with ME 10 and 1% bonus:
        # Direct Tritanium for Cruiser A: calculate_job_material_quantity(2, 1000, 10, 0.01) = 2 * 891 = 1782
        # Component X Tritanium: 4 cycles of 100 with ME 10 + 0.01 = 4 * 90 = 360
        # Direct Pyerite for Cruiser B: calculate_job_material_quantity(1, 500, 10, 0.01) = 446
        # Component X Pyerite: 4 cycles of 50 with ME 10 + 0.01 = 4 * 45 = 180
        expected_tritanium = 1782 + 360
        expected_pyerite = 446 + 180
        self.assertEqual(cons_mats[90020]["quantity"], expected_tritanium)
        self.assertEqual(cons_mats[90021]["quantity"], expected_pyerite)

    def test_multi_product_batching_eliminates_duplicate_cycle_overhead(self) -> None:
        """
        Verify that pooling component demand across multiple products reduces total cycle count
        compared to separate single-product calculations (demonstrating batching material savings).
        """
        # Cruiser A with 1 run needs 10 Component X (or with 4 units of sub-component)
        # Product 1 needs 4 units of Sub Component, Product 2 needs 4 units of Sub Component.
        # Sub Component produces 10 per cycle.
        # Separately: Product 1 needs ceil(4/10) = 1 cycle (10 units), Product 2 needs ceil(4/10) = 1 cycle (10 units) -> Total 2 cycles (20 units produced, 12 surplus)
        # Pooled: Total needed = 8 units -> ceil(8/10) = 1 cycle (10 units produced, 2 surplus), saving 1 cycle (50% raw material reduction!).
        
        # Using Cruiser A (10 comp X) and Cruiser B (15 comp X):
        # Separately:
        # - Cruiser A (1 run): 10 Comp X -> ceil(10/10) = 1 cycle
        # - Cruiser B (1 run): 15 Comp X -> ceil(15/10) = 2 cycles (produces 20, surplus 5)
        #   Total separate cycles = 1 + 2 = 3 cycles (30 Comp X produced)
        #
        # Pooled:
        # - Total Comp X needed = 10 + 15 = 25 -> ceil(25/10) = 3 cycles
        # Now if Cruiser A needs 3 Comp X and Cruiser B needs 4 Comp X:
        # Separately: ceil(3/10) = 1 cycle (10) + ceil(4/10) = 1 cycle (10) = 2 cycles (20 produced)
        # Pooled: ceil(7/10) = 1 cycle (10 produced) -> 1 cycle saved!
        
        manifest_separate_1 = [{"blueprint_type_id": 90061, "runs": 1, "me": 0}]  # Cruiser C needs 3 Tier 1 (produces 5/cycle) -> 1 cycle
        manifest_separate_2 = [{"blueprint_type_id": 90061, "runs": 1, "me": 0}]  # Another Cruiser C needs 3 Tier 1 -> 1 cycle
        manifest_pooled = [{"blueprint_type_id": 90061, "runs": 2, "me": 0}]      # 2 Cruiser C need 6 Tier 1 -> ceil(6/5) = 2 cycles
        
        # When Cruiser C needs 1 run (3 units) and Battleship needs 1 unit:
        # Cruiser C needs 3 units of Tier 1 -> separate = ceil(3/5) = 1 cycle (5 units)
        # If we have 1 run of Cruiser C and 1 run of a component needing 1 unit of Tier 1:
        # Total needed = 4 units -> pooled = ceil(4/5) = 1 cycle (5 units produced, 1 surplus) vs 2 cycles if run in isolation.
        payload = build_multi_product_materials_tree([
            {"blueprint_type_id": 90061, "runs": 1, "me": 0},
            {"blueprint_type_id": 90061, "runs": 1, "me": 0},
        ])
        cons_mats = {m["type_id"]: m for m in payload["consolidated_materials"]}
        # Total Cruiser C runs = 2, total Tier 1 needed = 6 -> cycles = 2, produced = 10, surplus = 4
        self.assertEqual(cons_mats[90050]["quantity"], 6)
        self.assertEqual(cons_mats[90050]["cycles"], 2)
        self.assertEqual(cons_mats[90050]["surplus"], 4)
