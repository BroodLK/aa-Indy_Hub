"""Tests for craft planner material quantity math."""

# Django
from django.test import SimpleTestCase

# AA Example App
from indy_hub.services.craft_materials import calculate_job_material_quantity


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
