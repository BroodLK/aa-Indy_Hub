"""Tests for reprocessing services models and helpers."""

# Standard Library
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

# Django
from django.contrib.auth.models import User
from django.test import TestCase

# Local
from indy_hub.models import (
    ReprocessingServiceProfile,
    ReprocessingServiceRequest,
    generate_reprocessing_reference,
)
from indy_hub.services.reprocessing import (
    build_reprocessing_estimate,
    compute_estimated_yield_percent,
    contract_items_match_exact,
    contract_items_match_with_tolerance,
    resolve_processing_skill_level_for_item,
)


class ReprocessingReferenceTests(TestCase):
    def test_generate_reprocessing_reference_format(self):
        ref = generate_reprocessing_reference()
        self.assertTrue(ref.startswith("REPROC-"))
        suffix = ref.split("-", 1)[1]
        self.assertEqual(len(suffix), 10)
        self.assertTrue(suffix.isdigit())


class ReprocessingProfileModelTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user("reproc_user", password="secret")

    def test_pending_profile_forces_unavailable(self):
        profile = ReprocessingServiceProfile.objects.create(
            user=self.user,
            character_id=9000001,
            character_name="Refiner One",
            is_available=True,
            approval_status=ReprocessingServiceProfile.ApprovalStatus.PENDING,
        )
        profile.refresh_from_db()
        self.assertFalse(profile.is_available)

    def test_approved_profile_can_stay_available(self):
        profile = ReprocessingServiceProfile.objects.create(
            user=self.user,
            character_id=9000002,
            character_name="Refiner Two",
            is_available=True,
            approval_status=ReprocessingServiceProfile.ApprovalStatus.APPROVED,
        )
        profile.refresh_from_db()
        self.assertTrue(profile.is_available)

    def test_request_reference_auto_generated(self):
        profile = ReprocessingServiceProfile.objects.create(
            user=self.user,
            character_id=9000003,
            character_name="Refiner Three",
            approval_status=ReprocessingServiceProfile.ApprovalStatus.APPROVED,
        )
        req = ReprocessingServiceRequest.objects.create(
            requester=self.user,
            processor_profile=profile,
            processor_user=self.user,
            processor_character_id=profile.character_id,
            processor_character_name=profile.character_name,
        )
        self.assertTrue(req.request_reference.startswith("REPROC-"))


class ReprocessingContractMatcherTests(TestCase):
    def test_exact_match_requires_same_types_and_quantities(self):
        items = [
            SimpleNamespace(type_id=34, quantity=1000, is_included=True),
            SimpleNamespace(type_id=35, quantity=500, is_included=True),
        ]
        self.assertTrue(
            contract_items_match_exact(
                contract_items=items,
                expected_by_type={34: 1000, 35: 500},
            )
        )
        self.assertFalse(
            contract_items_match_exact(
                contract_items=items,
                expected_by_type={34: 999, 35: 500},
            )
        )

    def test_tolerance_rejects_substitutions(self):
        items = [
            SimpleNamespace(type_id=34, quantity=1000, is_included=True),
            SimpleNamespace(type_id=36, quantity=500, is_included=True),
        ]
        matches, errors = contract_items_match_with_tolerance(
            contract_items=items,
            expected_by_type={34: 1000, 35: 500},
            tolerance_percent=Decimal("1.00"),
        )
        self.assertFalse(matches)
        self.assertTrue(any("Unexpected types" in error for error in errors))

    def test_tolerance_allows_small_delta(self):
        items = [SimpleNamespace(type_id=34, quantity=1008, is_included=True)]
        matches, errors = contract_items_match_with_tolerance(
            contract_items=items,
            expected_by_type={34: 1000},
            tolerance_percent=Decimal("1.00"),
        )
        self.assertTrue(matches)
        self.assertEqual(errors, [])


class ReprocessingEstimateTests(TestCase):
    @patch("indy_hub.services.reprocessing.fetch_fuzzwork_prices")
    @patch("indy_hub.services.reprocessing.get_reprocessing_outputs_for_type")
    def test_estimate_value_and_reward(
        self,
        mock_outputs,
        mock_prices,
    ):
        # 1 unit input -> 2 tritanium output; 10 units input, 50% yield => 10 output units.
        mock_outputs.return_value = {34: 2}
        mock_prices.return_value = {34: {"sell": Decimal("5.00")}}

        estimate = build_reprocessing_estimate(
            input_items=[{"type_id": 1234, "quantity": 10}],
            yield_percent=Decimal("50.0"),
            margin_percent=Decimal("10.0"),
        )

        self.assertEqual(len(estimate["outputs"]), 1)
        output_row = estimate["outputs"][0]
        self.assertEqual(output_row["type_id"], 34)
        self.assertEqual(output_row["expected_quantity"], 10)
        self.assertEqual(estimate["total_output_value"], Decimal("50.00"))
        self.assertEqual(estimate["reward_isk"], Decimal("5.00"))

    @patch("indy_hub.services.reprocessing.fetch_fuzzwork_prices")
    @patch("indy_hub.services.reprocessing.get_reprocessing_portion_size")
    @patch("indy_hub.services.reprocessing.get_reprocessing_outputs_for_type")
    def test_estimate_respects_portion_size(
        self,
        mock_outputs,
        mock_portion,
        mock_prices,
    ):
        # 150 input, portion size 100 => 1 processable batch.
        mock_outputs.return_value = {34: 200}
        mock_portion.return_value = 100
        mock_prices.return_value = {34: {"sell": Decimal("1.00")}}

        estimate = build_reprocessing_estimate(
            input_items=[{"type_id": 1234, "quantity": 150}],
            yield_percent=Decimal("50.0"),
            margin_percent=Decimal("10.0"),
        )

        self.assertEqual(estimate["outputs"][0]["expected_quantity"], 100)

    @patch("indy_hub.services.reprocessing.fetch_fuzzwork_prices")
    @patch("indy_hub.services.reprocessing.get_reprocessing_portion_size")
    @patch("indy_hub.services.reprocessing.get_reprocessing_outputs_for_type")
    def test_estimate_respects_per_type_yield_override(
        self,
        mock_outputs,
        mock_portion,
        mock_prices,
    ):
        mock_outputs.return_value = {34: 200}
        mock_portion.return_value = 1
        mock_prices.return_value = {34: {"sell": Decimal("1.00")}}

        estimate = build_reprocessing_estimate(
            input_items=[{"type_id": 1234, "quantity": 1}],
            yield_percent=Decimal("90.0"),
            margin_percent=Decimal("10.0"),
            yield_percent_by_type={1234: Decimal("50.0")},
        )

        self.assertEqual(estimate["outputs"][0]["expected_quantity"], 100)


class ReprocessingYieldFormulaTests(TestCase):
    def test_station_like_formula_matches_reference(self):
        # 50 * (1 + 5*0.03) * (1 + 5*0.02) * (1 + 5*0.02)
        value = compute_estimated_yield_percent(
            skill_snapshot={
                "reprocessing": 5,
                "reprocessing_efficiency": 5,
                "processing": 5,
            },
            implant_bonus_percent=Decimal("0"),
            structure_bonus_percent=Decimal("0"),
            rig_bonus_percent=Decimal("0"),
            security_bonus_percent=Decimal("0"),
        )
        self.assertEqual(value, Decimal("69.575"))

    def test_upwell_tatara_t2_rig_nullsec_rx804_example(self):
        # (50 + 3) * (1 + 0.12) * (1 + 0.055) * (1 + 5*0.03) * (1 + 5*0.02) * (1 + 5*0.02) * (1 + 0.04)
        value = compute_estimated_yield_percent(
            skill_snapshot={
                "reprocessing": 5,
                "reprocessing_efficiency": 5,
                "processing": 5,
            },
            implant_bonus_percent=Decimal("4"),
            structure_bonus_percent=Decimal("0.055"),
            rig_bonus_percent=Decimal("3"),
            security_bonus_percent=Decimal("0.12"),
        )
        self.assertEqual(value, Decimal("90.628"))


class ReprocessingSkillResolutionTests(TestCase):
    @patch("indy_hub.services.reprocessing.resolve_processing_skill_type_id_for_item")
    def test_uses_specific_processing_skill_when_available(self, mock_skill_type):
        mock_skill_type.return_value = 12245
        level = resolve_processing_skill_level_for_item(
            type_id=1234,
            skill_levels_by_id={12245: 4},
            fallback_level=5,
        )
        self.assertEqual(level, 4)

    @patch("indy_hub.services.reprocessing.resolve_processing_skill_type_id_for_item")
    def test_falls_back_to_profile_processing_skill(self, mock_skill_type):
        mock_skill_type.return_value = None
        level = resolve_processing_skill_level_for_item(
            type_id=1234,
            skill_levels_by_id={},
            fallback_level=5,
        )
        self.assertEqual(level, 5)
