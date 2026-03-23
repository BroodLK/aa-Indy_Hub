"""Tests for reprocessing services models and helpers."""

# Standard Library
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

# Django
from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

# Local
from indy_hub.models import (
    ESIContract,
    ESIContractItem,
    ReprocessingServiceProfile,
    ReprocessingServiceRequest,
    ReprocessingServiceRequestItem,
    ReprocessingServiceRequestOutput,
    generate_reprocessing_reference,
)
from indy_hub.tasks.material_exchange_contracts import auto_progress_reprocessing_requests
from indy_hub.services.reprocessing import (
    build_reprocessing_estimate,
    compute_estimated_yield_percent,
    contract_items_match_exact,
    contract_items_match_with_tolerance,
    fetch_character_clone_options,
    fetch_character_skill_levels,
    resolve_processing_skill_level_for_item,
)
from indy_hub.views.reprocessing_services import _parse_request_item_lines


class ReprocessingReferenceTests(TestCase):
    def test_generate_reprocessing_reference_format(self):
        ref = generate_reprocessing_reference()
        self.assertTrue(ref.startswith("REPROCESSING-"))
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

    def test_admin_force_unavailable_overrides_availability(self):
        profile = ReprocessingServiceProfile.objects.create(
            user=self.user,
            character_id=9000005,
            character_name="Refiner Five",
            is_available=True,
            admin_force_unavailable=True,
            approval_status=ReprocessingServiceProfile.ApprovalStatus.APPROVED,
        )
        profile.refresh_from_db()
        self.assertFalse(profile.is_available)

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
        self.assertTrue(req.request_reference.startswith("REPROCESSING-"))

    @patch(
        "indy_hub.models.generate_reprocessing_reference",
        return_value="REPROCESSING-1111111111",
    )
    def test_request_reference_collision_fallback_keeps_reprocessing_prefix(self, _mock_ref):
        profile = ReprocessingServiceProfile.objects.create(
            user=self.user,
            character_id=9000004,
            character_name="Refiner Four",
            approval_status=ReprocessingServiceProfile.ApprovalStatus.APPROVED,
        )
        ReprocessingServiceRequest.objects.create(
            requester=self.user,
            processor_profile=profile,
            processor_user=self.user,
            processor_character_id=profile.character_id,
            processor_character_name=profile.character_name,
            request_reference="REPROCESSING-1111111111",
        )
        req = ReprocessingServiceRequest.objects.create(
            requester=self.user,
            processor_profile=profile,
            processor_user=self.user,
            processor_character_id=profile.character_id,
            processor_character_name=profile.character_name,
        )
        self.assertEqual(req.request_reference, f"REPROCESSING-{req.id:010d}")


class ReprocessingAdminAvailabilityToggleTests(TestCase):
    def setUp(self):
        self.admin = User.objects.create_user("reproc_admin", password="secret")
        self.processor = User.objects.create_user("reproc_processor", password="secret")
        permission = Permission.objects.get(
            content_type__app_label="indy_hub",
            codename="can_manage_material_hub",
        )
        self.admin.user_permissions.add(permission)

    @patch("indy_hub.views.reprocessing_services.notify_user")
    def test_admin_disable_sets_lock_and_unavailable(self, _mock_notify):
        profile = ReprocessingServiceProfile.objects.create(
            user=self.processor,
            character_id=92000001,
            character_name="Toggle Pilot",
            approval_status=ReprocessingServiceProfile.ApprovalStatus.APPROVED,
            is_available=True,
            admin_force_unavailable=False,
        )

        self.client.force_login(self.admin)
        response = self.client.post(
            reverse("indy_hub:reprocessing_admin_review", args=[profile.id]),
            {"action": "admin_disable"},
        )

        self.assertEqual(response.status_code, 302)
        profile.refresh_from_db()
        self.assertTrue(profile.admin_force_unavailable)
        self.assertFalse(profile.is_available)
        self.assertEqual(profile.reviewed_by, self.admin)
        self.assertIsNotNone(profile.reviewed_at)

    @patch("indy_hub.views.reprocessing_services.notify_user")
    def test_admin_enable_clears_lock_and_available(self, _mock_notify):
        profile = ReprocessingServiceProfile.objects.create(
            user=self.processor,
            character_id=92000002,
            character_name="Toggle Pilot Two",
            approval_status=ReprocessingServiceProfile.ApprovalStatus.APPROVED,
            is_available=False,
            admin_force_unavailable=True,
        )

        self.client.force_login(self.admin)
        response = self.client.post(
            reverse("indy_hub:reprocessing_admin_review", args=[profile.id]),
            {"action": "admin_enable"},
        )

        self.assertEqual(response.status_code, 302)
        profile.refresh_from_db()
        self.assertFalse(profile.admin_force_unavailable)
        self.assertTrue(profile.is_available)
        self.assertEqual(profile.reviewed_by, self.admin)
        self.assertIsNotNone(profile.reviewed_at)

    @patch("indy_hub.views.reprocessing_services.notify_user")
    def test_admin_toggle_rejected_for_non_approved_profiles(self, _mock_notify):
        profile = ReprocessingServiceProfile.objects.create(
            user=self.processor,
            character_id=92000003,
            character_name="Pending Pilot",
            approval_status=ReprocessingServiceProfile.ApprovalStatus.PENDING,
            is_available=False,
            admin_force_unavailable=False,
        )

        self.client.force_login(self.admin)
        response = self.client.post(
            reverse("indy_hub:reprocessing_admin_review", args=[profile.id]),
            {"action": "admin_disable"},
        )

        self.assertEqual(response.status_code, 302)
        profile.refresh_from_db()
        self.assertFalse(profile.admin_force_unavailable)
        self.assertFalse(profile.is_available)


class ReprocessingMyRequestsViewTests(TestCase):
    def setUp(self):
        self.viewer = User.objects.create_user("reproc_viewer", password="secret")
        self.other_user = User.objects.create_user("reproc_other", password="secret")
        permission = Permission.objects.get(
            content_type__app_label="indy_hub",
            codename="can_access_indy_hub",
        )
        self.viewer.user_permissions.add(permission)

        self.other_profile = ReprocessingServiceProfile.objects.create(
            user=self.other_user,
            character_id=93000001,
            character_name="Other Processor",
            approval_status=ReprocessingServiceProfile.ApprovalStatus.APPROVED,
            is_available=True,
        )
        self.viewer_profile = ReprocessingServiceProfile.objects.create(
            user=self.viewer,
            character_id=93000002,
            character_name="Viewer Processor",
            approval_status=ReprocessingServiceProfile.ApprovalStatus.APPROVED,
            is_available=True,
        )

    def test_my_requests_shows_client_and_processor_contracts(self):
        requester_side = ReprocessingServiceRequest.objects.create(
            request_reference="REPROCESSING-0000000101",
            requester=self.viewer,
            requester_character_id=94000001,
            requester_character_name="Viewer Client",
            processor_profile=self.other_profile,
            processor_user=self.other_user,
            processor_character_id=self.other_profile.character_id,
            processor_character_name=self.other_profile.character_name,
        )
        processor_side = ReprocessingServiceRequest.objects.create(
            request_reference="REPROCESSING-0000000102",
            requester=self.other_user,
            requester_character_id=94000002,
            requester_character_name="Other Client",
            processor_profile=self.viewer_profile,
            processor_user=self.viewer,
            processor_character_id=self.viewer_profile.character_id,
            processor_character_name=self.viewer_profile.character_name,
        )

        self.client.force_login(self.viewer)
        response = self.client.get(reverse("indy_hub:reprocessing_my_requests"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["requester_rows"]), 1)
        self.assertEqual(len(response.context["processor_rows"]), 1)
        self.assertContains(response, requester_side.request_reference)
        self.assertContains(response, processor_side.request_reference)
        self.assertContains(
            response,
            reverse("indy_hub:reprocessing_request_detail", args=[requester_side.id]),
        )
        self.assertContains(
            response,
            reverse("indy_hub:reprocessing_request_detail", args=[processor_side.id]),
        )


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


class ReprocessingCorptoolsFallbackTests(TestCase):
    @patch("indy_hub.services.reprocessing.Token.get_token", side_effect=Exception("no token"))
    @patch("indy_hub.services.reprocessing._get_operation", return_value=None)
    @patch(
        "indy_hub.services.reprocessing._fetch_corptools_skill_levels",
        return_value={3385: {"active": 5, "trained": 5}},
    )
    def test_skill_lookup_uses_corptools_cache_when_esi_unavailable(
        self,
        _mock_corptools,
        _mock_op,
        _mock_token,
    ):
        levels = fetch_character_skill_levels(9000001)
        self.assertEqual(levels.get(3385, {}).get("active"), 5)

    @patch("indy_hub.services.reprocessing.Token.get_token", side_effect=Exception("no token"))
    @patch("indy_hub.services.reprocessing._get_operation", return_value=None)
    @patch(
        "indy_hub.services.reprocessing._fetch_corptools_clone_options",
        return_value=[
            {
                "clone_id": 11,
                "clone_label": "Clone A",
                "location_id": 60000001,
                "location_name": "Jita IV",
                "implant_type_ids": [27118],
                "implant_names": ["Eifyr and Co. 'Beancounter' Reprocessing RX-804"],
                "beancounter_implants": ["Eifyr and Co. 'Beancounter' Reprocessing RX-804"],
                "beancounter_bonus_percent": Decimal("4.000"),
            }
        ],
    )
    def test_clone_lookup_uses_corptools_cache_when_esi_unavailable(
        self,
        _mock_corptools,
        _mock_op,
        _mock_token,
    ):
        clones = fetch_character_clone_options(9000001)
        self.assertEqual(len(clones), 1)
        self.assertEqual(clones[0]["clone_id"], 11)


class ReprocessingRequestLineParsingTests(TestCase):
    @patch("indy_hub.views.reprocessing_services.get_type_name")
    @patch("indy_hub.views.reprocessing_services._resolve_type_id_from_text")
    def test_parses_ingame_tab_delimited_lines(
        self,
        mock_resolve,
        mock_get_type_name,
    ):
        mock_resolve.side_effect = lambda text: {"Tritanium": 34, "Pyerite": 35}.get(str(text).strip())
        mock_get_type_name.side_effect = lambda type_id: {34: "Tritanium", 35: "Pyerite"}.get(
            int(type_id),
            f"Type {type_id}",
        )

        rows, errors = _parse_request_item_lines("Tritanium\t1,250\nPyerite\t2\u202F000")

        self.assertEqual(errors, [])
        self.assertEqual(rows, [{"type_id": 35, "quantity": 2000}, {"type_id": 34, "quantity": 1250}])

    @patch("indy_hub.views.reprocessing_services.get_type_name")
    @patch("indy_hub.views.reprocessing_services._resolve_type_id_from_text")
    def test_parses_tab_delimited_line_with_extra_columns(
        self,
        mock_resolve,
        mock_get_type_name,
    ):
        mock_resolve.side_effect = lambda text: {"Tritanium": 34}.get(str(text).strip())
        mock_get_type_name.side_effect = lambda type_id: {34: "Tritanium"}.get(int(type_id), f"Type {type_id}")

        rows, errors = _parse_request_item_lines("Tritanium\t1,000\t4.50 m3")

        self.assertEqual(errors, [])
        self.assertEqual(rows, [{"type_id": 34, "quantity": 1000}])


class ReprocessingAutomationTaskTests(TestCase):
    def setUp(self):
        self.requester = User.objects.create_user("req_user", password="secret")
        self.processor = User.objects.create_user("proc_user", password="secret")
        self.profile = ReprocessingServiceProfile.objects.create(
            user=self.processor,
            character_id=91000001,
            character_name="Processor Main",
            approval_status=ReprocessingServiceProfile.ApprovalStatus.APPROVED,
            is_available=True,
        )
        self.request = ReprocessingServiceRequest.objects.create(
            requester=self.requester,
            requester_character_id=90000001,
            requester_character_name="Requester Main",
            processor_profile=self.profile,
            processor_user=self.processor,
            processor_character_id=self.profile.character_id,
            processor_character_name=self.profile.character_name,
            status=ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT,
            reward_isk=Decimal("123456.78"),
            tolerance_percent=Decimal("1.00"),
        )
        ReprocessingServiceRequestItem.objects.create(
            request=self.request,
            type_id=12345,
            type_name="Compressed Arkonor",
            quantity=100,
        )
        ReprocessingServiceRequestOutput.objects.create(
            request=self.request,
            type_id=34,
            type_name="Tritanium",
            expected_quantity=1000,
        )

    def _create_contract(
        self,
        *,
        contract_id: int,
        issuer_id: int,
        assignee_id: int,
        title: str,
        status: str = "outstanding",
        price: Decimal | str = "0",
        reward: Decimal | str = "0",
    ) -> ESIContract:
        now = timezone.now()
        return ESIContract.objects.create(
            contract_id=int(contract_id),
            issuer_id=int(issuer_id),
            issuer_corporation_id=98000001,
            assignee_id=int(assignee_id),
            acceptor_id=0,
            contract_type="item_exchange",
            status=str(status),
            title=str(title),
            start_location_id=60003760,
            end_location_id=60003760,
            price=Decimal(str(price)),
            reward=Decimal(str(reward)),
            collateral=Decimal("0"),
            date_issued=now - timedelta(minutes=5),
            date_expired=now + timedelta(days=7),
            corporation_id=98000001,
        )

    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    def test_auto_progresses_inbound_and_return_contracts(self, mock_notify_user):
        inbound = self._create_contract(
            contract_id=700000001,
            issuer_id=90000001,
            assignee_id=91000001,
            title=f"Inbound {self.request.request_reference}",
            status="outstanding",
            price="0",
            reward="0",
        )
        ESIContractItem.objects.create(
            contract=inbound,
            record_id=1,
            type_id=12345,
            quantity=100,
            is_included=True,
            is_singleton=False,
        )

        auto_progress_reprocessing_requests()
        self.request.refresh_from_db()
        self.assertEqual(self.request.inbound_contract_id, inbound.contract_id)
        self.assertEqual(
            self.request.status,
            ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT,
        )
        self.assertTrue(
            any(
                call.args[1] == "Inbound reprocessing contract sent"
                for call in mock_notify_user.call_args_list
            )
        )

        inbound.status = "in_progress"
        inbound.acceptor_id = 91000001
        inbound.date_accepted = timezone.now()
        inbound.save(update_fields=["status", "acceptor_id", "date_accepted", "last_synced"])

        auto_progress_reprocessing_requests()
        self.request.refresh_from_db()
        self.assertEqual(self.request.status, ReprocessingServiceRequest.Status.PROCESSING)
        self.assertIsNotNone(self.request.inbound_contract_verified_at)
        self.assertTrue(
            any(
                call.args[1] == "Inbound reprocessing contract accepted"
                for call in mock_notify_user.call_args_list
            )
        )

        return_contract = self._create_contract(
            contract_id=700000002,
            issuer_id=91000001,
            assignee_id=90000001,
            title=f"Return {self.request.request_reference}",
            status="outstanding",
            price=self.request.reward_isk,
            reward="0",
        )
        ESIContractItem.objects.create(
            contract=return_contract,
            record_id=2,
            type_id=34,
            quantity=1000,
            is_included=True,
            is_singleton=False,
        )

        auto_progress_reprocessing_requests()
        self.request.refresh_from_db()
        self.assertEqual(self.request.return_contract_id, return_contract.contract_id)
        self.assertEqual(self.request.status, ReprocessingServiceRequest.Status.COMPLETED)
        self.assertIsNotNone(self.request.completed_at)
        self.assertTrue(
            any(
                call.args[1] == "Return reprocessing contract sent"
                for call in mock_notify_user.call_args_list
            )
        )

    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    def test_auto_marks_disputed_on_inbound_item_mismatch(self, mock_notify_user):
        bad_inbound = self._create_contract(
            contract_id=700000003,
            issuer_id=90000001,
            assignee_id=91000001,
            title=f"Inbound {self.request.request_reference}",
            status="outstanding",
            price="0",
            reward="0",
        )
        ESIContractItem.objects.create(
            contract=bad_inbound,
            record_id=3,
            type_id=12345,
            quantity=99,
            is_included=True,
            is_singleton=False,
        )

        auto_progress_reprocessing_requests()
        self.request.refresh_from_db()
        self.assertEqual(self.request.status, ReprocessingServiceRequest.Status.DISPUTED)
        self.assertIn("Inbound contract items", self.request.dispute_reason)
        self.assertGreaterEqual(mock_notify_user.call_count, 2)
        titles = [call.args[1] for call in mock_notify_user.call_args_list]
        self.assertTrue(all(title == "Reprocessing contract anomaly" for title in titles))
