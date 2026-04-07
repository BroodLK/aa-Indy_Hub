"""
Tests for Material Exchange contract validation system
"""

# Standard Library
from datetime import timedelta
from unittest.mock import patch

# Django
from django.contrib.auth.models import User
from django.db import transaction
from django.test import TestCase
from django.utils import timezone

# AA Example App
# Local
from indy_hub.models import (
    CachedCorporationAsset,
    MaterialExchangeBuyOrder,
    MaterialExchangeBuyOrderItem,
    MaterialExchangeConfig,
    MaterialExchangeSellOrder,
    MaterialExchangeSellOrderItem,
)
from indy_hub.tasks.material_exchange_contracts import (
    _build_contract_state_webhook_line,
    _extract_contract_id,
    _get_effective_contract_location_id,
    _log_buy_order_transactions,
    _log_sell_order_transactions,
    _validate_buy_order_from_db,
    _validate_sell_order_from_db,
    check_completed_material_exchange_contracts,
    handle_material_exchange_buy_order_created,
    handle_material_exchange_sell_order_created,
    validate_material_exchange_buy_orders,
    validate_material_exchange_sell_orders,
)

# Note: Legacy test functions _contract_items_match_order and _matches_sell_order_criteria
# have been replaced with _db variants that work with database models instead of dicts


class ContractValidationTestCase(TestCase):
    """Tests for contract matching and validation logic"""

    def setUp(self):
        """Set up test data"""
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=60003760,
            structure_name="Test Structure",
            is_active=True,
        )
        self.seller = User.objects.create_user(username="test_seller")
        self.buyer = User.objects.create_user(username="test_buyer")

        # Create a sell order with an item
        self.sell_order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.seller,
            status=MaterialExchangeSellOrder.Status.DRAFT,
        )
        self.sell_item = MaterialExchangeSellOrderItem.objects.create(
            order=self.sell_order,
            type_id=34,  # Tritanium
            type_name="Tritanium",
            quantity=1000,
            unit_price=5.5,
            total_price=5500,
        )

        # Create a buy order with an item
        self.buy_order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.buyer,
            status=MaterialExchangeBuyOrder.Status.DRAFT,
        )
        self.buy_item = MaterialExchangeBuyOrderItem.objects.create(
            order=self.buy_order,
            type_id=34,  # Tritanium
            type_name="Tritanium",
            quantity=500,
            unit_price=6.0,
            total_price=3000,
            stock_available_at_creation=1000,
        )

    def test_extract_contract_id(self):
        """Test contract ID extraction from notes"""
        # Valid format
        notes = "Contract validated: 123456789"
        self.assertEqual(_extract_contract_id(notes), 123456789)

        # Different prefix
        notes2 = "Some message: 987654321"
        self.assertEqual(_extract_contract_id(notes2), 987654321)

        # No contract ID
        self.assertIsNone(_extract_contract_id("No contract here"))
        self.assertIsNone(_extract_contract_id(""))
        self.assertIsNone(_extract_contract_id(None))

    def test_sell_order_status_transitions(self):
        """Test sell order status field values"""
        self.assertEqual(
            self.sell_order.status,
            MaterialExchangeSellOrder.Status.DRAFT,
        )

        # Check all status choices exist
        status_values = [s[0] for s in MaterialExchangeSellOrder.Status.choices]
        self.assertIn(MaterialExchangeSellOrder.Status.DRAFT, status_values)
        self.assertIn(MaterialExchangeSellOrder.Status.ANOMALY, status_values)
        self.assertIn(MaterialExchangeSellOrder.Status.ANOMALY_REJECTED, status_values)
        self.assertIn(MaterialExchangeSellOrder.Status.VALIDATED, status_values)
        self.assertIn(MaterialExchangeSellOrder.Status.COMPLETED, status_values)
        self.assertIn(MaterialExchangeSellOrder.Status.REJECTED, status_values)

    def test_buy_order_status_transitions(self):
        """Test buy order status field values"""
        self.assertEqual(
            self.buy_order.status,
            MaterialExchangeBuyOrder.Status.DRAFT,
        )

        # Check all status choices exist
        status_values = [s[0] for s in MaterialExchangeBuyOrder.Status.choices]
        self.assertIn(MaterialExchangeBuyOrder.Status.DRAFT, status_values)
        self.assertIn(MaterialExchangeBuyOrder.Status.VALIDATED, status_values)
        self.assertIn(MaterialExchangeBuyOrder.Status.COMPLETED, status_values)
        self.assertIn(MaterialExchangeBuyOrder.Status.REJECTED, status_values)

    def test_log_buy_order_transactions_consumes_cached_buy_scope_assets(self):
        in_scope_asset = CachedCorporationAsset.objects.create(
            corporation_id=self.config.corporation_id,
            item_id=910001,
            location_id=self.config.structure_id,
            location_flag="CorpSAG1",
            type_id=self.buy_item.type_id,
            quantity=600,
            is_singleton=False,
            is_blueprint=False,
        )
        out_of_scope_asset = CachedCorporationAsset.objects.create(
            corporation_id=self.config.corporation_id,
            item_id=910002,
            location_id=70009999,
            location_flag="CorpSAG1",
            type_id=self.buy_item.type_id,
            quantity=600,
            is_singleton=False,
            is_blueprint=False,
        )

        _log_buy_order_transactions(self.buy_order)

        in_scope_asset.refresh_from_db()
        out_of_scope_asset.refresh_from_db()
        self.assertEqual(in_scope_asset.quantity, 100)
        self.assertEqual(out_of_scope_asset.quantity, 600)

    def test_log_sell_order_transactions_adds_cached_sell_scope_assets(self):
        self.sell_order.source_location_id = int(self.config.structure_id)
        self.sell_order.source_location_name = "Test Structure"
        self.sell_order.save(update_fields=["source_location_id", "source_location_name"])

        existing = CachedCorporationAsset.objects.create(
            corporation_id=self.config.corporation_id,
            item_id=None,
            location_id=int(self.config.structure_id),
            location_flag="CorpSAG1",
            type_id=self.sell_item.type_id,
            quantity=25,
            is_singleton=False,
            is_blueprint=False,
        )

        _log_sell_order_transactions(self.sell_order)

        existing.refresh_from_db()
        self.assertEqual(existing.quantity, 1025)


class ContractValidationTaskTest(TestCase):
    """Tests for Celery task execution"""

    def setUp(self):
        """Set up test data"""
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=60003760,
            structure_name="Test Structure",
            is_active=True,
        )
        self.seller = User.objects.create_user(username="test_seller")
        self.sell_order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.seller,
            status=MaterialExchangeSellOrder.Status.DRAFT,
        )
        self.sell_item = MaterialExchangeSellOrderItem.objects.create(
            order=self.sell_order,
            type_id=34,
            type_name="Tritanium",
            quantity=1000,
            unit_price=5.5,
            total_price=5500,
        )

    @patch("indy_hub.tasks.material_exchange_contracts.shared_client")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_validate_sell_orders_no_pending(
        self, mock_notify_multi, mock_notify_user, mock_client
    ):
        """Test task when no pending orders exist"""
        self.sell_order.status = MaterialExchangeSellOrder.Status.VALIDATED
        self.sell_order.save()

        validate_material_exchange_sell_orders()

        # Should not call ESI
        mock_client.fetch_corporation_contracts.assert_not_called()
        mock_notify_user.assert_not_called()
        mock_notify_multi.assert_not_called()

    @patch("indy_hub.tasks.material_exchange_contracts._get_character_for_scope")
    @patch("indy_hub.tasks.material_exchange_contracts.shared_client")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_validate_sell_orders_contract_found(
        self, mock_notify_multi, mock_client, mock_get_char
    ):
        """Test successful contract validation"""
        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        seller_char_id = 111111111
        mock_get_char.return_value = seller_char_id

        # Create cached contract in database (instead of mocking ESI)
        contract = ESIContract.objects.create(
            contract_id=1,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            acceptor_id=0,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            price=self.sell_item.total_price,
            title=self.sell_order.order_reference,
            date_issued="2024-01-01T00:00:00Z",
            date_expired="2024-12-31T23:59:59Z",
        )

        # Create contract item
        ESIContractItem.objects.create(
            contract=contract,
            record_id=1,
            type_id=34,
            quantity=1000,
            is_included=True,
        )

        # Mock getting user's characters
        with patch(
            "indy_hub.tasks.material_exchange_contracts._get_user_character_ids",
            return_value=[seller_char_id],
        ):
            validate_material_exchange_sell_orders()

        # Check order was approved
        self.sell_order.refresh_from_db()
        self.assertEqual(
            self.sell_order.status,
            MaterialExchangeSellOrder.Status.VALIDATED,
        )
        self.assertIn("Contract validated", self.sell_order.notes)

        # Check admins were notified
        mock_notify_multi.assert_called()

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_validate_sell_orders_contract_found_with_split_item_stack(
        self, mock_notify_multi, mock_user_chars
    ):
        """Split contract stacks for the same type should still match total quantity."""
        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        seller_char_id = 111111111
        mock_user_chars.return_value = [seller_char_id]

        contract = ESIContract.objects.create(
            contract_id=101,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            acceptor_id=0,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            price=self.sell_item.total_price,
            title=self.sell_order.order_reference,
            date_issued="2024-01-01T00:00:00Z",
            date_expired="2024-12-31T23:59:59Z",
        )

        ESIContractItem.objects.create(
            contract=contract,
            record_id=1011,
            type_id=self.sell_item.type_id,
            quantity=400,
            is_included=True,
        )
        ESIContractItem.objects.create(
            contract=contract,
            record_id=1012,
            type_id=self.sell_item.type_id,
            quantity=600,
            is_included=True,
        )

        validate_material_exchange_sell_orders()

        self.sell_order.refresh_from_db()
        self.assertEqual(
            self.sell_order.status,
            MaterialExchangeSellOrder.Status.VALIDATED,
        )
        self.assertEqual(self.sell_order.esi_contract_id, contract.contract_id)
        mock_notify_multi.assert_called()

    @patch("indy_hub.tasks.material_exchange_contracts._get_character_for_scope")
    @patch("indy_hub.tasks.material_exchange_contracts.shared_client")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    def test_validate_sell_orders_no_contract(
        self, mock_notify_user, mock_client, mock_get_char
    ):
        """Test when contract is not found"""
        seller_char_id = 111111111
        mock_get_char.return_value = seller_char_id

        # No contracts in database (empty queryset simulates no cached contracts)
        # The validation function now queries ESIContract.objects instead of calling ESI

        # Mock getting user's characters
        with patch(
            "indy_hub.tasks.material_exchange_contracts._get_user_character_ids",
            return_value=[seller_char_id],
        ):
            validate_material_exchange_sell_orders()

        # Check order stays pending when no contracts in database (warning logged instead)
        self.sell_order.refresh_from_db()
        # Note: Order stays DRAFT when no cached contracts exist (validation can't run)
        self.assertEqual(
            self.sell_order.status,
            MaterialExchangeSellOrder.Status.DRAFT,
        )
        # User is not notified when no contracts are cached (just a warning log)
        mock_notify_user.assert_not_called()

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_validate_sell_orders_wrong_reference_only_sets_anomaly(
        self, mock_notify_multi, mock_notify_user, mock_user_chars
    ):
        """Strict near-match without title reference must move order to anomaly."""
        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        seller_char_id = 111111111
        mock_user_chars.return_value = [seller_char_id]

        near_match_contract = ESIContract.objects.create(
            contract_id=2001,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            price=self.sell_item.total_price,
            title="WRONG-REF-ONLY",
            date_issued="2024-01-01T00:00:00Z",
            date_expired="2024-12-31T23:59:59Z",
        )
        ESIContractItem.objects.create(
            contract=near_match_contract,
            record_id=20011,
            type_id=self.sell_item.type_id,
            quantity=self.sell_item.quantity,
            is_included=True,
        )

        validate_material_exchange_sell_orders()

        self.sell_order.refresh_from_db()
        self.assertEqual(
            self.sell_order.status,
            MaterialExchangeSellOrder.Status.ANOMALY,
        )
        self.assertIn("title reference is incorrect", self.sell_order.notes)
        self.assertIn("Expected reference", self.sell_order.notes)
        mock_notify_user.assert_called()
        mock_notify_multi.assert_called()

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_validate_sell_orders_wrong_price_has_priority_over_wrong_ref(
        self, mock_notify_multi, mock_notify_user, mock_user_chars
    ):
        """Wrong price with exact reference must win over wrong-reference near-match."""
        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        seller_char_id = 111111111
        mock_user_chars.return_value = [seller_char_id]

        wrong_price_exact_ref = ESIContract.objects.create(
            contract_id=3001,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            price=self.sell_item.total_price + 1,
            title=self.sell_order.order_reference,
            date_issued="2024-01-01T00:00:00Z",
            date_expired="2024-12-31T23:59:59Z",
        )
        ESIContractItem.objects.create(
            contract=wrong_price_exact_ref,
            record_id=30011,
            type_id=self.sell_item.type_id,
            quantity=self.sell_item.quantity,
            is_included=True,
        )

        near_match_wrong_ref = ESIContract.objects.create(
            contract_id=3002,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            price=self.sell_item.total_price,
            title="NO-ORDER-REFERENCE",
            date_issued="2024-01-01T00:00:00Z",
            date_expired="2024-12-31T23:59:59Z",
        )
        ESIContractItem.objects.create(
            contract=near_match_wrong_ref,
            record_id=30021,
            type_id=self.sell_item.type_id,
            quantity=self.sell_item.quantity,
            is_included=True,
        )

        validate_material_exchange_sell_orders()

        self.sell_order.refresh_from_db()
        self.assertEqual(
            self.sell_order.status,
            MaterialExchangeSellOrder.Status.ANOMALY,
        )
        self.assertIn("wrong price", self.sell_order.notes)
        self.assertNotIn("title reference is incorrect", self.sell_order.notes)
        mock_notify_user.assert_called()
        mock_notify_multi.assert_called()

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    def test_validate_sell_orders_no_match_keeps_order_open(
        self, mock_notify_user, mock_user_chars
    ):
        """When no contract matches sell criteria, order must stay open (not anomaly)."""
        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        seller_char_id = 111111111
        mock_user_chars.return_value = [seller_char_id]

        non_matching_contract = ESIContract.objects.create(
            contract_id=4001,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            price=self.sell_item.total_price,
            title="UNRELATED-CONTRACT",
            date_issued="2024-01-01T00:00:00Z",
            date_expired="2024-12-31T23:59:59Z",
        )
        ESIContractItem.objects.create(
            contract=non_matching_contract,
            record_id=40011,
            type_id=35,
            quantity=self.sell_item.quantity,
            is_included=True,
        )

        validate_material_exchange_sell_orders()

        self.sell_order.refresh_from_db()
        self.assertEqual(
            self.sell_order.status,
            MaterialExchangeSellOrder.Status.DRAFT,
        )
        self.assertIn("Waiting for matching contract", self.sell_order.notes)
        self.assertNotIn("title reference is incorrect", self.sell_order.notes)
        mock_notify_user.assert_not_called()

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    def test_validate_sell_orders_finished_wrong_reference_force_validates(
        self, mock_user_chars
    ):
        """Finished near-match with wrong reference should not stay in anomaly."""
        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        seller_char_id = 111111111
        mock_user_chars.return_value = [seller_char_id]

        contract = ESIContract.objects.create(
            contract_id=4101,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="finished",
            price=self.sell_item.total_price,
            title="WRONG-REF-FINISHED",
            date_issued="2024-01-01T00:00:00Z",
            date_expired="2024-12-31T23:59:59Z",
        )
        ESIContractItem.objects.create(
            contract=contract,
            record_id=41011,
            type_id=self.sell_item.type_id,
            quantity=self.sell_item.quantity,
            is_included=True,
        )

        validate_material_exchange_sell_orders()

        self.sell_order.refresh_from_db()
        self.assertEqual(
            self.sell_order.status,
            MaterialExchangeSellOrder.Status.VALIDATED,
        )
        self.assertEqual(self.sell_order.esi_contract_id, contract.contract_id)
        self.assertIn("accepted in-game despite anomaly", self.sell_order.notes)

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_validate_sell_orders_items_mismatch_notification_includes_deltas(
        self, mock_notify_multi, mock_notify_user, mock_user_chars
    ):
        """Sell mismatch notifications should include exact missing and surplus quantities."""
        # AA Example App
        from indy_hub.models import (
            ESIContract,
            ESIContractItem,
            MaterialExchangeSellOrderItem,
        )

        seller_char_id = 111111111
        mock_user_chars.return_value = [seller_char_id]

        MaterialExchangeSellOrderItem.objects.create(
            order=self.sell_order,
            type_id=37,
            type_name="Isogen",
            quantity=4,
            unit_price=7,
            total_price=28,
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=self.sell_order,
            type_id=35,
            type_name="Pyerite",
            quantity=10,
            unit_price=8,
            total_price=80,
        )

        mismatch_contract = ESIContract.objects.create(
            contract_id=4201,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            price=self.sell_order.total_price,
            title=self.sell_order.order_reference,
            date_issued="2024-01-01T00:00:00Z",
            date_expired="2024-12-31T23:59:59Z",
        )
        ESIContractItem.objects.create(
            contract=mismatch_contract,
            record_id=42011,
            type_id=34,
            quantity=1000,
            is_included=True,
        )
        ESIContractItem.objects.create(
            contract=mismatch_contract,
            record_id=42012,
            type_id=37,
            quantity=7,
            is_included=True,
        )
        ESIContractItem.objects.create(
            contract=mismatch_contract,
            record_id=42013,
            type_id=35,
            quantity=3,
            is_included=True,
        )

        validate_material_exchange_sell_orders()

        self.sell_order.refresh_from_db()
        self.assertEqual(
            self.sell_order.status, MaterialExchangeSellOrder.Status.ANOMALY
        )
        self.assertIn("Missing:", self.sell_order.notes)
        self.assertIn("- 7 Pyerite", self.sell_order.notes)
        self.assertIn("Surplus:", self.sell_order.notes)
        self.assertIn("- 3 Isogen", self.sell_order.notes)

        self.assertTrue(mock_notify_user.called)
        notify_message = mock_notify_user.call_args[0][2]
        self.assertIn("Missing:", notify_message)
        self.assertIn("- 7 Pyerite", notify_message)
        self.assertIn("Surplus:", notify_message)
        self.assertIn("- 3 Isogen", notify_message)
        mock_notify_multi.assert_called()

    @patch("indy_hub.tasks.material_exchange_contracts.get_type_name")
    def test_build_items_mismatch_details_resolves_unknown_surplus_names(
        self, mock_get_type_name
    ):
        """Surplus-only contract items should render with resolved type names."""
        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem
        from indy_hub.tasks.material_exchange_contracts import (
            _build_items_mismatch_details,
        )

        mock_get_type_name.side_effect = lambda type_id: (
            "Hyperion" if int(type_id) == 81348 else ""
        )

        mismatch_contract = ESIContract.objects.create(
            contract_id=4202,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=111111111,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            price=self.sell_order.total_price,
            title=self.sell_order.order_reference,
            date_issued="2024-01-01T00:00:00Z",
            date_expired="2024-12-31T23:59:59Z",
        )
        ESIContractItem.objects.create(
            contract=mismatch_contract,
            record_id=42021,
            type_id=81348,
            quantity=449,
            is_included=True,
        )

        details = _build_items_mismatch_details(mismatch_contract, self.sell_order)
        self.assertIn("Surplus:", details)
        self.assertIn("- 449 Hyperion", details)
        self.assertNotIn("Type 81348", details)

    @patch(
        "indy_hub.tasks.material_exchange_contracts._is_type_accepted_for_sell_location"
    )
    def test_build_sell_surplus_item_location_guidance_recommends_other_location(
        self, mock_is_type_accepted
    ):
        """Surplus guidance should suggest configured sell locations that accept the item."""
        # AA Example App
        from indy_hub.tasks.material_exchange_contracts import (
            _build_sell_surplus_item_location_guidance,
        )

        self.config.sell_structure_ids = [70000001, 70000002]
        self.config.sell_structure_names = ["Alpha Hub", "Beta Hub"]
        self.config.allowed_market_groups_sell_by_structure = {
            "70000001": [100],
            "70000002": [200],
        }
        self.config.save(
            update_fields=[
                "sell_structure_ids",
                "sell_structure_names",
                "allowed_market_groups_sell_by_structure",
            ]
        )

        def _acceptance_side_effect(*args, **kwargs):
            location_id = int(kwargs.get("location_id"))
            if location_id == 70000001:
                return False
            if location_id == 70000002:
                return True
            return False

        mock_is_type_accepted.side_effect = _acceptance_side_effect

        guidance = _build_sell_surplus_item_location_guidance(
            config=self.config,
            contract_location_id=70000001,
            surplus_type_ids=[34],
            type_names={34: "Tritanium"},
        )

        self.assertIn("Sell-location guidance:", guidance)
        self.assertIn("Tritanium", guidance)
        self.assertIn("accepted at Beta Hub", guidance)

    def test_effective_contract_location_prefers_single_expected_location_id(self):
        """Guidance should map name-matched contracts back to a single configured sell location."""
        resolved = _get_effective_contract_location_id(
            start_location_id=1045722708748,
            end_location_id=1045722708748,
            expected_location_ids=[70000001],
        )
        self.assertEqual(resolved, 70000001)

    def test_effective_contract_location_uses_first_expected_when_no_id_match(self):
        """Guidance should deterministically use the first configured expected location."""
        resolved = _get_effective_contract_location_id(
            start_location_id=1045722708748,
            end_location_id=1045722708749,
            expected_location_ids=[70000002, 70000001],
        )
        self.assertEqual(resolved, 70000002)

    @patch(
        "indy_hub.tasks.material_exchange_contracts._is_type_accepted_for_sell_location"
    )
    def test_build_sell_surplus_item_location_guidance_uses_expected_location_when_ids_differ(
        self, mock_is_type_accepted
    ):
        """Name-only location matches should not produce false 'not accepted here' guidance."""
        # AA Example App
        from indy_hub.tasks.material_exchange_contracts import (
            _build_sell_surplus_item_location_guidance,
        )

        self.config.sell_structure_ids = [70000001]
        self.config.sell_structure_names = ["Alpha Hub"]
        self.config.allowed_market_groups_sell_by_structure = {
            "70000001": [100],
        }
        self.config.save(
            update_fields=[
                "sell_structure_ids",
                "sell_structure_names",
                "allowed_market_groups_sell_by_structure",
            ]
        )

        # Accepted for the configured location, rejected for unknown IDs.
        def _acceptance_side_effect(*args, **kwargs):
            return int(kwargs.get("location_id") or 0) == 70000001

        mock_is_type_accepted.side_effect = _acceptance_side_effect

        effective_contract_location_id = _get_effective_contract_location_id(
            start_location_id=1045722708748,
            end_location_id=1045722708748,
            expected_location_ids=[70000001],
        )
        guidance = _build_sell_surplus_item_location_guidance(
            config=self.config,
            contract_location_id=effective_contract_location_id,
            surplus_type_ids=[34],
            type_names={34: "Tritanium"},
        )

        self.assertEqual(guidance, "")

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    @patch(
        "indy_hub.tasks.material_exchange_contracts._build_sell_surplus_item_location_guidance"
    )
    def test_items_mismatch_anomaly_includes_sell_location_guidance(
        self,
        mock_build_guidance,
        mock_notify_multi,
        mock_notify_user,
        mock_user_chars,
    ):
        """Sell items mismatch anomaly should include sell-location recommendations."""
        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        guidance_text = (
            "Sell-location guidance:\n"
            "- Isogen: not accepted at this contract location; accepted at Beta Hub."
        )
        mock_build_guidance.return_value = guidance_text

        seller_char_id = 111111111
        mock_user_chars.return_value = [seller_char_id]

        mismatch_contract = ESIContract.objects.create(
            contract_id=4210,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            price=self.sell_order.total_price,
            title=self.sell_order.order_reference,
            date_issued="2024-01-01T00:00:00Z",
            date_expired="2024-12-31T23:59:59Z",
        )
        ESIContractItem.objects.create(
            contract=mismatch_contract,
            record_id=42101,
            type_id=34,
            quantity=999,
            is_included=True,
        )
        ESIContractItem.objects.create(
            contract=mismatch_contract,
            record_id=42102,
            type_id=37,
            quantity=3,
            is_included=True,
        )

        validate_material_exchange_sell_orders()

        self.sell_order.refresh_from_db()
        self.assertEqual(self.sell_order.status, MaterialExchangeSellOrder.Status.ANOMALY)
        self.assertIn("Sell-location guidance:", self.sell_order.notes)
        self.assertIn("accepted at Beta Hub", self.sell_order.notes)

        self.assertTrue(mock_notify_user.called)
        notify_message = mock_notify_user.call_args[0][2]
        self.assertIn("Sell-location guidance:", notify_message)
        self.assertIn("accepted at Beta Hub", notify_message)
        mock_notify_multi.assert_called()


class BuyOrderValidationTaskTest(TestCase):
    """Tests for buy order validation task behavior."""

    def setUp(self):
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=60003760,
            structure_name="Test Structure",
            is_active=True,
        )
        self.buyer = User.objects.create_user(username="test_buyer")

        self.buy_order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.buyer,
            status=MaterialExchangeBuyOrder.Status.DRAFT,
            order_reference="INDY-9380811210",
        )
        self.buy_item = MaterialExchangeBuyOrderItem.objects.create(
            order=self.buy_order,
            type_id=34,
            type_name="Tritanium",
            quantity=500,
            unit_price=6.0,
            total_price=3000,
            stock_available_at_creation=1000,
        )

    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_validate_buy_order_in_draft_with_matching_contract(
        self, mock_multi, mock_user
    ):
        """Draft buy orders should be auto-validated when a matching cached contract exists."""
        # Standard Library
        from datetime import timedelta

        # Django
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        buyer_char_id = 999999999

        contract = ESIContract.objects.create(
            contract_id=227079044,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=0,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=buyer_char_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            title=self.buy_order.order_reference,
            price=self.buy_order.total_price,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=contract,
            record_id=1,
            type_id=self.buy_item.type_id,
            quantity=self.buy_item.quantity,
            is_included=True,
        )

        with patch(
            "indy_hub.tasks.material_exchange_contracts._get_user_character_ids",
            return_value=[buyer_char_id],
        ):
            validate_material_exchange_buy_orders()

        self.buy_order.refresh_from_db()
        self.assertEqual(
            self.buy_order.status, MaterialExchangeBuyOrder.Status.VALIDATED
        )
        self.assertEqual(self.buy_order.esi_contract_id, contract.contract_id)
        self.assertIn("Contract validated", self.buy_order.notes)

        mock_user.assert_called()
        user_title = str(mock_user.call_args[0][1])
        user_message = str(mock_user.call_args[0][2])
        self.assertIn("Contract", user_title)
        self.assertIn("created your in-game contract", user_message)
        self.assertIn(f"Contract #{contract.contract_id}", user_message)
        mock_multi.assert_called()

    @patch("indy_hub.tasks.material_exchange_contracts._log_buy_order_transactions")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts._notify_material_exchange_admins")
    @patch("indy_hub.tasks.material_exchange_contracts._get_character_for_scope")
    @patch("indy_hub.tasks.material_exchange_contracts.shared_client")
    def test_check_completed_buy_order_notifies_admins_only(
        self,
        mock_client,
        mock_get_char,
        mock_notify_admins,
        mock_notify_user,
        _mock_log_buy_tx,
    ):
        """When corp contract is finished, completion should notify admins/webhook only."""
        now = timezone.now()
        self.buy_order.status = MaterialExchangeBuyOrder.Status.VALIDATED
        self.buy_order.esi_contract_id = 227079244
        self.buy_order.save(update_fields=["status", "esi_contract_id", "updated_at"])

        mock_get_char.return_value = 999999999
        mock_client.fetch_corporation_contracts.return_value = [
            {
                "contract_id": 227079244,
                "status": "finished",
                "date_completed": now,
            }
        ]

        check_completed_material_exchange_contracts()

        self.buy_order.refresh_from_db()
        self.assertEqual(self.buy_order.status, MaterialExchangeBuyOrder.Status.COMPLETED)
        self.assertIsNotNone(self.buy_order.delivered_at)
        mock_notify_user.assert_not_called()
        mock_notify_admins.assert_called()
        admin_title = str(mock_notify_admins.call_args[0][1])
        admin_message = str(mock_notify_admins.call_args[0][2])
        self.assertIn("Completed", admin_title)
        self.assertEqual(
            admin_message,
            f"Contract for {self.buy_order.buyer.username} has completed.",
        )

    @patch("indy_hub.tasks.material_exchange_contracts._log_buy_order_transactions")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts._notify_material_exchange_admins")
    @patch("indy_hub.tasks.material_exchange_contracts._get_character_for_scope")
    @patch("indy_hub.tasks.material_exchange_contracts.shared_client")
    def test_check_completed_buy_order_still_notifies_when_tx_logging_fails(
        self,
        mock_client,
        mock_get_char,
        mock_notify_admins,
        mock_notify_user,
        mock_log_buy_tx,
    ):
        """Transaction logging failure must not suppress buy completion notifications."""
        now = timezone.now()
        self.buy_order.status = MaterialExchangeBuyOrder.Status.VALIDATED
        self.buy_order.esi_contract_id = 227079355
        self.buy_order.save(update_fields=["status", "esi_contract_id", "updated_at"])

        mock_get_char.return_value = 999999999
        mock_client.fetch_corporation_contracts.return_value = [
            {
                "contract_id": 227079355,
                "status": "finished",
                "date_completed": now,
            }
        ]
        mock_log_buy_tx.side_effect = RuntimeError("simulated buy tx logging failure")

        check_completed_material_exchange_contracts()

        self.buy_order.refresh_from_db()
        self.assertEqual(self.buy_order.status, MaterialExchangeBuyOrder.Status.COMPLETED)
        self.assertIsNotNone(self.buy_order.delivered_at)
        mock_notify_user.assert_not_called()
        mock_notify_admins.assert_called_once()

    @patch("indy_hub.tasks.material_exchange_contracts._log_sell_order_transactions")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts._notify_material_exchange_admins")
    @patch("indy_hub.tasks.material_exchange_contracts._get_character_for_scope")
    @patch("indy_hub.tasks.material_exchange_contracts.shared_client")
    def test_check_completed_sell_order_notifies_admins_only(
        self,
        mock_client,
        mock_get_char,
        mock_notify_admins,
        mock_notify_user,
        _mock_log_sell_tx,
    ):
        """When sell contract is finished, completion should notify admins/webhook only."""
        # AA Example App
        from indy_hub.models import (
            MaterialExchangeSellOrder,
            MaterialExchangeSellOrderItem,
        )

        seller = User.objects.create_user(username="test_seller_completion")
        sell_order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=seller,
            status=MaterialExchangeSellOrder.Status.VALIDATED,
            order_reference="INDY-SELL-COMPLETE-1",
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=sell_order,
            type_id=34,
            type_name="Tritanium",
            quantity=1000,
            unit_price=5.0,
            total_price=5000,
        )
        sell_order.esi_contract_id = 227079299
        sell_order.save(update_fields=["esi_contract_id", "updated_at"])

        mock_get_char.return_value = 999999999
        mock_client.fetch_corporation_contracts.return_value = [
            {"contract_id": 227079299, "status": "finished"}
        ]

        check_completed_material_exchange_contracts()

        sell_order.refresh_from_db()
        self.assertEqual(
            sell_order.status, MaterialExchangeSellOrder.Status.COMPLETED
        )
        self.assertIsNotNone(sell_order.payment_verified_at)
        mock_notify_user.assert_not_called()
        mock_notify_admins.assert_called()
        admin_title = str(mock_notify_admins.call_args[0][1])
        admin_message = str(mock_notify_admins.call_args[0][2])
        self.assertIn("Completed", admin_title)
        self.assertEqual(
            admin_message,
            f"Contract from {sell_order.seller.username} has completed.",
        )

    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_validate_buy_order_accepts_lowercase_reference_and_missing_issuer_corp(
        self, mock_multi, mock_user
    ):
        """Buy matching should tolerate lowercase refs and missing issuer corp ID."""
        # Standard Library
        from datetime import timedelta

        # Django
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        buyer_char_id = 999999999

        contract = ESIContract.objects.create(
            contract_id=227079144,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=0,
            issuer_corporation_id=0,
            assignee_id=buyer_char_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            title=str(self.buy_order.order_reference or "").lower(),
            price=self.buy_order.total_price,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=contract,
            record_id=101,
            type_id=self.buy_item.type_id,
            quantity=self.buy_item.quantity,
            is_included=True,
        )

        with patch(
            "indy_hub.tasks.material_exchange_contracts._get_user_character_ids",
            return_value=[buyer_char_id],
        ):
            validate_material_exchange_buy_orders()

        self.buy_order.refresh_from_db()
        self.assertEqual(
            self.buy_order.status, MaterialExchangeBuyOrder.Status.VALIDATED
        )
        self.assertEqual(self.buy_order.esi_contract_id, contract.contract_id)
        mock_user.assert_called()
        mock_multi.assert_called()

    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_validate_buy_order_finished_contract_items_mismatch_force_validates(
        self, mock_multi, mock_user
    ):
        """Finished in-game contract with item mismatch should not leave buy order pending."""
        # Standard Library
        from datetime import timedelta

        # Django
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        buyer_char_id = 999999999

        contract = ESIContract.objects.create(
            contract_id=227079045,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=0,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=buyer_char_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="finished",
            title=self.buy_order.order_reference,
            price=self.buy_order.total_price,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=contract,
            record_id=2,
            type_id=self.buy_item.type_id,
            quantity=self.buy_item.quantity + 1,
            is_included=True,
        )

        with patch(
            "indy_hub.tasks.material_exchange_contracts._get_user_character_ids",
            return_value=[buyer_char_id],
        ):
            validate_material_exchange_buy_orders()

        self.buy_order.refresh_from_db()
        self.assertEqual(
            self.buy_order.status, MaterialExchangeBuyOrder.Status.VALIDATED
        )
        self.assertEqual(self.buy_order.esi_contract_id, contract.contract_id)
        self.assertIn("accepted in-game despite anomaly", self.buy_order.notes)

        mock_user.assert_called()
        mock_multi.assert_called()

    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_validate_buy_order_finished_wrong_reference_force_validates(
        self, mock_multi, mock_user
    ):
        """Finished near-match with wrong title reference should not remain pending."""
        # Standard Library
        from datetime import timedelta

        # Django
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        buyer_char_id = 999999999

        contract = ESIContract.objects.create(
            contract_id=227079046,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=0,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=buyer_char_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="finished",
            title="NO-REF-HERE",
            price=self.buy_order.total_price,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=contract,
            record_id=3,
            type_id=self.buy_item.type_id,
            quantity=self.buy_item.quantity,
            is_included=True,
        )

        with patch(
            "indy_hub.tasks.material_exchange_contracts._get_user_character_ids",
            return_value=[buyer_char_id],
        ):
            validate_material_exchange_buy_orders()

        self.buy_order.refresh_from_db()
        self.assertEqual(
            self.buy_order.status, MaterialExchangeBuyOrder.Status.VALIDATED
        )
        self.assertEqual(self.buy_order.esi_contract_id, contract.contract_id)
        self.assertIn("accepted in-game despite anomaly", self.buy_order.notes)

        mock_user.assert_called()
        mock_multi.assert_called()

    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_validate_buy_order_finished_criteria_mismatch_force_validates(
        self, mock_multi, mock_user
    ):
        """Finished contract with matching reference but criteria mismatch should not remain pending."""
        # Standard Library
        from datetime import timedelta

        # Django
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        buyer_char_id = 999999999

        contract = ESIContract.objects.create(
            contract_id=227079047,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=0,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=buyer_char_id,
            start_location_id=70000001,
            end_location_id=70000001,
            status="finished",
            title=self.buy_order.order_reference,
            price=self.buy_order.total_price,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=contract,
            record_id=4,
            type_id=self.buy_item.type_id,
            quantity=self.buy_item.quantity,
            is_included=True,
        )

        with patch(
            "indy_hub.tasks.material_exchange_contracts._get_user_character_ids",
            return_value=[buyer_char_id],
        ):
            validate_material_exchange_buy_orders()

        self.buy_order.refresh_from_db()
        self.assertEqual(
            self.buy_order.status, MaterialExchangeBuyOrder.Status.VALIDATED
        )
        self.assertEqual(self.buy_order.esi_contract_id, contract.contract_id)
        self.assertIn("accepted in-game despite anomaly", self.buy_order.notes)

        mock_user.assert_called()
        mock_multi.assert_called()

    @patch(
        "indy_hub.tasks.material_exchange_contracts._notify_material_exchange_admins"
    )
    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    def test_validate_buy_order_pending_mismatch_notification_includes_deltas(
        self, mock_user_chars, mock_notify_admins
    ):
        """Buy pending mismatch alert should include exact missing and surplus quantities."""
        # Standard Library
        from datetime import timedelta

        # Django
        from django.core.cache import cache
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import (
            ESIContract,
            ESIContractItem,
            MaterialExchangeBuyOrderItem,
        )

        buyer_char_id = 999999999
        mock_user_chars.return_value = [buyer_char_id]

        MaterialExchangeBuyOrderItem.objects.create(
            order=self.buy_order,
            type_id=37,
            type_name="Isogen",
            quantity=4,
            unit_price=7,
            total_price=28,
            stock_available_at_creation=1000,
        )
        MaterialExchangeBuyOrderItem.objects.create(
            order=self.buy_order,
            type_id=35,
            type_name="Pyerite",
            quantity=10,
            unit_price=8,
            total_price=80,
            stock_available_at_creation=1000,
        )

        pending_contract = ESIContract.objects.create(
            contract_id=227079048,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=0,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=buyer_char_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            title=self.buy_order.order_reference,
            price=self.buy_order.total_price,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=pending_contract,
            record_id=5,
            type_id=34,
            quantity=500,
            is_included=True,
        )
        ESIContractItem.objects.create(
            contract=pending_contract,
            record_id=6,
            type_id=37,
            quantity=7,
            is_included=True,
        )
        ESIContractItem.objects.create(
            contract=pending_contract,
            record_id=7,
            type_id=35,
            quantity=3,
            is_included=True,
        )

        old_created_at = timezone.now() - timedelta(hours=25)
        MaterialExchangeBuyOrder.objects.filter(pk=self.buy_order.pk).update(
            created_at=old_created_at
        )

        cache.delete(
            f"material_exchange:buy_order:{self.buy_order.id}:contract_reminder"
        )

        validate_material_exchange_buy_orders()

        self.buy_order.refresh_from_db()
        self.assertIn("Missing:", self.buy_order.notes)
        self.assertIn("- 7 Pyerite", self.buy_order.notes)
        self.assertIn("Surplus:", self.buy_order.notes)
        self.assertIn("- 3 Isogen", self.buy_order.notes)

        self.assertTrue(mock_notify_admins.called)
        admin_message = mock_notify_admins.call_args[0][2]
        self.assertIn("Missing:", admin_message)
        self.assertIn("- 7 Pyerite", admin_message)
        self.assertIn("Surplus:", admin_message)
        self.assertIn("- 3 Isogen", admin_message)

    @patch(
        "indy_hub.tasks.material_exchange_contracts._notify_material_exchange_admins"
    )
    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    def test_validate_buy_order_pending_mismatch_notifies_immediately(
        self, mock_user_chars, mock_notify_admins
    ):
        """Buy mismatch should notify admins immediately, even for newly created orders."""
        # Standard Library
        from datetime import timedelta

        # Django
        from django.core.cache import cache
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        buyer_char_id = 999999999
        mock_user_chars.return_value = [buyer_char_id]

        pending_contract = ESIContract.objects.create(
            contract_id=227079149,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=0,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=buyer_char_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            title=self.buy_order.order_reference,
            price=self.buy_order.total_price,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=pending_contract,
            record_id=149,
            type_id=self.buy_item.type_id,
            quantity=self.buy_item.quantity + 2,
            is_included=True,
        )

        cache.delete(
            f"material_exchange:buy_order:{self.buy_order.id}:contract_reminder"
        )

        validate_material_exchange_buy_orders()

        self.buy_order.refresh_from_db()
        self.assertEqual(self.buy_order.status, MaterialExchangeBuyOrder.Status.DRAFT)
        self.assertIn("Issue(s): items mismatch", self.buy_order.notes)
        self.assertEqual(mock_notify_admins.call_count, 1)

        admin_title = mock_notify_admins.call_args[0][1]
        admin_message = mock_notify_admins.call_args[0][2]
        self.assertIn("Buy Order Contract Issue Detected", admin_title)
        self.assertIn("Reason: contract mismatch.", admin_message)
        self.assertIn("Issue(s): items mismatch", admin_message)


class StructureNameMatchingTest(TestCase):
    """Tests for structure name-based matching instead of ID-only"""

    def setUp(self):
        """Set up test data"""
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=1045667241057,
            structure_name="C-N4OD - Fountain of Life",
            sell_structure_ids=[1045667241057],
            sell_structure_names=["C-N4OD - Fountain of Life"],
            location_match_mode="name_or_id",
            is_active=True,
        )
        self.seller = User.objects.create_user(username="test_seller")
        self.sell_order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.seller,
            status=MaterialExchangeSellOrder.Status.DRAFT,
        )
        self.sell_item = MaterialExchangeSellOrderItem.objects.create(
            order=self.sell_order,
            type_id=34,
            type_name="Tritanium",
            quantity=1000,
            unit_price=5.5,
            total_price=5500,
        )

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_contract_matches_by_structure_name(
        self, mock_notify_multi, mock_get_char_ids
    ):
        """Test that contract with different structure ID matches by name"""
        # Standard Library
        from datetime import timedelta

        # Django
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        seller_char_id = 111111111
        mock_get_char_ids.return_value = [seller_char_id]

        # Create contract with different structure ID (1045722708748 instead of 1045667241057)
        # but same structure name "C-N4OD - Fountain of Life"
        contract = ESIContract.objects.create(
            contract_id=226598409,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=1045722708748,  # Different ID, same structure
            end_location_id=1045722708748,
            price=5500,
            title=self.sell_order.order_reference,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=contract,
            record_id=1,
            type_id=34,
            quantity=1000,
            is_included=True,
        )

        # Mock ESI client to return the structure name
        mock_esi_client = patch(
            "indy_hub.tasks.material_exchange_contracts.shared_client"
        )
        mock_client_instance = mock_esi_client.start()
        mock_client_instance.get_structure_info.return_value = {
            "name": "C-N4OD - Fountain of Life"
        }

        validate_material_exchange_sell_orders()

        # Check order was approved (matched by structure name)
        self.sell_order.refresh_from_db()
        self.assertEqual(
            self.sell_order.status, MaterialExchangeSellOrder.Status.VALIDATED
        )
        self.assertIn("226598409", self.sell_order.notes)

        # Verify admin notification was sent
        mock_notify_multi.assert_called_once()

        mock_esi_client.stop()

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    def test_contract_falls_back_to_id_matching(self, mock_get_char_ids):
        """Test that ID matching still works if ESI lookup fails"""
        # Standard Library
        from datetime import timedelta

        # Django
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        seller_char_id = 111111111
        mock_get_char_ids.return_value = [seller_char_id]

        # Create contract with matching structure ID
        contract = ESIContract.objects.create(
            contract_id=226598410,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            price=5500,
            title=self.sell_order.order_reference,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=contract,
            record_id=1,
            type_id=34,
            quantity=1000,
            is_included=True,
        )

        # Mock ESI client to fail (returns None)
        with patch(
            "indy_hub.tasks.material_exchange_contracts.shared_client"
        ) as mock_client:
            mock_client.get_structure_info.side_effect = Exception("ESI Error")

            with patch("indy_hub.tasks.material_exchange_contracts.notify_multi"):
                validate_material_exchange_sell_orders()

        # Check order was approved (matched by ID fallback)
        self.sell_order.refresh_from_db()
        self.assertEqual(
            self.sell_order.status, MaterialExchangeSellOrder.Status.VALIDATED
        )

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_strict_id_mode_rejects_name_only_match(
        self, _mock_notify_multi, _mock_notify_user, mock_get_char_ids
    ):
        """strict_id mode must reject contracts that only match by location name."""
        # Standard Library
        from datetime import timedelta

        # Django
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        self.config.location_match_mode = "strict_id"
        self.config.save(update_fields=["location_match_mode"])

        seller_char_id = 111111111
        mock_get_char_ids.return_value = [seller_char_id]

        contract = ESIContract.objects.create(
            contract_id=226598411,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=1045722708748,
            end_location_id=1045722708748,
            price=5500,
            title=self.sell_order.order_reference,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=contract,
            record_id=1,
            type_id=34,
            quantity=1000,
            is_included=True,
        )

        with patch("indy_hub.tasks.material_exchange_contracts.shared_client") as mock_client:
            mock_client.get_structure_info.return_value = {
                "name": "C-N4OD - Fountain of Life"
            }
            validate_material_exchange_sell_orders()

        self.sell_order.refresh_from_db()
        self.assertEqual(self.sell_order.status, MaterialExchangeSellOrder.Status.ANOMALY)


class BuyOrderSignalTest(TestCase):
    """Tests for buy order creation signal"""

    def setUp(self):
        """Set up test data"""
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=60003760,
            structure_name="Test Structure",
            is_active=True,
        )
        self.buyer = User.objects.create_user(username="test_buyer")
        self.seller = User.objects.create_user(username="test_seller")

    @patch(
        "indy_hub.tasks.material_exchange_contracts.handle_material_exchange_buy_order_created"
    )
    def test_buy_order_signal_on_create(self, mock_task):
        """Test that signal is triggered on buy order creation"""
        buy_order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.buyer,
        )
        MaterialExchangeBuyOrderItem.objects.create(
            order=buy_order,
            type_id=34,
            type_name="Tritanium",
            quantity=500,
            unit_price=6.0,
            total_price=3000,
            stock_available_at_creation=1000,
        )

        mock_task.apply_async.assert_called_once_with(
            args=(buy_order.id,),
            countdown=2,
            expires=1800,
        )
        self.assertEqual(buy_order.status, MaterialExchangeBuyOrder.Status.DRAFT)

    @patch(
        "indy_hub.tasks.material_exchange_contracts.handle_material_exchange_buy_order_created"
    )
    def test_buy_order_signal_queues_on_commit(self, mock_task):
        """Task should queue only after outer transaction commit."""
        with self.captureOnCommitCallbacks(execute=False) as callbacks:
            with transaction.atomic():
                buy_order = MaterialExchangeBuyOrder.objects.create(
                    config=self.config,
                    buyer=self.buyer,
                )
                self.assertFalse(mock_task.apply_async.called)

            self.assertEqual(len(callbacks), 1)
            self.assertFalse(mock_task.apply_async.called)

            callbacks[0]()

        mock_task.apply_async.assert_called_once_with(
            args=(buy_order.id,),
            countdown=2,
            expires=1800,
        )

    @patch(
        "indy_hub.tasks.material_exchange_contracts.handle_material_exchange_sell_order_created"
    )
    def test_sell_order_signal_on_create(self, mock_task):
        """Test that signal is triggered on sell order creation."""
        sell_order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.seller,
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=sell_order,
            type_id=34,
            type_name="Tritanium",
            quantity=500,
            unit_price=5.0,
            total_price=2500,
        )

        mock_task.apply_async.assert_called_once_with(
            args=(sell_order.id,),
            countdown=2,
            expires=300,
        )
        self.assertEqual(sell_order.status, MaterialExchangeSellOrder.Status.DRAFT)

    @patch(
        "indy_hub.tasks.material_exchange_contracts.handle_material_exchange_sell_order_created"
    )
    def test_sell_order_signal_queues_on_commit(self, mock_task):
        """Sell-notification task should queue only after outer transaction commit."""
        with self.captureOnCommitCallbacks(execute=False) as callbacks:
            with transaction.atomic():
                sell_order = MaterialExchangeSellOrder.objects.create(
                    config=self.config,
                    seller=self.seller,
                )
                self.assertFalse(mock_task.apply_async.called)

            self.assertEqual(len(callbacks), 1)
            self.assertFalse(mock_task.apply_async.called)

            callbacks[0]()

        mock_task.apply_async.assert_called_once_with(
            args=(sell_order.id,),
            countdown=2,
            expires=300,
        )


class MaterialExchangeWebhookMessageFormatTest(TestCase):
    def setUp(self):
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=22334455,
            structure_id=60003760,
            structure_name="Test Structure",
            is_active=True,
        )
        self.buyer = User.objects.create_user(username="webhook_buyer")
        self.seller = User.objects.create_user(username="webhook_seller")

    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_buy_order_created_lists_items_being_bought(self, mock_notify_multi):
        order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.buyer,
            order_reference="INDY-BUY-WEBHOOK-1",
        )
        MaterialExchangeBuyOrderItem.objects.create(
            order=order,
            type_id=34,
            type_name="Tritanium",
            quantity=1200,
            unit_price=6.0,
            total_price=7200,
            stock_available_at_creation=9999,
        )

        handle_material_exchange_buy_order_created(order.id)

        self.assertTrue(mock_notify_multi.called)
        title = str(mock_notify_multi.call_args[0][1])
        message = str(mock_notify_multi.call_args[0][2])
        self.assertIn("New Buy Order", title)
        self.assertIn("Items being bought:", message)
        self.assertIn("Tritanium", message)

    @patch("indy_hub.tasks.material_exchange_contracts._notify_material_exchange_admins")
    def test_sell_order_created_lists_items_being_sold(self, mock_notify_admins):
        order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.seller,
            order_reference="INDY-SELL-WEBHOOK-1",
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=order,
            type_id=35,
            type_name="Pyerite",
            quantity=500,
            unit_price=8.0,
            total_price=4000,
        )

        handle_material_exchange_sell_order_created(order.id)

        self.assertTrue(mock_notify_admins.called)
        title = str(mock_notify_admins.call_args[0][1])
        message = str(mock_notify_admins.call_args[0][2])
        self.assertIn("New Sell Order", title)
        self.assertIn("Items being sold:", message)
        self.assertIn("Pyerite", message)

    def test_contract_state_webhook_line_is_one_line(self):
        self.assertEqual(
            _build_contract_state_webhook_line("pilot_x", "validated"),
            "Contract from pilot_x has validated.",
        )
        self.assertEqual(
            _build_contract_state_webhook_line("pilot_x", "completed"),
            "Contract from pilot_x has completed.",
        )
        self.assertEqual(
            _build_contract_state_webhook_line(
                "pilot_x", "validated", relation="for"
            ),
            "Contract for pilot_x has validated.",
        )
        self.assertEqual(
            _build_contract_state_webhook_line(
                "pilot_x", "completed", relation="for"
            ),
            "Contract for pilot_x has completed.",
        )


class NotificationDeduplicationTest(TestCase):
    """Ensure periodic material exchange cycle does not re-send identical alerts."""

    def setUp(self):
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=60003760,
            structure_name="Test Structure",
            is_active=True,
        )
        self.seller = User.objects.create_user(username="dedupe_seller")
        self.buyer = User.objects.create_user(username="dedupe_buyer")

    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    def test_awaiting_buy_notification_throttled_across_cycles(self, mock_notify_user):
        """Awaiting-validation buy order ping should be sent once per throttle window."""
        # Django
        from django.core.cache import cache

        order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.buyer,
            status=MaterialExchangeBuyOrder.Status.AWAITING_VALIDATION,
            order_reference="INDY-AWAIT-1",
        )

        cache.delete(f"material_exchange:buy_order:{order.id}:awaiting_validation_ping")

        validate_material_exchange_buy_orders()
        validate_material_exchange_buy_orders()

        self.assertEqual(mock_notify_user.call_count, 1)

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    def test_sell_anomaly_notifications_not_repeated_for_unchanged_state(
        self,
        mock_notify_user,
        mock_notify_multi,
        mock_get_character_ids,
    ):
        """Same sell-order anomaly should not notify user/admin every cycle."""
        # Standard Library
        from datetime import timedelta

        # Django
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract

        seller_char_id = 987654321
        mock_get_character_ids.return_value = [seller_char_id]

        sell_order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.seller,
            status=MaterialExchangeSellOrder.Status.DRAFT,
            order_reference="INDY-ANOM-1",
        )

        MaterialExchangeSellOrderItem.objects.create(
            order=sell_order,
            type_id=34,
            type_name="Tritanium",
            quantity=100,
            unit_price=10,
            total_price=1000,
        )

        ESIContract.objects.create(
            contract_id=555001,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=70000001,
            end_location_id=70000001,
            status="outstanding",
            price=sell_order.total_price,
            title=sell_order.order_reference,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )

        validate_material_exchange_sell_orders()
        validate_material_exchange_sell_orders()

        self.assertEqual(mock_notify_user.call_count, 1)
        self.assertEqual(mock_notify_multi.call_count, 1)

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    def test_anomaly_contract_finished_is_force_validated(
        self,
        mock_notify_user,
        mock_notify_multi,
        mock_get_character_ids,
    ):
        """An anomalous contract accepted in-game should move sell order to validated."""
        # Standard Library
        from datetime import timedelta

        # Django
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract

        seller_char_id = 222333444
        mock_get_character_ids.return_value = [seller_char_id]

        sell_order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.seller,
            status=MaterialExchangeSellOrder.Status.ANOMALY,
            order_reference="INDY-ANOM-FINISHED-1",
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=sell_order,
            type_id=34,
            type_name="Tritanium",
            quantity=100,
            unit_price=10,
            total_price=1000,
        )

        ESIContract.objects.create(
            contract_id=777001,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=70000001,
            end_location_id=70000001,
            status="finished",
            price=sell_order.total_price,
            title=sell_order.order_reference,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )

        validate_material_exchange_sell_orders()

        sell_order.refresh_from_db()
        self.assertEqual(sell_order.status, MaterialExchangeSellOrder.Status.VALIDATED)
        self.assertEqual(sell_order.esi_contract_id, 777001)
        self.assertIn("accepted in-game despite anomaly", sell_order.notes)
        self.assertTrue(mock_notify_user.called)
        self.assertTrue(mock_notify_multi.called)

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    def test_anomaly_contract_rejected_stays_open_for_redo(
        self,
        mock_notify_user,
        mock_get_character_ids,
    ):
        """Rejected in-game anomaly contract should not cancel order and must allow later recovery."""
        # Standard Library
        from datetime import timedelta

        # Django
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        seller_char_id = 555666777
        mock_get_character_ids.return_value = [seller_char_id]

        sell_order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.seller,
            status=MaterialExchangeSellOrder.Status.ANOMALY,
            order_reference="INDY-ANOM-REJECTED-1",
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=sell_order,
            type_id=34,
            type_name="Tritanium",
            quantity=100,
            unit_price=10,
            total_price=1000,
        )

        ESIContract.objects.create(
            contract_id=888001,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=70000001,
            end_location_id=70000001,
            status="rejected",
            price=sell_order.total_price,
            title=sell_order.order_reference,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )

        validate_material_exchange_sell_orders()

        sell_order.refresh_from_db()
        self.assertEqual(
            sell_order.status,
            MaterialExchangeSellOrder.Status.ANOMALY_REJECTED,
        )
        self.assertIn("remains open", sell_order.notes)

        valid_contract = ESIContract.objects.create(
            contract_id=888002,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            price=sell_order.total_price,
            title=sell_order.order_reference,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=valid_contract,
            record_id=9001,
            type_id=34,
            quantity=100,
            is_included=True,
        )

        validate_material_exchange_sell_orders()

        sell_order.refresh_from_db()
        self.assertEqual(sell_order.status, MaterialExchangeSellOrder.Status.VALIDATED)
        self.assertEqual(sell_order.esi_contract_id, valid_contract.contract_id)
        self.assertTrue(mock_notify_user.called)

    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    def test_sell_validation_uses_order_source_location_when_present(
        self, mock_get_character_ids
    ):
        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        self.config.sell_structure_ids = [70000001, 70000002]
        self.config.sell_structure_names = ["Alpha Hub", "Beta Hub"]
        self.config.save(update_fields=["sell_structure_ids", "sell_structure_names"])

        seller_char_id = 111222333
        mock_get_character_ids.return_value = [seller_char_id]

        sell_order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.seller,
            status=MaterialExchangeSellOrder.Status.DRAFT,
            order_reference="INDY-SRC-LOC-1",
            source_location_id=70000002,
            source_location_name="Beta Hub",
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=sell_order,
            type_id=34,
            type_name="Tritanium",
            quantity=100,
            unit_price=10,
            total_price=1000,
        )

        wrong_location_contract = ESIContract.objects.create(
            contract_id=999001,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=70000001,
            end_location_id=70000001,
            status="outstanding",
            price=sell_order.total_price,
            title=sell_order.order_reference,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=wrong_location_contract,
            record_id=999101,
            type_id=34,
            quantity=100,
            is_included=True,
        )

        validate_material_exchange_sell_orders()

        sell_order.refresh_from_db()
        self.assertEqual(sell_order.status, MaterialExchangeSellOrder.Status.ANOMALY)
        self.assertIn("Expected: Beta Hub", sell_order.notes)

        correct_location_contract = ESIContract.objects.create(
            contract_id=999002,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=70000002,
            end_location_id=70000002,
            status="outstanding",
            price=sell_order.total_price,
            title=sell_order.order_reference,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=correct_location_contract,
            record_id=999102,
            type_id=34,
            quantity=100,
            is_included=True,
        )

        validate_material_exchange_sell_orders()

        sell_order.refresh_from_db()
        self.assertEqual(sell_order.status, MaterialExchangeSellOrder.Status.VALIDATED)
        self.assertEqual(sell_order.esi_contract_id, correct_location_contract.contract_id)

    @patch("indy_hub.tasks.material_exchange_contracts._notify_material_exchange_admins")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    def test_sell_validation_does_not_re_notify_when_already_validated(
        self,
        mock_get_character_ids,
        mock_notify_user,
        mock_notify_admins,
    ):
        # Standard Library
        from datetime import timedelta

        # Django
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        seller_char_id = 445566778
        mock_get_character_ids.return_value = [seller_char_id]

        sell_order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.seller,
            status=MaterialExchangeSellOrder.Status.DRAFT,
            order_reference="INDY-VALIDATED-DEDUP-SELL-1",
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=sell_order,
            type_id=34,
            type_name="Tritanium",
            quantity=100,
            unit_price=10,
            total_price=1000,
        )

        contract = ESIContract.objects.create(
            contract_id=990001,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=seller_char_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=self.config.corporation_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            price=sell_order.total_price,
            title=sell_order.order_reference,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=contract,
            record_id=990101,
            type_id=34,
            quantity=100,
            is_included=True,
        )

        stale_order = MaterialExchangeSellOrder.objects.get(pk=sell_order.pk)
        now = timezone.now()
        MaterialExchangeSellOrder.objects.filter(pk=sell_order.pk).update(
            status=MaterialExchangeSellOrder.Status.VALIDATED,
            esi_contract_id=contract.contract_id,
            contract_validated_at=now,
            notes=f"Contract validated: {contract.contract_id} @ {sell_order.total_price:,.0f} ISK",
            updated_at=now,
        )

        contracts = ESIContract.objects.filter(
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
        ).prefetch_related("items")
        _validate_sell_order_from_db(self.config, stale_order, contracts, esi_client=None)

        mock_notify_user.assert_not_called()
        mock_notify_admins.assert_not_called()

    @patch("indy_hub.tasks.material_exchange_contracts._notify_material_exchange_admins")
    @patch("indy_hub.tasks.material_exchange_contracts.notify_user")
    @patch("indy_hub.tasks.material_exchange_contracts._get_user_character_ids")
    def test_buy_validation_does_not_re_notify_when_already_validated(
        self,
        mock_get_character_ids,
        mock_notify_user,
        mock_notify_admins,
    ):
        # Standard Library
        from datetime import timedelta

        # Django
        from django.utils import timezone

        # AA Example App
        from indy_hub.models import ESIContract, ESIContractItem

        buyer_char_id = 112233445
        mock_get_character_ids.return_value = [buyer_char_id]

        buy_order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.buyer,
            status=MaterialExchangeBuyOrder.Status.DRAFT,
            order_reference="INDY-VALIDATED-DEDUP-BUY-1",
        )
        buy_item = MaterialExchangeBuyOrderItem.objects.create(
            order=buy_order,
            type_id=34,
            type_name="Tritanium",
            quantity=100,
            unit_price=10,
            total_price=1000,
            stock_available_at_creation=500,
        )

        contract = ESIContract.objects.create(
            contract_id=990002,
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
            issuer_id=self.config.corporation_id,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=buyer_char_id,
            start_location_id=self.config.structure_id,
            end_location_id=self.config.structure_id,
            status="outstanding",
            price=buy_order.total_price,
            title=buy_order.order_reference,
            date_issued=timezone.now(),
            date_expired=timezone.now() + timedelta(days=30),
        )
        ESIContractItem.objects.create(
            contract=contract,
            record_id=990201,
            type_id=34,
            quantity=100,
            is_included=True,
        )

        stale_order = MaterialExchangeBuyOrder.objects.get(pk=buy_order.pk)
        now = timezone.now()
        MaterialExchangeBuyOrder.objects.filter(pk=buy_order.pk).update(
            status=MaterialExchangeBuyOrder.Status.VALIDATED,
            esi_contract_id=contract.contract_id,
            contract_validated_at=now,
            notes=f"Contract validated: {contract.contract_id} @ {buy_order.total_price:,.0f} ISK",
            updated_at=now,
        )

        contracts = ESIContract.objects.filter(
            corporation_id=self.config.corporation_id,
            contract_type="item_exchange",
        ).prefetch_related("items")
        _validate_buy_order_from_db(self.config, stale_order, contracts, esi_client=None)

        mock_notify_user.assert_not_called()
        mock_notify_admins.assert_not_called()

        buy_item.refresh_from_db()
        self.assertTrue(buy_item.esi_contract_validated)
        self.assertEqual(buy_item.esi_contract_id, contract.contract_id)


if __name__ == "__main__":
    # Standard Library
    import unittest

    unittest.main()
