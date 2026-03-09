# Standard Library
from types import SimpleNamespace
from unittest.mock import patch

# Django
from django.test import TestCase

# AA Example App
from indy_hub.models import MaterialExchangeConfig, MaterialExchangeStock
from indy_hub.services.asset_cache import make_managed_hangar_location_id
from indy_hub.tasks.material_exchange import _sync_stock_impl
from indy_hub.views.material_exchange import (
    _selected_buy_stock_items_share_source_location,
)


class MaterialExchangeStockMultiLocationTests(TestCase):
    def setUp(self):
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=1001,
            structure_name="Primary Structure",
            buy_structure_ids=[1001, 1002],
            buy_structure_names=["Structure Alpha", "Structure Beta"],
            buy_enabled=True,
            hangar_division=7,
            is_active=True,
        )

    @patch("indy_hub.tasks.material_exchange.sync_material_exchange_prices")
    @patch("indy_hub.tasks.material_exchange.get_type_name", return_value="Tritanium")
    @patch("indy_hub.tasks.material_exchange.get_corp_assets_cached")
    def test_sync_stock_aggregates_all_buy_locations(
        self,
        mock_get_corp_assets_cached,
        _mock_get_type_name,
        _mock_sync_prices,
    ):
        corp_assets = [
            # Structure 1001 represented via office-folder item id.
            {
                "item_id": 5001,
                "location_id": 1001,
                "location_flag": "OfficeFolder",
                "type_id": 27,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            {
                "item_id": 6001,
                "location_id": 5001,
                "location_flag": "CorpSAG7",
                "type_id": 34,
                "quantity": 10,
                "is_singleton": False,
                "is_blueprint": False,
            },
            # Structure 1002 represented directly by structure id.
            {
                "item_id": 6002,
                "location_id": 1002,
                "location_flag": "CorpSAG7",
                "type_id": 34,
                "quantity": 5,
                "is_singleton": False,
                "is_blueprint": False,
            },
        ]
        mock_get_corp_assets_cached.return_value = (corp_assets, False)

        _sync_stock_impl()

        stock = MaterialExchangeStock.objects.get(config=self.config, type_id=34)
        self.assertEqual(stock.quantity, 15)
        self.assertEqual(stock.source_structure_ids, [1001, 1002])
        self.assertEqual(stock.source_structure_names, ["Structure Alpha", "Structure Beta"])

    @patch("indy_hub.tasks.material_exchange.sync_material_exchange_prices")
    @patch("indy_hub.tasks.material_exchange.get_type_name", return_value="Tritanium")
    @patch("indy_hub.tasks.material_exchange.get_corp_assets_cached")
    def test_sync_stock_accepts_managed_hangar_location_context(
        self,
        mock_get_corp_assets_cached,
        _mock_get_type_name,
        _mock_sync_prices,
    ):
        office_folder_item_id = 5002
        managed_location_id = make_managed_hangar_location_id(
            office_folder_item_id, self.config.hangar_division
        )

        corp_assets = [
            {
                "item_id": office_folder_item_id,
                "location_id": 1002,
                "location_flag": "OfficeFolder",
                "type_id": 27,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            {
                "item_id": 7001,
                "location_id": managed_location_id,
                "location_flag": "Hangar",
                "type_id": 34,
                "quantity": 12,
                "is_singleton": False,
                "is_blueprint": False,
            },
        ]
        mock_get_corp_assets_cached.return_value = (corp_assets, False)

        _sync_stock_impl()

        stock = MaterialExchangeStock.objects.get(config=self.config, type_id=34)
        self.assertEqual(stock.quantity, 12)
        self.assertEqual(stock.source_structure_ids, [1002])
        self.assertEqual(stock.source_structure_names, ["Structure Beta"])

    @patch("indy_hub.tasks.material_exchange.sync_material_exchange_prices")
    @patch("indy_hub.tasks.material_exchange.get_type_name", return_value="Tritanium")
    @patch("indy_hub.tasks.material_exchange.get_corp_assets_cached")
    def test_sync_stock_accepts_station_hangar_fallback_for_container_children(
        self,
        mock_get_corp_assets_cached,
        _mock_get_type_name,
        _mock_sync_prices,
    ):
        """Items in containers should still count when station-style assets use Hangar flags."""
        self.config.buy_structure_ids = [1001]
        self.config.buy_structure_names = ["Structure Alpha"]
        self.config.hangar_division = 1
        self.config.save(
            update_fields=["buy_structure_ids", "buy_structure_names", "hangar_division"]
        )

        corp_assets = [
            # Station-style parent container in hangar (no OfficeFolder row).
            {
                "item_id": 9001,
                "location_id": 1001,
                "location_flag": "Hangar",
                "type_id": 23,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            # Mineral stack inside the container.
            {
                "item_id": 9002,
                "location_id": 9001,
                "location_flag": "Unlocked",
                "type_id": 34,
                "quantity": 44,
                "is_singleton": False,
                "is_blueprint": False,
            },
        ]
        mock_get_corp_assets_cached.return_value = (corp_assets, False)

        _sync_stock_impl()

        stock = MaterialExchangeStock.objects.get(config=self.config, type_id=34)
        self.assertEqual(stock.quantity, 44)
        self.assertEqual(stock.source_structure_ids, [1001])
        self.assertEqual(stock.source_structure_names, ["Structure Alpha"])


class MaterialExchangeBuyLocationCompatibilityTests(TestCase):
    def test_selected_buy_stock_items_share_source_location_when_overlapping(self):
        selected_rows = [
            SimpleNamespace(source_structure_ids=[1001, 1002]),
            SimpleNamespace(source_structure_ids=[1002]),
        ]

        self.assertTrue(
            _selected_buy_stock_items_share_source_location(selected_rows)
        )

    def test_selected_buy_stock_items_share_source_location_when_disjoint(self):
        selected_rows = [
            SimpleNamespace(source_structure_ids=[1001]),
            SimpleNamespace(source_structure_ids=[1002]),
        ]

        self.assertFalse(
            _selected_buy_stock_items_share_source_location(selected_rows)
        )
