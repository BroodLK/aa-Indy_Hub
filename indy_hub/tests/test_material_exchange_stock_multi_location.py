# Standard Library
from unittest.mock import patch

# Django
from django.test import TestCase

# AA Example App
from indy_hub.models import MaterialExchangeConfig, MaterialExchangeStock
from indy_hub.services.asset_cache import make_managed_hangar_location_id
from indy_hub.tasks.material_exchange import _sync_stock_impl


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
