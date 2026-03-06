# Standard Library
from unittest.mock import patch

# Django
from django.contrib.auth.models import User
from django.test import TestCase

# AA Example App
from indy_hub.models import MaterialExchangeConfig
from indy_hub.views.material_exchange import _fetch_user_assets_for_structure_data


class MaterialExchangeSellAssetFilteringTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user("seller_filter", password="secret123")
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=60003760,
            structure_name="Test Structure",
            sell_structure_ids=[60003760],
            sell_structure_names=["Test Structure"],
            allow_fitted_ships=False,
            is_active=True,
        )
        self.structure_id = 60003760

    @patch("indy_hub.views.material_exchange._get_ship_type_ids", return_value={999})
    @patch("indy_hub.views.material_exchange.get_user_assets_cached")
    def test_fitted_ship_and_contents_excluded_when_toggle_disabled(
        self,
        mock_get_user_assets_cached,
        _mock_get_ship_type_ids,
    ):
        assets = [
            {
                "character_id": 1,
                "item_id": 100,
                "raw_location_id": self.structure_id,
                "location_id": self.structure_id,
                "location_flag": "Hangar",
                "type_id": 999,  # ship hull
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            {
                "character_id": 1,
                "item_id": 101,
                "raw_location_id": 100,  # inside fitted ship
                "location_id": self.structure_id,
                "location_flag": "HiSlot0",
                "type_id": 34,
                "quantity": 5,
                "is_singleton": False,
                "is_blueprint": False,
            },
            {
                "character_id": 1,
                "item_id": 102,
                "raw_location_id": 100,  # inside fitted ship cargo
                "location_id": self.structure_id,
                "location_flag": "Cargo",
                "type_id": 35,
                "quantity": 7,
                "is_singleton": False,
                "is_blueprint": False,
            },
            {
                "character_id": 1,
                "item_id": 103,
                "raw_location_id": self.structure_id,
                "location_id": self.structure_id,
                "location_flag": "Hangar",
                "type_id": 36,
                "quantity": 9,
                "is_singleton": False,
                "is_blueprint": False,
            },
        ]
        mock_get_user_assets_cached.return_value = (assets, False)

        aggregated, by_character, by_location, scope_missing = (
            _fetch_user_assets_for_structure_data(
                self.user,
                self.structure_id,
                allow_refresh=False,
                config=self.config,
            )
        )

        self.assertFalse(scope_missing)
        self.assertEqual(aggregated, {36: 9})
        self.assertEqual(by_character.get(1), {36: 9})
        self.assertEqual(by_location.get(self.structure_id), {36: 9})

    @patch("indy_hub.views.material_exchange._get_ship_type_ids", return_value={999})
    @patch("indy_hub.views.material_exchange.get_user_assets_cached")
    def test_fitted_ship_and_contents_included_when_toggle_enabled(
        self,
        mock_get_user_assets_cached,
        _mock_get_ship_type_ids,
    ):
        self.config.allow_fitted_ships = True
        self.config.save(update_fields=["allow_fitted_ships"])

        assets = [
            {
                "character_id": 1,
                "item_id": 100,
                "raw_location_id": self.structure_id,
                "location_id": self.structure_id,
                "location_flag": "Hangar",
                "type_id": 999,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            {
                "character_id": 1,
                "item_id": 101,
                "raw_location_id": 100,
                "location_id": self.structure_id,
                "location_flag": "HiSlot0",
                "type_id": 34,
                "quantity": 5,
                "is_singleton": False,
                "is_blueprint": False,
            },
        ]
        mock_get_user_assets_cached.return_value = (assets, False)

        aggregated, _by_character, _by_location, _scope_missing = (
            _fetch_user_assets_for_structure_data(
                self.user,
                self.structure_id,
                allow_refresh=False,
                config=self.config,
            )
        )

        self.assertEqual(aggregated.get(999), 1)
        self.assertEqual(aggregated.get(34), 5)
