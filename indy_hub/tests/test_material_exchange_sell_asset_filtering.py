# Standard Library
from decimal import Decimal
from unittest.mock import patch

# Django
from django.contrib.auth.models import User
from django.test import TestCase

# AA Example App
from indy_hub.models import MaterialExchangeConfig
from indy_hub.views.material_exchange import (
    _build_sell_material_rows,
    _fetch_user_assets_for_structure_data,
)


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

    @patch("indy_hub.views.material_exchange.get_type_name")
    def test_build_sell_material_rows_groups_container_assets(
        self,
        mock_get_type_name,
    ):
        name_map = {
            1000: "Small Secure Container",
            34: "Tritanium",
            35: "Pyerite",
            36: "Mexallon",
        }
        mock_get_type_name.side_effect = lambda type_id: name_map.get(
            int(type_id), f"Type {type_id}"
        )

        assets = [
            {
                "character_id": 1,
                "item_id": 100,
                "raw_location_id": self.structure_id,
                "location_id": self.structure_id,
                "type_id": 1000,
                "quantity": 1,
                "is_singleton": True,
            },
            {
                "character_id": 1,
                "item_id": 101,
                "raw_location_id": 100,
                "location_id": self.structure_id,
                "type_id": 34,
                "quantity": 5,
                "is_singleton": False,
            },
            {
                "character_id": 1,
                "item_id": 102,
                "raw_location_id": 100,
                "location_id": self.structure_id,
                "type_id": 35,
                "quantity": 2,
                "is_singleton": False,
            },
            {
                "character_id": 1,
                "item_id": 103,
                "raw_location_id": self.structure_id,
                "location_id": self.structure_id,
                "type_id": 36,
                "quantity": 4,
                "is_singleton": False,
            },
        ]
        price_data = {
            34: {"buy": Decimal("5.00"), "sell": Decimal("6.00")},
            35: {"buy": Decimal("7.00"), "sell": Decimal("8.00")},
            36: {"buy": Decimal("9.00"), "sell": Decimal("10.00")},
        }

        rows = _build_sell_material_rows(
            assets=assets,
            config=self.config,
            price_data=price_data,
            reserved_quantities={34: 2},
            allowed_type_ids={34, 35, 36},
            sell_override_map={35: Decimal("12.34")},
        )

        self.assertTrue(rows)
        self.assertEqual(rows[0]["row_kind"], "container")
        self.assertEqual(rows[0]["container_name"], "Small Secure Container")

        item_rows = [row for row in rows if row.get("row_kind") == "item"]
        self.assertEqual(len(item_rows), 3)

        by_type = {int(row["type_id"]): row for row in item_rows}
        self.assertEqual(by_type[34]["depth"], 1)
        self.assertEqual(by_type[34]["available_quantity"], 3)
        self.assertEqual(by_type[34]["reserved_quantity"], 2)

        self.assertEqual(by_type[35]["depth"], 1)
        self.assertEqual(by_type[35]["buy_price_from_member"], Decimal("12.34"))
        self.assertTrue(by_type[35]["has_sell_price_override"])

        self.assertEqual(by_type[36]["depth"], 0)
