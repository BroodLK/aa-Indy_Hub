# Standard Library
from decimal import Decimal
from unittest.mock import patch

# Django
from django.contrib.auth.models import User
from django.http import HttpResponse
from django.test import RequestFactory
from django.test import TestCase
from django.utils import timezone

# AA Example App
from indy_hub.models import MaterialExchangeConfig
from indy_hub.views.material_exchange import (
    _asset_is_blueprint,
    _build_sell_material_rows,
    _fetch_user_assets_for_structure_data,
    material_exchange_sell,
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
        self.factory = RequestFactory()

    def test_asset_is_blueprint_does_not_treat_positive_stacks_as_blueprints(self):
        self.assertFalse(
            _asset_is_blueprint(
                {
                    "type_id": 34,
                    "quantity": 5000,
                    "is_singleton": False,
                    "is_blueprint": False,
                }
            )
        )

    @patch(
        "indy_hub.views.material_exchange.get_type_name",
        return_value="Capital Construction Parts Blueprint",
    )
    def test_asset_is_blueprint_detects_singleton_blueprint_from_type_name_lookup(
        self, _mock_get_type_name
    ):
        self.assertTrue(
            _asset_is_blueprint(
                {
                    "type_id": 77777,
                    "quantity": 1,
                    "is_singleton": True,
                    "is_blueprint": False,
                }
            )
        )
        self.assertTrue(
            _asset_is_blueprint(
                {
                    "type_id": 77777,
                    "quantity": -2,
                    "is_singleton": True,
                    "is_blueprint": False,
                }
            )
        )

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
                "set_name": "Example Production Container",
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
            sell_override_map={
                35: {"kind": "fixed", "price": Decimal("12.34")}
            },
            character_name_by_id={1: "Pilot One"},
        )

        self.assertTrue(rows)
        self.assertEqual(rows[0]["row_kind"], "container")
        self.assertEqual(rows[0]["container_name"], "Example Production Container")
        self.assertEqual(rows[0]["character_name"], "Pilot One")

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

    @patch("indy_hub.views.material_exchange.get_type_name")
    def test_build_sell_material_rows_nests_with_location_parent_fallback(
        self, mock_get_type_name
    ):
        mock_get_type_name.side_effect = lambda type_id: {
            1000: "Station Container",
            34: "Tritanium",
        }.get(int(type_id), f"Type {type_id}")

        assets = [
            {
                "character_id": 1,
                "item_id": 500,
                "raw_location_id": self.structure_id,
                "location_id": self.structure_id,
                "type_id": 1000,
                "set_name": "Legacy Parent Container",
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            {
                "character_id": 1,
                "item_id": 501,
                # Legacy-style row: raw parent missing, but location_id points at container item_id.
                "raw_location_id": None,
                "location_id": 500,
                "type_id": 34,
                "quantity": 8,
                "is_singleton": False,
                "is_blueprint": False,
            },
        ]
        price_data = {34: {"buy": Decimal("5.00"), "sell": Decimal("6.00")}}

        rows = _build_sell_material_rows(
            assets=assets,
            config=self.config,
            price_data=price_data,
            reserved_quantities={},
            allowed_type_ids={34},
            sell_override_map={},
        )

        self.assertTrue(rows)
        self.assertEqual(rows[0]["row_kind"], "container")
        item_rows = [row for row in rows if row.get("row_kind") == "item"]
        self.assertEqual(len(item_rows), 1)
        self.assertEqual(item_rows[0]["depth"], 1)
        self.assertTrue(item_rows[0]["container_path"])

    @patch("indy_hub.views.material_exchange.get_type_name")
    def test_build_sell_material_rows_blueprint_copy_is_zero_priced(
        self, mock_get_type_name
    ):
        mock_get_type_name.side_effect = (
            lambda type_id: "Capital Armor Plates Blueprint"
            if int(type_id) == 77777
            else f"Type {type_id}"
        )

        assets = [
            {
                "character_id": 1,
                "item_id": 701,
                "raw_location_id": self.structure_id,
                "location_id": self.structure_id,
                "type_id": 77777,
                "quantity": -2,
                "is_singleton": True,
                "is_blueprint": True,
            }
        ]

        rows = _build_sell_material_rows(
            assets=assets,
            config=self.config,
            price_data={},
            reserved_quantities={},
            allowed_type_ids={77777},
            sell_override_map={},
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["row_kind"], "item")
        self.assertEqual(rows[0]["type_id"], 77777)
        self.assertEqual(rows[0]["buy_price_from_member"], Decimal("0"))
        self.assertEqual(rows[0]["default_buy_price_from_member"], Decimal("0"))
        self.assertTrue(rows[0]["is_blueprint_copy"])
        self.assertEqual(rows[0]["blueprint_variant"], "bpc")
        self.assertIn("(BPC)", rows[0]["type_name"])
        self.assertIn("/bpc?", rows[0]["icon_url"])
        self.assertIn("_bpc_", rows[0]["form_quantity_field_name"])

    @patch("indy_hub.views.material_exchange.get_type_name")
    def test_build_sell_material_rows_splits_same_type_by_character(
        self, mock_get_type_name
    ) -> None:
        mock_get_type_name.side_effect = (
            lambda type_id: "Tritanium" if int(type_id) == 34 else f"Type {type_id}"
        )

        assets = [
            {
                "character_id": 1001,
                "item_id": 88001,
                "raw_location_id": self.structure_id,
                "location_id": self.structure_id,
                "type_id": 34,
                "quantity": 5,
                "is_singleton": False,
                "is_blueprint": False,
            },
            {
                "character_id": 2002,
                "item_id": 88002,
                "raw_location_id": self.structure_id,
                "location_id": self.structure_id,
                "type_id": 34,
                "quantity": 7,
                "is_singleton": False,
                "is_blueprint": False,
            },
        ]
        price_data = {34: {"buy": Decimal("5.00"), "sell": Decimal("6.00")}}

        rows = _build_sell_material_rows(
            assets=assets,
            config=self.config,
            price_data=price_data,
            reserved_quantities={},
            allowed_type_ids={34},
            sell_override_map={},
            character_name_by_id={1001: "Pilot Alpha", 2002: "Pilot Bravo"},
        )

        item_rows = [row for row in rows if row.get("row_kind") == "item"]
        self.assertEqual(len(item_rows), 2)

        by_character_name = {str(row["character_name"]): row for row in item_rows}
        self.assertEqual(by_character_name["Pilot Alpha"]["user_quantity"], 5)
        self.assertEqual(by_character_name["Pilot Alpha"]["available_quantity"], 5)
        self.assertEqual(by_character_name["Pilot Bravo"]["user_quantity"], 7)
        self.assertEqual(by_character_name["Pilot Bravo"]["available_quantity"], 7)

    @patch("indy_hub.views.material_exchange.build_nav_context", return_value={})
    @patch("indy_hub.views.material_exchange._build_nav_context", return_value={})
    @patch("indy_hub.views.material_exchange._get_corp_name_for_hub", return_value="Test Corp")
    @patch("indy_hub.views.material_exchange._get_allowed_type_ids_for_config", return_value=None)
    @patch("indy_hub.views.material_exchange._fetch_fuzzwork_prices")
    @patch("indy_hub.views.material_exchange.get_user_assets_cached")
    @patch("indy_hub.views.material_exchange._fetch_user_assets_for_structure_data")
    @patch("indy_hub.views.material_exchange._get_reserved_sell_quantities")
    @patch(
        "indy_hub.views.material_exchange._get_item_price_override_maps",
        return_value=({}, {}),
    )
    @patch("indy_hub.views.material_exchange._get_material_exchange_config")
    @patch("indy_hub.views.material_exchange._is_material_exchange_enabled", return_value=True)
    @patch("indy_hub.views.material_exchange.render")
    def test_sell_view_display_reservations_do_not_type_filter_query(
        self,
        mock_render,
        _mock_enabled,
        mock_get_config,
        _mock_override_maps,
        mock_reserved_sell,
        mock_fetch_assets_for_structures,
        mock_get_user_assets_cached,
        mock_fetch_prices,
        _mock_allowed_type_ids,
        _mock_corp_name,
        _mock_build_nav,
        _mock_build_main_nav,
    ) -> None:
        self.config.last_stock_sync = timezone.now()
        self.config.save(update_fields=["last_stock_sync"])

        mock_get_config.return_value = self.config
        mock_render.return_value = HttpResponse("ok")
        mock_fetch_prices.return_value = {34: {"buy": Decimal("10"), "sell": Decimal("11")}}

        # Stale aggregated data misses type 35, while raw asset data still contains it.
        mock_fetch_assets_for_structures.return_value = (
            {34: 5},
            {9001: {34: 5}},
            {self.structure_id: {34: 5}},
            False,
        )
        mock_get_user_assets_cached.return_value = (
            [
                {
                    "character_id": 9001,
                    "item_id": 101,
                    "raw_location_id": self.structure_id,
                    "location_id": self.structure_id,
                    "type_id": 34,
                    "quantity": 5,
                    "is_singleton": False,
                },
                {
                    "character_id": 9001,
                    "item_id": 102,
                    "raw_location_id": self.structure_id,
                    "location_id": self.structure_id,
                    "type_id": 35,
                    "quantity": 7,
                    "is_singleton": False,
                },
            ],
            False,
        )
        mock_reserved_sell.return_value = {34: 2, 35: 7}

        request = self.factory.get("/material-exchange/sell/")
        request.user = self.user

        sell_view = material_exchange_sell.__wrapped__.__wrapped__.__wrapped__
        response = sell_view(request, tokens=None)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(mock_reserved_sell.called)
        self.assertNotIn("type_ids", mock_reserved_sell.call_args.kwargs)
