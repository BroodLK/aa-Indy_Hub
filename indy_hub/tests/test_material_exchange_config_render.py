# Standard Library
from unittest.mock import patch

# Django
from django.contrib.auth.models import User
from django.test import RequestFactory, TestCase
from django.urls import reverse

# AA Example App
from indy_hub.models import MaterialExchangeConfig, MaterialExchangeSettings
from indy_hub.views.material_exchange_config import material_exchange_config


class MaterialExchangeConfigRenderContractTests(TestCase):
    def setUp(self) -> None:
        self.user = User.objects.create_user("config-render", password="secret123")
        self.factory = RequestFactory()
        settings_obj = MaterialExchangeSettings.get_solo()
        settings_obj.is_enabled = True
        settings_obj.save(update_fields=["is_enabled", "updated_at"])

        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456,
            structure_id=60000001,
            structure_name="Primary Hub",
            hangar_division=1,
            sell_markup_percent="0.00",
            sell_markup_base="buy",
            buy_markup_percent="5.00",
            buy_markup_base="buy",
            enforce_jita_price_bounds=True,
            notify_admins_on_sell_anomaly=True,
            is_active=True,
            buy_enabled=True,
            allow_fitted_ships=True,
            sell_structure_ids=[60000002],
            sell_structure_names=["Sell Alpha"],
            buy_structure_ids=[60000003],
            buy_structure_names=["Buy Beta"],
            allowed_market_groups_sell_by_structure={"60000002": [200]},
            allowed_market_groups_buy_by_structure={"60000003": None},
        )

    @staticmethod
    def _unwrap_view(view_func):
        unwrapped = view_func
        while hasattr(unwrapped, "__wrapped__"):
            unwrapped = unwrapped.__wrapped__
        return unwrapped

    @patch("indy_hub.views.navigation.build_nav_context", return_value={})
    @patch(
        "indy_hub.views.material_exchange_config._get_market_group_search_index_for_ids",
        return_value={200: {"label": "Manufacture & Research", "items": ["Mexallon"]}},
    )
    @patch(
        "indy_hub.views.material_exchange_config._build_market_group_index",
        return_value={
            200: {
                "id": 200,
                "name": "Manufacture & Research",
                "parent_market_group_id": None,
            }
        },
    )
    @patch(
        "indy_hub.views.material_exchange_config._get_market_group_tree",
        return_value=[
            {
                "id": 200,
                "label": "Manufacture & Research",
                "children": [],
                "expandable": False,
            }
        ],
    )
    @patch(
        "indy_hub.views.material_exchange_config._get_corp_hangar_divisions",
        return_value=({1: "Division 1"}, False),
    )
    @patch(
        "indy_hub.views.material_exchange_config._get_user_corporations",
        return_value=[
            {
                "id": 123456,
                "ticker": "TEST",
                "name": "Test Corporation",
            }
        ],
    )
    def test_config_page_renders_mockup_layout_and_live_controls(
        self,
        _mock_corps,
        _mock_divisions,
        _mock_tree,
        _mock_index,
        _mock_search,
        _mock_nav,
    ) -> None:
        request = self.factory.get(reverse("indy_hub:material_exchange_config"))
        request.user = self.user

        response = self._unwrap_view(material_exchange_config)(request, tokens=[])

        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")

        expected_sections = [
            "Buyback Configuration",
            "Hub Context",
            "Station Assignment",
            "Profiles Used By This Station",
            "Pricing Policy",
            "Price Overrides",
            "Configuration Summary",
            "Health",
            "Profile Preview",
            "Operations",
            "Add All Scopes",
            "Manage Tokens",
            "Open Capital Order Settings",
            "Save Configuration",
        ]
        for text in expected_sections:
            self.assertIn(text, content)

        expected_controls = [
            'id="configToastRegion"',
            'id="corpSelect"',
            'id="primaryStructureIdInput"',
            'id="sellStructureSelect"',
            'id="buyStructureSelect"',
            'id="hangarSelect"',
            'id="refreshAssetsBtn"',
            'id="itemPriceOverridesJson"',
            'id="marketGroupPriceOverridesJson"',
            'id="allowedMarketGroupsSellByStructureJson"',
            'id="allowedMarketGroupsBuyByStructureJson"',
            'id="buyMarketGroupStructureSelect"',
            'id="buyMarketGroupsAllowAll"',
            'id="sellMarketGroupTree"',
            'id="buyMarketGroupTree"',
            'id="marketGroupModeTop"',
            'id="marketGroupModeDeep"',
            'id="marketGroupModeDeepest"',
            'id="notifyAdminsOnSellAnomaly"',
            'id="isActiveToggle"',
            'id="submitBtn"',
        ]
        for control in expected_controls:
            self.assertIn(control, content)

        self.assertIn('"60000003": null', content)
        self.assertIn('showMaterialExchangeConfigToast', content)
        self.assertIn('me-market-tree-state is-partial', content)

        expected_fields = [
            'name="corporation_id"',
            'name="primary_structure_id"',
            'name="sell_structure_ids"',
            'name="buy_structure_ids"',
            'name="hangar_division"',
            'name="location_match_mode"',
            'name="sell_markup_percent"',
            'name="buy_markup_percent"',
            'name="item_price_overrides_json"',
            'name="market_group_price_overrides_json"',
            'name="allowed_market_groups_sell_by_structure_json"',
            'name="allowed_market_groups_sell_json"',
            'name="allowed_market_groups_buy_by_structure_json"',
            'name="allowed_market_groups_buy_json"',
            'name="sell_market_group_profiles_json"',
            'name="buy_market_group_profiles_json"',
            'name="notify_admins_on_sell_anomaly"',
            'name="is_active"',
        ]
        for field_name in expected_fields:
            self.assertIn(field_name, content)
