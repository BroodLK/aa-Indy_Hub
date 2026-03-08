# Standard Library
from unittest.mock import patch

# Django
from django.contrib.auth.models import User
from django.contrib.messages.storage.fallback import FallbackStorage
from django.contrib.sessions.middleware import SessionMiddleware
from django.test import RequestFactory, TestCase

# AA Example App
from indy_hub.models import MaterialExchangeConfig
from indy_hub.views.material_exchange_config import _handle_config_save


class MaterialExchangeConfigSaveCheckboxTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user("config-admin", password="testpass123")
        self.factory = RequestFactory()
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456,
            structure_id=60000001,
            structure_name="Test Structure",
            hangar_division=1,
            sell_markup_percent="0.00",
            sell_markup_base="buy",
            buy_markup_percent="5.00",
            buy_markup_base="buy",
            enforce_jita_price_bounds=True,
            notify_admins_on_sell_anomaly=True,
            is_active=True,
        )

    def _build_request(self, post_data):
        request = self.factory.post("/indy-hub/material-exchange/config/", post_data)
        request.user = self.user

        session_middleware = SessionMiddleware(lambda _request: None)
        session_middleware.process_request(request)
        request.session.save()
        setattr(request, "_messages", FallbackStorage(request))

        return request

    def _base_post_data(self):
        return {
            "corporation_id": str(self.config.corporation_id),
            "sell_structure_ids": [str(self.config.structure_id)],
            "buy_structure_ids": [str(self.config.structure_id)],
            "buy_enabled": "on",
            "location_match_mode": "name_or_id",
            "hangar_division": str(self.config.hangar_division),
            "sell_markup_percent": "0",
            "sell_markup_base": "buy",
            "buy_markup_percent": "5",
            "buy_markup_base": "buy",
        }

    def test_unchecked_notification_checkbox_is_saved_false(self):
        post_data = self._base_post_data()

        request = self._build_request(post_data)
        response = _handle_config_save(request, self.config)

        self.assertEqual(response.status_code, 302)
        self.config.refresh_from_db()
        self.assertFalse(self.config.notify_admins_on_sell_anomaly)

    def test_unchecked_enforce_bounds_checkbox_is_saved_false(self):
        post_data = self._base_post_data()

        request = self._build_request(post_data)
        response = _handle_config_save(request, self.config)

        self.assertEqual(response.status_code, 302)
        self.config.refresh_from_db()
        self.assertFalse(self.config.enforce_jita_price_bounds)

    def test_checked_checkboxes_are_saved_true(self):
        self.config.notify_admins_on_sell_anomaly = False
        self.config.enforce_jita_price_bounds = False
        self.config.allow_fitted_ships = False
        self.config.save(
            update_fields=[
                "notify_admins_on_sell_anomaly",
                "enforce_jita_price_bounds",
                "allow_fitted_ships",
            ]
        )

        post_data = self._base_post_data()
        post_data["notify_admins_on_sell_anomaly"] = "on"
        post_data["enforce_jita_price_bounds"] = "on"
        post_data["allow_fitted_ships"] = "on"

        request = self._build_request(post_data)
        response = _handle_config_save(request, self.config)

        self.assertEqual(response.status_code, 302)
        self.config.refresh_from_db()
        self.assertTrue(self.config.notify_admins_on_sell_anomaly)
        self.assertTrue(self.config.enforce_jita_price_bounds)
        self.assertTrue(self.config.allow_fitted_ships)

    def test_unchecked_allow_fitted_ships_is_saved_false(self):
        self.config.allow_fitted_ships = True
        self.config.save(update_fields=["allow_fitted_ships"])

        post_data = self._base_post_data()
        post_data.pop("allow_fitted_ships", None)

        request = self._build_request(post_data)
        response = _handle_config_save(request, self.config)

        self.assertEqual(response.status_code, 302)
        self.config.refresh_from_db()
        self.assertFalse(self.config.allow_fitted_ships)

    def test_is_active_keeps_existing_value_when_field_missing(self):
        self.config.is_active = False
        self.config.save(update_fields=["is_active"])

        post_data = self._base_post_data()
        post_data["notify_admins_on_sell_anomaly"] = "on"
        post_data["enforce_jita_price_bounds"] = "on"

        request = self._build_request(post_data)
        response = _handle_config_save(request, self.config)

        self.assertEqual(response.status_code, 302)
        self.config.refresh_from_db()
        self.assertFalse(self.config.is_active)

    def test_buy_enabled_requires_buy_locations(self):
        post_data = self._base_post_data()
        post_data["buy_enabled"] = "on"
        post_data["buy_structure_ids"] = []

        request = self._build_request(post_data)
        response = _handle_config_save(request, self.config)

        self.assertEqual(response.status_code, 302)
        self.config.refresh_from_db()
        self.assertTrue(self.config.buy_enabled)

    def test_buy_disabled_allows_empty_buy_locations(self):
        post_data = self._base_post_data()
        post_data.pop("buy_enabled", None)
        post_data["buy_structure_ids"] = []

        request = self._build_request(post_data)
        response = _handle_config_save(request, self.config)

        self.assertEqual(response.status_code, 302)
        self.config.refresh_from_db()
        self.assertFalse(self.config.buy_enabled)
        self.assertEqual(self.config.buy_structure_ids, [])

    def test_sell_market_groups_can_be_saved_per_structure(self):
        second_structure_id = int(self.config.structure_id) + 1
        post_data = self._base_post_data()
        post_data["sell_structure_ids"] = [
            str(self.config.structure_id),
            str(second_structure_id),
        ]
        post_data["buy_structure_ids"] = [str(self.config.structure_id)]
        post_data["allowed_market_groups_sell"] = ["200", "300"]
        post_data["allowed_market_groups_sell_by_structure_json"] = (
            f'{{"{int(self.config.structure_id)}":[200],"{second_structure_id}":null}}'
        )

        request = self._build_request(post_data)
        with patch(
            "indy_hub.views.material_exchange_config._get_industry_market_group_choice_ids",
            return_value=set(),
        ):
            response = _handle_config_save(request, self.config)

        self.assertEqual(response.status_code, 302)
        self.config.refresh_from_db()
        self.assertEqual(
            self.config.allowed_market_groups_sell_by_structure,
            {
                str(int(self.config.structure_id)): [200],
                str(second_structure_id): None,
            },
        )

    def test_sell_market_groups_payload_defaults_missing_structure_to_all(self):
        second_structure_id = int(self.config.structure_id) + 1
        post_data = self._base_post_data()
        post_data["sell_structure_ids"] = [
            str(self.config.structure_id),
            str(second_structure_id),
        ]
        post_data["buy_structure_ids"] = [str(self.config.structure_id)]
        post_data["allowed_market_groups_sell"] = ["200", "300"]
        post_data["allowed_market_groups_sell_by_structure_json"] = (
            f'{{"{int(self.config.structure_id)}":[200]}}'
        )

        request = self._build_request(post_data)
        with patch(
            "indy_hub.views.material_exchange_config._get_industry_market_group_choice_ids",
            return_value=set(),
        ):
            response = _handle_config_save(request, self.config)

        self.assertEqual(response.status_code, 302)
        self.config.refresh_from_db()
        self.assertEqual(
            self.config.allowed_market_groups_sell_by_structure,
            {
                str(int(self.config.structure_id)): [200],
                str(second_structure_id): None,
            },
        )

    def test_market_groups_can_be_saved_from_json_payload_fields(self):
        post_data = self._base_post_data()
        post_data["allowed_market_groups_buy_json"] = "[200,300]"
        post_data["allowed_market_groups_sell_json"] = "[400]"

        request = self._build_request(post_data)
        response = _handle_config_save(request, self.config)

        self.assertEqual(response.status_code, 302)
        self.config.refresh_from_db()
        self.assertEqual(self.config.allowed_market_groups_buy, [200, 300])
        self.assertEqual(self.config.allowed_market_groups_sell, [400])
        self.assertEqual(
            self.config.allowed_market_groups_sell_by_structure,
            {str(int(self.config.structure_id)): [400]},
        )

    def test_location_match_mode_defaults_to_name_or_id_when_invalid(self):
        post_data = self._base_post_data()
        post_data["location_match_mode"] = "invalid"

        request = self._build_request(post_data)
        response = _handle_config_save(request, self.config)

        self.assertEqual(response.status_code, 302)
        self.config.refresh_from_db()
        self.assertEqual(self.config.location_match_mode, "name_or_id")

    @patch("indy_hub.views.material_exchange_config.resolve_structure_names")
    @patch("indy_hub.views.material_exchange_config._get_corp_structures")
    def test_buy_locations_with_unknown_hangar_flags_are_allowed(
        self,
        mock_get_corp_structures,
        mock_resolve_structure_names,
    ):
        mock_get_corp_structures.return_value = (
            [
                {
                    "id": int(self.config.structure_id),
                    "name": "Test Structure",
                    "flags": [],
                }
            ],
            False,
        )
        mock_resolve_structure_names.return_value = {
            int(self.config.structure_id): "Test Structure"
        }

        post_data = self._base_post_data()
        request = self._build_request(post_data)
        response = _handle_config_save(request, self.config)

        self.assertEqual(response.status_code, 302)
        self.config.refresh_from_db()
        self.assertTrue(self.config.buy_enabled)
        self.assertEqual(
            self.config.buy_structure_ids,
            [int(self.config.structure_id)],
        )

    @patch("indy_hub.views.material_exchange_config.resolve_structure_names")
    @patch("indy_hub.views.material_exchange_config._get_corp_structures")
    def test_buy_locations_missing_required_hangar_flag_are_warning_only(
        self,
        mock_get_corp_structures,
        mock_resolve_structure_names,
    ):
        mock_get_corp_structures.return_value = (
            [
                {
                    "id": int(self.config.structure_id),
                    "name": "Test Structure",
                    "flags": ["CorpSAG1"],
                }
            ],
            False,
        )
        mock_resolve_structure_names.return_value = {
            int(self.config.structure_id): "Test Structure"
        }

        post_data = self._base_post_data()
        post_data["hangar_division"] = "7"
        request = self._build_request(post_data)
        response = _handle_config_save(request, self.config)

        self.assertEqual(response.status_code, 302)
        self.config.refresh_from_db()
        self.assertEqual(self.config.hangar_division, 7)
        self.assertTrue(self.config.buy_enabled)
        self.assertEqual(
            self.config.buy_structure_ids,
            [int(self.config.structure_id)],
        )
