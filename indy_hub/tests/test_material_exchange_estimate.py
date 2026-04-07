# Standard Library
from decimal import Decimal
from unittest.mock import patch

# Django
from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership, UserProfile
from allianceauth.eveonline.models import EveCharacter

# AA Example App
from indy_hub.models import MaterialExchangeConfig, MaterialExchangeSettings
from indy_hub.views.material_exchange import _parse_sell_estimate_input


class MaterialExchangeSellEstimateTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user("estimate_user", password="secret123")
        permission = Permission.objects.get(
            content_type__app_label="indy_hub",
            codename="can_access_indy_hub",
        )
        self.user.user_permissions.add(permission)

        character, _created = EveCharacter.objects.get_or_create(
            character_id=72000001,
            defaults={
                "character_name": "Estimate Main",
                "corporation_id": 2000001,
                "corporation_name": "Test Corp",
                "corporation_ticker": "TEST",
            },
        )
        CharacterOwnership.objects.update_or_create(
            user=self.user,
            character=character,
            defaults={"owner_hash": f"hash-{self.user.id}-72000001"},
        )
        profile, _created = UserProfile.objects.get_or_create(user=self.user)
        profile.main_character = character
        profile.save(update_fields=["main_character"])

        settings_obj = MaterialExchangeSettings.get_solo()
        settings_obj.is_enabled = True
        settings_obj.save(update_fields=["is_enabled"])

        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=70000001,
            structure_name="Primary Hub",
            sell_structure_ids=[70000001, 70000002],
            sell_structure_names=["Alpha Citadel", "Beta Citadel"],
            sell_markup_percent=Decimal("0"),
            sell_markup_base="buy",
            is_active=True,
        )

        self.client.force_login(self.user)

    def test_index_renders_get_estimate_button_and_modal(self):
        response = self.client.get(reverse("indy_hub:material_exchange_index"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Get an Estimate")
        self.assertContains(response, 'id="sellEstimateModal"')
        self.assertContains(
            response, reverse("indy_hub:material_exchange_sell_estimate")
        )

    @patch("indy_hub.views.material_exchange._get_allowed_type_ids_for_config")
    @patch("indy_hub.views.material_exchange._compute_effective_sell_unit_price")
    @patch("indy_hub.views.material_exchange._fetch_fuzzwork_prices")
    @patch("indy_hub.views.material_exchange.get_type_name")
    @patch("indy_hub.views.material_exchange._resolve_type_id_from_sell_estimate_text")
    def test_estimate_endpoint_returns_itemized_rows_with_accepting_locations(
        self,
        mock_resolve_type_id,
        mock_get_type_name,
        mock_fetch_prices,
        mock_compute_sell_price,
        mock_allowed_ids,
    ):
        type_name_map = {34: "Tritanium", 35: "Pyerite"}

        def resolve_type_side_effect(value: str):
            normalized = str(value).strip().lower()
            if normalized == "tritanium":
                return 34
            if normalized == "pyerite":
                return 35
            return None

        def allowed_side_effect(config, mode, structure_id=None):
            if mode != "sell":
                return None
            if int(structure_id or 0) == 70000001:
                return {34}
            if int(structure_id or 0) == 70000002:
                return {34, 35}
            return set()

        def compute_price_side_effect(**kwargs):
            type_id = int(kwargs["type_id"])
            if type_id == 34:
                return Decimal("4.00"), Decimal("4.00"), False
            if type_id == 35:
                return Decimal("8.00"), Decimal("8.00"), False
            return Decimal("0"), Decimal("0"), False

        mock_resolve_type_id.side_effect = resolve_type_side_effect
        mock_get_type_name.side_effect = lambda type_id: type_name_map.get(
            int(type_id), f"Type {type_id}"
        )
        mock_fetch_prices.return_value = {
            34: {"buy": Decimal("4.00"), "sell": Decimal("4.20")},
            35: {"buy": Decimal("8.00"), "sell": Decimal("8.40")},
        }
        mock_compute_sell_price.side_effect = compute_price_side_effect
        mock_allowed_ids.side_effect = allowed_side_effect

        response = self.client.post(
            reverse("indy_hub:material_exchange_sell_estimate"),
            {
                "estimate_text": "Tritanium\t10\nPyerite 5\nUnknown Item 3",
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["estimated_total"], "80.00")
        self.assertEqual(payload["rounded_estimated_total"], "80")
        self.assertEqual(payload["invalid_lines"], ["Unknown Item 3"])

        rows_by_type = {
            int(row["type_id"]): row for row in payload.get("items", [])
        }
        self.assertEqual(
            rows_by_type[34]["accepted_locations"],
            ["Alpha Citadel", "Beta Citadel"],
        )
        self.assertEqual(
            rows_by_type[35]["accepted_locations"],
            ["Beta Citadel"],
        )
        self.assertEqual(rows_by_type[34]["unit_price"], "4.00")
        self.assertEqual(rows_by_type[34]["total_price"], "40.00")
        self.assertEqual(rows_by_type[35]["unit_price"], "8.00")
        self.assertEqual(rows_by_type[35]["total_price"], "40.00")

    @patch("indy_hub.views.material_exchange._resolve_type_id_from_sell_estimate_text")
    @patch("indy_hub.views.material_exchange.get_type_name")
    def test_parse_sell_estimate_input_supports_tab_and_space_format(
        self,
        mock_get_type_name,
        mock_resolve_type_id,
    ):
        mock_get_type_name.side_effect = lambda type_id: {
            34: "Tritanium",
            35: "Pyerite",
        }.get(int(type_id), f"Type {type_id}")
        mock_resolve_type_id.side_effect = lambda value: {
            "tritanium": 34,
            "pyerite": 35,
        }.get(str(value).strip().lower())

        rows, invalid_lines = _parse_sell_estimate_input(
            "Tritanium\t10\nTritanium 15\nPyerite 3\nInvalidLine"
        )

        rows_by_type = {int(row["type_id"]): int(row["quantity"]) for row in rows}
        self.assertEqual(rows_by_type, {34: 25, 35: 3})
        self.assertEqual(invalid_lines, ["InvalidLine"])

    @patch("indy_hub.views.material_exchange._resolve_type_id_from_sell_estimate_text")
    def test_estimate_endpoint_returns_400_when_no_valid_lines(self, mock_resolve_type_id):
        mock_resolve_type_id.return_value = None

        response = self.client.post(
            reverse("indy_hub:material_exchange_sell_estimate"),
            {"estimate_text": "Unknown Item 10"},
        )

        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertFalse(payload["ok"])
        self.assertIn("No valid lines were detected", payload["summary"])
