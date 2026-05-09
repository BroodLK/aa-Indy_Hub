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


class MaterialExchangeBuyMultibuyParseTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user("buy_import_user", password="secret123")
        permission = Permission.objects.get(
            content_type__app_label="indy_hub",
            codename="can_access_indy_hub",
        )
        self.user.user_permissions.add(permission)

        character, _created = EveCharacter.objects.get_or_create(
            character_id=73000001,
            defaults={
                "character_name": "Buy Import Main",
                "corporation_id": 2000001,
                "corporation_name": "Test Corp",
                "corporation_ticker": "TEST",
            },
        )
        CharacterOwnership.objects.update_or_create(
            user=self.user,
            character=character,
            defaults={"owner_hash": f"hash-{self.user.id}-73000001"},
        )
        profile, _created = UserProfile.objects.get_or_create(user=self.user)
        profile.main_character = character
        profile.save(update_fields=["main_character"])

        settings_obj = MaterialExchangeSettings.get_solo()
        settings_obj.is_enabled = True
        settings_obj.save(update_fields=["is_enabled"])

        MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=70000001,
            structure_name="Primary Hub",
            buy_structure_ids=[70000001, 70000002],
            buy_structure_names=["Alpha Citadel", "Beta Citadel"],
            buy_enabled=True,
            is_active=True,
        )

        self.client.force_login(self.user)

    @staticmethod
    def _resolve_type_ids(values: list[str]) -> dict[str, int | None]:
        resolved: dict[str, int | None] = {}
        for value in values:
            normalized = str(value).strip().lower()
            if normalized == "tritanium":
                resolved[normalized] = 34
            elif normalized == "pyerite":
                resolved[normalized] = 35
            else:
                resolved[normalized] = None
        return resolved

    def test_parse_sell_estimate_input_supports_leading_quantity_format(self):
        from unittest.mock import patch

        with patch(
            "indy_hub.views.material_exchange._resolve_type_ids_for_sell_estimate_texts",
            side_effect=self._resolve_type_ids,
        ):
            rows, invalid_lines = _parse_sell_estimate_input(
                "10 Tritanium\nPyerite 5\n3 Tritanium\nBad Line"
            )

        rows_by_type = {int(row["type_id"]): int(row["quantity"]) for row in rows}
        self.assertEqual(rows_by_type, {34: 13, 35: 5})
        self.assertEqual(invalid_lines, ["Bad Line"])

    def test_buy_multibuy_parse_endpoint_returns_type_ids_and_invalid_lines(self):
        from unittest.mock import patch

        with patch(
            "indy_hub.views.material_exchange._resolve_type_ids_for_sell_estimate_texts",
            side_effect=self._resolve_type_ids,
        ), patch(
            "indy_hub.views.material_exchange._get_type_name_map",
            return_value={34: "Tritanium", 35: "Pyerite"},
        ):
            response = self.client.post(
                reverse("indy_hub:material_exchange_buy_multibuy_parse"),
                {"multibuy_text": "Tritanium\t10\n5 Pyerite\nUnknown Item 3"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["invalid_lines"], ["Unknown Item 3"])
        self.assertEqual(
            payload["items"],
            [
                {"type_id": 34, "type_name": "Tritanium", "quantity": 10},
                {"type_id": 35, "type_name": "Pyerite", "quantity": 5},
            ],
        )

    def test_sell_multibuy_parse_endpoint_returns_type_ids_and_invalid_lines(self):
        from unittest.mock import patch

        with patch(
            "indy_hub.views.material_exchange._resolve_type_ids_for_sell_estimate_texts",
            side_effect=self._resolve_type_ids,
        ), patch(
            "indy_hub.views.material_exchange._get_type_name_map",
            return_value={34: "Tritanium", 35: "Pyerite"},
        ):
            response = self.client.post(
                reverse("indy_hub:material_exchange_sell_multibuy_parse"),
                {"multibuy_text": "Tritanium\t10\n5 Pyerite\nUnknown Item 3"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["invalid_lines"], ["Unknown Item 3"])
        self.assertEqual(
            payload["items"],
            [
                {"type_id": 34, "type_name": "Tritanium", "quantity": 10},
                {"type_id": 35, "type_name": "Pyerite", "quantity": 5},
            ],
        )
