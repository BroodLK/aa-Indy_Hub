# Django
from unittest.mock import patch
from decimal import Decimal
from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership, UserProfile
from allianceauth.eveonline.models import EveCharacter

# AA Example App
from indy_hub.models import (
    CapitalShipOrder,
    MaterialExchangeConfig,
    MaterialExchangeSettings,
)


def assign_main_character(user: User, *, character_id: int) -> EveCharacter:
    character, _ = EveCharacter.objects.get_or_create(
        character_id=character_id,
        defaults={
            "character_name": f"Pilot {character_id}",
            "corporation_id": 2_000_000,
            "corporation_name": "Test Corp",
            "corporation_ticker": "TEST",
        },
    )
    CharacterOwnership.objects.update_or_create(
        user=user,
        character=character,
        defaults={"owner_hash": f"hash-{character_id}-{user.id}"},
    )
    profile, _ = UserProfile.objects.get_or_create(user=user)
    profile.main_character = character
    profile.save(update_fields=["main_character"])
    return character


class CapitalOrderUpfrontPaymentTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        settings_obj = MaterialExchangeSettings.get_solo()
        settings_obj.is_enabled = True
        settings_obj.save(update_fields=["is_enabled", "updated_at"])
        self.config = MaterialExchangeConfig.objects.create(
            is_active=True,
            structure_id=60003760,
            corporation_id=1234,
            hangar_division=1,
        )

        self.pilot = User.objects.create_user(username="test_pilot", password="password")
        self.pilot.user_permissions.add(Permission.objects.get(codename="can_access_indy_hub"))
        assign_main_character(self.pilot, character_id=9001)

        self.manager = User.objects.create_user(username="cap_manager", password="password")
        self.manager.user_permissions.add(
            Permission.objects.get(codename="can_access_indy_hub"),
            Permission.objects.get(codename="can_manage_capital_orders"),
        )
        assign_main_character(self.manager, character_id=9002)

    def _force_login(self, user: User) -> None:
        self.client.force_login(user)

    @patch("indy_hub.views.capital_ship_orders._load_capital_ship_options_for_editor")
    def test_config_view_saves_upfront_payment_settings(self, mock_editor_options) -> None:
        mock_editor_options.return_value = [
            {
                "type_id": 19720,
                "type_name": "Revelation",
                "ship_class": "dread",
                "ship_class_label": "Dreadnought",
                "enabled": True,
            }
        ]
        self._force_login(self.manager)
        url = reverse("indy_hub:capital_ship_orders_config")

        response = self.client.post(
            url,
            {
                "capital_default_lead_time_days": "5",
                "capital_auto_cancel_delay_value": "0",
                "capital_auto_cancel_delay_unit": "hours",
                "capital_auto_cancel_preapproved_state_names": ["Pre-Approved"],
                "capital_upfront_payment_required": "on",
                "capital_upfront_payment_reason": "High mineral market volatility requires pre-funding.",
                "capital_upfront_payment_refunds_allowed": "on",
            },
        )
        self.assertEqual(response.status_code, 302)

        self.config.refresh_from_db()
        self.assertTrue(self.config.capital_upfront_payment_required)
        self.assertEqual(
            self.config.capital_upfront_payment_reason,
            "High mineral market volatility requires pre-funding.",
        )
        self.assertTrue(self.config.capital_upfront_payment_refunds_allowed)

    @patch("indy_hub.views.capital_ship_orders._load_capital_ship_options")
    def test_order_view_renders_alert_and_modal_when_upfront_required(
        self, mock_options
    ) -> None:
        mock_options.return_value = [
            {
                "type_id": 19720,
                "type_name": "Revelation",
                "ship_class": "dread",
                "ship_class_label": "Dreadnought",
                "enabled": True,
                "estimated_price": Decimal("4850000000.00"),
            }
        ]
        self.config.capital_upfront_payment_required = True
        self.config.capital_upfront_payment_reason = "Payment must be secured before build."
        self.config.capital_upfront_payment_refunds_allowed = True
        self.config.save(
            update_fields=[
                "capital_upfront_payment_required",
                "capital_upfront_payment_reason",
                "capital_upfront_payment_refunds_allowed",
            ]
        )

        self._force_login(self.pilot)
        url = reverse("indy_hub:capital_ship_orders")
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Notice: Upfront Payment Required")
        self.assertContains(response, "Payment must be secured before build.")
        self.assertContains(response, "Structure Loss Protection")
        self.assertContains(response, "upfrontPaymentModal")

    @patch("indy_hub.views.capital_ship_orders._load_capital_ship_options")
    def test_order_submission_requires_agreement_when_upfront_required(
        self, mock_options
    ) -> None:
        mock_options.return_value = [
            {
                "type_id": 19720,
                "type_name": "Revelation",
                "ship_class": "dread",
                "ship_class_label": "Dreadnought",
                "enabled": True,
                "estimated_price": Decimal("4850000000.00"),
            }
        ]
        self.config.capital_upfront_payment_required = True
        self.config.capital_upfront_payment_reason = "Payment must be secured before build."
        self.config.capital_upfront_payment_refunds_allowed = False
        self.config.save(
            update_fields=[
                "capital_upfront_payment_required",
                "capital_upfront_payment_reason",
                "capital_upfront_payment_refunds_allowed",
            ]
        )

        self._force_login(self.pilot)
        url = reverse("indy_hub:capital_ship_orders")

        # Attempt to order without agreeing
        response = self.client.post(
            url,
            {
                "ship_type_id": "19720",
                "reason": "no_cap",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(CapitalShipOrder.objects.count(), 0)

        # Attempt to order with agree_upfront_payment
        response = self.client.post(
            url,
            {
                "ship_type_id": "19720",
                "reason": "no_cap",
                "agree_upfront_payment": "1",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(CapitalShipOrder.objects.count(), 1)

        order = CapitalShipOrder.objects.first()
        self.assertIsNotNone(order)
        self.assertTrue(order.upfront_payment_required)
        self.assertEqual(order.upfront_payment_reason, "Payment must be secured before build.")
        self.assertFalse(order.upfront_payment_refunds_allowed)
        self.assertIsNotNone(order.upfront_payment_agreed_at)
