"""Tests for Capital Orders Dashboard Hook and Notification Integration."""

from decimal import Decimal

# Django
from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership, UserProfile
from allianceauth.eveonline.models import EveCharacter

# Local
from indy_hub.models import (
    CapitalShipOrder,
    CapitalShipOrderChat,
    MaterialExchangeConfig,
    MaterialExchangeSettings,
    UserOnboardingProgress,
)
from indy_hub.services.dashboard_hooks import get_capital_orders_dashboard_data
from indy_hub.utils.menu_badge import compute_menu_badge_count


def _assign_main_character(user: User, *, character_id: int) -> EveCharacter:
    character, _ = EveCharacter.objects.update_or_create(
        character_id=character_id,
        defaults={
            "character_name": f"Character {character_id}",
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


class CapitalOrdersDashboardHookTests(TestCase):
    def setUp(self):
        super().setUp()
        self.user = User.objects.create_user(username="cap_requester", password="password")
        self.builder = User.objects.create_user(username="cap_builder", password="password")
        _assign_main_character(self.user, character_id=987001)
        _assign_main_character(self.builder, character_id=987002)

        # Assign permissions
        perm_access = Permission.objects.get(codename="can_access_indy_hub")
        perm_build = Permission.objects.get(codename="can_build_capital_orders")
        perm_manage = Permission.objects.get(codename="can_manage_capital_orders")

        self.user.user_permissions.add(perm_access)
        self.builder.user_permissions.add(perm_access, perm_build, perm_manage)

        # Mark onboarding intro seen
        progress, _ = UserOnboardingProgress.objects.get_or_create(user=self.user)
        progress.mark_step("overview_intro_seen", True)
        progress.save(update_fields=["manual_steps", "updated_at"])

        # Enable MaterialExchange settings & active config
        settings_obj = MaterialExchangeSettings.get_solo()
        settings_obj.is_enabled = True
        settings_obj.save(update_fields=["is_enabled", "updated_at"])

        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=1234,
            structure_id=60003760,
            is_active=True,
        )

    def test_hook_returns_none_when_settings_disabled(self):
        settings_obj = MaterialExchangeSettings.get_solo()
        settings_obj.is_enabled = False
        settings_obj.save(update_fields=["is_enabled", "updated_at"])

        data = get_capital_orders_dashboard_data(self.user)
        self.assertIsNone(data)

    def test_hook_returns_empty_active_orders_when_none_exist(self):
        data = get_capital_orders_dashboard_data(self.user)
        self.assertIsNotNone(data)
        self.assertEqual(data["active_count"], 0)
        self.assertEqual(len(data["active_orders"]), 0)
        self.assertEqual(data["pending_offer_count"], 0)
        self.assertEqual(len(data["alerts"]), 0)

    def test_hook_surfaces_active_order_and_step_progress(self):
        order = CapitalShipOrder.objects.create(
            config=self.config,
            requester=self.user,
            ship_type_id=19720,
            ship_type_name="Revelation",
            ship_class=CapitalShipOrder.ShipClass.DREAD,
            reason=CapitalShipOrder.Reason.NO_CAP,
            status=CapitalShipOrder.Status.IN_PRODUCTION,
            agreed_price_isk=Decimal("5500000000.00"),
            likely_eta_min_days=3,
            likely_eta_max_days=5,
        )
        order.ensure_chat()

        data = get_capital_orders_dashboard_data(self.user)
        self.assertIsNotNone(data)
        self.assertEqual(data["active_count"], 1)

        active_order = data["active_orders"][0]
        self.assertEqual(active_order["ship_type_name"], "Revelation")
        self.assertEqual(active_order["order_reference"], order.order_reference)
        self.assertEqual(active_order["step"], 4)  # IN_PRODUCTION step
        self.assertIn("5,500,000,000.00 ISK", active_order["price_display"])
        self.assertIn("3–5 days", active_order["eta_display"])
        self.assertIn(reverse("indy_hub:capital_ship_orders"), active_order["url"])

    def test_hook_surfaces_pending_offer_confirmation_action_alert(self):
        order = CapitalShipOrder.objects.create(
            config=self.config,
            requester=self.user,
            ship_type_id=19720,
            ship_type_name="Revelation",
            ship_class=CapitalShipOrder.ShipClass.DREAD,
            reason=CapitalShipOrder.Reason.NO_CAP,
            status=CapitalShipOrder.Status.WAITING,
            offer_price_isk=Decimal("5400000000.00"),
            offer_eta_min_days=2,
            offer_eta_max_days=4,
            offer_updated_by=self.builder,
            offer_updated_at=timezone.now(),
        )
        chat = order.ensure_chat()

        data = get_capital_orders_dashboard_data(self.user)
        self.assertIsNotNone(data)
        self.assertEqual(data["pending_offer_count"], 1)
        self.assertEqual(len(data["alerts"]), 1)

        alert = data["alerts"][0]
        self.assertEqual(alert["type"], "capital_offer_pending")
        self.assertIn("5,400,000,000.00 ISK", alert["message"])
        self.assertIn(f"open_chat={chat.id}", alert["url"])

    def test_hook_surfaces_builder_queue_summary_for_builders(self):
        # Order 1: Unclaimed
        CapitalShipOrder.objects.create(
            config=self.config,
            requester=self.user,
            ship_type_id=19720,
            ship_type_name="Revelation",
            ship_class=CapitalShipOrder.ShipClass.DREAD,
            reason=CapitalShipOrder.Reason.NO_CAP,
            status=CapitalShipOrder.Status.WAITING,
        )
        # Order 2: In Production
        CapitalShipOrder.objects.create(
            config=self.config,
            requester=self.user,
            ship_type_id=23757,
            ship_type_name="Archon",
            ship_class=CapitalShipOrder.ShipClass.CARRIER,
            reason=CapitalShipOrder.Reason.ALT_NEEDS_CAP,
            status=CapitalShipOrder.Status.IN_PRODUCTION,
            in_production_by=self.builder,
        )

        data = get_capital_orders_dashboard_data(self.builder)
        self.assertIsNotNone(data)
        self.assertTrue(data["is_builder_or_manager"])
        self.assertIsNotNone(data["builder_queue_summary"])
        self.assertEqual(data["builder_queue_summary"]["unclaimed_count"], 1)
        self.assertEqual(data["builder_queue_summary"]["in_production_count"], 1)
        self.assertEqual(data["builder_queue_summary"]["total_active"], 2)

    def test_compute_menu_badge_includes_pending_capital_actions(self):
        initial_badge = compute_menu_badge_count(self.user.id)
        self.assertEqual(initial_badge, 0)

        # Create order with pending offer confirmation
        CapitalShipOrder.objects.create(
            config=self.config,
            requester=self.user,
            ship_type_id=19720,
            ship_type_name="Revelation",
            ship_class=CapitalShipOrder.ShipClass.DREAD,
            reason=CapitalShipOrder.Reason.NO_CAP,
            status=CapitalShipOrder.Status.WAITING,
            offer_price_isk=Decimal("5400000000.00"),
            offer_eta_min_days=2,
            offer_eta_max_days=4,
        )

        badge_after_offer = compute_menu_badge_count(self.user.id)
        self.assertEqual(badge_after_offer, 1)

    def test_dashboard_index_view_renders_capital_orders_hook(self):
        CapitalShipOrder.objects.create(
            config=self.config,
            requester=self.user,
            ship_type_id=19720,
            ship_type_name="Revelation",
            ship_class=CapitalShipOrder.ShipClass.DREAD,
            reason=CapitalShipOrder.Reason.NO_CAP,
            status=CapitalShipOrder.Status.WAITING,
            offer_price_isk=Decimal("5400000000.00"),
            offer_eta_min_days=2,
            offer_eta_max_days=4,
        )

        self.client.force_login(self.user)
        response = self.client.get(reverse("indy_hub:index"))

        self.assertEqual(response.status_code, 200)
        self.assertIn("capital_orders_dashboard", response.context)
        self.assertContains(response, "Capital orders")
        self.assertContains(response, "Revelation")
        self.assertContains(response, "INDY-CAP-")
