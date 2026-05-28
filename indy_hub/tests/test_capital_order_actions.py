# Django
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
    CapitalShipOrderEvent,
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


class CapitalOrderActionsTests(TestCase):
    backend_path = "django.contrib.auth.backends.ModelBackend"

    def setUp(self) -> None:
        self.manager = User.objects.create_user("capmanager_actions", password="secret123")
        self.requester = User.objects.create_user("caprequester_actions", password="secret123")
        self.other_manager = User.objects.create_user("capmanager_other_actions", password="secret123")
        assign_main_character(self.manager, character_id=2025101)
        assign_main_character(self.requester, character_id=2025102)
        assign_main_character(self.other_manager, character_id=2025103)

        self._grant_perm(self.manager, "can_access_indy_hub", "can_manage_capital_orders")
        self._grant_perm(self.requester, "can_access_indy_hub")
        self._grant_perm(
            self.other_manager,
            "can_access_indy_hub",
            "can_manage_capital_orders",
        )

        settings_obj = MaterialExchangeSettings.get_solo()
        settings_obj.is_enabled = True
        settings_obj.save(update_fields=["is_enabled", "updated_at"])

        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=1234,
            structure_id=60003760,
            is_active=True,
        )

    def _grant_perm(self, user: User, *codenames: str) -> None:
        permissions = list(
            Permission.objects.filter(
                codename__in=codenames,
                content_type__app_label="indy_hub",
            )
        )
        user.user_permissions.add(*permissions)

    def _force_login(self, user: User) -> None:
        self.client.force_login(user, backend=self.backend_path)

    def _create_order(
        self,
        *,
        status: str = CapitalShipOrder.Status.WAITING,
    ) -> CapitalShipOrder:
        return CapitalShipOrder.objects.create(
            config=self.config,
            requester=self.requester,
            ship_type_id=19720,
            ship_type_name="Revelation",
            ship_class=CapitalShipOrder.ShipClass.DREAD,
            reason=CapitalShipOrder.Reason.NO_CAP,
            status=status,
        )

    def test_requester_page_shows_cancel_then_reopen_for_own_order(self) -> None:
        order = self._create_order(status=CapitalShipOrder.Status.WAITING)
        self._force_login(self.requester)

        response = self.client.get(reverse("indy_hub:capital_ship_orders"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(
            response,
            reverse("indy_hub:capital_ship_order_cancel", args=[order.id]),
        )

        response = self.client.post(reverse("indy_hub:capital_ship_order_cancel", args=[order.id]))
        self.assertEqual(response.status_code, 302)
        order.refresh_from_db()
        self.assertEqual(order.status, CapitalShipOrder.Status.CANCELLED)

        cancel_event = (
            order.events.filter(event_type=CapitalShipOrderEvent.EventType.STATUS_CHANGED)
            .order_by("-created_at", "-id")
            .first()
        )
        self.assertIsNotNone(cancel_event)
        self.assertEqual(cancel_event.payload.get("cancelled_by_role"), "requester")
        self.assertEqual(
            int(cancel_event.payload.get("cancelled_by_user_id") or 0),
            int(self.requester.id),
        )

        response = self.client.get(reverse("indy_hub:capital_ship_orders"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(
            response,
            reverse("indy_hub:capital_ship_order_uncancel", args=[order.id]),
        )

        response = self.client.post(reverse("indy_hub:capital_ship_order_uncancel", args=[order.id]))
        self.assertEqual(response.status_code, 302)
        order.refresh_from_db()
        self.assertEqual(order.status, CapitalShipOrder.Status.WAITING)

    def test_requester_cannot_reopen_manager_cancelled_order(self) -> None:
        order = self._create_order(status=CapitalShipOrder.Status.WAITING)

        self._force_login(self.manager)
        response = self.client.post(reverse("indy_hub:capital_ship_order_cancel", args=[order.id]))
        self.assertEqual(response.status_code, 302)
        order.refresh_from_db()
        self.assertEqual(order.status, CapitalShipOrder.Status.CANCELLED)

        self._force_login(self.requester)
        response = self.client.post(reverse("indy_hub:capital_ship_order_uncancel", args=[order.id]))
        self.assertEqual(response.status_code, 302)
        order.refresh_from_db()
        self.assertEqual(order.status, CapitalShipOrder.Status.CANCELLED)

    def test_admin_page_shows_release_claim_and_route(self) -> None:
        order = self._create_order(status=CapitalShipOrder.Status.GATHERING_MATERIALS)
        order.gathering_materials_by = self.manager
        order.save(update_fields=["gathering_materials_by", "updated_at"])

        self._force_login(self.manager)
        response = self.client.get(reverse("indy_hub:capital_ship_orders_admin"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'data-cap-action-form="release"')
        self.assertContains(response, "Release Claim")
        self.assertContains(
            response,
            reverse("indy_hub:capital_ship_order_release_claim", args=[order.id]),
        )

    def test_claiming_manager_can_release_claim_back_to_waiting(self) -> None:
        order = self._create_order(status=CapitalShipOrder.Status.WAITING)
        order.ensure_chat()

        self._force_login(self.manager)
        response = self.client.post(reverse("indy_hub:capital_ship_order_set_gathering_materials", args=[order.id]))
        self.assertEqual(response.status_code, 302)
        order.refresh_from_db()
        self.assertEqual(order.status, CapitalShipOrder.Status.GATHERING_MATERIALS)
        self.assertEqual(int(order.gathering_materials_by_id or 0), int(self.manager.id))

        order.esi_contract_id = 777001
        order.contract_created_at = timezone.now()
        order.definitive_eta_min_days = 4
        order.definitive_eta_max_days = 6
        order.definitive_eta_updated_at = timezone.now()
        order.definitive_eta_updated_by = self.manager
        order.save(
            update_fields=[
                "esi_contract_id",
                "contract_created_at",
                "definitive_eta_min_days",
                "definitive_eta_max_days",
                "definitive_eta_updated_at",
                "definitive_eta_updated_by",
                "updated_at",
            ]
        )

        response = self.client.post(reverse("indy_hub:capital_ship_order_release_claim", args=[order.id]))
        self.assertEqual(response.status_code, 302)
        order.refresh_from_db()
        self.assertEqual(order.status, CapitalShipOrder.Status.WAITING)
        self.assertIsNone(order.gathering_materials_by_id)
        self.assertIsNone(order.gathering_materials_at)
        self.assertIsNone(order.in_production_by_id)
        self.assertIsNone(order.in_production_at)
        self.assertIsNone(order.esi_contract_id)
        self.assertIsNone(order.contract_created_at)
        self.assertIsNone(order.definitive_eta_min_days)
        self.assertIsNone(order.definitive_eta_max_days)
        self.assertIsNone(order.definitive_eta_updated_at)
        self.assertIsNone(order.definitive_eta_updated_by_id)

        self._force_login(self.other_manager)
        response = self.client.get(reverse("indy_hub:capital_ship_order_chat_history", args=[order.id]))
        self.assertEqual(response.status_code, 200)
