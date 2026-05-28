# Standard Library
from unittest.mock import patch

# Django
from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership, UserProfile
from allianceauth.eveonline.models import EveCharacter

# AA Example App
from indy_hub.models import (
    MaterialExchangeBuyOrder,
    MaterialExchangeConfig,
    MaterialExchangeSellOrder,
)


def assign_main_character(user: User, *, character_id: int, name: str) -> None:
    character, _ = EveCharacter.objects.get_or_create(
        character_id=character_id,
        defaults={
            "character_name": name,
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


class MaterialExchangeAdminOrderActionsTests(TestCase):
    def setUp(self) -> None:
        self.admin = User.objects.create_user("buyback-admin", password="secret123")
        self.member = User.objects.create_user("buyback-member", password="secret123")

        assign_main_character(
            self.admin,
            character_id=7010001,
            name="Buyback Admin",
        )
        assign_main_character(
            self.member,
            character_id=7010002,
            name="Buyback Member",
        )

        permissions = Permission.objects.filter(
            content_type__app_label="indy_hub",
            codename__in=["can_access_indy_hub", "can_manage_material_hub"],
        )
        self.admin.user_permissions.add(*permissions)
        self.member.user_permissions.add(
            Permission.objects.get(
                content_type__app_label="indy_hub",
                codename="can_access_indy_hub",
            )
        )

        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456,
            structure_id=60000001,
            structure_name="Test Structure",
            hangar_division=1,
            sell_markup_percent="0.00",
            sell_markup_base="buy",
            buy_markup_percent="5.00",
            buy_markup_base="buy",
            enforce_jita_price_bounds=False,
            notify_admins_on_sell_anomaly=True,
            is_active=True,
        )

    def test_admin_index_links_to_closed_orders_history(self) -> None:
        self.client.force_login(self.admin)

        response = self.client.get(reverse("indy_hub:material_exchange_index"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, reverse("indy_hub:material_exchange_history"))
        self.assertContains(
            response,
            f"{reverse('indy_hub:material_exchange_history')}?status=rejected",
        )

    def test_reject_and_reopen_sell_order_restores_previous_status(self) -> None:
        order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.member,
            status=MaterialExchangeSellOrder.Status.VALIDATED,
        )

        self.client.force_login(self.admin)
        response = self.client.post(
            reverse("indy_hub:material_exchange_reject_sell", args=[order.id]),
            {"next": reverse("indy_hub:material_exchange_history")},
        )
        self.assertEqual(response.status_code, 302)

        order.refresh_from_db()
        self.assertEqual(order.status, MaterialExchangeSellOrder.Status.REJECTED)
        self.assertEqual(order.status_before_rejection, MaterialExchangeSellOrder.Status.VALIDATED)

        response = self.client.post(
            reverse("indy_hub:material_exchange_reopen_sell", args=[order.id]),
            {"next": reverse("indy_hub:material_exchange_history")},
        )
        self.assertEqual(response.status_code, 302)

        order.refresh_from_db()
        self.assertEqual(order.status, MaterialExchangeSellOrder.Status.VALIDATED)
        self.assertEqual(order.status_before_rejection, "")

    @patch("indy_hub.notifications.notify_user")
    def test_reject_and_reopen_buy_order_restores_previous_status(self, mock_notify_user) -> None:
        order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.member,
            status=MaterialExchangeBuyOrder.Status.VALIDATED,
        )

        self.client.force_login(self.admin)
        response = self.client.post(
            reverse("indy_hub:material_exchange_reject_buy", args=[order.id]),
            {"next": reverse("indy_hub:material_exchange_history")},
        )
        self.assertEqual(response.status_code, 302)
        mock_notify_user.assert_called_once()

        order.refresh_from_db()
        self.assertEqual(order.status, MaterialExchangeBuyOrder.Status.REJECTED)
        self.assertEqual(order.status_before_rejection, MaterialExchangeBuyOrder.Status.VALIDATED)

        response = self.client.post(
            reverse("indy_hub:material_exchange_reopen_buy", args=[order.id]),
            {"next": reverse("indy_hub:material_exchange_history")},
        )
        self.assertEqual(response.status_code, 302)

        order.refresh_from_db()
        self.assertEqual(order.status, MaterialExchangeBuyOrder.Status.VALIDATED)
        self.assertEqual(order.status_before_rejection, "")

    def test_history_page_shows_reopen_actions_for_rejected_orders(self) -> None:
        sell_order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.member,
            status=MaterialExchangeSellOrder.Status.REJECTED,
            status_before_rejection=MaterialExchangeSellOrder.Status.ANOMALY,
        )
        buy_order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.member,
            status=MaterialExchangeBuyOrder.Status.REJECTED,
            status_before_rejection=MaterialExchangeBuyOrder.Status.DRAFT,
        )

        self.client.force_login(self.admin)
        response = self.client.get(reverse("indy_hub:material_exchange_history") + "?status=rejected")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, reverse("indy_hub:material_exchange_reopen_sell", args=[sell_order.id]))
        self.assertContains(response, reverse("indy_hub:material_exchange_reopen_buy", args=[buy_order.id]))
