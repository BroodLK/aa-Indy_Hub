"""Regression tests for material exchange order deletion permissions."""

# Django
from django.contrib.auth.models import Permission, User
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.urls import reverse

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership, UserProfile
from allianceauth.eveonline.models import EveCharacter

# AA Example App
# Local
from indy_hub.models import (
    MaterialExchangeBuyOrder,
    MaterialExchangeBuyOrderItem,
    MaterialExchangeConfig,
    MaterialExchangeSellOrder,
    MaterialExchangeSellOrderItem,
)


def _assign_main_character(user: User, *, character_id: int) -> EveCharacter:
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


def _grant_indy_permissions(user: User, *codenames: str) -> None:
    required = {"can_access_indy_hub"}
    required.update(codenames)
    permissions = Permission.objects.filter(codename__in=required)
    found = {perm.codename: perm for perm in permissions}
    missing = required - found.keys()
    if missing:
        raise AssertionError(f"Missing permissions: {sorted(missing)}")
    user.user_permissions.add(*found.values())


class MaterialExchangeOrderDeletePermissionTests(TestCase):
    def setUp(self) -> None:
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=60003760,
            structure_name="Test Structure",
            is_active=True,
        )
        self.owner = User.objects.create_user(username="order_owner")
        _assign_main_character(self.owner, character_id=72000001)
        _grant_indy_permissions(self.owner)

        self.manager = User.objects.create_user(username="hub_manager")
        _assign_main_character(self.manager, character_id=72000002)
        _grant_indy_permissions(self.manager, "can_manage_material_hub")

        self.other = User.objects.create_user(username="other_member")
        _assign_main_character(self.other, character_id=72000003)
        _grant_indy_permissions(self.other)

    def test_manager_can_delete_foreign_buy_order(self) -> None:
        order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.owner,
            status=MaterialExchangeBuyOrder.Status.DRAFT,
        )
        MaterialExchangeBuyOrderItem.objects.create(
            order=order,
            type_id=34,
            type_name="Tritanium",
            quantity=100,
            unit_price=10,
            total_price=1000,
            stock_available_at_creation=200,
        )

        self.client.force_login(self.manager)
        url = reverse("indy_hub:buy_order_delete", args=[order.id])

        get_response = self.client.get(url)
        self.assertEqual(get_response.status_code, 200)

        post_response = self.client.post(url, follow=True)
        self.assertEqual(post_response.status_code, 200)
        self.assertFalse(MaterialExchangeBuyOrder.objects.filter(id=order.id).exists())

    def test_legacy_manager_permission_can_delete_foreign_buy_order(self) -> None:
        order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.owner,
            status=MaterialExchangeBuyOrder.Status.DRAFT,
        )

        legacy_manager = User.objects.create_user(username="legacy_manager")
        _assign_main_character(legacy_manager, character_id=72000004)
        access_perm = Permission.objects.get(
            content_type__app_label="indy_hub",
            codename="can_access_indy_hub",
        )
        blueprint_ct = ContentType.objects.get(app_label="indy_hub", model="blueprint")
        legacy_manage_perm, _ = Permission.objects.get_or_create(
            content_type=blueprint_ct,
            codename="can_manage_material_exchange",
            defaults={"name": "Can manage Material Exchange (legacy)"},
        )
        legacy_manager.user_permissions.add(access_perm, legacy_manage_perm)

        self.client.force_login(legacy_manager)
        url = reverse("indy_hub:buy_order_delete", args=[order.id])

        get_response = self.client.get(url)
        self.assertEqual(get_response.status_code, 200)

    def test_buy_delete_legacy_aliases_resolve_for_manager(self) -> None:
        order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.owner,
            status=MaterialExchangeBuyOrder.Status.DRAFT,
        )

        self.client.force_login(self.manager)
        canonical_url = reverse("indy_hub:buy_order_delete", args=[order.id])
        legacy_urls = [
            canonical_url.rstrip("/"),
            canonical_url.replace("/my-orders/", "/my-order/"),
            canonical_url.replace("/my-orders/", "/my-order/").rstrip("/"),
        ]

        for url in legacy_urls:
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200)

    def test_manager_can_delete_terminal_buy_order(self) -> None:
        order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.owner,
            status=MaterialExchangeBuyOrder.Status.COMPLETED,
        )

        self.client.force_login(self.manager)
        url = reverse("indy_hub:buy_order_delete", args=[order.id])
        post_response = self.client.post(url, follow=True)

        self.assertEqual(post_response.status_code, 200)
        self.assertFalse(MaterialExchangeBuyOrder.objects.filter(id=order.id).exists())

    def test_manager_can_delete_foreign_sell_order(self) -> None:
        order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.other,
            status=MaterialExchangeSellOrder.Status.DRAFT,
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=order,
            type_id=34,
            type_name="Tritanium",
            quantity=100,
            unit_price=10,
            total_price=1000,
        )

        self.client.force_login(self.manager)
        url = reverse("indy_hub:sell_order_delete", args=[order.id])

        get_response = self.client.get(url)
        self.assertEqual(get_response.status_code, 200)

        post_response = self.client.post(url, follow=True)
        self.assertEqual(post_response.status_code, 200)
        self.assertFalse(MaterialExchangeSellOrder.objects.filter(id=order.id).exists())
