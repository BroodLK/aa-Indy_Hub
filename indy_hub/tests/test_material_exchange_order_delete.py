"""Regression tests for material exchange order deletion permissions."""

# Django
from django.contrib.auth.models import Permission, User
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.urls import reverse

# Local
from indy_hub.models import (
    MaterialExchangeBuyOrder,
    MaterialExchangeBuyOrderItem,
    MaterialExchangeConfig,
    MaterialExchangeSellOrder,
    MaterialExchangeSellOrderItem,
)


class MaterialExchangeOrderDeletePermissionTests(TestCase):
    def setUp(self) -> None:
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=60003760,
            structure_name="Test Structure",
            is_active=True,
        )
        self.owner = User.objects.create_user(username="order_owner")
        self.manager = User.objects.create_user(username="hub_manager")
        self.other = User.objects.create_user(username="other_member")

        perms = Permission.objects.filter(
            content_type__app_label="indy_hub",
            codename__in=["can_access_indy_hub", "can_manage_material_hub"],
        )
        perm_map = {permission.codename: permission for permission in perms}
        missing = {"can_access_indy_hub", "can_manage_material_hub"} - set(perm_map)
        self.assertFalse(missing, f"Missing required test permissions: {missing}")

        self.owner.user_permissions.add(perm_map["can_access_indy_hub"])
        self.other.user_permissions.add(perm_map["can_access_indy_hub"])
        self.manager.user_permissions.add(
            perm_map["can_access_indy_hub"],
            perm_map["can_manage_material_hub"],
        )

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
