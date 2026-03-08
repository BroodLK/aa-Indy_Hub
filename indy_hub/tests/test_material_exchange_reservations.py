# Django
from django.contrib.auth.models import User
from django.test import TestCase
from django.utils import timezone

# Standard Library
from datetime import timedelta

# AA Example App
from indy_hub.models import (
    MaterialExchangeBuyOrder,
    MaterialExchangeBuyOrderItem,
    MaterialExchangeConfig,
    MaterialExchangeSellOrder,
    MaterialExchangeSellOrderItem,
)
from indy_hub.views.material_exchange import (
    _get_reserved_buy_quantities,
    _get_reserved_sell_quantities,
)


class MaterialExchangeReservationTests(TestCase):
    def setUp(self):
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=60003760,
            structure_name="Primary Hub",
            sell_structure_ids=[70000001, 70000002],
            sell_structure_names=["Alpha Hub", "Beta Hub"],
            buy_structure_ids=[70000001, 70000002],
            buy_structure_names=["Alpha Hub", "Beta Hub"],
            buy_enabled=True,
            is_active=True,
        )
        self.user_a = User.objects.create_user("buyer_a", password="secret123")
        self.user_b = User.objects.create_user("buyer_b", password="secret123")

    def _create_buy_item(self, *, status: str, quantity: int, type_id: int = 34) -> None:
        order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.user_a,
            status=status,
        )
        MaterialExchangeBuyOrderItem.objects.create(
            order=order,
            type_id=type_id,
            type_name="Tritanium",
            quantity=quantity,
            unit_price=1,
            total_price=quantity,
            stock_available_at_creation=999999,
        )

    def test_reserved_buy_quantities_include_only_active_statuses(self):
        self._create_buy_item(status=MaterialExchangeBuyOrder.Status.DRAFT, quantity=2)
        self._create_buy_item(
            status=MaterialExchangeBuyOrder.Status.AWAITING_VALIDATION,
            quantity=3,
        )
        self._create_buy_item(
            status=MaterialExchangeBuyOrder.Status.VALIDATED,
            quantity=4,
        )
        self._create_buy_item(
            status=MaterialExchangeBuyOrder.Status.REJECTED,
            quantity=50,
        )
        self._create_buy_item(
            status=MaterialExchangeBuyOrder.Status.CANCELLED,
            quantity=60,
        )
        self._create_buy_item(
            status=MaterialExchangeBuyOrder.Status.COMPLETED,
            quantity=70,
        )

        reserved = _get_reserved_buy_quantities(config=self.config, type_ids={34})
        self.assertEqual(reserved.get(34), 9)

    def test_reserved_buy_quantities_support_type_filter(self):
        self._create_buy_item(status=MaterialExchangeBuyOrder.Status.DRAFT, quantity=2, type_id=34)
        self._create_buy_item(status=MaterialExchangeBuyOrder.Status.DRAFT, quantity=5, type_id=35)

        reserved_34 = _get_reserved_buy_quantities(config=self.config, type_ids={34})
        reserved_35 = _get_reserved_buy_quantities(config=self.config, type_ids={35})
        self.assertEqual(reserved_34.get(34), 2)
        self.assertEqual(reserved_35.get(35), 5)

    def test_reserved_sell_quantities_are_per_user_and_location(self):
        order_alpha = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.user_a,
            status=MaterialExchangeSellOrder.Status.DRAFT,
            source_location_id=70000001,
            source_location_name="Alpha Hub",
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=order_alpha,
            type_id=34,
            type_name="Tritanium",
            quantity=5,
            unit_price=1,
            total_price=5,
        )

        order_beta = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.user_a,
            status=MaterialExchangeSellOrder.Status.VALIDATED,
            source_location_id=70000002,
            source_location_name="Beta Hub",
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=order_beta,
            type_id=34,
            type_name="Tritanium",
            quantity=3,
            unit_price=1,
            total_price=3,
        )

        order_unknown_location = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.user_a,
            status=MaterialExchangeSellOrder.Status.ANOMALY,
            source_location_id=None,
            source_location_name="",
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=order_unknown_location,
            type_id=34,
            type_name="Tritanium",
            quantity=4,
            unit_price=1,
            total_price=4,
        )

        order_other_user = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.user_b,
            status=MaterialExchangeSellOrder.Status.DRAFT,
            source_location_id=70000001,
            source_location_name="Alpha Hub",
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=order_other_user,
            type_id=34,
            type_name="Tritanium",
            quantity=100,
            unit_price=1,
            total_price=100,
        )

        order_terminal = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.user_a,
            status=MaterialExchangeSellOrder.Status.REJECTED,
            source_location_id=70000001,
            source_location_name="Alpha Hub",
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=order_terminal,
            type_id=34,
            type_name="Tritanium",
            quantity=200,
            unit_price=1,
            total_price=200,
        )

        reserved_alpha = _get_reserved_sell_quantities(
            config=self.config,
            seller=self.user_a,
            location_id=70000001,
            type_ids={34},
        )
        reserved_beta = _get_reserved_sell_quantities(
            config=self.config,
            seller=self.user_a,
            location_id=70000002,
            type_ids={34},
        )

        # Alpha: explicit alpha (5) + unknown location (4)
        self.assertEqual(reserved_alpha.get(34), 9)
        # Beta: explicit beta (3) + unknown location (4)
        self.assertEqual(reserved_beta.get(34), 7)

    def test_completed_sell_reservation_released_after_newer_asset_sync(self):
        completed_order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.user_a,
            status=MaterialExchangeSellOrder.Status.COMPLETED,
            source_location_id=70000001,
            source_location_name="Alpha Hub",
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=completed_order,
            type_id=34,
            type_name="Tritanium",
            quantity=8,
            unit_price=1,
            total_price=8,
        )

        completed_at = timezone.now()
        MaterialExchangeSellOrder.objects.filter(pk=completed_order.pk).update(
            updated_at=completed_at
        )

        # No post-completion asset sync yet -> still reserved.
        reserved_without_sync = _get_reserved_sell_quantities(
            config=self.config,
            seller=self.user_a,
            location_id=70000001,
            type_ids={34},
            assets_synced_at=None,
        )
        self.assertEqual(reserved_without_sync.get(34), 8)

        # Asset sync happened before completion -> still reserved.
        reserved_with_old_sync = _get_reserved_sell_quantities(
            config=self.config,
            seller=self.user_a,
            location_id=70000001,
            type_ids={34},
            assets_synced_at=completed_at - timedelta(minutes=1),
        )
        self.assertEqual(reserved_with_old_sync.get(34), 8)

        # Asset sync happened after completion -> reservation released.
        reserved_with_new_sync = _get_reserved_sell_quantities(
            config=self.config,
            seller=self.user_a,
            location_id=70000001,
            type_ids={34},
            assets_synced_at=completed_at + timedelta(minutes=1),
        )
        self.assertIsNone(reserved_with_new_sync.get(34))
