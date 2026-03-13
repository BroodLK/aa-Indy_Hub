"""Tests for manager-only Material Exchange stats history view."""

# Standard Library
from datetime import timedelta
from decimal import Decimal

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
    ESIContract,
    MaterialExchangeBuyOrder,
    MaterialExchangeConfig,
    MaterialExchangeSellOrder,
    MaterialExchangeTransaction,
)


def _assign_main_character(user: User, *, character_id: int) -> None:
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


def _grant_permissions(user: User, *codenames: str) -> None:
    permissions = Permission.objects.filter(
        content_type__app_label="indy_hub",
        codename__in=codenames,
    )
    user.user_permissions.add(*permissions)


class MaterialExchangeStatsHistoryViewTests(TestCase):
    def setUp(self) -> None:
        self.manager = User.objects.create_user("manager", password="secret123")
        _assign_main_character(self.manager, character_id=8010001)
        _grant_permissions(
            self.manager,
            "can_access_indy_hub",
            "can_manage_material_hub",
        )

        self.member = User.objects.create_user("member", password="secret123")
        _assign_main_character(self.member, character_id=8010002)
        _grant_permissions(self.member, "can_access_indy_hub")

        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456,
            structure_id=60003760,
            structure_name="Jita IV - Moon 4",
            hangar_division=1,
            is_active=True,
        )

    def test_stats_history_requires_manage_permission(self) -> None:
        self.client.force_login(self.member)
        response = self.client.get(reverse("indy_hub:material_exchange_stats_history"))
        self.assertEqual(response.status_code, 302)

    def test_stats_history_exposes_extended_manager_metrics(self) -> None:
        now = timezone.now()

        MaterialExchangeTransaction.objects.create(
            config=self.config,
            transaction_type=MaterialExchangeTransaction.TransactionType.SELL,
            user=self.member,
            type_id=34,
            type_name="Tritanium",
            quantity=10,
            unit_price=Decimal("100.00"),
            total_price=Decimal("1000.00"),
        )
        MaterialExchangeTransaction.objects.create(
            config=self.config,
            transaction_type=MaterialExchangeTransaction.TransactionType.BUY,
            user=self.member,
            type_id=34,
            type_name="Tritanium",
            quantity=5,
            unit_price=Decimal("150.00"),
            total_price=Decimal("750.00"),
            jita_buy_unit_price_snapshot=Decimal("120.00"),
            jita_sell_unit_price_snapshot=Decimal("170.00"),
            jita_split_unit_price_snapshot=Decimal("145.00"),
            jita_buy_total_value_snapshot=Decimal("600.00"),
            jita_sell_total_value_snapshot=Decimal("850.00"),
            jita_split_total_value_snapshot=Decimal("725.00"),
        )
        MaterialExchangeTransaction.objects.create(
            config=self.config,
            transaction_type=MaterialExchangeTransaction.TransactionType.BUY,
            user=self.member,
            type_id=35,
            type_name="Pyerite",
            quantity=1,
            unit_price=Decimal("100.00"),
            total_price=Decimal("100.00"),
        )

        MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.member,
            status=MaterialExchangeBuyOrder.Status.COMPLETED,
        )
        MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.member,
            status=MaterialExchangeBuyOrder.Status.REJECTED,
        )
        MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.member,
            status=MaterialExchangeSellOrder.Status.VALIDATED,
        )

        ESIContract.objects.create(
            contract_id=2000001,
            issuer_id=10,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=20,
            contract_type="item_exchange",
            status="finished",
            price=Decimal("1.00"),
            reward=Decimal("0.00"),
            collateral=Decimal("0.00"),
            date_issued=now - timedelta(days=1),
            date_expired=now + timedelta(days=7),
            corporation_id=self.config.corporation_id,
        )
        ESIContract.objects.create(
            contract_id=2000002,
            issuer_id=10,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=20,
            contract_type="item_exchange",
            status="rejected",
            price=Decimal("1.00"),
            reward=Decimal("0.00"),
            collateral=Decimal("0.00"),
            date_issued=now - timedelta(days=1),
            date_expired=now + timedelta(days=7),
            corporation_id=self.config.corporation_id,
        )
        ESIContract.objects.create(
            contract_id=2000003,
            issuer_id=10,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=20,
            contract_type="item_exchange",
            status="deleted",
            price=Decimal("1.00"),
            reward=Decimal("0.00"),
            collateral=Decimal("0.00"),
            date_issued=now - timedelta(days=1),
            date_expired=now + timedelta(days=7),
            corporation_id=self.config.corporation_id,
        )
        ESIContract.objects.create(
            contract_id=2000004,
            issuer_id=10,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=20,
            contract_type="item_exchange",
            status="deleted",
            price=Decimal("1.00"),
            reward=Decimal("0.00"),
            collateral=Decimal("0.00"),
            date_issued=now - timedelta(days=1),
            date_expired=now + timedelta(days=7),
            date_accepted=now - timedelta(hours=10),
            corporation_id=self.config.corporation_id,
        )

        self.client.force_login(self.manager)
        response = self.client.get(reverse("indy_hub:material_exchange_stats_history"))
        self.assertEqual(response.status_code, 200)

        ctx = response.context
        self.assertEqual(ctx["member_sales_volume"], Decimal("850"))
        self.assertEqual(ctx["jita_buy_value"], Decimal("600"))
        self.assertEqual(ctx["jita_sell_value"], Decimal("850"))
        self.assertEqual(ctx["jita_split_value"], Decimal("725"))
        self.assertEqual(ctx["actual_exchange_profit"], Decimal("-150"))
        self.assertEqual(ctx["realized_member_sale_profit"], Decimal("250.00"))
        self.assertEqual(ctx["potential_profit_jita_buy"], Decimal("100.00"))
        self.assertEqual(ctx["potential_profit_jita_sell"], Decimal("350.00"))
        self.assertEqual(ctx["potential_profit_jita_split"], Decimal("225.00"))
        self.assertEqual(ctx["snapshot_coverage_pct"], 50.0)
        self.assertEqual(ctx["potential_priced_type_count"], 1)

        self.assertEqual(ctx["contract_stats"]["total"], 4)
        self.assertEqual(ctx["contract_stats"]["completed"], 1)
        self.assertEqual(ctx["contract_stats"]["rejected"], 1)
        self.assertEqual(ctx["contract_stats"]["deleted"], 2)
        self.assertEqual(ctx["contract_stats"]["deleted_before_acceptance"], 1)
        self.assertEqual(ctx["contract_stats"]["deleted_after_acceptance"], 1)

        self.assertEqual(
            ctx["buy_order_status_counts"].get(MaterialExchangeBuyOrder.Status.COMPLETED),
            1,
        )
        self.assertEqual(
            ctx["buy_order_status_counts"].get(MaterialExchangeBuyOrder.Status.REJECTED),
            1,
        )
        self.assertEqual(
            ctx["sell_order_status_counts"].get(MaterialExchangeSellOrder.Status.VALIDATED),
            1,
        )

    def test_stats_history_filters_by_custom_date_range(self) -> None:
        now = timezone.now()

        older = MaterialExchangeTransaction.objects.create(
            config=self.config,
            transaction_type=MaterialExchangeTransaction.TransactionType.BUY,
            user=self.member,
            type_id=34,
            type_name="Tritanium",
            quantity=1,
            unit_price=Decimal("100.00"),
            total_price=Decimal("100.00"),
        )
        recent = MaterialExchangeTransaction.objects.create(
            config=self.config,
            transaction_type=MaterialExchangeTransaction.TransactionType.BUY,
            user=self.member,
            type_id=34,
            type_name="Tritanium",
            quantity=1,
            unit_price=Decimal("200.00"),
            total_price=Decimal("200.00"),
        )
        MaterialExchangeTransaction.objects.filter(pk=older.pk).update(
            completed_at=now - timedelta(days=10)
        )
        MaterialExchangeTransaction.objects.filter(pk=recent.pk).update(
            completed_at=now - timedelta(days=1)
        )

        start_date = (now - timedelta(days=2)).date().isoformat()
        end_date = now.date().isoformat()

        self.client.force_login(self.manager)
        response = self.client.get(
            reverse("indy_hub:material_exchange_stats_history"),
            {"start_date": start_date, "end_date": end_date},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["start_date"], start_date)
        self.assertEqual(response.context["end_date"], end_date)
        self.assertEqual(response.context["total_transactions"], 1)
        self.assertEqual(response.context["total_buy_volume"], Decimal("200"))
