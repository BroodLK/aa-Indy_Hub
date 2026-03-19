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
    MaterialExchangeBuyOrderItem,
    MaterialExchangeConfig,
    MaterialExchangeSellOrderItem,
    MaterialExchangeSellOrder,
    MaterialExchangeSettings,
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

        sell_order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.member,
            status=MaterialExchangeSellOrder.Status.COMPLETED,
            esi_contract_id=2000101,
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=sell_order,
            type_id=34,
            type_name="Tritanium",
            quantity=10,
            unit_price=Decimal("100.00"),
            total_price=Decimal("1000.00"),
        )

        buy_order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.member,
            status=MaterialExchangeBuyOrder.Status.COMPLETED,
            esi_contract_id=2000102,
        )
        MaterialExchangeBuyOrderItem.objects.create(
            order=buy_order,
            type_id=34,
            type_name="Tritanium",
            quantity=5,
            unit_price=Decimal("150.00"),
            total_price=Decimal("750.00"),
            stock_available_at_creation=999,
        )

        fallback_buy_order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.member,
            status=MaterialExchangeBuyOrder.Status.REJECTED,
        )
        MaterialExchangeBuyOrderItem.objects.create(
            order=fallback_buy_order,
            type_id=35,
            type_name="Pyerite",
            quantity=1,
            unit_price=Decimal("100.00"),
            total_price=Decimal("100.00"),
            stock_available_at_creation=999,
        )

        MaterialExchangeTransaction.objects.create(
            config=self.config,
            transaction_type=MaterialExchangeTransaction.TransactionType.SELL,
            sell_order=sell_order,
            user=self.member,
            type_id=34,
            type_name="Tritanium",
            quantity=10,
            unit_price=Decimal("100.00"),
            total_price=Decimal("1000.00"),
            jita_buy_total_value_snapshot=Decimal("900.00"),
            jita_sell_total_value_snapshot=Decimal("1100.00"),
            jita_split_total_value_snapshot=Decimal("1000.00"),
        )
        MaterialExchangeTransaction.objects.create(
            config=self.config,
            transaction_type=MaterialExchangeTransaction.TransactionType.BUY,
            buy_order=buy_order,
            user=self.member,
            type_id=34,
            type_name="Tritanium",
            quantity=5,
            unit_price=Decimal("150.00"),
            total_price=Decimal("750.00"),
            jita_buy_total_value_snapshot=Decimal("600.00"),
            jita_sell_total_value_snapshot=Decimal("850.00"),
            jita_split_total_value_snapshot=Decimal("725.00"),
        )

        ESIContract.objects.create(
            contract_id=2000101,
            issuer_id=10,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=20,
            contract_type="item_exchange",
            status="finished",
            price=Decimal("950.00"),
            reward=Decimal("0.00"),
            collateral=Decimal("0.00"),
            date_issued=now - timedelta(days=1),
            date_expired=now + timedelta(days=7),
            corporation_id=self.config.corporation_id,
        )
        ESIContract.objects.create(
            contract_id=2000102,
            issuer_id=10,
            issuer_corporation_id=self.config.corporation_id,
            assignee_id=20,
            contract_type="item_exchange",
            status="finished",
            price=Decimal("780.00"),
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
        self.assertEqual(ctx["sell_expected_cost_total"], Decimal("1000"))
        self.assertEqual(ctx["sell_actual_cost_total"], Decimal("950"))
        self.assertEqual(ctx["sell_cost_delta"], Decimal("-50"))
        self.assertEqual(ctx["buy_expected_jita_sell_total"], Decimal("850"))
        self.assertEqual(ctx["buy_expected_jita_buy_total"], Decimal("600"))
        self.assertEqual(ctx["buy_actual_revenue_total"], Decimal("780"))
        self.assertEqual(ctx["buy_revenue_delta_jita_sell"], Decimal("-70"))
        self.assertEqual(ctx["buy_revenue_delta_jita_buy"], Decimal("180"))
        self.assertEqual(ctx["actual_exchange_profit"], Decimal("-170"))
        self.assertEqual(ctx["actual_exchange_profit_with_wallet"], Decimal("-170"))
        self.assertEqual(ctx["realized_member_sale_profit"], Decimal("250.00"))
        self.assertEqual(ctx["potential_profit_jita_buy"], Decimal("100.00"))
        self.assertEqual(ctx["potential_profit_jita_sell"], Decimal("350.00"))
        self.assertEqual(ctx["potential_profit_jita_split"], Decimal("225.00"))
        self.assertEqual(ctx["expected_profit_jita_split_with_wallet"], Decimal("225.00"))
        self.assertEqual(ctx["contract_profit_margin_pct"], -21.79)
        self.assertEqual(ctx["net_profit_margin_pct"], -21.79)
        self.assertEqual(ctx["expected_margin_jita_split_pct"], 31.03)
        self.assertEqual(ctx["projected_profit"], Decimal("-170.00"))
        self.assertEqual(ctx["projected_margin_pct"], -21.79)
        self.assertEqual(ctx["forecast_30d_profit"], Decimal("-5100.00"))
        self.assertEqual(ctx["forecast_90d_profit"], Decimal("-15300.00"))
        self.assertEqual(ctx["snapshot_coverage_pct"], 100.0)
        self.assertEqual(ctx["potential_priced_type_count"], 1)

        self.assertEqual(ctx["contract_stats"]["total"], 4)
        self.assertEqual(ctx["contract_stats"]["completed"], 2)
        self.assertEqual(ctx["contract_stats"]["rejected"], 0)
        self.assertEqual(ctx["contract_stats"]["deleted"], 2)
        self.assertEqual(ctx["contract_stats"]["deleted_before_acceptance"], 1)
        self.assertEqual(ctx["contract_stats"]["deleted_after_acceptance"], 1)

        self.assertEqual(ctx["contract_progress_stats"]["made"], 3)
        self.assertEqual(ctx["contract_progress_stats"]["completed"], 2)
        self.assertEqual(ctx["contract_progress_stats"]["rejected"], 1)
        self.assertEqual(ctx["contract_progress_stats"]["current_validated"], 0)

        self.assertEqual(
            ctx["buy_order_status_counts"].get(MaterialExchangeBuyOrder.Status.COMPLETED),
            1,
        )
        self.assertEqual(
            ctx["buy_order_status_counts"].get(MaterialExchangeBuyOrder.Status.REJECTED),
            1,
        )
        self.assertEqual(
            ctx["sell_order_status_counts"].get(MaterialExchangeSellOrder.Status.COMPLETED),
            1,
        )

    def test_stats_history_filters_by_custom_date_range(self) -> None:
        now = timezone.now()

        older_order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.member,
            status=MaterialExchangeBuyOrder.Status.COMPLETED,
        )
        MaterialExchangeBuyOrderItem.objects.create(
            order=older_order,
            type_id=34,
            type_name="Tritanium",
            quantity=1,
            unit_price=Decimal("100.00"),
            total_price=Decimal("100.00"),
            stock_available_at_creation=999,
        )

        recent_order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.member,
            status=MaterialExchangeBuyOrder.Status.COMPLETED,
        )
        MaterialExchangeBuyOrderItem.objects.create(
            order=recent_order,
            type_id=34,
            type_name="Tritanium",
            quantity=1,
            unit_price=Decimal("200.00"),
            total_price=Decimal("200.00"),
            stock_available_at_creation=999,
        )

        MaterialExchangeBuyOrder.objects.filter(pk=older_order.pk).update(
            created_at=now - timedelta(days=10)
        )
        MaterialExchangeBuyOrder.objects.filter(pk=recent_order.pk).update(
            created_at=now - timedelta(days=1)
        )
        older_tx = MaterialExchangeTransaction.objects.create(
            config=self.config,
            transaction_type=MaterialExchangeTransaction.TransactionType.BUY,
            buy_order=older_order,
            user=self.member,
            type_id=34,
            type_name="Tritanium",
            quantity=1,
            unit_price=Decimal("100.00"),
            total_price=Decimal("100.00"),
        )
        recent_tx = MaterialExchangeTransaction.objects.create(
            config=self.config,
            transaction_type=MaterialExchangeTransaction.TransactionType.BUY,
            buy_order=recent_order,
            user=self.member,
            type_id=34,
            type_name="Tritanium",
            quantity=1,
            unit_price=Decimal("200.00"),
            total_price=Decimal("200.00"),
        )
        MaterialExchangeTransaction.objects.filter(pk=older_tx.pk).update(
            completed_at=now - timedelta(days=10)
        )
        MaterialExchangeTransaction.objects.filter(pk=recent_tx.pk).update(
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

    def test_stats_history_saves_buyback_preferences(self) -> None:
        MaterialExchangeConfig.objects.create(
            corporation_id=789456,
            structure_id=60008494,
            structure_name="Another Hub",
            hangar_division=3,
            is_active=True,
        )

        self.client.force_login(self.manager)
        response = self.client.post(
            reverse("indy_hub:material_exchange_stats_history"),
            {
                "action": "save_stats_preferences",
                "chosen_corporation_id": "789456",
                "chosen_wallet_division": "4",
            },
        )
        self.assertEqual(response.status_code, 302)

        settings_obj = MaterialExchangeSettings.get_solo()
        self.assertEqual(settings_obj.stats_selected_corporation_id, 789456)
        self.assertEqual(settings_obj.stats_selected_wallet_division, 4)

        follow = self.client.get(reverse("indy_hub:material_exchange_stats_history"))
        self.assertEqual(follow.status_code, 200)
        self.assertEqual(follow.context["chosen_corporation_id"], 789456)
        self.assertEqual(follow.context["chosen_wallet_division"], 4)

    def test_stats_history_scopes_metrics_to_saved_corporation(self) -> None:
        other_config = MaterialExchangeConfig.objects.create(
            corporation_id=789456,
            structure_id=60008494,
            structure_name="Another Hub",
            hangar_division=4,
            is_active=True,
        )

        base_buy_order = MaterialExchangeBuyOrder.objects.create(
            config=self.config,
            buyer=self.member,
            status=MaterialExchangeBuyOrder.Status.COMPLETED,
        )
        MaterialExchangeBuyOrderItem.objects.create(
            order=base_buy_order,
            type_id=34,
            type_name="Tritanium",
            quantity=1,
            unit_price=Decimal("100.00"),
            total_price=Decimal("100.00"),
            stock_available_at_creation=999,
        )

        selected_buy_order = MaterialExchangeBuyOrder.objects.create(
            config=other_config,
            buyer=self.member,
            status=MaterialExchangeBuyOrder.Status.COMPLETED,
        )
        MaterialExchangeBuyOrderItem.objects.create(
            order=selected_buy_order,
            type_id=34,
            type_name="Tritanium",
            quantity=1,
            unit_price=Decimal("500.00"),
            total_price=Decimal("500.00"),
            stock_available_at_creation=999,
        )
        MaterialExchangeTransaction.objects.create(
            config=self.config,
            transaction_type=MaterialExchangeTransaction.TransactionType.BUY,
            buy_order=base_buy_order,
            user=self.member,
            type_id=34,
            type_name="Tritanium",
            quantity=1,
            unit_price=Decimal("100.00"),
            total_price=Decimal("100.00"),
        )
        MaterialExchangeTransaction.objects.create(
            config=other_config,
            transaction_type=MaterialExchangeTransaction.TransactionType.BUY,
            buy_order=selected_buy_order,
            user=self.member,
            type_id=34,
            type_name="Tritanium",
            quantity=1,
            unit_price=Decimal("500.00"),
            total_price=Decimal("500.00"),
        )

        self.client.force_login(self.manager)
        save_response = self.client.post(
            reverse("indy_hub:material_exchange_stats_history"),
            {
                "action": "save_stats_preferences",
                "chosen_corporation_id": "789456",
                "chosen_wallet_division": "4",
            },
        )
        self.assertEqual(save_response.status_code, 302)

        response = self.client.get(reverse("indy_hub:material_exchange_stats_history"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["chosen_corporation_id"], 789456)
        self.assertEqual(response.context["chosen_wallet_division"], 4)
        self.assertEqual(response.context["total_transactions"], 1)
        self.assertEqual(response.context["total_buy_volume"], Decimal("500"))
        self.assertEqual(response.context["stats_scope_mode"], "corp")

    def test_stats_history_uses_corp_scope_when_wallet_division_has_no_config(self) -> None:
        scoped_config = MaterialExchangeConfig.objects.create(
            corporation_id=789456,
            structure_id=60008494,
            structure_name="Another Hub",
            hangar_division=4,
            is_active=True,
        )
        scoped_buy_order = MaterialExchangeBuyOrder.objects.create(
            config=scoped_config,
            buyer=self.member,
            status=MaterialExchangeBuyOrder.Status.COMPLETED,
        )
        MaterialExchangeBuyOrderItem.objects.create(
            order=scoped_buy_order,
            type_id=34,
            type_name="Tritanium",
            quantity=1,
            unit_price=Decimal("500.00"),
            total_price=Decimal("500.00"),
            stock_available_at_creation=999,
        )
        MaterialExchangeTransaction.objects.create(
            config=scoped_config,
            transaction_type=MaterialExchangeTransaction.TransactionType.BUY,
            buy_order=scoped_buy_order,
            user=self.member,
            type_id=34,
            type_name="Tritanium",
            quantity=1,
            unit_price=Decimal("500.00"),
            total_price=Decimal("500.00"),
        )

        self.client.force_login(self.manager)
        save_response = self.client.post(
            reverse("indy_hub:material_exchange_stats_history"),
            {
                "action": "save_stats_preferences",
                "chosen_corporation_id": "789456",
                "chosen_wallet_division": "3",
            },
        )
        self.assertEqual(save_response.status_code, 302)

        response = self.client.get(reverse("indy_hub:material_exchange_stats_history"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["chosen_corporation_id"], 789456)
        self.assertEqual(response.context["chosen_wallet_division"], 3)
        self.assertEqual(response.context["stats_scope_mode"], "corp")
        self.assertEqual(response.context["stats_scope_note"], "")
        self.assertEqual(response.context["selected_config_count"], 1)
        self.assertEqual(response.context["total_transactions"], 1)
        self.assertEqual(response.context["total_buy_volume"], Decimal("500"))

    def test_stats_rankings_are_account_scoped_and_show_main_character(self) -> None:
        alt_character, _ = EveCharacter.objects.get_or_create(
            character_id=8010999,
            defaults={
                "character_name": "Alt Pilot",
                "corporation_id": 2_000_000,
                "corporation_name": "Test Corp",
                "corporation_ticker": "TEST",
            },
        )
        CharacterOwnership.objects.update_or_create(
            user=self.member,
            character=alt_character,
            defaults={"owner_hash": f"hash-alt-{self.member.id}"},
        )

        other_member = User.objects.create_user("other_member", password="secret123")
        _assign_main_character(other_member, character_id=8010200)
        _grant_permissions(other_member, "can_access_indy_hub")

        for _ in range(2):
            sell_order = MaterialExchangeSellOrder.objects.create(
                config=self.config,
                seller=self.member,
                status=MaterialExchangeSellOrder.Status.COMPLETED,
            )
            MaterialExchangeSellOrderItem.objects.create(
                order=sell_order,
                type_id=34,
                type_name="Tritanium",
                quantity=10,
                unit_price=Decimal("10.00"),
                total_price=Decimal("100.00"),
            )

        other_sell = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=other_member,
            status=MaterialExchangeSellOrder.Status.COMPLETED,
        )
        MaterialExchangeSellOrderItem.objects.create(
            order=other_sell,
            type_id=34,
            type_name="Tritanium",
            quantity=1,
            unit_price=Decimal("10.00"),
            total_price=Decimal("10.00"),
        )

        self.client.force_login(self.manager)
        response = self.client.get(reverse("indy_hub:material_exchange_stats_history"))
        self.assertEqual(response.status_code, 200)

        most_sold = response.context["most_sold_users"]
        self.assertGreaterEqual(len(most_sold), 2)
        top_row = most_sold[0]
        self.assertEqual(top_row["username"], self.member.username)
        self.assertEqual(top_row["main_character"], "Pilot 8010002")
