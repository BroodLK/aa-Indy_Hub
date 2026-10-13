"""Tests for Fleet Loot Splitter feature."""

from __future__ import annotations

# Standard Library
from decimal import Decimal
from unittest.mock import MagicMock, patch

# Django
from django.contrib.auth.models import Permission, User
from django.test import RequestFactory, TestCase
from django.urls import resolve, reverse

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership, UserProfile
from allianceauth.eveonline.models import EveCharacter

from indy_hub.auth_hooks import FleetLootSplitMenu, IndyHubMenu
from indy_hub.services.loot_splitter import (
    calculate_fleet_loot_split,
    fetch_zkillboard_killmail,
    parse_and_price_loot,
    resolve_fleet_pilots,
)


def create_user_with_chars(username: str, main_id: int, main_name: str, alt_tuples: list[tuple[int, str]] | None = None) -> tuple[User, EveCharacter, list[EveCharacter]]:
    user = User.objects.create_user(username=username, password="password123")
    main_char, _ = EveCharacter.objects.get_or_create(
        character_id=main_id,
        defaults={
            "character_name": main_name,
            "corporation_id": 2000001,
            "corporation_name": "Test Corp",
            "corporation_ticker": "TEST",
        },
    )
    CharacterOwnership.objects.update_or_create(
        user=user,
        character=main_char,
        defaults={"owner_hash": f"hash-{main_id}"},
    )
    profile, _ = UserProfile.objects.get_or_create(user=user)
    profile.main_character = main_char
    profile.save(update_fields=["main_character"])

    alts = []
    for alt_id, alt_name in alt_tuples or []:
        alt_char, _ = EveCharacter.objects.get_or_create(
            character_id=alt_id,
            defaults={
                "character_name": alt_name,
                "corporation_id": 2000001,
                "corporation_name": "Test Corp",
                "corporation_ticker": "TEST",
            },
        )
        CharacterOwnership.objects.update_or_create(
            user=user,
            character=alt_char,
            defaults={"owner_hash": f"hash-{alt_id}"},
        )
        alts.append(alt_char)

    return user, main_char, alts


class FleetLootSplitterServiceTests(TestCase):
    def setUp(self):
        self.user1, self.main1, self.alts1 = create_user_with_chars(
            "user1", 1001, "Pilot Alpha", [(1002, "Alpha Alt1"), (1003, "Alpha Alt2")]
        )
        self.user2, self.main2, _ = create_user_with_chars(
            "user2", 2001, "Pilot Beta", []
        )
        self.user3, self.main3, _ = create_user_with_chars(
            "user3", 3001, "Pilot Gamma", []
        )
        self.user4, self.main4, _ = create_user_with_chars(
            "user4", 4001, "Pilot Delta", []
        )

    def test_resolve_fleet_pilots_maps_alts_to_mains_and_groups(self):
        input_text = "Alpha Alt1\nPilot Beta\nAlpha Alt2\nExternal Pilot\nPilot Alpha"
        pilots = resolve_fleet_pilots(input_text)

        self.assertEqual(len(pilots), 3)
        
        # Alpha should be grouped with alts
        alpha_pilot = next(p for p in pilots if p["name"] == "Pilot Alpha")
        self.assertEqual(alpha_pilot["character_id"], 1001)
        self.assertTrue(alpha_pilot["is_registered"])
        self.assertIn("Alpha Alt1", alpha_pilot["alts"])
        self.assertIn("Alpha Alt2", alpha_pilot["alts"])

        # Beta
        beta_pilot = next(p for p in pilots if p["name"] == "Pilot Beta")
        self.assertEqual(beta_pilot["character_id"], 2001)
        self.assertTrue(beta_pilot["is_registered"])

        # External pilot
        ext_pilot = next(p for p in pilots if p["name"] == "External Pilot")
        self.assertFalse(ext_pilot["is_registered"])
        self.assertIsNone(ext_pilot["character_id"])

    @patch("indy_hub.services.loot_splitter.fetch_fuzzwork_prices")
    @patch("indy_hub.views.material_exchange._parse_sell_estimate_input")
    def test_parse_and_price_loot(self, mock_parse_input, mock_fuzz):
        mock_parse_input.return_value = (
            [{"type_id": 34, "quantity": 1000}, {"type_id": 35, "quantity": 500}],
            ["Invalid Line"],
        )
        mock_fuzz.return_value = {
            34: {"buy": Decimal("5.00"), "sell": Decimal("7.00")},
            35: {"buy": Decimal("10.00"), "sell": Decimal("12.00")},
        }

        items, invalid = parse_and_price_loot("dummy input", valuation_mode="market")

        self.assertEqual(len(items), 2)
        self.assertEqual(invalid, ["Invalid Line"])
        
        # Jita split for 34 is (5 + 7) / 2 = 6.00
        item_34 = next(i for i in items if i["type_id"] == 34)
        self.assertEqual(item_34["chosen_unit_price"], 6.0)
        self.assertEqual(item_34["total_value"], 6000.0)

    @patch("indy_hub.services.loot_splitter.requests.get")
    def test_fetch_zkillboard_killmail(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {
                "killmail_id": 999999,
                "zkb": {"totalValue": 30000000.0, "fittedValue": 25000000.0},
                "victim": {"character_id": 3001, "ship_type_id": 587},
            }
        ]
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        km = fetch_zkillboard_killmail("https://zkillboard.com/kill/999999/")
        self.assertEqual(km["killmail_id"], 999999)
        self.assertEqual(km["loss_value"], 30000000.0)
        self.assertEqual(km["victim_name"], "Pilot Gamma")

    def test_calculate_fleet_loot_split_exact_user_example(self):
        """
        User Example:
          - 4 pilots: P1, P2, P3 (lost Squall 30M), P4 (elected to receive 150M item)
          - Total loot: 150M
          - Reimbursement: 30M
          - Distributable pool: 120M (150M - 30M)
          - 4 eligible pilots -> 30M base share each
          - Payouts:
            P1: 30M
            P2: 30M
            P3: 60M (30M base + 30M reimbursement)
            P4: 0M payout, owes 120M (kept 150M item, receives 30M base share -> net -120M)
        """
        pilots = [
            {"id": "p1", "name": "Pilot 1", "declined": False},
            {"id": "p2", "name": "Pilot 2", "declined": False},
            {"id": "p3", "name": "Pilot 3", "declined": False},
            {"id": "p4", "name": "Pilot 4", "declined": False},
        ]
        loot_items = [
            {
                "id": "i1",
                "type_id": 999,
                "name": "Valuable Loot Module",
                "quantity": 1,
                "chosen_unit_price": 150000000.0,
                "kept_by_pilot_id": "p4",
                "kept_quantity": 1,
            }
        ]
        killmails = [
            {
                "id": "km1",
                "killmail_id": 12345,
                "assigned_pilot_id": "p3",
                "victim_name": "Pilot 3",
                "ship_name": "Squall",
                "loss_value": 30000000.0,
            }
        ]

        result = calculate_fleet_loot_split(
            pilots=pilots,
            loot_items=loot_items,
            killmails=killmails,
            valuation_mode="market",
        )

        summary = result["summary"]
        self.assertEqual(summary["gross_loot_value"], 150000000.0)
        self.assertEqual(summary["total_loss_reimbursements"], 30000000.0)
        self.assertEqual(summary["net_distributable_pot"], 120000000.0)
        self.assertEqual(summary["base_share_per_pilot"], 30000000.0)

        p_map = {p["id"]: p for p in result["pilots"]}
        
        # P1: 30M
        self.assertEqual(p_map["p1"]["payout_received"], 30000000.0)
        self.assertEqual(p_map["p1"]["amount_owed"], 0.0)

        # P2: 30M
        self.assertEqual(p_map["p2"]["payout_received"], 30000000.0)
        self.assertEqual(p_map["p2"]["amount_owed"], 0.0)

        # P3: 60M (30M base + 30M reimb)
        self.assertEqual(p_map["p3"]["reimbursement"], 30000000.0)
        self.assertEqual(p_map["p3"]["payout_received"], 60000000.0)
        self.assertEqual(p_map["p3"]["amount_owed"], 0.0)

        # P4: 0M payout, owes 120M
        self.assertEqual(p_map["p4"]["kept_value"], 150000000.0)
        self.assertEqual(p_map["p4"]["payout_received"], 0.0)
        self.assertEqual(p_map["p4"]["amount_owed"], 120000000.0)

        # Check transfers: P4 pays 30M to P1, 30M to P2, 60M to P3
        transfers = result["transfers"]
        self.assertEqual(len(transfers), 3)
        total_transferred = sum(t["amount"] for t in transfers)
        self.assertEqual(total_transferred, 120000000.0)
        self.assertTrue(all(t["from_pilot"] == "Pilot 4" for t in transfers))

    def test_calculate_fleet_loot_split_with_declined_pilot(self):
        """Test calculation when one pilot opts out / declines payout."""
        pilots = [
            {"id": "p1", "name": "Pilot 1", "declined": False},
            {"id": "p2", "name": "Pilot 2", "declined": True}, # opted out
            {"id": "p3", "name": "Pilot 3", "declined": False},
        ]
        loot_items = [
            {
                "id": "i1",
                "quantity": 1,
                "chosen_unit_price": 100000000.0,
                "kept_by_pilot_id": None,
                "kept_quantity": 0,
            }
        ]
        killmails = []

        result = calculate_fleet_loot_split(
            pilots=pilots,
            loot_items=loot_items,
            killmails=killmails,
        )

        summary = result["summary"]
        self.assertEqual(summary["eligible_pilots_count"], 2)
        # 100M split 2 ways = 50M each
        self.assertEqual(summary["base_share_per_pilot"], 50000000.0)

        p_map = {p["id"]: p for p in result["pilots"]}
        self.assertEqual(p_map["p1"]["payout_received"], 50000000.0)
        self.assertEqual(p_map["p2"]["payout_received"], 0.0)
        self.assertTrue(p_map["p2"]["declined"])
        self.assertEqual(p_map["p3"]["payout_received"], 50000000.0)

    def test_to_decimal_shorthand_support(self):
        """Test _to_decimal converts human strings like 150m, 1.5b, 50k."""
        from indy_hub.services.loot_splitter import _to_decimal

        self.assertEqual(_to_decimal("150m"), Decimal("150000000.00"))
        self.assertEqual(_to_decimal("1.5b"), Decimal("1500000000.00"))
        self.assertEqual(_to_decimal("50k"), Decimal("50000.00"))
        self.assertEqual(_to_decimal("150,000,000"), Decimal("150000000.00"))
        self.assertEqual(_to_decimal(None), Decimal("0.00"))
        self.assertEqual(_to_decimal("invalid"), Decimal("0.00"))

    def test_calculate_all_pilots_declined(self):
        """Test calculation when all participants decline base loot share."""
        pilots = [
            {"id": "p1", "name": "Pilot 1", "declined": True},
            {"id": "p2", "name": "Pilot 2", "declined": True},
        ]
        loot_items = [{"id": "i1", "quantity": 1, "chosen_unit_price": 50000000.0}]
        result = calculate_fleet_loot_split(pilots=pilots, loot_items=loot_items, killmails=[])
        self.assertEqual(result["summary"]["eligible_pilots_count"], 0)
        self.assertEqual(result["summary"]["base_share_per_pilot"], 0.0)

    def test_calculate_loss_exceeds_loot(self):
        """Test calculation when ship losses exceed total loot value."""
        pilots = [
            {"id": "p1", "name": "Pilot 1", "declined": False},
            {"id": "p2", "name": "Pilot 2", "declined": False},
        ]
        loot_items = [{"id": "i1", "quantity": 1, "chosen_unit_price": 20000000.0}]
        killmails = [{"id": "km1", "loss_value": 50000000.0, "assigned_pilot_id": "p1"}]
        result = calculate_fleet_loot_split(pilots=pilots, loot_items=loot_items, killmails=killmails)
        self.assertEqual(result["summary"]["net_distributable_pot"], 0.0)
        self.assertEqual(result["summary"]["base_share_per_pilot"], 0.0)

    def test_calculate_fleet_loot_split_with_bag_man(self):
        """Test calculation when a Bag Man is designated to liquidate loot and execute payouts."""
        pilots = [
            {"id": "p1", "name": "Pilot Alpha (Bag Man)"},
            {"id": "p2", "name": "Pilot Beta"},
            {"id": "p3", "name": "Pilot Gamma"},
        ]
        # 300M total loot, none kept -> 100M split each
        loot_items = [
            {"id": "i1", "quantity": 1, "chosen_unit_price": 300000000.0, "kept_by_pilot_id": None, "kept_quantity": 0}
        ]
        killmails = []

        result = calculate_fleet_loot_split(
            pilots=pilots,
            loot_items=loot_items,
            killmails=killmails,
            bag_man_pilot_id="p1",
        )

        summary = result["summary"]
        self.assertEqual(summary["bag_man_pilot_id"], "p1")
        self.assertEqual(summary["bag_man_name"], "Pilot Alpha (Bag Man)")
        self.assertEqual(summary["liquidated_loot_value"], 300000000.0)
        self.assertEqual(summary["base_share_per_pilot"], 100000000.0)

        # Bag Man should pay Pilot Beta 100M and Pilot Gamma 100M
        transfers = result["transfers"]
        self.assertEqual(len(transfers), 2)
        self.assertEqual(transfers[0]["from_pilot"], "Pilot Alpha (Bag Man)")
        self.assertEqual(transfers[0]["to_pilot"], "Pilot Beta")
        self.assertEqual(transfers[0]["amount"], 100000000.0)
        self.assertEqual(transfers[0]["type"], "payout")

        self.assertEqual(transfers[1]["from_pilot"], "Pilot Alpha (Bag Man)")
        self.assertEqual(transfers[1]["to_pilot"], "Pilot Gamma")
        self.assertEqual(transfers[1]["amount"], 100000000.0)
        self.assertEqual(transfers[1]["type"], "payout")

        # Discord summary includes Bag Man
        self.assertIn("Bag Man (Liquidator)", result["exports"]["discord"])
        self.assertIn("Pilot Alpha (Bag Man)", result["exports"]["discord"])
        self.assertIn("Bag Man (Liquidator)", result["exports"]["ingame"])

    def test_calculate_fleet_loot_split_bag_man_with_debtor(self):
        """Test calculation where another pilot kept an item exceeding their share and pays Bag Man."""
        pilots = [
            {"id": "p1", "name": "Pilot 1 (Bag Man)"},
            {"id": "p2", "name": "Pilot 2 (Survivor)"},
            {"id": "p3", "name": "Pilot 3 (Kept Item)"},
        ]
        # Item 1: 150M kept by Pilot 3. Item 2: 150M unkept (liquidated by Bag Man). Total 300M gross pot -> 100M each.
        loot_items = [
            {"id": "i1", "quantity": 1, "chosen_unit_price": 150000000.0, "kept_by_pilot_id": "p3", "kept_quantity": 1},
            {"id": "i2", "quantity": 1, "chosen_unit_price": 150000000.0, "kept_by_pilot_id": None, "kept_quantity": 0},
        ]
        killmails = []

        result = calculate_fleet_loot_split(
            pilots=pilots,
            loot_items=loot_items,
            killmails=killmails,
            bag_man_pilot_id="p1",
        )

        p_map = {p["id"]: p for p in result["pilots"]}
        self.assertEqual(p_map["p1"]["payout_received"], 100000000.0)
        self.assertEqual(p_map["p2"]["payout_received"], 100000000.0)
        self.assertEqual(p_map["p3"]["amount_owed"], 50000000.0)

        # Transfers: Bag Man pays P2 100M, and P3 pays Bag Man 50M
        transfers = result["transfers"]
        self.assertEqual(len(transfers), 2)
        p2_transfer = next(t for t in transfers if t["to_pilot"] == "Pilot 2 (Survivor)")
        self.assertEqual(p2_transfer["from_pilot"], "Pilot 1 (Bag Man)")
        self.assertEqual(p2_transfer["amount"], 100000000.0)
        self.assertEqual(p2_transfer["type"], "payout")

        p3_transfer = next(t for t in transfers if t["from_pilot"] == "Pilot 3 (Kept Item)")
        self.assertEqual(p3_transfer["to_pilot"], "Pilot 1 (Bag Man)")
        self.assertEqual(p3_transfer["amount"], 50000000.0)
        self.assertEqual(p3_transfer["type"], "debt_collection")


class FleetLootSplitterViewTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="fleetleader", password="password123")
        char, _ = EveCharacter.objects.get_or_create(
            character_id=9001,
            defaults={"character_name": "Fleet Leader", "corporation_id": 2001, "corporation_name": "Test"},
        )
        CharacterOwnership.objects.create(user=self.user, character=char, owner_hash="h1")
        profile, _ = UserProfile.objects.get_or_create(user=self.user)
        profile.main_character = char
        profile.save()

        perm = Permission.objects.get(codename="can_access_loot_splitter")
        self.user.user_permissions.add(perm)
        self.client.force_login(self.user)

    def test_loot_split_page_renders_successfully(self):
        url = reverse("indy_hub:loot_split")
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Fleet Loot Splitter")
        self.assertContains(response, "Step 1: Fleet Pilots")
        self.assertContains(response, "copy-inline-btn")

    def test_loot_split_page_denied_without_permission(self):
        user_no_perm = User.objects.create_user(username="nopermuser", password="password123")
        # Give only can_access_indy_hub, NOT can_access_loot_splitter
        indy_perm = Permission.objects.get(codename="can_access_indy_hub")
        user_no_perm.user_permissions.add(indy_perm)
        self.client.force_login(user_no_perm)

        url = reverse("indy_hub:loot_split")
        response = self.client.get(url)
        # Should redirect to index with permission denied
        self.assertEqual(response.status_code, 302)

    def test_resolve_pilots_api(self):
        url = reverse("indy_hub:loot_split_resolve_pilots_api")
        response = self.client.post(
            url,
            data={"names_text": "Fleet Leader\nUnknown Alt"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["ok"])
        self.assertEqual(len(data["pilots"]), 2)

    def test_calculate_api(self):
        url = reverse("indy_hub:loot_split_calculate_api")
        payload = {
            "pilots": [{"id": "p1", "name": "Fleet Leader", "declined": False}],
            "loot_items": [{"id": "i1", "quantity": 1, "chosen_unit_price": 5000000.0}],
            "killmails": [],
            "valuation_mode": "buyback",
        }
        response = self.client.post(url, data=payload, content_type="application/json")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["summary"]["gross_loot_value"], 5000000.0)


class FleetLootSplitMenuTests(TestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.user = User.objects.create_user(username="menuuser", password="password123")
        self.perm = Permission.objects.get(codename="can_access_loot_splitter")

    def test_menu_renders_for_authorized_user(self):
        self.user.user_permissions.add(self.perm)
        request = self.factory.get("/")
        request.user = self.user

        menu = FleetLootSplitMenu()
        rendered = menu.render(request)
        self.assertTrue(bool(rendered))
        self.assertIn("Loot Split", rendered)

    def test_menu_hidden_for_unauthorized_user(self):
        # Give only can_access_indy_hub, NOT can_access_loot_splitter
        indy_perm = Permission.objects.get(codename="can_access_indy_hub")
        self.user.user_permissions.add(indy_perm)

        request = self.factory.get("/")
        request.user = self.user

        menu = FleetLootSplitMenu()
        rendered = menu.render(request)
        self.assertEqual(rendered, "")

    def test_menu_active_isolation_between_indy_hub_and_loot_split(self):
        self.user.user_permissions.add(self.perm)
        indy_perm = Permission.objects.get(codename="can_access_indy_hub")
        self.user.user_permissions.add(indy_perm)

        loot_path = reverse("indy_hub:loot_split")
        req_loot = self.factory.get(loot_path)
        req_loot.user = self.user
        req_loot.resolver_match = resolve(loot_path)

        indy_path = reverse("indy_hub:index")
        req_indy = self.factory.get(indy_path)
        req_indy.user = self.user
        req_indy.resolver_match = resolve(indy_path)

        indy_menu = IndyHubMenu()
        loot_menu = FleetLootSplitMenu()

        # On loot_split page: Loot Split should be active, Indy Hub should NOT be active
        loot_rendered_on_loot = loot_menu.render(req_loot)
        indy_rendered_on_loot = indy_menu.render(req_loot)
        self.assertIn('class="active"', loot_rendered_on_loot)
        self.assertNotIn('class="active"', indy_rendered_on_loot)

        # On indy_hub index page: Indy Hub should be active, Loot Split should NOT be active
        loot_rendered_on_indy = loot_menu.render(req_indy)
        indy_rendered_on_indy = indy_menu.render(req_indy)
        self.assertNotIn('class="active"', loot_rendered_on_indy)
        self.assertIn('class="active"', indy_rendered_on_indy)
