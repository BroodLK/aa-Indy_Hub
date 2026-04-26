from __future__ import annotations

# Standard Library
from decimal import Decimal
from unittest.mock import patch

# Django
from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership, UserProfile
from allianceauth.eveonline.models import EveCharacter

# Local
from indy_hub.models import MaterialExchangeConfig
from indy_hub.services.capital_price_estimates import sync_capital_ship_auto_estimates
from indy_hub.views.capital_ship_orders import _get_ship_default_price


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


def grant_indy_permissions(user: User, *codenames: str) -> None:
    required = {"can_access_indy_hub"}
    required.update(codenames)
    permissions = Permission.objects.filter(codename__in=required)
    found = {perm.codename: perm for perm in permissions}
    missing = required - found.keys()
    if missing:
        raise AssertionError(f"Missing permissions: {sorted(missing)}")
    user.user_permissions.add(*found.values())


class CapitalPriceEstimateSyncTests(TestCase):
    @patch("indy_hub.services.capital_price_estimates._resolve_fuel_like_type_ids")
    @patch("indy_hub.services.capital_price_estimates._fetch_public_contract_items_cached")
    @patch("indy_hub.services.capital_price_estimates._fetch_public_contract_page_cached")
    @patch("indy_hub.services.capital_price_estimates._resolve_operation")
    @patch("indy_hub.services.capital_price_estimates._load_capital_ship_options")
    def test_sync_updates_auto_estimate_from_contract_median_and_preserves_missing_types(
        self,
        mock_load_options,
        mock_resolve_operation,
        mock_fetch_pages,
        mock_fetch_items,
        mock_resolve_fuel_types,
    ) -> None:
        config = MaterialExchangeConfig.objects.create(
            corporation_id=1234,
            structure_id=60003760,
            is_active=True,
            capital_ship_auto_estimated_prices=[
                {
                    "type_id": 37604,
                    "price_isk": "5550000000.00",
                    "contract_count": 2,
                    "updated_at": "2026-01-01T00:00:00+00:00",
                }
            ],
        )
        mock_load_options.return_value = [
            {
                "type_id": 19720,
                "type_name": "Revelation",
                "ship_class": "dread",
                "ship_class_label": "Dreadnought",
            },
            {
                "type_id": 37604,
                "type_name": "Apostle",
                "ship_class": "fax",
                "ship_class_label": "FAX",
            },
        ]
        mock_resolve_operation.return_value = lambda **kwargs: None
        mock_fetch_pages.side_effect = [
            [
                {
                    "contract_id": 1,
                    "contract_type": "item_exchange",
                    "status": "outstanding",
                    "price": "3000000000.00",
                },
                {
                    "contract_id": 2,
                    "contract_type": "item_exchange",
                    "status": "outstanding",
                    "price": "3400000000.00",
                },
                {
                    "contract_id": 3,
                    "contract_type": "item_exchange",
                    "status": "outstanding",
                    "price": "4200000000.00",
                },
                {
                    "contract_id": 4,
                    "contract_type": "item_exchange",
                    "status": "outstanding",
                    "price": "999000000.00",
                },
            ],
            [],
            [],
        ]

        def items_side_effect(*, contract_id, **kwargs):
            if int(contract_id) == 1:
                return [{"type_id": 19720, "quantity": 1, "is_included": True}]
            if int(contract_id) == 2:
                return [{"type_id": 19720, "quantity": 1, "is_included": True}]
            if int(contract_id) == 3:
                return [
                    {"type_id": 19720, "quantity": 1, "is_included": True},
                    {"type_id": 4247, "quantity": 10, "is_included": True},
                ]
            if int(contract_id) == 4:
                return [
                    {"type_id": 19720, "quantity": 1, "is_included": True},
                    {"type_id": 34, "quantity": 1, "is_included": True},
                ]
            return []

        mock_fetch_items.side_effect = items_side_effect
        mock_resolve_fuel_types.return_value = {4247}

        result = sync_capital_ship_auto_estimates(max_pages=10)

        self.assertTrue(result["ok"])
        self.assertEqual(result["contracts_matched"], 3)
        self.assertEqual(result["types_updated"], 1)

        config.refresh_from_db()
        auto_row_map = config.get_capital_ship_auto_estimate_row_map()
        self.assertEqual(
            auto_row_map[19720]["price_isk"],
            Decimal("3400000000.00"),
        )
        self.assertEqual(auto_row_map[19720]["contract_count"], 3)
        self.assertEqual(
            auto_row_map[37604]["price_isk"],
            Decimal("5550000000.00"),
        )


class CapitalPriceEstimateFallbackTests(TestCase):
    def test_ship_default_price_prefers_manual_then_auto_then_group_default(self) -> None:
        config = MaterialExchangeConfig.objects.create(
            corporation_id=1234,
            structure_id=60003760,
            is_active=True,
            capital_default_price_dread=Decimal("3100000000.00"),
            capital_ship_estimated_price_overrides=[
                {"type_id": 19720, "price_isk": "3600000000.00"}
            ],
            capital_ship_auto_estimated_prices=[
                {
                    "type_id": 19720,
                    "price_isk": "3400000000.00",
                    "contract_count": 3,
                    "updated_at": "2026-04-22T00:00:00+00:00",
                }
            ],
        )

        price, source = _get_ship_default_price(
            config,
            ship_type_id=19720,
            ship_class="dread",
        )
        self.assertEqual(price, Decimal("3600000000.00"))
        self.assertEqual(source, "ship_config_override")

        config.capital_ship_estimated_price_overrides = []
        price, source = _get_ship_default_price(
            config,
            ship_type_id=19720,
            ship_class="dread",
        )
        self.assertEqual(price, Decimal("3400000000.00"))
        self.assertEqual(source, "public_contract_median")

        config.capital_ship_auto_estimated_prices = []
        price, source = _get_ship_default_price(
            config,
            ship_type_id=19720,
            ship_class="dread",
        )
        self.assertEqual(price, Decimal("3100000000.00"))
        self.assertEqual(source, "class_config_default")


class CapitalOrderConfigViewTests(TestCase):
    def setUp(self) -> None:
        self.manager = User.objects.create_user("capmanager", password="secret123")
        assign_main_character(self.manager, character_id=2025001)
        grant_indy_permissions(self.manager, "can_manage_capital_orders")
        self.client.force_login(self.manager)

        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=1234,
            structure_id=60003760,
            is_active=True,
            capital_ship_estimated_price_overrides=[
                {"type_id": 19720, "price_isk": "3600000000.00"}
            ],
            capital_ship_auto_estimated_prices=[
                {
                    "type_id": 19720,
                    "price_isk": "3400000000.00",
                    "contract_count": 3,
                    "updated_at": "2026-04-22T00:00:00+00:00",
                }
            ],
        )

    @patch("indy_hub.views.capital_ship_orders._load_capital_ship_options_for_editor")
    def test_config_view_shows_auto_estimate_and_blank_manual_override_reverts_to_auto(
        self,
        mock_editor_options,
    ) -> None:
        mock_editor_options.return_value = [
            {
                "type_id": 19720,
                "type_name": "Revelation",
                "ship_class": "dread",
                "ship_class_label": "Dreadnought",
                "enabled": True,
            }
        ]
        url = reverse("indy_hub:capital_ship_orders_config")

        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Auto Estimate (ISK)")
        self.assertContains(response, "3,400,000,000.00")
        self.assertContains(response, "3600000000.00")

        response = self.client.post(
            url,
            {
                "capital_default_price_dread": "",
                "capital_default_price_carrier": "",
                "capital_default_price_fax": "",
                "capital_default_eta_min_days_dread": "14",
                "capital_default_eta_max_days_dread": "28",
                "capital_default_eta_min_days_carrier": "14",
                "capital_default_eta_max_days_carrier": "28",
                "capital_default_eta_min_days_fax": "14",
                "capital_default_eta_max_days_fax": "28",
                "capital_default_lead_time_days": "0",
                "capital_auto_cancel_delay_value": "0",
                "capital_auto_cancel_delay_unit": "hours",
                "capital_auto_cancel_preapproved_state_names": ["Pre-Approved"],
                "estimated_price_19720": "",
            },
        )
        self.assertEqual(response.status_code, 302)

        self.config.refresh_from_db()
        self.assertEqual(self.config.get_capital_ship_estimated_price_map(), {})
        self.assertEqual(
            self.config.get_capital_ship_effective_estimated_price_map()[19720],
            Decimal("3400000000.00"),
        )
