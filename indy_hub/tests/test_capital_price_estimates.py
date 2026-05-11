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
from indy_hub.services.capital_price_estimates import (
    _build_capital_buy_cost_map,
    sync_capital_ship_auto_estimates,
)
from indy_hub.views.capital_ship_orders import (
    _get_ship_default_price,
    _load_capital_ship_options,
    _resolve_ship_class_for_group_name,
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
    @patch("indy_hub.services.capital_price_estimates._build_capital_buy_cost_map")
    @patch("indy_hub.services.capital_price_estimates._load_capital_ship_options")
    def test_sync_updates_auto_estimate_from_buy_cost_plus_markup_and_preserves_missing_types(
        self,
        mock_load_options,
        mock_build_cost_map,
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
        mock_build_cost_map.return_value = (
            {
                19720: Decimal("3000000000.00"),
            },
            {
                "types_requested": 2,
                "blueprints_found": 1,
                "requirements_built": 1,
                "material_types_needed": 3,
                "material_price_hits": 3,
                "material_price_misses": 0,
                "types_priced": 1,
                "types_skipped_missing_prices": 0,
            },
        )

        result = sync_capital_ship_auto_estimates(max_pages=10)

        self.assertTrue(result["ok"])
        self.assertEqual(result["types_priced"], 1)
        self.assertEqual(result["types_updated"], 1)

        config.refresh_from_db()
        auto_row_map = config.get_capital_ship_auto_estimate_row_map()
        self.assertEqual(
            auto_row_map[19720]["price_isk"],
            Decimal("3300000000.00"),
        )
        self.assertNotIn("contract_count", auto_row_map[19720])
        self.assertEqual(auto_row_map[37604]["price_isk"], Decimal("5550000000.00"))

    @patch("indy_hub.services.capital_price_estimates._build_capital_buy_cost_map")
    @patch("indy_hub.services.capital_price_estimates._load_capital_ship_options")
    def test_sync_ceils_auto_estimate_to_next_hundred_million(
        self,
        mock_load_options,
        mock_build_cost_map,
    ) -> None:
        config = MaterialExchangeConfig.objects.create(
            corporation_id=1234,
            structure_id=60003760,
            is_active=True,
        )
        mock_load_options.return_value = [
            {
                "type_id": 19720,
                "type_name": "Revelation",
                "ship_class": "dread",
                "ship_class_label": "Dreadnought",
            }
        ]
        mock_build_cost_map.return_value = (
            {
                19720: Decimal("7686924416.81"),
            },
            {
                "types_requested": 1,
                "blueprints_found": 1,
                "requirements_built": 1,
                "material_types_needed": 3,
                "material_price_hits": 3,
                "material_price_misses": 0,
                "types_priced": 1,
                "types_skipped_missing_prices": 0,
            },
        )

        result = sync_capital_ship_auto_estimates(max_pages=10)

        self.assertTrue(result["ok"])
        self.assertEqual(result["types_updated"], 1)

        config.refresh_from_db()
        auto_row_map = config.get_capital_ship_auto_estimate_row_map()
        self.assertEqual(
            auto_row_map[19720]["price_isk"],
            Decimal("8500000000.00"),
        )


class CapitalPriceEstimateCostingTests(TestCase):
    @patch("indy_hub.services.capital_price_estimates.fetch_fuzzwork_prices")
    @patch("indy_hub.services.capital_price_estimates._get_blueprint_output_qty")
    @patch("indy_hub.services.capital_price_estimates._get_blueprint_material_rows")
    @patch("indy_hub.services.capital_price_estimates._get_blueprint_for_product")
    def test_build_cost_map_uses_recursive_leaf_buy_costs(
        self,
        mock_get_blueprint_for_product,
        mock_get_blueprint_material_rows,
        mock_get_blueprint_output_qty,
        mock_fetch_fuzzwork_prices,
    ) -> None:
        def blueprint_for_product_side_effect(
            product_type_id,
            *,
            blueprint_by_product_cache,
            blueprint_output_qty_cache,
        ):
            mapping = {
                19720: 101,
                5001: 201,
            }
            return mapping.get(int(product_type_id))

        def material_rows_side_effect(blueprint_id, *, blueprint_material_cache):
            mapping = {
                101: [(5001, 2), (5002, 5)],
                201: [(5003, 3)],
            }
            return list(mapping.get(int(blueprint_id), []))

        def output_qty_side_effect(blueprint_id, *, blueprint_output_qty_cache):
            mapping = {
                201: 2,
            }
            return mapping.get(int(blueprint_id), 1)

        mock_get_blueprint_for_product.side_effect = blueprint_for_product_side_effect
        mock_get_blueprint_material_rows.side_effect = material_rows_side_effect
        mock_get_blueprint_output_qty.side_effect = output_qty_side_effect
        mock_fetch_fuzzwork_prices.return_value = {
            5002: {"buy": Decimal("0"), "sell": Decimal("4")},
            5003: {"buy": Decimal("0"), "sell": Decimal("2")},
        }

        cost_map, stats = _build_capital_buy_cost_map({19720})

        self.assertEqual(cost_map[19720], Decimal("26.00"))
        self.assertEqual(stats["blueprints_found"], 1)
        self.assertEqual(stats["material_types_needed"], 2)
        self.assertEqual(stats["types_priced"], 1)

    @patch("indy_hub.services.capital_price_estimates.get_public_jita_bpc_offers")
    @patch(
        "indy_hub.services.capital_price_estimates._requires_blueprint_copy_cost_for_capital_hull"
    )
    @patch("indy_hub.services.capital_price_estimates.fetch_fuzzwork_prices")
    @patch("indy_hub.services.capital_price_estimates._get_blueprint_output_qty")
    @patch("indy_hub.services.capital_price_estimates._get_blueprint_material_rows")
    @patch("indy_hub.services.capital_price_estimates._get_blueprint_for_product")
    def test_build_cost_map_adds_public_bpc_cost_for_special_hulls(
        self,
        mock_get_blueprint_for_product,
        mock_get_blueprint_material_rows,
        mock_get_blueprint_output_qty,
        mock_fetch_fuzzwork_prices,
        mock_requires_copy_cost,
        mock_get_public_bpc_offers,
    ) -> None:
        mock_get_blueprint_for_product.return_value = 101
        mock_get_blueprint_material_rows.return_value = [(5002, 5)]
        mock_get_blueprint_output_qty.return_value = 1
        mock_fetch_fuzzwork_prices.return_value = {
            5002: {"buy": Decimal("0"), "sell": Decimal("4")},
        }
        mock_requires_copy_cost.return_value = True
        mock_get_public_bpc_offers.return_value = [
            {"price_per_run": Decimal("10")},
            {"price_per_run": Decimal("12")},
        ]

        cost_map, stats = _build_capital_buy_cost_map({19720})

        self.assertEqual(cost_map[19720], Decimal("30.00"))
        self.assertEqual(stats["bpc_eligible_types"], 1)
        self.assertEqual(stats["bpc_price_hits"], 1)
        self.assertEqual(stats["types_priced_with_bpc"], 1)

    @patch("indy_hub.services.capital_price_estimates.get_public_jita_bpc_offers")
    @patch(
        "indy_hub.services.capital_price_estimates._requires_blueprint_copy_cost_for_capital_hull"
    )
    @patch("indy_hub.services.capital_price_estimates.fetch_fuzzwork_prices")
    @patch("indy_hub.services.capital_price_estimates._get_blueprint_output_qty")
    @patch("indy_hub.services.capital_price_estimates._get_blueprint_material_rows")
    @patch("indy_hub.services.capital_price_estimates._get_blueprint_for_product")
    def test_build_cost_map_special_hulls_fall_back_to_material_cost_without_bpc_offers(
        self,
        mock_get_blueprint_for_product,
        mock_get_blueprint_material_rows,
        mock_get_blueprint_output_qty,
        mock_fetch_fuzzwork_prices,
        mock_requires_copy_cost,
        mock_get_public_bpc_offers,
    ) -> None:
        mock_get_blueprint_for_product.return_value = 101
        mock_get_blueprint_material_rows.return_value = [(5002, 5)]
        mock_get_blueprint_output_qty.return_value = 1
        mock_fetch_fuzzwork_prices.return_value = {
            5002: {"buy": Decimal("0"), "sell": Decimal("4")},
        }
        mock_requires_copy_cost.return_value = True
        mock_get_public_bpc_offers.return_value = []

        cost_map, stats = _build_capital_buy_cost_map({19720})

        self.assertEqual(cost_map[19720], Decimal("20.00"))
        self.assertEqual(stats["bpc_eligible_types"], 1)
        self.assertEqual(stats["bpc_price_hits"], 0)
        self.assertEqual(stats["bpc_price_misses"], 1)
        self.assertEqual(stats["types_priced_with_bpc"], 0)


class CapitalPriceEstimateFallbackTests(TestCase):
    def test_ship_default_price_prefers_manual_then_auto_only(self) -> None:
        config = MaterialExchangeConfig.objects.create(
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
        self.assertEqual(source, "craft_buy_cost_plus_10")

        config.capital_ship_auto_estimated_prices = []
        price, source = _get_ship_default_price(
            config,
            ship_type_id=19720,
            ship_class="dread",
        )
        self.assertIsNone(price)
        self.assertEqual(source, "")


class CapitalShipGroupMappingTests(TestCase):
    def test_sde_group_mapping_covers_extended_capital_families(self) -> None:
        expected = {
            "Dreadnought": "dread",
            "Lancer Dreadnought": "dread",
            "Carrier": "carrier",
            "Force Auxiliary": "fax",
            "Supercarrier": "super",
            "Titan": "titan",
            "Freighter": "freighter",
            "Jump Freighter": "jump_freighter",
            "Capital Industrial Ship": "capital_indy",
        }

        for group_name, ship_class in expected.items():
            self.assertEqual(_resolve_ship_class_for_group_name(group_name), ship_class)


class CapitalShipOptionsTests(TestCase):
    @patch("indy_hub.views.capital_ship_orders._load_base_capital_ship_options")
    def test_custom_ship_rows_are_ignored_when_loading_options(
        self,
        mock_load_base_options,
    ) -> None:
        mock_load_base_options.return_value = [
            {
                "type_id": 19720,
                "type_name": "Revelation",
                "ship_class": "dread",
                "ship_class_label": "Dreadnought",
            }
        ]
        config = MaterialExchangeConfig.objects.create(
            corporation_id=1234,
            structure_id=60003760,
            is_active=True,
            capital_custom_ship_options=[
                {
                    "type_id": 67111,
                    "type_name": "Legacy Custom Hull",
                    "ship_class": "super",
                    "ship_class_label": "Supercarrier",
                    "enabled": True,
                }
            ],
        )

        options = _load_capital_ship_options(config=config)

        self.assertEqual({row["type_id"] for row in options}, {19720})


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
        self.assertContains(response, 'value="3,600,000,000.00"')
        self.assertNotContains(response, "Estimated Defaults")
        self.assertNotContains(response, "Add Custom Ship")

        response = self.client.post(
            url,
            {
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

    @patch("indy_hub.views.capital_ship_orders._load_capital_ship_options_for_editor")
    def test_config_view_accepts_comma_formatted_manual_override_submission(
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

        response = self.client.post(
            url,
            {
                "capital_default_lead_time_days": "0",
                "capital_auto_cancel_delay_value": "0",
                "capital_auto_cancel_delay_unit": "hours",
                "capital_auto_cancel_preapproved_state_names": ["Pre-Approved"],
                "estimated_price_19720": "3,650,000,000.00",
            },
        )
        self.assertEqual(response.status_code, 302)

        self.config.refresh_from_db()
        self.assertEqual(
            self.config.get_capital_ship_estimated_price_map()[19720],
            Decimal("3650000000.00"),
        )
