"""
Tests for Material Exchange pricing with configurable base prices.
"""

# Standard Library
from decimal import Decimal

# Django
from django.http import QueryDict
from django.test import TestCase

# AA Example App
from indy_hub.models import (
    MaterialExchangeConfig,
    MaterialExchangeItemPriceOverride,
    MaterialExchangeStock,
)
from indy_hub.views.material_exchange import (
    _compute_effective_buy_unit_price,
    _compute_effective_sell_unit_price,
    _format_buy_stock_type_name,
    _get_item_price_override_maps,
    _parse_submitted_sell_item_quantities,
    _parse_submitted_quantities,
)


class MaterialExchangePricingTests(TestCase):
    """Test price calculations with different base price configurations."""

    def setUp(self):
        """Create test config and stock item."""
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456,
            structure_id=60003760,
            structure_name="Test Structure",
            hangar_division=1,
            sell_markup_percent=Decimal("5.00"),
            sell_markup_base="buy",  # Default: Sell orders based on Jita Buy
            buy_markup_percent=Decimal("10.00"),
            buy_markup_base="buy",  # Default: Buy orders based on Jita Buy
        )

        self.stock = MaterialExchangeStock.objects.create(
            config=self.config,
            type_id=34,  # Tritanium
            type_name="Tritanium",
            quantity=1000000,
            jita_buy_price=Decimal("5.00"),
            jita_sell_price=Decimal("6.00"),
        )

    def test_member_buys_from_hub_using_jita_buy_base(self):
        """Test sell_price_to_member when using Jita Buy as base."""
        # Config: buy_markup_base = "buy", buy_markup_percent = 10%
        # Expected: 5.00 * 1.10 = 5.50
        expected = Decimal("5.50")
        actual = self.stock.sell_price_to_member
        self.assertAlmostEqual(float(actual), float(expected), places=2)

    def test_member_buys_from_hub_using_jita_sell_base(self):
        """Test sell_price_to_member when using Jita Sell as base."""
        self.config.buy_markup_base = "sell"
        self.config.save()

        # Expected: 6.00 * 1.10 = 6.60
        expected = Decimal("6.60")
        actual = self.stock.sell_price_to_member
        self.assertAlmostEqual(float(actual), float(expected), places=2)

    def test_member_sells_to_hub_using_jita_buy_base(self):
        """Test buy_price_from_member when using Jita Buy as base."""
        # Config: sell_markup_base = "buy", sell_markup_percent = 5%
        # Expected: 5.00 * 1.05 = 5.25
        expected = Decimal("5.25")
        actual = self.stock.buy_price_from_member
        self.assertAlmostEqual(float(actual), float(expected), places=2)

    def test_member_sells_to_hub_using_jita_sell_base(self):
        """Test buy_price_from_member when using Jita Sell as base."""
        self.config.sell_markup_base = "sell"
        self.config.save()

        # Expected: 6.00 * 1.05 = 6.30
        expected = Decimal("6.30")
        actual = self.stock.buy_price_from_member
        self.assertAlmostEqual(float(actual), float(expected), places=2)

    def test_zero_markup_on_buy_base(self):
        """Test that 0% markup returns base price exactly."""
        self.config.buy_markup_percent = Decimal("0.00")
        self.config.buy_markup_base = "buy"
        self.config.save()

        # Expected: 5.00 * 1.00 = 5.00
        expected = Decimal("5.00")
        actual = self.stock.sell_price_to_member
        self.assertAlmostEqual(float(actual), float(expected), places=2)

    def test_zero_markup_on_sell_base(self):
        """Test that 0% markup returns base price exactly."""
        self.config.sell_markup_percent = Decimal("0.00")
        self.config.sell_markup_base = "sell"
        self.config.save()

        # Expected: 6.00 * 1.00 = 6.00
        expected = Decimal("6.00")
        actual = self.stock.buy_price_from_member
        self.assertAlmostEqual(float(actual), float(expected), places=2)

    def test_high_markup_calculation(self):
        """Test with higher markup percentage."""
        self.config.buy_markup_percent = Decimal("25.00")
        self.config.buy_markup_base = "sell"
        self.config.save()

        # Expected: 6.00 * 1.25 = 7.50
        expected = Decimal("7.50")
        actual = self.stock.sell_price_to_member
        self.assertAlmostEqual(float(actual), float(expected), places=2)

    def test_default_values_are_buy(self):
        """Test that default markup base is 'buy' for both settings."""
        new_config = MaterialExchangeConfig.objects.create(
            corporation_id=999999,
            structure_id=60003760,
            structure_name="New Test Structure",
            hangar_division=2,
        )

        self.assertEqual(new_config.sell_markup_base, "buy")
        self.assertEqual(new_config.buy_markup_base, "buy")
        self.assertFalse(new_config.enforce_jita_price_bounds)

    def test_bounds_clamp_sell_base_negative_floors_at_buy(self):
        """When enabled, Jita Sell + negative % cannot go below Jita Buy."""
        self.config.enforce_jita_price_bounds = True
        self.config.buy_markup_base = "sell"
        self.config.buy_markup_percent = Decimal("-50.00")
        self.config.save()

        # Base sell is 6.00; -50% would be 3.00, but floor is Jita Buy (5.00)
        expected = Decimal("5.00")
        actual = self.stock.sell_price_to_member
        self.assertAlmostEqual(float(actual), float(expected), places=2)

    def test_bounds_clamp_buy_base_positive_caps_at_sell(self):
        """When enabled, Jita Buy + positive % cannot go above Jita Sell."""
        self.config.enforce_jita_price_bounds = True
        self.config.sell_markup_base = "buy"
        self.config.sell_markup_percent = Decimal("50.00")
        self.config.save()

        # Base buy is 5.00; +50% would be 7.50, but cap is Jita Sell (6.00)
        expected = Decimal("6.00")
        actual = self.stock.buy_price_from_member
        self.assertAlmostEqual(float(actual), float(expected), places=2)

    def test_effective_sell_unit_price_uses_item_markup_override(self):
        effective_price, default_price, has_override = _compute_effective_sell_unit_price(
            config=self.config,
            type_id=self.stock.type_id,
            jita_buy=Decimal("5.00"),
            jita_sell=Decimal("6.00"),
            sell_override_map={
                self.stock.type_id: {
                    "kind": "markup",
                    "percent": Decimal("20.00"),
                    "base": "sell",
                }
            },
        )

        self.assertEqual(default_price, Decimal("5.25"))
        self.assertEqual(effective_price, Decimal("7.20"))
        self.assertTrue(has_override)

    def test_effective_buy_unit_price_uses_item_markup_override(self):
        effective_price, default_price, has_override = _compute_effective_buy_unit_price(
            stock_item=self.stock,
            buy_override_map={
                self.stock.type_id: {
                    "kind": "markup",
                    "percent": Decimal("-10.00"),
                    "base": "sell",
                }
            },
        )

        self.assertEqual(default_price, Decimal("5.50"))
        self.assertEqual(effective_price, Decimal("5.40"))
        self.assertTrue(has_override)

    def test_effective_sell_unit_price_uses_market_group_override_when_no_item_rule(self):
        effective_price, default_price, has_override = _compute_effective_sell_unit_price(
            config=self.config,
            type_id=self.stock.type_id,
            jita_buy=Decimal("5.00"),
            jita_sell=Decimal("6.00"),
            sell_override_map={},
            sell_market_group_override_map={
                300: {
                    "kind": "markup",
                    "percent": Decimal("10.00"),
                    "base": "buy",
                }
            },
            type_market_group_path_map={self.stock.type_id: [100, 200, 300]},
        )

        self.assertEqual(default_price, Decimal("5.25"))
        self.assertEqual(effective_price, Decimal("5.50"))
        self.assertTrue(has_override)

    def test_effective_sell_unit_price_uses_container_override_when_in_container(self):
        effective_price, default_price, has_override = _compute_effective_sell_unit_price(
            config=self.config,
            type_id=self.stock.type_id,
            jita_buy=Decimal("5.00"),
            jita_sell=Decimal("6.00"),
            sell_override_map={},
            sell_market_group_override_map={},
            sell_container_override={
                "kind": "fixed",
                "price": Decimal("4.10"),
            },
            in_container=True,
        )

        self.assertEqual(default_price, Decimal("5.25"))
        self.assertEqual(effective_price, Decimal("4.10"))
        self.assertTrue(has_override)

    def test_effective_sell_unit_price_item_rule_beats_container_rule(self):
        effective_price, default_price, has_override = _compute_effective_sell_unit_price(
            config=self.config,
            type_id=self.stock.type_id,
            jita_buy=Decimal("5.00"),
            jita_sell=Decimal("6.00"),
            sell_override_map={
                self.stock.type_id: {
                    "kind": "fixed",
                    "price": Decimal("3.90"),
                }
            },
            sell_market_group_override_map={},
            sell_container_override={
                "kind": "fixed",
                "price": Decimal("4.10"),
            },
            in_container=True,
        )

        self.assertEqual(default_price, Decimal("5.25"))
        self.assertEqual(effective_price, Decimal("3.90"))
        self.assertTrue(has_override)

    def test_effective_sell_unit_price_item_rule_beats_market_group_rule(self):
        effective_price, default_price, has_override = _compute_effective_sell_unit_price(
            config=self.config,
            type_id=self.stock.type_id,
            jita_buy=Decimal("5.00"),
            jita_sell=Decimal("6.00"),
            sell_override_map={
                self.stock.type_id: {
                    "kind": "fixed",
                    "price": Decimal("4.00"),
                }
            },
            sell_market_group_override_map={
                300: {
                    "kind": "markup",
                    "percent": Decimal("25.00"),
                    "base": "sell",
                }
            },
            type_market_group_path_map={self.stock.type_id: [100, 200, 300]},
        )

        self.assertEqual(default_price, Decimal("5.25"))
        self.assertEqual(effective_price, Decimal("4.00"))
        self.assertTrue(has_override)

    def test_effective_buy_unit_price_uses_market_group_override_when_no_item_rule(self):
        effective_price, default_price, has_override = _compute_effective_buy_unit_price(
            stock_item=self.stock,
            buy_override_map={},
            buy_market_group_override_map={
                200: {
                    "kind": "fixed",
                    "price": Decimal("14.00"),
                }
            },
            type_market_group_path_map={self.stock.type_id: [100, 200]},
        )

        self.assertEqual(default_price, Decimal("5.50"))
        self.assertEqual(effective_price, Decimal("14.00"))
        self.assertTrue(has_override)

    def test_effective_buy_unit_price_uses_container_override_when_in_container(self):
        effective_price, default_price, has_override = _compute_effective_buy_unit_price(
            stock_item=self.stock,
            buy_override_map={},
            buy_market_group_override_map={},
            buy_container_override={
                "kind": "markup",
                "percent": Decimal("10.00"),
                "base": "sell",
            },
            in_container=True,
        )

        self.assertEqual(default_price, Decimal("5.50"))
        self.assertEqual(effective_price, Decimal("6.60"))
        self.assertTrue(has_override)

    def test_item_override_map_prefers_fixed_over_markup_when_both_are_set(self):
        MaterialExchangeItemPriceOverride.objects.create(
            config=self.config,
            type_id=self.stock.type_id,
            type_name="Tritanium",
            sell_markup_percent_override=Decimal("10.00"),
            sell_markup_base_override="sell",
            sell_price_override=Decimal("4.00"),
        )

        sell_override_map, _buy_override_map = _get_item_price_override_maps(self.config)
        self.assertIn(self.stock.type_id, sell_override_map)
        self.assertEqual(sell_override_map[self.stock.type_id]["kind"], "fixed")
        self.assertEqual(
            sell_override_map[self.stock.type_id]["price"],
            Decimal("4.00"),
        )

    def test_parse_submitted_quantities_sums_split_row_inputs(self):
        payload = QueryDict("", mutable=True)
        payload.update(
            {
                "qty_34_0": "2",
                "qty_34_7": "5",
                "qty_35": "3",
                "qty_invalid": "10",
                "qty_36_1": "0",
            }
        )

        parsed = _parse_submitted_quantities(payload)
        self.assertEqual(parsed.get(34), 7)
        self.assertEqual(parsed.get(35), 3)
        self.assertNotIn(36, parsed)

    def test_parse_submitted_sell_item_quantities_keeps_blueprint_variant(self):
        payload = QueryDict("", mutable=True)
        payload.update(
            {
                "qty_34_std_0": "2",
                "qty_34_std_1": "3",
                "qty_33003_bpc_4": "1",
                "qty_33003_bpo_5": "1",
                "qty_35_7": "4",  # legacy format
                "qty_36_std_incan_9": "3",
                "qty_36_std_incan_11": "2",
                "qty_36_std_root_12": "1",
                "qty_36_bpc_9": "0",
            }
        )

        parsed = _parse_submitted_sell_item_quantities(payload)
        by_key = {
            (
                int(entry["type_id"]),
                str(entry["blueprint_variant"] or ""),
                bool(entry.get("in_container")),
            ): int(entry["quantity"])
            for entry in parsed
        }

        self.assertEqual(by_key.get((34, "", False)), 5)
        self.assertEqual(by_key.get((33003, "bpc", False)), 1)
        self.assertEqual(by_key.get((33003, "bpo", False)), 1)
        self.assertEqual(by_key.get((35, "", False)), 4)
        self.assertEqual(by_key.get((36, "", True)), 5)
        self.assertEqual(by_key.get((36, "", False)), 1)
        self.assertNotIn((36, "bpc", False), by_key)

    def test_format_buy_stock_type_name_appends_blueprint_variant_suffix(self):
        self.assertEqual(
            _format_buy_stock_type_name("Capital Armor Plates Blueprint", "bpc"),
            "Capital Armor Plates Blueprint (BPC)",
        )
        self.assertEqual(
            _format_buy_stock_type_name(
                "Capital Armor Plates Blueprint (BPC)",
                "bpo",
            ),
            "Capital Armor Plates Blueprint (BPO)",
        )
        self.assertEqual(
            _format_buy_stock_type_name("Tritanium", ""),
            "Tritanium",
        )
