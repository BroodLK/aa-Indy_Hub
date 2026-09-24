# Standard Library
from pathlib import Path

# Django
from django.template.loader import render_to_string
from django.test import SimpleTestCase

TEMPLATES = Path(__file__).resolve().parent.parent / "templates" / "indy_hub"
STATIC = Path(__file__).resolve().parent.parent / "static" / "indy_hub"

CRAFT_TEMPLATE = TEMPLATES / "industry" / "Craft_BP_v2.html"
CRAFT_JS = STATIC / "js" / "craft_bp.js"


class PageHeaderBackControlTests(SimpleTestCase):
    """The shared header renders Back only when a caller asks for it."""

    def test_back_control_renders_when_back_url_supplied(self):
        html = render_to_string(
            "indy_hub/partials/page_header.html",
            {"title": "Widget", "back_url": "/somewhere/"},
        )
        self.assertIn("/somewhere/", html)
        self.assertIn("fa-arrow-left", html)

    def test_back_control_absent_without_back_url(self):
        html = render_to_string(
            "indy_hub/partials/page_header.html",
            {"title": "Widget", "header_controls": "<button>Save</button>"},
        )
        self.assertNotIn("fa-arrow-left", html)
        self.assertIn("Save", html)


class CraftTemplateStructureTests(SimpleTestCase):
    """Source-level guards.

    The project has no browser test harness, so these pin the specific
    regressions rather than the rendered output.
    """

    def setUp(self) -> None:
        self.template = CRAFT_TEMPLATE.read_text(encoding="utf-8")
        self.script = CRAFT_JS.read_text(encoding="utf-8")

    def test_header_include_is_context_isolated(self):
        # Without `only` the view's back_url leaks into the shared partial and
        # renders a Back button next to Save/Load.
        include_line = next(
            line
            for line in self.template.splitlines()
            if "partials/page_header.html" in line
        )
        self.assertIn(" only ", f"{include_line} ")

    def test_stat_counters_explain_themselves(self):
        self.assertIn("craft-stat-help", self.template)
        self.assertIn(
            "Distinct items you need to buy or produce, not the total quantity.",
            self.template,
        )
        self.assertIn(
            "Blueprints this plan needs, including sub-components you chose to produce.",
            self.template,
        )

    def test_material_count_is_not_scraped_from_replaced_dom(self):
        # updateQuickStats() used to count .craft-item-row nodes that
        # updateMaterialsTabFromState() had already removed, so the card
        # permanently showed '-'.
        self.assertNotIn(
            "document.querySelectorAll('.craft-item-row').length", self.template
        )
        self.assertIn("totalMaterialsCount", self.script)

    def test_material_groups_render_as_collapsible_cards(self):
        # The Expand/Collapse all controls target .craft-group-card and the CSS
        # hides .craft-group-items, so the rendered cards must carry both.
        self.assertIn("craft-group-card card shadow-sm mb-4", self.script)
        self.assertIn("craft-group-items card-body p-0", self.script)

    def test_usage_summary_is_not_duplicated_in_the_group_header(self):
        # The per-row "Needed for" column already states this relationship.
        self.assertNotIn("fa-gears me-1", self.script)

    def test_how_to_states_which_values_are_estimates(self):
        self.assertIn('id="howto-estimates"', self.template)
        for fragment in (
            "Capital prices",
            "Industry fees and taxes",
            "Shipping quotes",
            "Schedule times",
        ):
            self.assertIn(fragment, self.template)

    def test_how_to_explains_optimize_objective_and_derivation(self):
        self.assertIn("What Optimize does", self.template)
        self.assertIn("working bottom-up from raw materials", self.template)

    def test_how_to_states_compute_order_of_operations(self):
        self.assertIn("Before you press Compute", self.template)
        self.assertIn("Pick exactly one material source.", self.template)


class PlannerTableStructureTests(SimpleTestCase):
    """Every planner row must have as many cells as the header has columns."""

    def setUp(self) -> None:
        self.template = CRAFT_TEMPLATE.read_text(encoding="utf-8")
        self.script = CRAFT_JS.read_text(encoding="utf-8")

    def _build_row_template(self) -> str:
        start = self.script.index("function buildFinancialRow(")
        end = self.script.index("function updateFinancialRow(")
        return self.script[start:end]

    def test_js_built_rows_have_a_margin_cell(self) -> None:
        # buildFinancialRow emitted 5 <td> into a 6-column table, so every
        # JS-built row (BPC contract, compressed ore, manual) rendered short
        # and shifted left under the Margin header.
        body = self._build_row_template()
        template_literal = body[body.index("row.innerHTML = `") : body.index("`;")]
        self.assertEqual(template_literal.count("<td"), 6)
        self.assertIn("item-margin", template_literal)

    def test_js_built_rows_match_the_server_row_cell_count(self) -> None:
        # Server material row: count the <td> between the row open and close.
        start = self.template.index('<tr data-type-id="{{ mat.type_id }}">')
        end = self.template.index("</tr>", start)
        self.assertEqual(self.template[start:end].count("<td"), 6)

    def test_margin_is_an_em_dash_with_an_explanation(self) -> None:
        body = self._build_row_template()
        self.assertIn("marginTooltip", body)
        self.assertIn("—", body)
        # BPC and non-BPC get different wording; neither invents a number.
        self.assertIn("no resale margin", body)
        self.assertIn("no separate market price", body)

    def test_unreachable_margin_colour_rules_are_gone(self) -> None:
        css = (STATIC / "css" / "craft_bp.css").read_text(encoding="utf-8")
        # Nothing ever added these classes; the only positive/negative toggles
        # in the JS target .hero-kpi.
        self.assertNotIn(".item-margin.positive", css)
        self.assertNotIn(".item-margin.negative", css)


class ShoppingListPresentationTests(SimpleTestCase):
    def setUp(self) -> None:
        self.template = CRAFT_TEMPLATE.read_text(encoding="utf-8")
        self.script = CRAFT_JS.read_text(encoding="utf-8")

    def test_items_needed_badge_exists_and_is_populated(self) -> None:
        self.assertIn('id="shoppingItemsNeededCount"', self.template)
        self.assertIn("shoppingItemsNeededCount", self.script)

    def test_quantity_columns_carry_icons_and_classes_not_colour_alone(self) -> None:
        for token in (
            "craft-needed-qty",
            "craft-owned-qty",
            "craft-buy-qty",
            "craft-surplus-qty",
            "fa-cart-plus",
        ):
            self.assertIn(token, self.script)

    def test_clipboard_still_reads_the_buy_quantity_by_attribute(self) -> None:
        # Guards the blocker-pass fix against the row markup changing again.
        self.assertIn("row.querySelector('[data-qty-buy]')", self.template)


class DerivedPriceEstimateTests(SimpleTestCase):
    def setUp(self) -> None:
        self.script = CRAFT_JS.read_text(encoding="utf-8")
        self.api = (STATIC / "js" / "craft_bp_simulation_api.js").read_text(
            encoding="utf-8"
        )

    def test_estimate_tier_sits_below_market_and_never_above_real(self) -> None:
        start = self.api.index("function getPrice(")
        end = self.api.index("function setPrice(")
        get_price = self.api[start:end]
        # In the 'buy' branch, real must be consulted before fuzzwork, and
        # fuzzwork before the derived estimate.
        buy = get_price[get_price.index("preference === 'buy'") :]
        buy = buy[: buy.index("preference === 'sale'")]
        self.assertLess(buy.index("record.real"), buy.index("record.fuzzwork"))
        self.assertLess(buy.index("record.fuzzwork"), buy.index("record.estimate"))

    def test_derived_estimates_are_marked_in_the_ui(self) -> None:
        self.assertIn("applyDerivedEstimateMarker", self.script)
        self.assertIn("craft-price-derived-estimate", self.script)
        self.assertIn("No market price for this item", self.script)


class ShippingSectionTests(SimpleTestCase):
    def setUp(self) -> None:
        self.template = CRAFT_TEMPLATE.read_text(encoding="utf-8")

    def test_route_selector_lives_in_the_shipping_section(self) -> None:
        shipping_start = self.template.index('<i class="fas fa-truck text-info"></i>')
        shipping_end = self.template.index("</section>", shipping_start)
        section = self.template[shipping_start:shipping_end]
        self.assertIn('id="importFeesRouteSelect"', section)
        self.assertIn('id="shippingFeeBreakdownSection"', section)

    def test_shipping_cost_row_stays_in_the_planner_table(self) -> None:
        # It is the DOM insert anchor for every JS-built row, a cost row in
        # recalcFinancials, and is special-cased by the price-input delegation.
        body_start = self.template.index('id="financialItemsBody"')
        body_end = self.template.index("</tbody>", body_start)
        self.assertIn('id="shippingCostRow"', self.template[body_start:body_end])


class BuyTabTaxDefaultTests(SimpleTestCase):
    def setUp(self) -> None:
        self.script = CRAFT_JS.read_text(encoding="utf-8")
        self.view = (
            Path(__file__).resolve().parent.parent / "views" / "industry.py"
        ).read_text(encoding="utf-8")

    def test_industry_fees_default_on(self) -> None:
        block = self.view[self.view.index("industry_fee_config = {") :][:400]
        self.assertIn('request.GET.get("industry_fee_enabled", "1")', block)
        self.assertIn("default=True", block)

    def test_buy_tab_loads_fees_without_forcing(self) -> None:
        start = self.script.index("const buyTabButton")
        block = self.script[start : start + 400]
        self.assertIn("ensureIndustryFeeEstimateUpToDate()", block)
        # force:true would bypass the in-flight dedupe, the current-signature
        # short-circuit and the error retry throttle.
        self.assertNotIn("force: true", block)


class RunOptimizedRetiredTests(SimpleTestCase):
    """Run optimized was removed; Plan's Optimize is the single optimizer."""

    def test_markup_and_code_are_gone(self):
        template = CRAFT_TEMPLATE.read_text(encoding="utf-8")
        script = CRAFT_JS.read_text(encoding="utf-8")
        for marker in (
            "run-optimized-pane",
            'data-tab-name="run_optimized"',
            "runOptimizedChart",
        ):
            self.assertNotIn(marker, template)
        for marker in (
            "initializeRunOptimizedTab",
            "findOptimalRuns",
            "CRAFT_RUN_OPTIMIZED_STATE",
            "runOptimized:",
        ):
            self.assertNotIn(marker, script)

    def test_how_to_documents_the_decision(self):
        template = CRAFT_TEMPLATE.read_text(encoding="utf-8")
        self.assertIn("Optimize on the Plan tab is the single optimizer", template)
        self.assertIn("Run count is your decision.", template)

    def test_optimizer_does_not_credit_surplus_against_cost(self):
        script = CRAFT_JS.read_text(encoding="utf-8")
        self.assertNotIn("inputsCost - credit", script)
        self.assertNotIn("inputsCost - surplusCredit", script)


class RunOptimizedStateMigrationTests(SimpleTestCase):
    def test_old_snapshot_on_run_optimized_opens_on_plan(self):
        # AA Example App
        from indy_hub.services.production_simulation_state import (
            STATE_SCHEMA_VERSION,
            migrate_ui_state,
        )

        state = migrate_ui_state(
            {
                "schemaVersion": 2,
                "craftMainTab": "run_optimized",
                "runOptimized": {"bestLabel": "Best up to 100"},
                "buildPlannerSlots": {"9001": 4},
            }
        )

        self.assertEqual(state["craftMainTab"], "plan")
        self.assertNotIn("runOptimized", state)
        self.assertEqual(state["buildPlannerSlots"], {"9001": 4})
        self.assertEqual(state["schemaVersion"], STATE_SCHEMA_VERSION)

    def test_valid_tab_is_kept(self):
        # AA Example App
        from indy_hub.services.production_simulation_state import (
            migrate_ui_state,
        )

        self.assertEqual(
            migrate_ui_state({"craftMainTab": "build"})["craftMainTab"], "build"
        )

    def test_retired_tab_is_not_a_valid_preference(self):
        # AA Example App
        from indy_hub.services.production_simulation_state import (
            normalize_preference_state,
        )

        self.assertEqual(normalize_preference_state({"activeTab": "run_optimized"}), {})
