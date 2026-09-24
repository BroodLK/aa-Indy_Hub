# Standard Library
from pathlib import Path

# Django
from django.test import SimpleTestCase

# AA Example App
from indy_hub.services.build_scheduler import (
    BuildSchedule,
    IndustrySlot,
    ManufacturingJob,
    generate_recommendations,
    slot_busy_seconds,
    slot_idle_seconds,
)

STATIC = Path(__file__).resolve().parent.parent / "static" / "indy_hub"
CRAFT_JS = STATIC / "js" / "craft_bp.js"
CRAFT_CSS = STATIC / "css" / "craft_bp.css"
CRAFT_TEMPLATE = (
    Path(__file__).resolve().parent.parent
    / "templates"
    / "indy_hub"
    / "industry"
    / "Craft_BP_v2.html"
)


def make_job(job_id, *, item_type_id=7001, name="Widget", runs=1, duration=1000):
    return ManufacturingJob(
        job_id=job_id,
        item_type_id=item_type_id,
        item_name=name,
        blueprint_type_id=8001,
        quantity_needed=runs,
        quantity_per_run=1,
        runs_required=runs,
        base_time_seconds=duration,
        adjusted_time_seconds=duration,
        total_time_seconds=duration,
    )


class SlotUtilizationTests(SimpleTestCase):
    """available_at_seconds is a completion timestamp, not busy time."""

    def _two_lane_schedule(self):
        # The audit's worked case: lane B waits 1000s on lane A, then runs
        # 2000s. Makespan 3000s.
        component = make_job(1, name="Component", duration=1000)
        final = make_job(2, item_type_id=7002, name="Final", duration=2000)

        lane_a = IndustrySlot(slot_id=0, character_id=1, character_name="Alice")
        lane_b = IndustrySlot(slot_id=1, character_id=2, character_name="Bob")
        lane_a.add_job(component, 0)
        lane_b.add_job(final, 1000)

        return BuildSchedule(
            jobs=[component, final],
            slots=[lane_a, lane_b],
            total_sequential_time_seconds=3000,
            total_parallel_time_seconds=3000,
        )

    def test_busy_and_idle_are_distinguished(self):
        schedule = self._two_lane_schedule()
        lane_a, lane_b = schedule.slots

        self.assertEqual(slot_busy_seconds(lane_a), 1000)
        self.assertEqual(slot_idle_seconds(lane_a), 0)

        # Lane B is busy 2000s but its completion stamp is 3000s.
        self.assertEqual(lane_b.available_at_seconds, 3000)
        self.assertEqual(slot_busy_seconds(lane_b), 2000)
        self.assertEqual(slot_idle_seconds(lane_b), 1000)

    def test_utilization_no_longer_reports_an_idle_lane_as_fully_busy(self):
        payload = self._two_lane_schedule().to_dict()
        lane_b = payload["slots"][1]

        # The old figure was span/makespan, which made the lane defining the
        # makespan tautologically 100% even while idle a third of the time.
        self.assertEqual(lane_b["span_percent"], 100.0)
        self.assertAlmostEqual(lane_b["utilization_percent"], 66.7, places=1)
        self.assertEqual(lane_b["idle_time_seconds"], 1000)
        self.assertEqual(lane_b["busy_time_seconds"], 2000)

    def test_slot_payload_carries_the_facts_the_dead_renderer_had(self):
        lane = self._two_lane_schedule().to_dict()["slots"][0]
        for key in (
            "jobs_count",
            "runs_assigned",
            "busy_time_formatted",
            "idle_time_formatted",
            "completion_time_formatted",
        ):
            self.assertIn(key, lane)


class RecommendationStructureTests(SimpleTestCase):
    def _recommend(self):
        busy = make_job(1, name="Widget", runs=10, duration=2000)
        idle_job = make_job(2, item_type_id=7002, name="Trinket", runs=1, duration=200)
        lane_a = IndustrySlot(slot_id=0, character_id=1, character_name="Alice")
        lane_b = IndustrySlot(slot_id=1, character_id=2, character_name="Bob")
        lane_a.add_job(busy, 0)
        lane_b.add_job(idle_job, 0)
        return generate_recommendations(
            [busy, idle_job], [lane_a, lane_b], 2000, [], {}
        )

    def test_recommendations_are_structured_not_strings(self):
        for rec in self._recommend():
            self.assertIsInstance(rec, dict)
            self.assertIn("code", rec)
            self.assertIn("severity", rec)
            self.assertIn("message", rec)
            self.assertIn("params", rec)

    def test_rebalance_states_a_measured_bound_not_a_promise(self):
        rebalance = next(
            (r for r in self._recommend() if r["code"] == "rebalance_lanes"), None
        )
        self.assertIsNotNone(rebalance)
        self.assertIn("at most", rebalance["message"])
        self.assertGreater(rebalance["params"]["max_saving_seconds"], 0)
        # It must point at a control the user can actually act on.
        self.assertIn("character_id", rebalance["target"])

    def test_no_recommendation_advises_something_impossible(self):
        messages = " ".join(r["message"] for r in self._recommend())
        # The scheduler already splits every item across every selected lane,
        # and a fewest-slots mode already exists.
        self.assertNotIn("splitting runs across multiple jobs", messages)
        self.assertNotIn("Consider using fewer slots", messages)


class BuildTabRendererTests(SimpleTestCase):
    """Source guards: no JS harness in this project."""

    def setUp(self) -> None:
        self.script = CRAFT_JS.read_text(encoding="utf-8")
        self.template = CRAFT_TEMPLATE.read_text(encoding="utf-8")
        self.css = CRAFT_CSS.read_text(encoding="utf-8")

    def test_dead_and_duplicate_renderers_are_gone(self):
        for symbol in (
            "function renderSlotUtilization",
            "function renderJobDetailsTable",
            "function renderSlots(",
            "summarizeGroupedRunDistribution",
        ):
            self.assertNotIn(symbol, self.script)
        self.assertNotIn('id="slotsContainer"', self.template)

    def test_generated_hue_colouring_is_gone(self):
        # Two anti-patterns: generating hues past 8, and eyeballing CVD safety.
        self.assertNotIn("getJobColor", self.script)
        self.assertNotIn("137.508", self.script)
        self.assertNotIn("darkenColor", self.script)

    def test_gantt_paints_from_theme_tokens(self):
        self.assertNotIn("'darkly'", self.script)
        self.assertIn("var(--gantt-surface)", self.script)
        self.assertIn("--gantt-planned", self.css)
        # bootstrap-dark sets data-bs-theme rather than data-theme.
        self.assertIn('[data-bs-theme="dark"] .craft-gantt-surface', self.css)

    def test_in_bar_text_uses_a_text_token_not_the_series_colour(self):
        self.assertNotIn('fill="#fff"', self.script)
        self.assertIn('fill="var(--gantt-label-text)"', self.script)

    def test_keyboard_focus_gets_the_same_tooltip_as_hover(self):
        # <title> never fires on keyboard focus, so focus is wired explicitly.
        self.assertIn("focusin", self.script)
        self.assertIn("craft-gantt-tooltip", self.css)
        self.assertIn(":focus-visible", self.css)

    def test_bars_use_a_roving_tabindex(self):
        self.assertIn('tabindex="-1"', self.script)
        self.assertIn("ArrowRight", self.script)

    def test_legend_names_each_status(self):
        self.assertIn('id="ganttLegend"', self.template)
        self.assertIn("renderGanttLegend", self.script)

    def test_runs_by_job_is_per_chunk(self):
        self.assertIn("One row per scheduled job chunk", self.template)
        self.assertIn('{% trans "Chunk" %}', self.template)
        self.assertNotIn('{% trans "Distribution" %}', self.template)

    def test_unassigned_job_is_not_coerced_into_the_first_lane(self):
        # slot_id 0 is a real lane, so Number(null) === 0 must not match it.
        self.assertIn("assigned === null || assigned === undefined", self.script)

    def test_empty_schedule_explains_itself(self):
        self.assertIn("No scheduled jobs to show", self.script)

    def test_axis_uses_the_clamped_max_time(self):
        self.assertIn("calculateTimeMarkers(safeMaxTime)", self.script)
