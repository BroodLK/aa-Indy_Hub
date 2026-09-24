# Standard Library
from datetime import timedelta
from types import SimpleNamespace

# Django
from django.test import SimpleTestCase
from django.utils import timezone

# AA Example App
from indy_hub.services.build_scheduler import (
    BuildSchedule,
    IndustrySlot,
    ManufacturingJob,
)
from indy_hub.services.schedule_tracking import (
    normalize_planned_chunks,
    planned_window,
    reconcile_schedule_jobs,
)

DURATION = 7200


def build_real_schedule(
    *,
    character_id=9001,
    product_type_id=7001,
    runs=2,
    activity_id=1,
    duration=DURATION,
):
    """Produce an authentic BuildSchedule.to_dict() payload.

    The reconciler must work against the fields the scheduler actually emits —
    the character lives on the slot and the times are offsets, not datetimes.
    """
    job = ManufacturingJob(
        job_id=1,
        item_type_id=product_type_id,
        item_name="Widget",
        blueprint_type_id=8001,
        quantity_needed=10,
        quantity_per_run=5,
        runs_required=runs,
        base_time_seconds=duration,
        adjusted_time_seconds=duration,
        total_time_seconds=duration,
        activity_id=activity_id,
    )
    slot = IndustrySlot(
        slot_id=1, character_id=character_id, character_name="Pilot One"
    )
    slot.add_job(job, 0)
    return BuildSchedule(
        jobs=[job],
        slots=[slot],
        total_sequential_time_seconds=duration,
        total_parallel_time_seconds=duration,
    ).to_dict()


def esi_job(**overrides):
    defaults = {
        "job_id": 42,
        "character_id": 9001,
        "installer_id": 9001,
        "product_type_id": 7001,
        "blueprint_type_id": 8001,
        "station_id": None,
        "activity_id": 1,
        "runs": 2,
        "status": "active",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class RealSchedulePayloadTests(SimpleTestCase):
    """The scheduler's own output shape must be understood end to end."""

    def test_character_is_resolved_from_the_assigned_slot(self):
        schedule = build_real_schedule()
        # The chunk itself carries no character; only assigned_slot links back.
        self.assertNotIn("character_id", schedule["jobs"][0])
        chunks = normalize_planned_chunks(schedule, anchor=timezone.now())
        self.assertEqual(chunks[0]["character_id"], 9001)
        self.assertEqual(chunks[0]["product_type_id"], 7001)
        self.assertEqual(chunks[0]["activity_id"], 1)

    def test_relative_offsets_need_an_anchor(self):
        schedule = build_real_schedule()
        without = normalize_planned_chunks(schedule, anchor=None)
        self.assertIsNone(without[0]["planned_start"])

        anchor = timezone.now()
        with_anchor = normalize_planned_chunks(schedule, anchor=anchor)
        self.assertEqual(with_anchor[0]["planned_start"], anchor)
        self.assertEqual(
            with_anchor[0]["planned_end"], anchor + timedelta(seconds=DURATION)
        )

    def test_matches_a_real_payload_against_a_live_job(self):
        anchor = timezone.now()
        job = esi_job(start_date=anchor, end_date=anchor + timedelta(seconds=DURATION))
        result = reconcile_schedule_jobs(build_real_schedule(), [job], anchor=anchor)
        self.assertEqual(result["matched"], 1)
        self.assertEqual(result["results"][0]["status"], "running")
        self.assertEqual(result["results"][0]["character_id"], 9001)
        self.assertEqual(result["results"][0]["planned_duration_seconds"], DURATION)
        self.assertEqual(result["results"][0]["observed_duration_seconds"], DURATION)
        self.assertEqual(result["results"][0]["duration_delta_seconds"], 0)

    def test_planned_window_bounds_the_candidate_search(self):
        anchor = timezone.now()
        start, end = planned_window(
            normalize_planned_chunks(build_real_schedule(), anchor=anchor)
        )
        self.assertLess(start, anchor)
        self.assertGreater(end, anchor + timedelta(seconds=DURATION))


class ConservativeMatchingTests(SimpleTestCase):
    def test_does_not_match_by_item_name_alone(self):
        job = esi_job(job_id=43, start_date=None, end_date=None)
        result = reconcile_schedule_jobs({"jobs": [{"item_name": "Same Item"}]}, [job])
        self.assertEqual(result["matched"], 0)
        self.assertEqual(result["results"][0]["status"], "unmatched")
        self.assertEqual(
            result["results"][0]["reason"], "insufficient identifying fields"
        )

    def test_does_not_match_without_a_character(self):
        schedule = build_real_schedule()
        # Drop the slot so no character can be resolved for the chunk.
        schedule["slots"] = []
        anchor = timezone.now()
        job = esi_job(start_date=anchor, end_date=anchor + timedelta(seconds=DURATION))
        result = reconcile_schedule_jobs(schedule, [job], anchor=anchor)
        self.assertEqual(result["matched"], 0)
        self.assertEqual(
            result["results"][0]["reason"], "insufficient identifying fields"
        )

    def test_other_character_job_is_not_matched(self):
        anchor = timezone.now()
        job = esi_job(
            character_id=9999,
            installer_id=9999,
            start_date=anchor,
            end_date=anchor + timedelta(seconds=DURATION),
        )
        result = reconcile_schedule_jobs(build_real_schedule(), [job], anchor=anchor)
        self.assertEqual(result["matched"], 0)
        self.assertEqual(result["results"][0]["reason"], "no conservative match")

    def test_other_activity_is_not_matched(self):
        anchor = timezone.now()
        # A reaction job must never satisfy a manufacturing chunk.
        job = esi_job(
            activity_id=11,
            start_date=anchor,
            end_date=anchor + timedelta(seconds=DURATION),
        )
        result = reconcile_schedule_jobs(
            build_real_schedule(activity_id=1), [job], anchor=anchor
        )
        self.assertEqual(result["matched"], 0)

    def test_job_outside_the_planned_window_is_not_matched(self):
        anchor = timezone.now()
        stale_start = anchor - timedelta(days=30)
        job = esi_job(
            start_date=stale_start,
            end_date=stale_start + timedelta(seconds=DURATION),
            status="delivered",
        )
        result = reconcile_schedule_jobs(build_real_schedule(), [job], anchor=anchor)
        self.assertEqual(result["matched"], 0)

    def test_run_count_mismatch_is_not_matched(self):
        anchor = timezone.now()
        job = esi_job(
            runs=7, start_date=anchor, end_date=anchor + timedelta(seconds=DURATION)
        )
        result = reconcile_schedule_jobs(
            build_real_schedule(runs=2), [job], anchor=anchor
        )
        self.assertEqual(result["matched"], 0)

    def test_product_id_is_never_compared_against_a_blueprint_id(self):
        anchor = timezone.now()
        # The job's blueprint_type_id equals the chunk's product; that
        # coincidence must not be treated as a match.
        job = esi_job(
            product_type_id=None,
            blueprint_type_id=7001,
            start_date=anchor,
            end_date=anchor + timedelta(seconds=DURATION),
        )
        result = reconcile_schedule_jobs(
            build_real_schedule(product_type_id=7001), [job], anchor=anchor
        )
        self.assertEqual(result["matched"], 0)

    def test_one_job_cannot_satisfy_two_chunks(self):
        anchor = timezone.now()
        schedule = build_real_schedule()
        schedule["jobs"].append(dict(schedule["jobs"][0], job_id=2))
        job = esi_job(start_date=anchor, end_date=anchor + timedelta(seconds=DURATION))
        result = reconcile_schedule_jobs(schedule, [job], anchor=anchor)
        self.assertEqual(result["matched"], 1)
        self.assertEqual(result["results"][1]["status"], "unmatched")


class JobStatusTests(SimpleTestCase):
    def test_cancelled_job_is_not_reported_as_running(self):
        anchor = timezone.now()
        job = esi_job(
            status="cancelled",
            start_date=anchor,
            end_date=anchor + timedelta(seconds=DURATION),
        )
        result = reconcile_schedule_jobs(build_real_schedule(), [job], anchor=anchor)
        self.assertEqual(result["results"][0]["status"], "cancelled")
        self.assertEqual(result["counts"]["cancelled"], 1)

    def test_reverted_job_is_not_reported_as_running(self):
        anchor = timezone.now()
        job = esi_job(
            status="reverted",
            start_date=anchor,
            end_date=anchor + timedelta(seconds=DURATION),
        )
        result = reconcile_schedule_jobs(build_real_schedule(), [job], anchor=anchor)
        self.assertEqual(result["results"][0]["status"], "cancelled")

    def test_delivered_job_is_completed(self):
        anchor = timezone.now()
        job = esi_job(
            status="delivered",
            start_date=anchor,
            end_date=anchor + timedelta(seconds=DURATION),
        )
        result = reconcile_schedule_jobs(build_real_schedule(), [job], anchor=anchor)
        self.assertEqual(result["results"][0]["status"], "completed")

    def test_marks_overdue_active_job_completed(self):
        end = timezone.now() - timedelta(minutes=1)
        anchor = end - timedelta(seconds=DURATION)
        job = esi_job(start_date=anchor, end_date=end)
        result = reconcile_schedule_jobs(build_real_schedule(), [job], anchor=anchor)
        self.assertEqual(result["results"][0]["status"], "completed")

    def test_results_are_json_serializable(self):
        anchor = timezone.now().replace(microsecond=0)
        job = esi_job(start_date=anchor, end_date=anchor + timedelta(seconds=DURATION))
        result = reconcile_schedule_jobs(
            build_real_schedule(), [job], anchor=anchor.isoformat()
        )
        self.assertIsInstance(result["refreshed_at"], str)
        self.assertIsInstance(result["anchor"], str)
        self.assertIsInstance(result["results"][0]["observed_start"], str)
        self.assertIsInstance(result["results"][0]["planned_start"], str)


class TrackingStateTests(SimpleTestCase):
    """Delayed, stale and unavailable are distinct from running and unmatched."""

    def _reconcile(self, *, job_end_offset, refreshed_offset):
        anchor = timezone.now() - timedelta(hours=1)
        schedule = build_real_schedule()
        job = esi_job(
            start_date=anchor,
            end_date=anchor + timedelta(seconds=job_end_offset),
        )
        return reconcile_schedule_jobs(
            schedule,
            [job],
            anchor=anchor,
            refreshed_at=anchor + timedelta(seconds=refreshed_offset),
        )["results"][0]

    def test_running_on_plan_is_running(self) -> None:
        row = self._reconcile(job_end_offset=DURATION, refreshed_offset=DURATION // 2)
        self.assertEqual(row["status"], "running")

    def test_job_ending_well_after_plan_is_delayed(self) -> None:
        row = self._reconcile(
            job_end_offset=DURATION * 2, refreshed_offset=DURATION // 2
        )
        self.assertEqual(row["status"], "delayed")
        self.assertEqual(row["duration_delta_seconds"], DURATION)

    def test_small_overrun_within_tolerance_is_still_running(self) -> None:
        row = self._reconcile(
            job_end_offset=DURATION + 300, refreshed_offset=DURATION // 2
        )
        self.assertEqual(row["status"], "running")

    def test_finished_job_is_completed_even_if_late(self) -> None:
        row = self._reconcile(
            job_end_offset=DURATION * 2, refreshed_offset=DURATION * 3
        )
        self.assertEqual(row["status"], "completed")


class TrackingFreshnessTests(SimpleTestCase):
    def test_missing_scope_is_unavailable(self) -> None:
        # AA Example App
        from indy_hub.services.schedule_tracking import tracking_freshness

        result = tracking_freshness(timezone.now(), has_scope=False)
        self.assertEqual(result["state"], "unavailable")
        self.assertEqual(result["reason"], "missing_scope")

    def test_old_sync_is_stale_and_never_synced_is_stale(self) -> None:
        # AA Example App
        from indy_hub.services.schedule_tracking import (
            TRACKING_STALE_AFTER,
            tracking_freshness,
        )

        old = timezone.now() - TRACKING_STALE_AFTER - timedelta(minutes=1)
        self.assertEqual(tracking_freshness(old, has_scope=True)["state"], "stale")
        never = tracking_freshness(None, has_scope=True)
        self.assertEqual((never["state"], never["reason"]), ("stale", "never_synced"))

    def test_recent_sync_is_fresh(self) -> None:
        # AA Example App
        from indy_hub.services.schedule_tracking import tracking_freshness

        result = tracking_freshness(
            timezone.now() - timedelta(minutes=5), has_scope=True
        )
        self.assertEqual(result["state"], "fresh")
        self.assertTrue(result["jobs_last_synced"])
