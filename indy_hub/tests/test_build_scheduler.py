"""Tests for build job splitting and slot scheduling."""

from django.test import SimpleTestCase

from indy_hub.services.build_scheduler import (
    IndustrySlot,
    ManufacturingJob,
    schedule_jobs_critical_path,
    split_jobs_evenly_across_slots,
    split_runs_evenly,
)


def _make_job(
    *,
    item_type_id: int,
    item_name: str,
    runs_required: int,
    adjusted_time_seconds: int,
    quantity_needed: int,
    quantity_per_run: int = 1,
    dependencies=None,
) -> ManufacturingJob:
    return ManufacturingJob(
        job_id=0,
        item_type_id=item_type_id,
        item_name=item_name,
        blueprint_type_id=item_type_id + 1000,
        quantity_needed=quantity_needed,
        quantity_per_run=quantity_per_run,
        runs_required=runs_required,
        base_time_seconds=adjusted_time_seconds,
        adjusted_time_seconds=adjusted_time_seconds,
        total_time_seconds=adjusted_time_seconds * runs_required,
        dependencies=list(dependencies or []),
    )


class BuildSchedulerTests(SimpleTestCase):
    def test_split_runs_evenly_balances_chunks(self):
        self.assertEqual(split_runs_evenly(10, 3), [4, 3, 3])
        self.assertEqual(split_runs_evenly(2, 5), [1, 1])

    def test_split_jobs_evenly_across_slots_preserves_total_quantity(self):
        job = _make_job(
            item_type_id=101,
            item_name="Capital Part",
            runs_required=10,
            adjusted_time_seconds=120,
            quantity_needed=95,
            quantity_per_run=10,
        )

        split_jobs = split_jobs_evenly_across_slots([job], 3)

        self.assertEqual(len(split_jobs), 3)
        self.assertEqual([job.runs_required for job in split_jobs], [4, 3, 3])
        self.assertEqual(sum(job.quantity_needed for job in split_jobs), 95)
        self.assertEqual([job.chunk_index for job in split_jobs], [1, 2, 3])
        self.assertTrue(all(job.chunk_count == 3 for job in split_jobs))
        self.assertEqual(len({job.job_id for job in split_jobs}), 3)

    def test_schedule_uses_multiple_slots_and_honors_split_dependencies(self):
        parent = _make_job(
            item_type_id=201,
            item_name="Component",
            runs_required=4,
            adjusted_time_seconds=100,
            quantity_needed=4,
        )
        child = _make_job(
            item_type_id=301,
            item_name="Final Hull",
            runs_required=2,
            adjusted_time_seconds=50,
            quantity_needed=2,
            dependencies=[201],
        )

        jobs = split_jobs_evenly_across_slots([parent, child], 2)
        slots = [
            IndustrySlot(slot_id=0, character_id=1, character_name="Builder", slot_name="Builder #1"),
            IndustrySlot(slot_id=1, character_id=1, character_name="Builder", slot_name="Builder #2"),
        ]

        schedule = schedule_jobs_critical_path(jobs, slots)

        parent_jobs = [job for job in schedule.jobs if job.item_type_id == 201]
        child_jobs = [job for job in schedule.jobs if job.item_type_id == 301]

        self.assertEqual(len(parent_jobs), 2)
        self.assertEqual(len(child_jobs), 2)
        self.assertEqual({job.assigned_slot for job in parent_jobs}, {0, 1})
        self.assertTrue(all(job.start_time_seconds == 0 for job in parent_jobs))
        self.assertTrue(all(job.start_time_seconds >= 200 for job in child_jobs))
        self.assertEqual(schedule.total_sequential_time_seconds, 500)
        self.assertEqual(schedule.total_parallel_time_seconds, 250)
