"""Tests for build job splitting and slot scheduling."""

from django.test import SimpleTestCase

from indy_hub.services.build_scheduler import (
    IndustrySlot,
    ManufacturingJob,
    calculate_schedule_for_mode,
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
    required_skills=None,
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
        required_skills=list(required_skills or []),
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

    def test_schedule_only_assigns_jobs_to_slots_with_required_skills(self):
        jobs = [
            _make_job(
                item_type_id=401,
                item_name="Capital Core",
                runs_required=2,
                adjusted_time_seconds=90,
                quantity_needed=2,
                required_skills=[
                    {
                        "skill_type_id": 3380,
                        "level": 4,
                        "skill_name": "Capital Construction",
                    }
                ],
            )
        ]
        slots = [
            IndustrySlot(
                slot_id=0,
                character_id=10,
                character_name="Builder A",
                slot_name="Builder A #1",
                skill_levels={3380: 3},
            ),
            IndustrySlot(
                slot_id=1,
                character_id=11,
                character_name="Builder B",
                slot_name="Builder B #1",
                skill_levels={3380: 5},
            ),
        ]

        schedule = schedule_jobs_critical_path(jobs, slots)

        self.assertEqual(schedule.jobs[0].assigned_slot, 1)

    def test_schedule_raises_when_no_slot_meets_required_skills(self):
        jobs = [
            _make_job(
                item_type_id=501,
                item_name="Advanced Hull",
                runs_required=1,
                adjusted_time_seconds=60,
                quantity_needed=1,
                required_skills=[
                    {
                        "skill_type_id": 11433,
                        "level": 5,
                        "skill_name": "Advanced Industry",
                    }
                ],
            )
        ]
        slots = [
            IndustrySlot(
                slot_id=0,
                character_id=10,
                character_name="Builder A",
                slot_name="Builder A #1",
                skill_levels={11433: 4},
            )
        ]

        with self.assertRaisesMessage(
            ValueError,
            "No selected characters meet the skill requirements for Advanced Hull: Advanced Industry 5.",
        ):
            schedule_jobs_critical_path(jobs, slots)

    def test_schedule_prioritizes_jobs_on_the_final_product_path(self):
        jobs = [
            _make_job(
                item_type_id=601,
                item_name="Component Chain",
                runs_required=1,
                adjusted_time_seconds=100,
                quantity_needed=1,
            ),
            _make_job(
                item_type_id=602,
                item_name="Long Side Job A",
                runs_required=1,
                adjusted_time_seconds=1000,
                quantity_needed=1,
            ),
            _make_job(
                item_type_id=603,
                item_name="Long Side Job B",
                runs_required=1,
                adjusted_time_seconds=1000,
                quantity_needed=1,
            ),
            _make_job(
                item_type_id=701,
                item_name="Final Product",
                runs_required=1,
                adjusted_time_seconds=50,
                quantity_needed=1,
                dependencies=[601],
            ),
        ]
        slots = [
            IndustrySlot(slot_id=0, character_id=1, character_name="Builder", slot_name="Builder #1"),
            IndustrySlot(slot_id=1, character_id=1, character_name="Builder", slot_name="Builder #2"),
        ]

        schedule = schedule_jobs_critical_path(
            jobs,
            slots,
            preferred_item_type_id=701,
        )

        jobs_by_item = {job.item_type_id: job for job in schedule.jobs}

        self.assertEqual(jobs_by_item[601].start_time_seconds, 0)
        self.assertEqual(jobs_by_item[701].start_time_seconds, 100)
        self.assertEqual(jobs_by_item[701].end_time_seconds, 150)
        self.assertEqual(jobs_by_item[603].start_time_seconds, 150)

    def test_fewest_slots_mode_uses_minimum_valid_slot_count(self):
        jobs = [
            _make_job(
                item_type_id=801,
                item_name="Capital Core",
                runs_required=1,
                adjusted_time_seconds=90,
                quantity_needed=1,
                required_skills=[
                    {
                        "skill_type_id": 3380,
                        "level": 4,
                        "skill_name": "Capital Construction",
                    }
                ],
            ),
            _make_job(
                item_type_id=802,
                item_name="Advanced Hull",
                runs_required=1,
                adjusted_time_seconds=90,
                quantity_needed=1,
                required_skills=[
                    {
                        "skill_type_id": 11433,
                        "level": 5,
                        "skill_name": "Advanced Industry",
                    }
                ],
            ),
        ]
        slots = [
            IndustrySlot(
                slot_id=0,
                character_id=10,
                character_name="Builder A",
                slot_name="Builder A #1",
                skill_levels={3380: 5},
            ),
            IndustrySlot(
                slot_id=1,
                character_id=11,
                character_name="Builder B",
                slot_name="Builder B #1",
                skill_levels={11433: 5},
            ),
        ]

        schedule = calculate_schedule_for_mode(
            jobs,
            slots,
            schedule_mode="fewest_slots",
        )

        self.assertEqual(schedule.used_slot_count, 2)
        self.assertEqual(len(schedule.slots), 2)

    def test_component_target_mode_picks_minimum_slots_that_meet_deadline(self):
        component = _make_job(
            item_type_id=901,
            item_name="Component Pack",
            runs_required=6,
            adjusted_time_seconds=100,
            quantity_needed=6,
        )
        final_product = _make_job(
            item_type_id=902,
            item_name="Final Product",
            runs_required=1,
            adjusted_time_seconds=50,
            quantity_needed=1,
            dependencies=[901],
        )
        slots = [
            IndustrySlot(slot_id=0, character_id=1, character_name="Builder", slot_name="Builder #1"),
            IndustrySlot(slot_id=1, character_id=1, character_name="Builder", slot_name="Builder #2"),
            IndustrySlot(slot_id=2, character_id=1, character_name="Builder", slot_name="Builder #3"),
        ]

        schedule = calculate_schedule_for_mode(
            [component, final_product],
            slots,
            schedule_mode="component_target",
            final_product_item_type_id=902,
            component_target_time_seconds=400,
        )

        self.assertEqual(schedule.used_slot_count, 2)
        self.assertEqual(schedule.component_completion_time_seconds, 300)
