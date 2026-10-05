"""Tests for build job splitting and slot scheduling."""

# Standard Library
import json

# Django
from django.contrib.auth.models import User
from django.test import RequestFactory, SimpleTestCase, TestCase

# AA Example App
from indy_hub.services.build_scheduler import (
    BuildSchedule,
    IndustrySlot,
    ManufacturingJob,
    build_dependency_tree,
    calculate_schedule_for_mode,
    schedule_jobs_critical_path,
    split_jobs_evenly_across_slots,
    split_runs_evenly,
)
from indy_hub.views.api import calculate_build_schedule


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
            IndustrySlot(
                slot_id=0,
                character_id=1,
                character_name="Builder",
                slot_name="Builder #1",
            ),
            IndustrySlot(
                slot_id=1,
                character_id=1,
                character_name="Builder",
                slot_name="Builder #2",
            ),
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
            IndustrySlot(
                slot_id=0,
                character_id=1,
                character_name="Builder",
                slot_name="Builder #1",
            ),
            IndustrySlot(
                slot_id=1,
                character_id=1,
                character_name="Builder",
                slot_name="Builder #2",
            ),
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
            IndustrySlot(
                slot_id=0,
                character_id=1,
                character_name="Builder",
                slot_name="Builder #1",
            ),
            IndustrySlot(
                slot_id=1,
                character_id=1,
                character_name="Builder",
                slot_name="Builder #2",
            ),
            IndustrySlot(
                slot_id=2,
                character_id=1,
                character_name="Builder",
                slot_name="Builder #3",
            ),
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

    def test_multi_product_dag_with_shared_intermediates_honors_prerequisites(self):
        # Shared intermediate Component X (type 100) needed by both Product A (type 201) and Product B (type 202)
        shared_comp = _make_job(
            item_type_id=100,
            item_name="Shared Component",
            runs_required=1,
            adjusted_time_seconds=200,
            quantity_needed=10,
        )
        prod_a = _make_job(
            item_type_id=201,
            item_name="Product A",
            runs_required=1,
            adjusted_time_seconds=100,
            quantity_needed=1,
            dependencies=[100],
        )
        prod_b = _make_job(
            item_type_id=202,
            item_name="Product B",
            runs_required=1,
            adjusted_time_seconds=150,
            quantity_needed=1,
            dependencies=[100],
        )

        slots = [
            IndustrySlot(
                slot_id=0, character_id=1, character_name="Builder", slot_name="Slot 1"
            ),
            IndustrySlot(
                slot_id=1, character_id=1, character_name="Builder", slot_name="Slot 2"
            ),
        ]

        schedule = schedule_jobs_critical_path(
            [shared_comp, prod_a, prod_b],
            slots,
            final_product_item_type_ids=[201, 202],
            final_assembly_priority="parallel",
        )

        job_map = {job.item_type_id: job for job in schedule.jobs}
        self.assertEqual(job_map[100].start_time_seconds, 0)
        self.assertEqual(job_map[100].end_time_seconds, 200)
        self.assertTrue(job_map[100].is_shared)
        self.assertEqual(set(job_map[100].parent_product_type_ids), {201, 202})
        self.assertFalse(job_map[100].is_final_product)

        # Both final assemblies must start only after shared component finishes at 200s
        self.assertGreaterEqual(job_map[201].start_time_seconds, 200)
        self.assertGreaterEqual(job_map[202].start_time_seconds, 200)
        self.assertTrue(job_map[201].is_final_product)
        self.assertTrue(job_map[202].is_final_product)

        # In parallel mode across 2 slots, both run concurrently:
        # One slot runs 100 (0..200) then 202 (200..350).
        # The other slot runs 201 (200..300).
        self.assertEqual(schedule.total_parallel_time_seconds, 350)
        self.assertEqual(schedule.total_sequential_time_seconds, 450)
        self.assertEqual(schedule.component_completion_time_seconds, 200)

        # Check payload conversion
        payload = schedule.to_dict()
        self.assertEqual(len(payload["products"]), 2)
        prod_a_summary = next(p for p in payload["products"] if p["product_type_id"] == 201)
        prod_b_summary = next(p for p in payload["products"] if p["product_type_id"] == 202)
        self.assertEqual(prod_a_summary["completion_time_seconds"], 300)
        self.assertEqual(prod_b_summary["completion_time_seconds"], 350)
        self.assertIn("slot_metrics", payload)
        self.assertEqual(payload["slot_metrics"]["total_slots"], 2)
        self.assertEqual(payload["slot_metrics"]["total_busy_time_seconds"], 450)

    def test_multi_product_prioritized_sequence_allocates_slots_to_priority_product(self):
        # 1 single slot available
        # Product A (201) needs Comp A (101, 100s). Assembly takes 50s.
        # Product B (202) needs Comp B (102, 100s). Assembly takes 50s.
        comp_a = _make_job(
            item_type_id=101,
            item_name="Comp A",
            runs_required=1,
            adjusted_time_seconds=100,
            quantity_needed=1,
        )
        prod_a = _make_job(
            item_type_id=201,
            item_name="Product A",
            runs_required=1,
            adjusted_time_seconds=50,
            quantity_needed=1,
            dependencies=[101],
        )
        comp_b = _make_job(
            item_type_id=102,
            item_name="Comp B",
            runs_required=1,
            adjusted_time_seconds=100,
            quantity_needed=1,
        )
        prod_b = _make_job(
            item_type_id=202,
            item_name="Product B",
            runs_required=1,
            adjusted_time_seconds=50,
            quantity_needed=1,
            dependencies=[102],
        )

        single_slot = [
            IndustrySlot(
                slot_id=0, character_id=1, character_name="Builder", slot_name="Slot 1"
            )
        ]

        # Prioritize Product A first: Comp A -> Prod A -> Comp B -> Prod B
        schedule_a = schedule_jobs_critical_path(
            [comp_a, prod_a, comp_b, prod_b],
            [IndustrySlot(slot_id=0, character_id=1, character_name="Builder")],
            product_priorities=[201, 202],
            final_assembly_priority="prioritized",
        )
        job_map_a = {j.item_type_id: j for j in schedule_a.jobs}
        self.assertEqual(job_map_a[101].start_time_seconds, 0)
        self.assertEqual(job_map_a[201].start_time_seconds, 100)
        self.assertEqual(job_map_a[201].end_time_seconds, 150)
        self.assertEqual(job_map_a[102].start_time_seconds, 150)
        self.assertEqual(job_map_a[202].end_time_seconds, 300)

        # Prioritize Product B first: Comp B -> Prod B -> Comp A -> Prod A
        schedule_b = schedule_jobs_critical_path(
            [comp_a, prod_a, comp_b, prod_b],
            [IndustrySlot(slot_id=0, character_id=1, character_name="Builder")],
            product_priorities=[202, 201],
            final_assembly_priority="prioritized",
        )
        job_map_b = {j.item_type_id: j for j in schedule_b.jobs}
        self.assertEqual(job_map_b[102].start_time_seconds, 0)
        self.assertEqual(job_map_b[202].start_time_seconds, 100)
        self.assertEqual(job_map_b[202].end_time_seconds, 150)
        self.assertEqual(job_map_b[101].start_time_seconds, 150)
        self.assertEqual(job_map_b[201].end_time_seconds, 300)

    def test_build_dependency_tree_with_explicit_dependencies(self):
        jobs_data = [
            {"item_type_id": 10, "dependencies": []},
            {"item_type_id": 20, "dependencies": [10]},
            {"item_type_id": 30, "dependencies": [10, 20]},
        ]
        deps = build_dependency_tree(jobs_data)
        self.assertEqual(deps[10], [])
        self.assertEqual(deps[20], [10])
        self.assertEqual(deps[30], [10, 20])


class BuildSchedulerApiTests(TestCase):
    def setUp(self) -> None:
        self.factory = RequestFactory()
        self.user = User.objects.create_user("schedule-api-user", password="secret123")

    def _unwrap_view(self, view_func):
        unwrapped = view_func
        while hasattr(unwrapped, "__wrapped__"):
            unwrapped = unwrapped.__wrapped__
        return unwrapped

    def test_calculate_build_schedule_endpoint_multi_product(self):
        payload = {
            "jobs": [
                {
                    "item_type_id": 100,
                    "item_name": "Shared Intermediate",
                    "blueprint_type_id": 1100,
                    "quantity_needed": 10,
                    "quantity_per_run": 5,
                    "material_efficiency": 10,
                    "time_efficiency": 20,
                    "dependencies": [],
                    "is_final_product": False,
                },
                {
                    "item_type_id": 201,
                    "item_name": "Product A",
                    "blueprint_type_id": 1201,
                    "quantity_needed": 1,
                    "quantity_per_run": 1,
                    "material_efficiency": 10,
                    "time_efficiency": 20,
                    "dependencies": [100],
                    "is_final_product": True,
                },
                {
                    "item_type_id": 202,
                    "item_name": "Product B",
                    "blueprint_type_id": 1202,
                    "quantity_needed": 1,
                    "quantity_per_run": 1,
                    "material_efficiency": 10,
                    "time_efficiency": 20,
                    "dependencies": [100],
                    "is_final_product": True,
                },
            ],
            "slots": [
                {
                    "character_id": 1234,
                    "character_name": "Builder",
                    "available_slots": 2,
                }
            ],
            "final_product_type_ids": [201, 202],
            "final_assembly_priority": "parallel",
        }

        request = self.factory.post(
            "/api/calculate-build-schedule/",
            data=json.dumps(payload),
            content_type="application/json",
        )
        request.user = self.user

        response = self._unwrap_view(calculate_build_schedule)(request)
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertIsNone(data.get("error"))
        schedule = data["schedule"]
        self.assertIn("products", schedule)
        self.assertEqual(len(schedule["products"]), 2)
        self.assertIn("slot_metrics", schedule)
        self.assertEqual(schedule["slot_metrics"]["total_slots"], 2)

    def test_calculate_build_schedule_endpoint_prioritized_mode(self):
        payload = {
            "jobs": [
                {
                    "item_type_id": 101,
                    "item_name": "Comp A",
                    "blueprint_type_id": 1101,
                    "quantity_needed": 1,
                    "quantity_per_run": 1,
                    "dependencies": [],
                },
                {
                    "item_type_id": 201,
                    "item_name": "Product A",
                    "blueprint_type_id": 1201,
                    "quantity_needed": 1,
                    "quantity_per_run": 1,
                    "dependencies": [101],
                },
                {
                    "item_type_id": 102,
                    "item_name": "Comp B",
                    "blueprint_type_id": 1102,
                    "quantity_needed": 1,
                    "quantity_per_run": 1,
                    "dependencies": [],
                },
                {
                    "item_type_id": 202,
                    "item_name": "Product B",
                    "blueprint_type_id": 1202,
                    "quantity_needed": 1,
                    "quantity_per_run": 1,
                    "dependencies": [102],
                },
            ],
            "slots": [
                {
                    "character_id": 1234,
                    "character_name": "Builder",
                    "available_slots": 1,
                }
            ],
            "product_priorities": [201, 202],
            "final_assembly_priority": "prioritized",
        }

        request = self.factory.post(
            "/api/calculate-build-schedule/",
            data=json.dumps(payload),
            content_type="application/json",
        )
        request.user = self.user

        response = self._unwrap_view(calculate_build_schedule)(request)
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        schedule = data["schedule"]
        self.assertEqual(schedule["final_assembly_priority"], "prioritized")
        jobs_by_type = {j["item_type_id"]: j for j in schedule["jobs"]}
        self.assertLess(
            jobs_by_type[201]["end_time_seconds"],
            jobs_by_type[202]["end_time_seconds"],
        )
