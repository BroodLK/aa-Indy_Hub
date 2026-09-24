# Standard Library
import json
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

# Django
from django.contrib.auth.models import Permission, User
from django.core.cache import cache
from django.db import IntegrityError, connection
from django.test import RequestFactory, SimpleTestCase, TestCase
from django.urls import reverse
from django.utils import timezone

# AA Example App
from indy_hub.models import (
    CachedCharacterAsset,
    CachedStructureName,
    CustomPrice,
    IndustryJob,
    ProductionSimulation,
    ProductionSimulationPreference,
)
from indy_hub.services.production_simulation_state import (
    MAX_PREFERENCE_BYTES,
    MAX_UI_STATE_BYTES,
    STATE_SCHEMA_VERSION,
    normalize_preference_state,
)
from indy_hub.tasks.material_exchange import me_sell_assets_esi_cooldown_key
from indy_hub.views.api import (
    _production_asset_progress_key,
    load_production_config,
    production_bpc_sources,
    production_material_source_assets,
    production_material_sources,
    production_simulation_preferences,
    refresh_production_material_sources,
    refresh_production_material_sources_status,
    refresh_production_schedule_tracking,
    save_production_config,
)

# Minimal schedule in BuildSchedule.to_dict() shape. Tracking now refuses an
# empty schedule, so tests that only care about the surrounding behaviour still
# need something real to track.
TRACKED_SCHEDULE = {
    "jobs": [
        {
            "job_id": 1,
            "item_type_id": 81002,
            "item_name": "Widget",
            "runs_required": 2,
            "assigned_slot": 0,
            "start_time_seconds": 0,
            "end_time_seconds": 3600,
            "total_time_seconds": 3600,
            "activity_id": 1,
        }
    ],
    "slots": [{"slot_id": 0, "character_id": 70002, "character_name": "Pilot"}],
    "total_parallel_time_seconds": 3600,
}


def grant_indy_permissions(user: User, *codenames: str) -> None:
    required = {"can_access_indy_hub", *codenames}
    permissions = Permission.objects.filter(codename__in=required)
    found = {permission.codename: permission for permission in permissions}
    missing = required.difference(found.keys())
    if missing:
        raise AssertionError(f"Missing permissions: {sorted(missing)}")
    user.user_permissions.add(*found.values())


class ProductionConfigUiStateTests(TestCase):
    def setUp(self) -> None:
        self.user = User.objects.create_user("sim-ui", password="secret123")
        grant_indy_permissions(self.user)
        self.factory = RequestFactory()
        self.save_url = reverse("indy_hub:save_production_config")
        self.load_url = reverse("indy_hub:load_production_config")

    @staticmethod
    def _unwrap_view(view_func):
        unwrapped = view_func
        while hasattr(unwrapped, "__wrapped__"):
            unwrapped = unwrapped.__wrapped__
        return unwrapped

    def test_save_production_config_persists_ui_state(self) -> None:
        payload = {
            "blueprint_type_id": 12001,
            "blueprint_name": "Test Blueprint",
            "runs": 7,
            "simulation_name": "UI State Save",
            "active_tab": "financial",
            "items": [
                {
                    "type_id": 34,
                    "mode": "buy",
                    "quantity": 100,
                }
            ],
            "blueprint_efficiencies": [],
            "custom_prices": [],
            "ui_state": {
                "craftMainTab": "buy",
                "ownedMaterialsText": "Compressed Veldspar\t50",
                "customPrices": [
                    {
                        "item_type_id": 12001,
                        "unit_price": 9876543.21,
                        "is_sale_price": True,
                    }
                ],
                "importFees": {
                    "selectedRoutePricingId": 77,
                    "actualCost": 1250000.5,
                    "actualCostDirty": True,
                },
                "industryFees": {
                    "signature": '{"cfg":{"enabled":true}}',
                    "loaded": True,
                    "totalJobCost": 543210.0,
                    "jobs": [
                        {"product_id": 12001, "runs": 7},
                    ],
                },
                "manualFinancial": {
                    "items": [
                        {
                            "typeId": 28432,
                            "typeName": "Compressed Veldspar",
                            "quantity": 50,
                            "rowKey": "manual-material:28432",
                        }
                    ],
                    "excludedTypeIds": [34],
                },
            },
        }

        request = self.factory.post(
            self.save_url,
            data=json.dumps(payload),
            content_type="application/json",
        )
        request.user = self.user
        response = self._unwrap_view(save_production_config)(request)

        self.assertEqual(response.status_code, 200)
        body = json.loads(response.content)
        self.assertTrue(body["success"])

        simulation = ProductionSimulation.objects.get(id=body["simulation_id"])
        self.assertEqual(simulation.active_tab, "financial")
        self.assertEqual(simulation.ui_state["craftMainTab"], "buy")
        self.assertEqual(
            simulation.ui_state["customPrices"][0]["unit_price"],
            9876543.21,
        )
        self.assertTrue(simulation.ui_state["importFees"]["actualCostDirty"])
        self.assertEqual(
            simulation.ui_state["industryFees"]["totalJobCost"],
            543210.0,
        )
        self.assertEqual(
            simulation.ui_state["manualFinancial"]["excludedTypeIds"],
            [34],
        )

    def test_load_production_config_returns_ui_state(self) -> None:
        simulation = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=12002,
            blueprint_name="Load Blueprint",
            runs=3,
            simulation_name="UI State Load",
            active_tab="needed",
            ui_state={
                "craftMainTab": "configure",
                "buildPlannerSlots": {"9001": 4},
                "importFees": {
                    "selectedRoutePricingId": 44,
                    "actualCost": 765432.1,
                    "actualCostDirty": True,
                },
                "industryFees": {
                    "signature": "fee-signature",
                    "loaded": True,
                    "totalJobCost": 98765.4,
                    "jobs": [{"product_id": 12002, "runs": 3}],
                    "errors": [],
                },
                "runOptimized": {
                    "bestLabel": "Best up to 100",
                    "statusText": "Optimal runs found.",
                },
            },
        )

        request = self.factory.get(
            self.load_url,
            {
                "simulation_id": simulation.id,
            },
        )
        request.user = self.user
        response = self._unwrap_view(load_production_config)(request)

        self.assertEqual(response.status_code, 200)
        body = json.loads(response.content)
        self.assertEqual(body["simulation_id"], simulation.id)
        self.assertEqual(body["active_tab"], "needed")
        self.assertEqual(body["ui_state"]["craftMainTab"], "configure")
        self.assertEqual(body["ui_state"]["buildPlannerSlots"], {"9001": 4})
        self.assertEqual(body["ui_state"]["importFees"]["selectedRoutePricingId"], 44)
        self.assertEqual(body["ui_state"]["industryFees"]["signature"], "fee-signature")

    def test_save_production_config_allows_cost_and_sale_price_for_same_type(
        self,
    ) -> None:
        payload = {
            "blueprint_type_id": 57518,
            "blueprint_name": "Price Collision Blueprint",
            "runs": 2,
            "simulation_name": "Price Collision Save",
            "active_tab": "financial",
            "items": [
                {
                    "type_id": 57518,
                    "mode": "buy",
                    "quantity": 1,
                }
            ],
            "blueprint_efficiencies": [],
            "custom_prices": [
                {
                    "item_type_id": 57518,
                    "unit_price": 1500000,
                    "is_sale_price": False,
                },
                {
                    "item_type_id": 57518,
                    "unit_price": 2500000,
                    "is_sale_price": True,
                },
            ],
            "ui_state": {},
        }

        request = self.factory.post(
            self.save_url,
            data=json.dumps(payload),
            content_type="application/json",
        )
        request.user = self.user
        response = self._unwrap_view(save_production_config)(request)

        self.assertEqual(response.status_code, 200)
        body = json.loads(response.content)
        self.assertTrue(body["success"])

        simulation = ProductionSimulation.objects.get(id=body["simulation_id"])
        saved_prices = list(
            CustomPrice.objects.filter(simulation=simulation).order_by("is_sale_price")
        )
        self.assertEqual(len(saved_prices), 2)
        self.assertEqual(saved_prices[0].item_type_id, 57518)
        self.assertFalse(saved_prices[0].is_sale_price)
        self.assertTrue(saved_prices[1].is_sale_price)

    def test_save_production_config_overwrites_existing_custom_prices(self) -> None:
        simulation = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=57518,
            blueprint_name="Overwrite Blueprint",
            runs=4,
            simulation_name="Existing Save",
        )
        CustomPrice.objects.create(
            user=self.user,
            simulation=simulation,
            item_type_id=34,
            unit_price=999,
            is_sale_price=False,
        )

        payload = {
            "simulation_id": simulation.id,
            "blueprint_type_id": 57518,
            "blueprint_name": "Overwrite Blueprint",
            "runs": 4,
            "simulation_name": "Existing Save Updated",
            "active_tab": "financial",
            "items": [],
            "blueprint_efficiencies": [],
            "custom_prices": [],
            "ui_state": {},
        }

        request = self.factory.post(
            self.save_url,
            data=json.dumps(payload),
            content_type="application/json",
        )
        request.user = self.user
        response = self._unwrap_view(save_production_config)(request)

        self.assertEqual(response.status_code, 200)
        body = json.loads(response.content)
        self.assertTrue(body["success"])

        simulation.refresh_from_db()
        self.assertEqual(simulation.simulation_name, "Existing Save Updated")
        self.assertFalse(CustomPrice.objects.filter(simulation=simulation).exists())

    def test_save_without_simulation_id_creates_new_snapshot_for_same_blueprint_and_runs(
        self,
    ) -> None:
        existing = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=70001,
            blueprint_name="Duplicate Snapshot Blueprint",
            runs=5,
            simulation_name="Original Snapshot",
        )

        payload = {
            "blueprint_type_id": 70001,
            "blueprint_name": "Duplicate Snapshot Blueprint",
            "runs": 5,
            "simulation_name": "Second Snapshot",
            "active_tab": "materials",
            "items": [],
            "blueprint_efficiencies": [],
            "custom_prices": [],
            "ui_state": {},
        }

        request = self.factory.post(
            self.save_url,
            data=json.dumps(payload),
            content_type="application/json",
        )
        request.user = self.user
        response = self._unwrap_view(save_production_config)(request)

        self.assertEqual(response.status_code, 200)
        body = json.loads(response.content)
        self.assertTrue(body["success"])
        self.assertTrue(body["simulation_created"])
        self.assertNotEqual(body["simulation_id"], existing.id)

        snapshots = ProductionSimulation.objects.filter(
            user=self.user,
            blueprint_type_id=70001,
            runs=5,
        ).order_by("id")
        self.assertEqual(snapshots.count(), 2)
        self.assertEqual(snapshots.first().simulation_name, "Original Snapshot")
        self.assertEqual(snapshots.last().simulation_name, "Second Snapshot")

    def test_load_by_blueprint_and_runs_returns_latest_snapshot(self) -> None:
        first = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=70002,
            blueprint_name="Latest Snapshot Blueprint",
            runs=6,
            simulation_name="Older Snapshot",
            active_tab="materials",
        )
        second = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=70002,
            blueprint_name="Latest Snapshot Blueprint",
            runs=6,
            simulation_name="Newer Snapshot",
            active_tab="financial",
        )

        request = self.factory.get(
            self.load_url,
            {
                "blueprint_type_id": 70002,
                "runs": 6,
            },
        )
        request.user = self.user
        response = self._unwrap_view(load_production_config)(request)

        self.assertEqual(response.status_code, 200)
        body = json.loads(response.content)
        self.assertEqual(body["simulation_id"], second.id)
        self.assertEqual(body["simulation_name"], "Newer Snapshot")
        self.assertEqual(body["active_tab"], "financial")
        self.assertNotEqual(body["simulation_id"], first.id)

    def test_overwrite_existing_snapshot_can_change_runs_to_match_another_snapshot(
        self,
    ) -> None:
        original = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=70003,
            blueprint_name="Run Collision Blueprint",
            runs=3,
            simulation_name="Original Snapshot",
        )
        other = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=70003,
            blueprint_name="Run Collision Blueprint",
            runs=9,
            simulation_name="Other Snapshot",
        )

        payload = {
            "simulation_id": original.id,
            "blueprint_type_id": 70003,
            "blueprint_name": "Run Collision Blueprint",
            "runs": 9,
            "simulation_name": "Original Snapshot Updated",
            "active_tab": "buy",
            "items": [],
            "blueprint_efficiencies": [],
            "custom_prices": [],
            "ui_state": {"craftMainTab": "buy"},
        }

        request = self.factory.post(
            self.save_url,
            data=json.dumps(payload),
            content_type="application/json",
        )
        request.user = self.user
        response = self._unwrap_view(save_production_config)(request)

        self.assertEqual(response.status_code, 200)
        body = json.loads(response.content)
        self.assertTrue(body["success"])
        self.assertEqual(body["simulation_id"], original.id)

        original.refresh_from_db()
        other.refresh_from_db()
        self.assertEqual(original.runs, 9)
        self.assertEqual(original.simulation_name, "Original Snapshot Updated")
        self.assertEqual(other.runs, 9)
        self.assertEqual(
            ProductionSimulation.objects.filter(
                user=self.user,
                blueprint_type_id=70003,
                runs=9,
            ).count(),
            2,
        )

    def test_simulation_names_are_normalized_per_user(self) -> None:
        ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=80001,
            blueprint_name="Named Blueprint",
            simulation_name="  Shared   Name ",
        )
        with self.assertRaises(IntegrityError):
            ProductionSimulation.objects.create(
                user=self.user,
                blueprint_type_id=80002,
                blueprint_name="Other Blueprint",
                simulation_name="shared name",
            )

    def test_preferences_are_user_scoped(self) -> None:
        other_user = User.objects.create_user("sim-other", password="secret123")
        first = ProductionSimulationPreference.objects.create(
            user=self.user, state={"materialsSourceMode": "manual"}
        )
        ProductionSimulationPreference.objects.create(
            user=other_user, state={"materialsSourceMode": "designated_bay"}
        )
        request = self.factory.get("/api/production-preferences/")
        request.user = self.user
        response = self._unwrap_view(production_simulation_preferences)(request)
        self.assertEqual(json.loads(response.content)["state"], first.state)

    def test_material_source_assets_reject_other_users_location(self) -> None:
        other_user = User.objects.create_user("asset-owner", password="secret123")
        CachedCharacterAsset.objects.create(
            user=other_user,
            character_id=9001,
            item_id=1,
            location_id=60000001,
            type_id=34,
            quantity=10,
        )
        request = self.factory.get(
            "/api/production-material-source-assets/", {"location_id": 60000001}
        )
        request.user = self.user
        response = self._unwrap_view(production_material_source_assets)(request)
        self.assertEqual(response.status_code, 403)

    @patch("indy_hub.views.api.refresh_material_exchange_sell_user_assets")
    def test_material_source_refresh_queues_user_asset_task(self, refresh_task) -> None:
        refresh_task.delay.return_value = type(
            "TaskResult", (), {"id": "asset-task-1"}
        )()
        request = self.factory.post("/api/production-material-sources/refresh/")
        request.user = self.user
        response = self._unwrap_view(refresh_production_material_sources)(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content)["task_id"], "asset-task-1")
        refresh_task.delay.assert_called_once_with(self.user.id)

    def test_schedule_tracking_requires_opt_in(self) -> None:
        simulation = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=90001,
            blueprint_name="Tracking Blueprint",
            ui_state={"buildSchedule": {"jobs": []}},
        )
        request = self.factory.post(
            "/api/production-schedule-tracking/refresh/",
            data=json.dumps({"simulation_id": simulation.id, "opt_in": False}),
            content_type="application/json",
        )
        request.user = self.user
        response = self._unwrap_view(refresh_production_schedule_tracking)(request)
        self.assertEqual(response.status_code, 400)

    @patch("indy_hub.views.api.request_manual_refresh", return_value=(True, None))
    def test_schedule_tracking_matches_owned_selected_character(
        self, refresh_task
    ) -> None:
        now = timezone.now()
        simulation = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=90002,
            blueprint_name="Tracking Blueprint",
        )
        IndustryJob.objects.create(
            owner_user=self.user,
            character_id=70001,
            installer_id=70001,
            job_id=970001,
            blueprint_id=80001,
            blueprint_type_id=80001,
            product_type_id=81001,
            runs=2,
            activity_id=1,
            status="active",
            duration=3600,
            start_date=now,
            end_date=now + timedelta(hours=1),
        )
        request = self.factory.post(
            "/api/production-schedule-tracking/refresh/",
            data=json.dumps(
                {
                    "simulation_id": simulation.id,
                    "opt_in": True,
                    "character_ids": [70001],
                    "schedule": {
                        "jobs": [
                            {
                                "character_id": 70001,
                                "product_type_id": 81001,
                                "blueprint_type_id": 80001,
                                "runs": 2,
                                "planned_start": now.isoformat(),
                                "planned_end": (now + timedelta(hours=1)).isoformat(),
                            }
                        ]
                    },
                }
            ),
            content_type="application/json",
        )
        request.user = self.user
        response = self._unwrap_view(refresh_production_schedule_tracking)(request)
        body = json.loads(response.content)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(body["tracking"]["matched"], 1)
        refresh_task.assert_called_once()


class SimulationNameUniquenessTests(TestCase):
    """Uniqueness must hold per user without blocking unnamed snapshots."""

    def setUp(self) -> None:
        self.user = User.objects.create_user("sim-uniq", password="secret123")

    def _create(self, name: str, blueprint_type_id: int) -> ProductionSimulation:
        return ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=blueprint_type_id,
            blueprint_name="Blueprint",
            simulation_name=name,
        )

    def test_unnamed_simulations_are_not_constrained(self) -> None:
        # Unnamed rows normalize to NULL, which every backend treats as
        # distinct, so a user may keep as many as they like.
        for index in range(3):
            self._create("", 81000 + index)
        self.assertEqual(
            ProductionSimulation.objects.filter(
                user=self.user, simulation_name_normalized__isnull=True
            ).count(),
            3,
        )

    def test_same_name_is_allowed_for_different_users(self) -> None:
        self._create("Shared Name", 81100)
        other_user = User.objects.create_user("sim-uniq-2", password="secret123")
        ProductionSimulation.objects.create(
            user=other_user,
            blueprint_type_id=81101,
            blueprint_name="Blueprint",
            simulation_name="Shared Name",
        )
        self.assertEqual(
            ProductionSimulation.objects.filter(
                simulation_name_normalized="shared name"
            ).count(),
            2,
        )

    def test_constraint_is_created_on_this_backend(self) -> None:
        # A conditional constraint would be silently skipped on MySQL, so
        # assert the index actually exists rather than trusting the model.
        with connection.cursor() as cursor:
            constraints = connection.introspection.get_constraints(
                cursor, ProductionSimulation._meta.db_table
            )
        self.assertIn("indy_sim_user_name_normalized_uniq", constraints)

    def test_partial_save_keeps_the_normalized_key_in_step(self) -> None:
        simulation = self._create("Original", 81200)
        simulation.simulation_name = "Renamed"
        simulation.save(update_fields=["simulation_name"])
        simulation.refresh_from_db()
        self.assertEqual(simulation.simulation_name_normalized, "renamed")

    def test_migration_renames_pre_existing_duplicates(self) -> None:
        # Standard Library
        from importlib import import_module

        # Django
        from django.apps import apps as django_apps

        first = self._create("Titan", 81300)
        second = self._create("Placeholder", 81301)
        # Bypass save() to recreate the pre-migration state: colliding display
        # names whose normalized keys have not been backfilled yet.
        ProductionSimulation.objects.filter(id=second.id).update(
            simulation_name="  titan "
        )

        migration = import_module(
            "indy_hub.migrations.0131_production_simulation_preferences"
        )
        migration.populate_normalized_names(django_apps, None)

        first.refresh_from_db()
        second.refresh_from_db()
        self.assertEqual(first.simulation_name_normalized, "titan")
        self.assertNotEqual(
            first.simulation_name_normalized, second.simulation_name_normalized
        )
        self.assertIn("(2)", second.simulation_name)


class ScheduleTrackingEndpointTests(TestCase):
    def setUp(self) -> None:
        self.factory = RequestFactory()
        self.user = User.objects.create_user("sim-tracking", password="secret123")
        grant_indy_permissions(self.user, "can_access_indy_hub")

    def _unwrap_view(self, view):
        while hasattr(view, "__wrapped__"):
            view = view.__wrapped__
        return view

    def _post(self, payload):
        request = self.factory.post(
            "/api/production-schedule-tracking/refresh/",
            data=json.dumps(payload),
            content_type="application/json",
        )
        request.user = self.user
        return self._unwrap_view(refresh_production_schedule_tracking)(request)

    def test_rejects_characters_the_user_does_not_own(self) -> None:
        simulation = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=90003,
            blueprint_name="Tracking Blueprint",
        )
        response = self._post(
            {
                "simulation_id": simulation.id,
                "opt_in": True,
                "character_ids": [424242],
                "schedule": TRACKED_SCHEDULE,
            }
        )
        self.assertEqual(response.status_code, 403)

    def test_stamps_a_tracking_anchor_on_the_simulation(self) -> None:
        simulation = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=90004,
            blueprint_name="Tracking Blueprint",
        )
        response = self._post(
            {
                "simulation_id": simulation.id,
                "opt_in": True,
                "schedule": TRACKED_SCHEDULE,
            }
        )
        self.assertEqual(response.status_code, 200)
        simulation.refresh_from_db()
        anchor = simulation.ui_state["scheduleTracking"]["trackingStartedAt"]
        self.assertTrue(anchor)

        # The anchor is stable across refreshes so the window does not drift.
        self._post(
            {
                "simulation_id": simulation.id,
                "opt_in": True,
                "schedule": TRACKED_SCHEDULE,
            }
        )
        simulation.refresh_from_db()
        self.assertEqual(
            simulation.ui_state["scheduleTracking"]["trackingStartedAt"], anchor
        )

    def test_ignores_stale_jobs_outside_the_planned_window(self) -> None:
        now = timezone.now()
        simulation = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=90005,
            blueprint_name="Tracking Blueprint",
        )
        stale_start = now - timedelta(days=60)
        IndustryJob.objects.create(
            owner_user=self.user,
            character_id=70002,
            installer_id=70002,
            job_id=970002,
            blueprint_id=80002,
            blueprint_type_id=80002,
            product_type_id=81002,
            runs=2,
            activity_id=1,
            status="delivered",
            duration=3600,
            start_date=stale_start,
            end_date=stale_start + timedelta(hours=1),
        )
        with patch(
            "indy_hub.views.api.request_manual_refresh",
            return_value=(True, None),
        ):
            response = self._post(
                {
                    "simulation_id": simulation.id,
                    "opt_in": True,
                    "character_ids": [70002],
                    "schedule": {
                        "jobs": [
                            {
                                "character_id": 70002,
                                "product_type_id": 81002,
                                "runs": 2,
                                "planned_start": now.isoformat(),
                                "planned_end": (now + timedelta(hours=1)).isoformat(),
                            }
                        ]
                    },
                }
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content)["tracking"]["matched"], 0)

    def test_reports_tracking_unavailable_without_jobs_scope(self) -> None:
        # The account owns the character (via a stored job) but has no token
        # with the industry jobs scope: tracking must say "unavailable" while
        # still returning the reconciled, unchanged plan.
        now = timezone.now()
        simulation = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=90006,
            blueprint_name="Tracking Blueprint",
        )
        IndustryJob.objects.create(
            owner_user=self.user,
            character_id=70002,
            installer_id=70002,
            job_id=970003,
            blueprint_id=80002,
            blueprint_type_id=80002,
            product_type_id=81002,
            runs=2,
            activity_id=1,
            status="active",
            duration=3600,
            start_date=now,
            end_date=now + timedelta(hours=1),
        )
        with patch(
            "indy_hub.views.api.request_manual_refresh",
            return_value=(True, None),
        ):
            response = self._post(
                {
                    "simulation_id": simulation.id,
                    "opt_in": True,
                    "character_ids": [70002],
                    "schedule": TRACKED_SCHEDULE,
                }
            )
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.content)
        self.assertEqual(body["freshness"]["state"], "unavailable")
        self.assertGreaterEqual(body["auto_refresh_seconds"], 300)
        self.assertIn("results", body["tracking"])


class ShareStateSerializerTests(SimpleTestCase):
    """Guard the share allowlist at the source level.

    There is no JavaScript test harness in this project, so this asserts the
    serializer cannot regress to matching key names by pattern — the bug that
    put the selected structure ID into share URLs.
    """

    def _source(self) -> str:
        path = (
            Path(__file__).resolve().parent.parent
            / "static"
            / "indy_hub"
            / "js"
            / "craft_bp.js"
        )
        return path.read_text(encoding="utf-8")

    def test_share_state_does_not_pattern_match_configure_keys(self) -> None:
        self.assertNotIn("/me|te|efficiency/i", self._source())

    def test_share_state_never_serializes_private_configure_sections(self) -> None:
        source = self._source()
        start = source.index("function collectShareableMeTe(")
        end = source.index("function collectCraftShareState()")
        serializer = source[start:end]
        for forbidden in ("buildEnvironment", "industryFee", "structureId", "systemId"):
            self.assertNotIn(forbidden, serializer)
        self.assertIn("blueprintConfigs", serializer)

    def _shedder(self) -> str:
        source = self._source()
        start = source.index("function buildCraftShareEncoding()")
        end = source.index("function updateCraftShareUrl()")
        return source[start:end]

    def test_oversized_share_state_sheds_instead_of_refusing(self) -> None:
        # A bare `return false` at the length cap silently declined to copy.
        shedder = self._shedder()
        self.assertIn("dropped.push(step.label)", shedder)
        self.assertIn("CRAFT_SHARE_MAX_ENCODED_LENGTH", shedder)

    def test_shed_order_runs_least_valuable_first(self) -> None:
        shedder = self._shedder()
        positions = [
            shedder.index("expanded sections"),
            shedder.index("price overrides"),
            shedder.index("per-blueprint ME/TE"),
        ]
        self.assertEqual(positions, sorted(positions))

    def test_buy_decisions_are_never_shed(self) -> None:
        # The Buy/Produce decisions are the point of a share link.
        shedder = self._shedder()
        self.assertNotIn("target.buy", shedder)
        self.assertNotIn(".buy =", shedder)

    def test_share_url_reports_what_it_dropped(self) -> None:
        source = self._source()
        start = source.index("function updateCraftShareUrl()")
        end = source.index("function clearCraftShareUrl()")
        updater = source[start:end]
        # Returns {dropped} or null so the caller can distinguish "copied" from
        # "copied without X"; a bare boolean could not carry that.
        self.assertIn("return { dropped: result.dropped }", updater)
        self.assertIn("return null", updater)

    def test_restore_does_not_dispatch_a_synthetic_runs_change(self) -> None:
        # Dispatching change on #runsInput fired markMETEChanges, which flagged
        # phantom pending config edits and scheduled a refresh that landed
        # after the restore and clobbered it.
        source = self._source()
        start = source.index("function restoreCraftShareState()")
        end = source.index("function applyFullUiState(")
        restore = source[start:end]
        self.assertNotIn("dispatchEvent", restore)
        self.assertIn("staticInputs: [{ id: 'runsInput'", restore)
        self.assertIn("refreshTabsAfterStateChange", restore)


class UiStateMigrationTests(TestCase):
    """migrate_ui_state must run on read, not only as a side effect of saving."""

    def setUp(self) -> None:
        self.factory = RequestFactory()
        self.user = User.objects.create_user("sim-migrate", password="secret123")
        grant_indy_permissions(self.user, "can_access_indy_hub")

    def _unwrap_view(self, view):
        while hasattr(view, "__wrapped__"):
            view = view.__wrapped__
        return view

    def test_legacy_snapshot_is_migrated_on_load(self) -> None:
        simulation = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=95001,
            blueprint_name="Legacy Blueprint",
            # A v1 row: snake_case version key, no displayPreferences.
            ui_state={"schema_version": 1, "craftMainTab": "buy"},
        )
        request = self.factory.get(
            "/api/load-production-config/", {"simulation_id": simulation.id}
        )
        request.user = self.user
        response = self._unwrap_view(load_production_config)(request)
        ui_state = json.loads(response.content)["ui_state"]

        self.assertEqual(ui_state["schemaVersion"], STATE_SCHEMA_VERSION)
        self.assertNotIn("schema_version", ui_state)
        self.assertIn("displayPreferences", ui_state)
        # Unknown keys survive so an older client does not lose controls.
        self.assertEqual(ui_state["craftMainTab"], "buy")

    def test_oversized_snapshot_is_rejected_on_save(self) -> None:
        payload = {
            "blueprint_type_id": 95002,
            "blueprint_name": "Huge Blueprint",
            "runs": 1,
            "ui_state": {"blob": "x" * (MAX_UI_STATE_BYTES + 1024)},
        }
        request = self.factory.post(
            "/api/save-production-config/",
            data=json.dumps(payload),
            content_type="application/json",
        )
        request.user = self.user
        response = self._unwrap_view(save_production_config)(request)
        body = json.loads(response.content)

        self.assertEqual(response.status_code, 413)
        self.assertEqual(body["error"], "ui_state_too_large")
        self.assertIn("too large", body["message"])
        self.assertFalse(
            ProductionSimulation.objects.filter(blueprint_type_id=95002).exists()
        )

    def test_snapshot_within_the_budget_is_accepted(self) -> None:
        payload = {
            "blueprint_type_id": 95003,
            "blueprint_name": "Normal Blueprint",
            "runs": 1,
            "ui_state": {"blob": "x" * 1024},
        }
        request = self.factory.post(
            "/api/save-production-config/",
            data=json.dumps(payload),
            content_type="application/json",
        )
        request.user = self.user
        response = self._unwrap_view(save_production_config)(request)
        self.assertEqual(response.status_code, 200)

    def test_tracking_anchor_write_keeps_the_snapshot_versioned(self) -> None:
        simulation = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=95004,
            blueprint_name="Anchor Blueprint",
            ui_state={"schema_version": 1},
        )
        request = self.factory.post(
            "/api/production-schedule-tracking/refresh/",
            data=json.dumps(
                {
                    "simulation_id": simulation.id,
                    "opt_in": True,
                    "schedule": TRACKED_SCHEDULE,
                }
            ),
            content_type="application/json",
        )
        request.user = self.user
        self._unwrap_view(refresh_production_schedule_tracking)(request)

        simulation.refresh_from_db()
        self.assertEqual(simulation.ui_state["schemaVersion"], STATE_SCHEMA_VERSION)
        self.assertNotIn("schema_version", simulation.ui_state)


class PreferenceValueValidationTests(SimpleTestCase):
    """The allowlist covered keys; values were accepted unchecked."""

    def test_active_tab_must_be_a_known_tab(self) -> None:
        # showCraftMainTab() interpolates this into a querySelector.
        self.assertEqual(
            normalize_preference_state({"activeTab": "buy"}), {"activeTab": "buy"}
        )
        for bad in ("x" * 5000, "plan'], [x", "not_a_tab", 42, None):
            self.assertEqual(normalize_preference_state({"activeTab": bad}), {})

    def test_booleans_must_actually_be_booleans(self) -> None:
        self.assertEqual(
            normalize_preference_state({"scheduleTrackingOptIn": True}),
            {"scheduleTrackingOptIn": True},
        )
        self.assertEqual(
            normalize_preference_state({"scheduleTrackingOptIn": False}),
            {"scheduleTrackingOptIn": False},
        )
        self.assertEqual(
            normalize_preference_state({"scheduleTrackingOptIn": "yes"}), {}
        )

    def test_tax_rate_is_bounded(self) -> None:
        self.assertEqual(normalize_preference_state({"taxRate": 7.5}), {"taxRate": 7.5})
        for bad in (-1, 101, float("inf"), float("nan"), "7.5", True):
            self.assertEqual(normalize_preference_state({"taxRate": bad}), {})

    def test_materials_source_mode_is_constrained(self) -> None:
        self.assertEqual(
            normalize_preference_state({"materialsSourceMode": "designated_bay"}),
            {"materialsSourceMode": "designated_bay"},
        )
        self.assertEqual(
            normalize_preference_state({"materialsSourceMode": "anything"}), {}
        )

    def test_location_id_must_be_numeric(self) -> None:
        self.assertEqual(
            normalize_preference_state({"materialsSourceLocationId": "60000001"}),
            {"materialsSourceLocationId": "60000001"},
        )
        self.assertEqual(
            normalize_preference_state({"materialsSourceLocationId": ""}),
            {"materialsSourceLocationId": ""},
        )
        self.assertEqual(
            normalize_preference_state({"materialsSourceLocationId": "60000001; DROP"}),
            {},
        )

    def test_oversized_nested_blob_is_dropped(self) -> None:
        big = {"k": "x" * (MAX_PREFERENCE_BYTES + 1024)}
        self.assertEqual(normalize_preference_state({"slotPreferences": big}), {})
        # A reasonable one survives.
        self.assertEqual(
            normalize_preference_state({"slotPreferences": {"k": 1}}),
            {"slotPreferences": {"k": 1}},
        )

    def test_one_bad_field_does_not_discard_the_payload(self) -> None:
        cleaned = normalize_preference_state(
            {"activeTab": "nonsense", "scheduleTrackingOptIn": True, "unknown": 1}
        )
        self.assertEqual(cleaned, {"scheduleTrackingOptIn": True})


class MaterialSourceNamingTests(TestCase):
    """The picker showed raw numeric IDs; names come from the cache, never ESI."""

    def setUp(self) -> None:
        self.factory = RequestFactory()
        self.user = User.objects.create_user("sim-sources", password="secret123")
        grant_indy_permissions(self.user, "can_access_indy_hub")

    def _unwrap_view(self, view):
        while hasattr(view, "__wrapped__"):
            view = view.__wrapped__
        return view

    def _asset(self, **kwargs):
        defaults = {
            "user": self.user,
            "character_id": 9001,
            "item_id": 5001,
            "location_id": 1030000000001,
            "type_id": 34,
            "quantity": 100,
            "location_flag": "Hangar",
        }
        defaults.update(kwargs)
        return CachedCharacterAsset.objects.create(**defaults)

    def _sources(self, view=None):
        request = self.factory.get("/api/production-material-sources/")
        request.user = self.user
        response = self._unwrap_view(view or production_material_sources)(request)
        return json.loads(response.content)

    def test_cached_structure_name_is_used(self) -> None:
        self._asset()
        CachedStructureName.objects.create(
            structure_id=1030000000001, name="1DQ1-A - Test Keepstar"
        )
        body = self._sources()
        source = body["sources"][0]
        self.assertEqual(source["location_name"], "1DQ1-A - Test Keepstar")
        self.assertFalse(source["name_is_placeholder"])
        self.assertIsNotNone(source["name_last_resolved"])

    def test_unresolved_location_is_flagged_not_dressed_up_as_a_name(self) -> None:
        self._asset()
        body = self._sources()
        source = body["sources"][0]
        self.assertTrue(source["name_is_placeholder"])
        self.assertTrue(source["location_name"].startswith("Structure "))

    def test_placeholder_row_is_still_flagged(self) -> None:
        self._asset()
        CachedStructureName.objects.create(
            structure_id=1030000000001, name="Structure 1030000000001"
        )
        self.assertTrue(self._sources()["sources"][0]["name_is_placeholder"])

    def test_response_carries_the_server_freshness_budget(self) -> None:
        self._asset()
        body = self._sources()
        # The client used to hardcode 24h; the budget is now server-supplied.
        self.assertGreater(body["max_age_seconds"], 0)
        self.assertLess(body["max_age_seconds"], 24 * 60 * 60)
        self.assertFalse(body["supports_divisions"])

    def test_stale_assets_are_marked_stale(self) -> None:
        self._asset(synced_at=timezone.now() - timedelta(days=3))
        self.assertTrue(self._sources()["sources"][0]["is_stale"])

    def test_no_structure_writes_on_the_get_path(self) -> None:
        self._asset()
        before = CachedStructureName.objects.count()
        self._sources()
        self.assertEqual(CachedStructureName.objects.count(), before)

    def test_source_service_never_calls_resolve_structure_names(self) -> None:
        # resolve_structure_names blocks on ESI in every mode (including a
        # 0.3s sleep per structure) and writes rows even with
        # schedule_async=True. A patch-based assertion would not catch a
        # from-import, so guard the source directly.
        service = (
            Path(__file__).resolve().parent.parent
            / "services"
            / "production_material_sources.py"
        ).read_text(encoding="utf-8")
        body = service[service.index("def asset_cache_max_age") :]
        self.assertNotIn("resolve_structure_names(", body)

    def test_containers_are_listed_for_the_location(self) -> None:
        # A container: its own row, plus contents pointing at it.
        self._asset(item_id=7001, type_id=3468, set_name="Mineral Can")
        self._asset(item_id=7002, type_id=34, raw_location_id=7001, quantity=500)
        containers = self._sources()["sources"][0]["containers"]
        self.assertEqual([c["item_id"] for c in containers], [7001])
        self.assertEqual(containers[0]["name"], "Mineral Can")

    def test_blueprint_sources_are_a_separate_listing(self) -> None:
        self._asset(item_id=8001, type_id=34, is_blueprint=False)
        self._asset(
            item_id=8002, type_id=12345, is_blueprint=True, location_id=60003760
        )
        materials = self._sources()["sources"]
        blueprints = self._sources(production_bpc_sources)["sources"]
        self.assertEqual([s["location_id"] for s in materials], [1030000000001])
        self.assertEqual([s["location_id"] for s in blueprints], [60003760])

    @patch("indy_hub.services.production_material_sources._get_ship_type_ids")
    def test_ships_are_excluded_from_containers_and_non_hangar_locations_excluded(
        self, mock_ship_ids
    ) -> None:
        mock_ship_ids.return_value = {649}  # Tayra is a ship
        # A ship in the hangar with cargo
        self._asset(
            item_id=7100, type_id=649, set_name="My Tayra", location_flag="Hangar"
        )
        self._asset(
            item_id=7101,
            type_id=34,
            raw_location_id=7100,
            quantity=200,
            location_flag="Cargo",
        )
        # A container in the hangar with materials
        self._asset(
            item_id=7200,
            type_id=3468,
            set_name="Mineral Can",
            location_flag="Hangar",
        )
        self._asset(
            item_id=7201,
            type_id=34,
            raw_location_id=7200,
            quantity=300,
            location_flag="Unlocked",
        )
        # Another location with ONLY rig slots (should be excluded from sources)
        self._asset(
            item_id=8100,
            type_id=26082,
            location_id=1099999999999,
            location_flag="RigSlot0",
            quantity=1,
        )

        sources = self._sources()["sources"]
        self.assertEqual([s["location_id"] for s in sources], [1030000000001])
        containers = sources[0]["containers"]
        # Only the Mineral Can should be listed as a container; the ship is excluded.
        self.assertEqual([c["item_id"] for c in containers], [7200])
        self.assertEqual(containers[0]["name"], "Mineral Can")
        self.assertEqual(sources[0]["location_flags"], ["Hangar"])

    def test_sources_are_delineated_by_character(self) -> None:
        self._asset(
            item_id=7299,
            character_id=9001,
            location_id=1030000000001,
            type_id=34,
            quantity=100,
        )
        self._asset(
            item_id=7300,
            character_id=9002,
            location_id=1030000000001,
            type_id=34,
            quantity=50,
        )
        sources = self._sources()["sources"]
        # Both characters have assets at the same station; returned as distinct sources with character metadata.
        self.assertEqual(len(sources), 2)
        char_ids = {s["character_id"] for s in sources}
        self.assertEqual(char_ids, {9001, 9002})


class MaterialSourceAssetFilterTests(TestCase):
    """Container narrowing: everything at a location used to be summed as one."""

    def setUp(self) -> None:
        self.factory = RequestFactory()
        self.user = User.objects.create_user("sim-source-assets", password="secret123")
        grant_indy_permissions(self.user, "can_access_indy_hub")
        # 100 loose in the hangar, 500 inside a can at the same structure.
        CachedCharacterAsset.objects.create(
            user=self.user,
            character_id=9001,
            item_id=7001,
            location_id=1030000000001,
            type_id=34,
            quantity=100,
            location_flag="Hangar",
        )
        CachedCharacterAsset.objects.create(
            user=self.user,
            character_id=9001,
            item_id=7002,
            location_id=1030000000001,
            raw_location_id=1030000000001,
            type_id=3468,
            quantity=1,
            location_flag="Hangar",
            set_name="Mineral Can",
        )
        CachedCharacterAsset.objects.create(
            user=self.user,
            character_id=9001,
            item_id=7003,
            location_id=1030000000001,
            raw_location_id=7002,
            type_id=34,
            quantity=500,
            location_flag="Unlocked",
        )

    def _unwrap_view(self, view):
        while hasattr(view, "__wrapped__"):
            view = view.__wrapped__
        return view

    def _get(self, **params):
        params.setdefault("location_id", 1030000000001)
        request = self.factory.get("/api/production-material-source-assets/", params)
        request.user = self.user
        response = self._unwrap_view(production_material_source_assets)(request)
        return response.status_code, json.loads(response.content)

    def _qty(self, body, type_id=34):
        return next(
            (a["quantity"] for a in body["assets"] if a["type_id"] == type_id), 0
        )

    def test_whole_location_sums_loose_and_contained(self) -> None:
        status, body = self._get()
        self.assertEqual(status, 200)
        self.assertEqual(self._qty(body), 600)

    def test_container_filter_returns_only_its_contents(self) -> None:
        status, body = self._get(container_item_id=7002)
        self.assertEqual(status, 200)
        self.assertEqual(self._qty(body), 500)

    def test_location_flag_filter_follows_the_container_chain(self) -> None:
        # Contents of a can sitting in the Hangar still count as Hangar.
        status, body = self._get(location_flag="Hangar")
        self.assertEqual(status, 200)
        self.assertEqual(self._qty(body), 600)

    def test_unknown_container_yields_nothing_rather_than_everything(self) -> None:
        status, body = self._get(container_item_id=999999)
        self.assertEqual(status, 200)
        self.assertEqual(self._qty(body), 0)

    def test_other_users_location_is_refused(self) -> None:
        other = User.objects.create_user("sim-source-other", password="secret123")
        CachedCharacterAsset.objects.create(
            user=other,
            character_id=9999,
            item_id=1,
            location_id=60000001,
            type_id=34,
            quantity=10,
        )
        status, _ = self._get(location_id=60000001)
        self.assertEqual(status, 403)

    def test_response_reports_freshness_and_name(self) -> None:
        CachedStructureName.objects.create(
            structure_id=1030000000001, name="Test Keepstar"
        )
        status, body = self._get()
        self.assertEqual(status, 200)
        self.assertEqual(body["location_name"], "Test Keepstar")
        self.assertIn("max_age_seconds", body)
        self.assertFalse(body["is_stale"])

    def test_stale_assets_are_reported_stale(self) -> None:
        CachedCharacterAsset.objects.filter(user=self.user).update(
            synced_at=timezone.now() - timedelta(days=5)
        )
        status, body = self._get()
        self.assertEqual(status, 200)
        self.assertTrue(body["is_stale"])

    @patch("indy_hub.services.production_material_sources._get_ship_type_ids")
    def test_ship_cargo_and_fittings_are_excluded_from_assets(
        self, mock_ship_ids
    ) -> None:
        mock_ship_ids.return_value = {649}
        # Add a ship in the hangar and items in its cargo & high slot
        CachedCharacterAsset.objects.create(
            user=self.user,
            character_id=9001,
            item_id=8001,
            location_id=1030000000001,
            raw_location_id=1030000000001,
            type_id=649,
            quantity=1,
            location_flag="Hangar",
        )
        CachedCharacterAsset.objects.create(
            user=self.user,
            character_id=9001,
            item_id=8002,
            location_id=1030000000001,
            raw_location_id=8001,
            type_id=34,
            quantity=1000,
            location_flag="Cargo",
        )
        CachedCharacterAsset.objects.create(
            user=self.user,
            character_id=9001,
            item_id=8003,
            location_id=1030000000001,
            raw_location_id=8001,
            type_id=34,
            quantity=200,
            location_flag="HiSlot0",
        )
        # Whole location query: 100 loose + 500 in Mineral Can = 600 (the 1000 cargo and 200 fitting in Tayra are excluded)
        status, body = self._get()
        self.assertEqual(status, 200)
        self.assertEqual(self._qty(body), 600)

        # Attempting to query the ship as a container should return empty list
        status, body = self._get(container_item_id=8001)
        self.assertEqual(status, 200)
        self.assertEqual(self._qty(body), 0)

    def test_character_filter_narrows_to_single_character(self) -> None:
        CachedCharacterAsset.objects.create(
            user=self.user,
            character_id=9002,
            item_id=9001,
            location_id=1030000000001,
            type_id=34,
            quantity=250,
            location_flag="Hangar",
        )
        # Without character_id: returns all characters at location (100 + 500 + 250 = 850)
        status, body = self._get()
        self.assertEqual(status, 200)
        self.assertEqual(self._qty(body), 850)

        # Narrowing by character 9002: returns 250
        status, body = self._get(character_id=9002)
        self.assertEqual(status, 200)
        self.assertEqual(self._qty(body), 250)

        # Narrowing by character 9001: returns 600
        status, body = self._get(character_id=9001)
        self.assertEqual(status, 200)
        self.assertEqual(self._qty(body), 600)


class ScheduleTrackingCooldownTests(TestCase):
    """Tracking used to queue one forced ESI task per character per click."""

    def setUp(self) -> None:
        self.factory = RequestFactory()
        self.user = User.objects.create_user("sim-cooldown", password="secret123")
        grant_indy_permissions(self.user, "can_access_indy_hub")
        cache.clear()
        self.simulation = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=96001,
            blueprint_name="Cooldown Blueprint",
        )
        # Ownership proof for character 70002 via an owned job row.
        now = timezone.now()
        IndustryJob.objects.create(
            owner_user=self.user,
            character_id=70002,
            installer_id=70002,
            job_id=996001,
            blueprint_id=80002,
            blueprint_type_id=80002,
            product_type_id=81002,
            runs=2,
            activity_id=1,
            status="active",
            duration=3600,
            start_date=now,
            end_date=now + timedelta(hours=1),
        )

    def _unwrap_view(self, view):
        while hasattr(view, "__wrapped__"):
            view = view.__wrapped__
        return view

    def _post(self, **extra):
        payload = {
            "simulation_id": self.simulation.id,
            "opt_in": True,
            "character_ids": [70002],
            "schedule": TRACKED_SCHEDULE,
        }
        payload.update(extra)
        request = self.factory.post(
            "/api/production-schedule-tracking/refresh/",
            data=json.dumps(payload),
            content_type="application/json",
        )
        request.user = self.user
        response = self._unwrap_view(refresh_production_schedule_tracking)(request)
        return response.status_code, json.loads(response.content)

    def test_refresh_targets_the_character_not_the_whole_account(self) -> None:
        # The old code passed the character as the ESI `scope`, which cooled
        # down per character while refreshing everything.
        with patch("indy_hub.views.api.request_manual_refresh") as throttle:
            throttle.return_value = (True, None)
            status, body = self._post()
        self.assertEqual(status, 200)
        kwargs = throttle.call_args.kwargs
        self.assertEqual(kwargs["character_id"], 70002)
        self.assertTrue(kwargs["force_refresh"])
        self.assertEqual(kwargs["cooldown_scope"], "tracking:70002")
        self.assertEqual(body["scheduled_character_ids"], [70002])

    def test_cooldown_returns_200_with_a_retry_hint(self) -> None:
        with patch("indy_hub.views.api.request_manual_refresh") as throttle:
            throttle.return_value = (False, timedelta(minutes=12))
            status, body = self._post()
        # 200 with a payload, matching craft_sync_owned_bpcs; no 429 anywhere.
        self.assertEqual(status, 200)
        self.assertFalse(body["scheduled"])
        self.assertEqual(body["cooldown_character_ids"], [70002])
        self.assertEqual(body["retry_seconds"], 720)
        self.assertEqual(body["retry_minutes"], 12)

    def test_reconciliation_still_runs_while_on_cooldown(self) -> None:
        # A cooldown must not deny the user the cached answer.
        with patch("indy_hub.views.api.request_manual_refresh") as throttle:
            throttle.return_value = (False, timedelta(minutes=5))
            status, body = self._post()
        self.assertEqual(status, 200)
        self.assertIn("tracking", body)
        self.assertIn("results", body["tracking"])

    def test_jobs_freshness_is_reported_separately(self) -> None:
        with patch("indy_hub.views.api.request_manual_refresh") as throttle:
            throttle.return_value = (True, None)
            _, body = self._post()
        # Two clocks: when we reconciled, and how old the ESI data is.
        self.assertIsNotNone(body["jobs_last_synced"])
        self.assertIsNotNone(body["tracking"]["refreshed_at"])


class ScheduleTrackingSnapshotTests(TestCase):
    """The reconciled schedule must be the stored one, not the posted one."""

    def setUp(self) -> None:
        self.factory = RequestFactory()
        self.user = User.objects.create_user("sim-snapshot", password="secret123")
        grant_indy_permissions(self.user, "can_access_indy_hub")
        cache.clear()
        self.simulation = ProductionSimulation.objects.create(
            user=self.user,
            blueprint_type_id=96002,
            blueprint_name="Snapshot Blueprint",
        )

    def _unwrap_view(self, view):
        while hasattr(view, "__wrapped__"):
            view = view.__wrapped__
        return view

    def _post(self, payload):
        request = self.factory.post(
            "/api/production-schedule-tracking/refresh/",
            data=json.dumps(payload),
            content_type="application/json",
        )
        request.user = self.user
        response = self._unwrap_view(refresh_production_schedule_tracking)(request)
        return response.status_code, json.loads(response.content)

    def test_first_refresh_stores_the_snapshot(self) -> None:
        status, body = self._post(
            {
                "simulation_id": self.simulation.id,
                "opt_in": True,
                "schedule": TRACKED_SCHEDULE,
            }
        )
        self.assertEqual(status, 200)
        self.assertIsNotNone(body["snapshot_stored_at"])
        self.simulation.refresh_from_db()
        stored = self.simulation.ui_state["scheduleTracking"]["schedule"]
        self.assertEqual(len(stored["jobs"]), 1)

    def test_a_later_client_payload_cannot_redefine_planned(self) -> None:
        self._post(
            {
                "simulation_id": self.simulation.id,
                "opt_in": True,
                "schedule": TRACKED_SCHEDULE,
            }
        )
        tampered = {
            "jobs": [dict(TRACKED_SCHEDULE["jobs"][0], **{"runs_required": 9999})],
            "slots": TRACKED_SCHEDULE["slots"],
            "total_parallel_time_seconds": 3600,
        }
        self._post(
            {
                "simulation_id": self.simulation.id,
                "opt_in": True,
                "schedule": tampered,
            }
        )
        self.simulation.refresh_from_db()
        stored = self.simulation.ui_state["scheduleTracking"]["schedule"]
        # Still the original snapshot: a stale tab cannot rewrite the plan.
        self.assertEqual(stored["jobs"][0]["runs_required"], 2)

    def test_reset_anchor_replaces_the_snapshot(self) -> None:
        self._post(
            {
                "simulation_id": self.simulation.id,
                "opt_in": True,
                "schedule": TRACKED_SCHEDULE,
            }
        )
        replacement = {
            "jobs": [dict(TRACKED_SCHEDULE["jobs"][0], **{"runs_required": 7})],
            "slots": TRACKED_SCHEDULE["slots"],
            "total_parallel_time_seconds": 3600,
        }
        self._post(
            {
                "simulation_id": self.simulation.id,
                "opt_in": True,
                "reset_anchor": True,
                "schedule": replacement,
            }
        )
        self.simulation.refresh_from_db()
        stored = self.simulation.ui_state["scheduleTracking"]["schedule"]
        self.assertEqual(stored["jobs"][0]["runs_required"], 7)

    def test_tracking_without_a_schedule_is_refused_clearly(self) -> None:
        status, body = self._post({"simulation_id": self.simulation.id, "opt_in": True})
        self.assertEqual(status, 409)
        self.assertEqual(body["error"], "no_tracked_schedule")
        self.assertIn("Calculate and save a schedule", body["message"])

    def test_string_anchor_is_accepted_by_the_window_bounding(self) -> None:
        # The stored anchor is an ISO string; normalize_planned_chunks used to
        # add a timedelta to it and raise.
        status, _ = self._post(
            {
                "simulation_id": self.simulation.id,
                "opt_in": True,
                "schedule": TRACKED_SCHEDULE,
            }
        )
        self.assertEqual(status, 200)


class MaterialSourceRefreshGuardTests(TestCase):
    """Clicking refresh must not stack forced ESI fan-out."""

    def setUp(self) -> None:
        self.factory = RequestFactory()
        self.user = User.objects.create_user("sim-asset-guard", password="secret123")
        grant_indy_permissions(self.user, "can_access_indy_hub")
        cache.clear()

    def _unwrap_view(self, view):
        while hasattr(view, "__wrapped__"):
            view = view.__wrapped__
        return view

    def _post(self):
        request = self.factory.post("/api/production-material-sources/refresh/")
        request.user = self.user
        response = self._unwrap_view(refresh_production_material_sources)(request)
        return response.status_code, json.loads(response.content)

    @patch("indy_hub.views.api.refresh_material_exchange_sell_user_assets")
    def test_queues_when_idle(self, task) -> None:
        task.delay.return_value = type("R", (), {"id": "t1"})()
        status, body = self._post()
        self.assertEqual(status, 200)
        self.assertTrue(body["scheduled"])
        task.delay.assert_called_once_with(self.user.id)

    @patch("indy_hub.views.api.refresh_material_exchange_sell_user_assets")
    def test_declines_while_a_run_is_in_flight(self, task) -> None:
        cache.set(
            _production_asset_progress_key(self.user.id),
            {
                "running": True,
                "last_progress_at": timezone.now().timestamp(),
                "done": 1,
                "total": 3,
            },
            600,
        )
        status, body = self._post()
        self.assertEqual(status, 200)
        self.assertFalse(body["scheduled"])
        self.assertEqual(body["error"], "already_running")
        task.delay.assert_not_called()

    @patch("indy_hub.views.api.refresh_material_exchange_sell_user_assets")
    def test_declines_while_esi_is_down(self, task) -> None:
        cache.set(
            me_sell_assets_esi_cooldown_key(self.user.id),
            (timezone.now() + timedelta(minutes=4)).timestamp(),
            600,
        )
        status, body = self._post()
        self.assertEqual(status, 200)
        self.assertFalse(body["scheduled"])
        self.assertEqual(body["error"], "esi_down")
        self.assertGreater(body["retry_seconds"], 0)
        task.delay.assert_not_called()

    @patch("indy_hub.views.api.refresh_material_exchange_sell_user_assets")
    def test_a_stalled_run_does_not_block_forever(self, task) -> None:
        task.delay.return_value = type("R", (), {"id": "t2"})()
        cache.set(
            _production_asset_progress_key(self.user.id),
            {
                "running": True,
                "last_progress_at": (
                    timezone.now() - timedelta(minutes=30)
                ).timestamp(),
            },
            600,
        )
        status, body = self._post()
        self.assertTrue(body["scheduled"])
        task.delay.assert_called_once()

    def test_status_endpoint_reports_idle_running_and_finished_states(self) -> None:
        def _get_status():
            request = self.factory.get(
                "/api/production-material-sources/refresh/status/"
            )
            request.user = self.user
            response = self._unwrap_view(refresh_production_material_sources_status)(
                request
            )
            return response.status_code, json.loads(response.content)

        # 1. Idle state
        status, body = _get_status()
        self.assertEqual(status, 200)
        self.assertFalse(body["running"])
        self.assertFalse(body["finished"])

        # 2. Running state
        cache.set(
            _production_asset_progress_key(self.user.id),
            {
                "running": True,
                "finished": False,
                "error": None,
                "started_at": timezone.now().timestamp(),
                "last_progress_at": timezone.now().timestamp(),
                "total": 3,
                "done": 1,
            },
            600,
        )
        status, body = _get_status()
        self.assertEqual(status, 200)
        self.assertTrue(body["running"])
        self.assertEqual(body["done"], 1)
        self.assertEqual(body["total"], 3)

        # 3. Finished state
        cache.set(
            _production_asset_progress_key(self.user.id),
            {
                "running": False,
                "finished": True,
                "error": None,
                "total": 3,
                "done": 3,
            },
            600,
        )
        status, body = _get_status()
        self.assertEqual(status, 200)
        self.assertFalse(body["running"])
        self.assertTrue(body["finished"])
