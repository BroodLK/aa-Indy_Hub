import json

from django.contrib.auth.models import Permission, User
from django.test import RequestFactory, TestCase
from django.urls import reverse

from indy_hub.models import CustomPrice, ProductionSimulation
from indy_hub.views.api import load_production_config, save_production_config


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
                    "signature": "{\"cfg\":{\"enabled\":true}}",
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

    def test_save_production_config_allows_cost_and_sale_price_for_same_type(self) -> None:
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

    def test_save_without_simulation_id_creates_new_snapshot_for_same_blueprint_and_runs(self) -> None:
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

    def test_overwrite_existing_snapshot_can_change_runs_to_match_another_snapshot(self) -> None:
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
