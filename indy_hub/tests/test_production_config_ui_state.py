import json

from django.contrib.auth.models import Permission, User
from django.test import RequestFactory, TestCase
from django.urls import reverse

from indy_hub.models import ProductionSimulation
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
