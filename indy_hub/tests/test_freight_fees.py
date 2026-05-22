from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from indy_hub.services.freight_fees import (
    JITA_4_4_STATION_ID,
    calculate_import_fees,
    get_available_routes,
    get_available_routes_from_jita,
)


class _FakeQuerySet:
    def __init__(self, items):
        self._items = list(items)

    def first(self):
        return self._items[0] if self._items else None

    def select_related(self, *args, **kwargs):
        return self

    def __iter__(self):
        return iter(self._items)


def _install_freight_models(pricing_model):
    freight_module = ModuleType("freight")
    freight_models_module = ModuleType("freight.models")
    freight_models_module.Pricing = pricing_model
    freight_module.models = freight_models_module
    return patch.dict(
        sys.modules,
        {
            "freight": freight_module,
            "freight.models": freight_models_module,
        },
    )


class FreightFeesServiceTests(SimpleTestCase):
    def test_calculate_import_fees_uses_selected_pricing_id(self):
        pricing = Mock()
        pricing.name = "Jita -> Indy"
        pricing.pk = 42
        pricing.get_calculated_price.return_value = 1250000
        pricing.get_contract_price_check_issues.return_value = ["collateral high"]

        manager = Mock()
        manager.filter.side_effect = lambda **kwargs: _FakeQuerySet(
            [pricing] if kwargs == {"pk": 42, "is_active": True} else []
        )
        pricing_model = SimpleNamespace(objects=manager)

        with _install_freight_models(pricing_model):
            result = calculate_import_fees(
                pricing_id=42,
                total_volume_m3=150.0,
                total_collateral_isk=900000000.0,
            )

        self.assertEqual(
            result,
            {
                "freight_cost": 1250000.0,
                "route_name": "Jita -> Indy",
                "pricing_id": 42,
                "issues": ["collateral high"],
            },
        )

    def test_calculate_import_fees_falls_back_to_destination_lookup(self):
        pricing = Mock()
        pricing.name = "Jita -> Destination"
        pricing.pk = 7
        pricing.get_calculated_price.return_value = 550000
        pricing.get_contract_price_check_issues.return_value = []

        manager = Mock()

        def filter_side_effect(**kwargs):
            if kwargs == {
                "start_location_id": JITA_4_4_STATION_ID,
                "end_location_id": 60012345,
                "is_active": True,
            }:
                return _FakeQuerySet([pricing])
            return _FakeQuerySet([])

        manager.filter.side_effect = filter_side_effect
        pricing_model = SimpleNamespace(objects=manager)

        with _install_freight_models(pricing_model):
            result = calculate_import_fees(
                destination_location_id=60012345,
                total_volume_m3=25.0,
                total_collateral_isk=12000000.0,
            )

        self.assertEqual(result["freight_cost"], 550000.0)
        self.assertEqual(result["route_name"], "Jita -> Destination")
        self.assertEqual(result["pricing_id"], 7)
        self.assertEqual(result["issues"], [])

    def test_get_available_routes_returns_all_active_routes_sorted(self):
        route_b = SimpleNamespace(
            pk=2,
            name="Beta Route",
            start_location_id=7002,
            start_location=SimpleNamespace(name="Zeta Start"),
            end_location_id=9002,
            end_location=SimpleNamespace(name="Zeta"),
            is_bidirectional=False,
        )
        route_a = SimpleNamespace(
            pk=1,
            name="Alpha Route",
            start_location_id=7001,
            start_location=SimpleNamespace(name="Alpha Start"),
            end_location_id=9001,
            end_location=SimpleNamespace(name="Alpha"),
            is_bidirectional=False,
        )
        bidirectional = SimpleNamespace(
            pk=3,
            name="Round Trip",
            start_location_id=7003,
            start_location=SimpleNamespace(name="Gamma"),
            end_location_id=9003,
            end_location=SimpleNamespace(name="Delta"),
            is_bidirectional=True,
        )

        manager = Mock()
        manager.filter.return_value = _FakeQuerySet([route_b, route_a, bidirectional])
        pricing_model = SimpleNamespace(objects=manager)

        with _install_freight_models(pricing_model):
            routes = get_available_routes()

        self.assertEqual(
            [route["route_name"] for route in routes],
            ["Alpha Route", "Round Trip", "Beta Route"],
        )
        self.assertEqual(routes[0]["route_label"], "Alpha Start -> Alpha")
        self.assertEqual(routes[1]["route_label"], "Gamma <-> Delta")
        self.assertEqual(routes[2]["start_location_name"], "Zeta Start")

    def test_get_available_routes_from_jita_alias_returns_all_routes(self):
        with patch("indy_hub.services.freight_fees.get_available_routes", return_value=[{"pricing_id": 1}]):
            self.assertEqual(get_available_routes_from_jita(), [{"pricing_id": 1}])
