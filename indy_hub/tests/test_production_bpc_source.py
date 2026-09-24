# Standard Library
from pathlib import Path

# Django
from django.contrib.auth.models import User
from django.test import SimpleTestCase, TestCase

# AA Example App
from indy_hub.models import CachedCharacterAsset
from indy_hub.services.production_material_sources import (
    blueprint_item_ids_at_source,
    describe_bpc_source,
    parse_bpc_source,
)
from indy_hub.services.production_simulation_state import normalize_preference_state

INDUSTRY_VIEW = Path(__file__).resolve().parents[1] / "views" / "industry.py"
CRAFT_TEMPLATE = Path(__file__).resolve().parents[1] / "templates" / "indy_hub" / "industry" / "Craft_BP_v2.html"

STATION = 60003760
OTHER_STATION = 60008494
CAN = 1_000_000_001
NESTED_CAN = 1_000_000_002


def _asset(user, item_id, *, location_id, raw_location_id=None, is_blueprint=True, type_id=1000):
    return CachedCharacterAsset.objects.create(
        user=user,
        character_id=90000001,
        item_id=item_id,
        raw_location_id=raw_location_id or location_id,
        location_id=location_id,
        location_flag="Hangar",
        type_id=type_id,
        quantity=1,
        is_blueprint=is_blueprint,
    )


class ParseBpcSourceTests(SimpleTestCase):
    def test_tokens(self) -> None:
        self.assertEqual(parse_bpc_source(""), (0, 0))
        self.assertEqual(parse_bpc_source("all"), (0, 0))
        self.assertEqual(parse_bpc_source(str(STATION)), (STATION, 0))
        self.assertEqual(parse_bpc_source(f"{STATION}:{CAN}"), (STATION, CAN))
        for bad in ("abc", "-5", f"{STATION}:x", "0", f"{STATION}:-1"):
            self.assertEqual(parse_bpc_source(bad), (0, 0), bad)


class BlueprintSourceTests(TestCase):
    def setUp(self) -> None:
        self.user = User.objects.create_user("bpc-source", password="x")
        self.other = User.objects.create_user("bpc-source-other", password="x")
        # Loose blueprint in the station hangar.
        _asset(self.user, 501, location_id=STATION)
        # A can, a nested can inside it, and a blueprint in the nested can.
        _asset(self.user, CAN, location_id=STATION, is_blueprint=False, type_id=3467)
        _asset(self.user, NESTED_CAN, location_id=STATION, raw_location_id=CAN, is_blueprint=False, type_id=3467)
        _asset(self.user, 502, location_id=STATION, raw_location_id=NESTED_CAN)
        # Blueprint elsewhere, and another user's blueprint at the same station.
        _asset(self.user, 503, location_id=OTHER_STATION)
        _asset(self.other, 601, location_id=STATION)

    def test_location_narrows_to_that_location(self) -> None:
        self.assertEqual(blueprint_item_ids_at_source(self.user, location_id=STATION), {501, 502})

    def test_container_includes_nested_containers(self) -> None:
        self.assertEqual(
            blueprint_item_ids_at_source(self.user, location_id=STATION, container_item_id=CAN),
            {502},
        )

    def test_location_without_own_blueprints_is_not_a_source(self) -> None:
        self.assertIsNone(blueprint_item_ids_at_source(self.other, location_id=OTHER_STATION))

    def test_describe_all_means_no_narrowing(self) -> None:
        result = describe_bpc_source(self.user, "all")
        self.assertIsNone(result["item_ids"])
        self.assertFalse(result["rejected"])

    def test_describe_foreign_or_bogus_source_is_rejected_not_empty(self) -> None:
        # A forged location must fall back to "all" (no narrowing), never to
        # an empty set that would silently zero the user's coverage.
        for token in ("123456", "nonsense"):
            result = describe_bpc_source(self.other, token)
            self.assertIsNone(result["item_ids"], token)
            self.assertTrue(result["rejected"], token)

    def test_describe_valid_source(self) -> None:
        result = describe_bpc_source(self.user, f"{STATION}:{CAN}")
        self.assertEqual(result["item_ids"], {502})
        self.assertEqual(result["token"], f"{STATION}:{CAN}")
        self.assertEqual(result["container_item_id"], CAN)
        self.assertTrue(result["location_name"])


class BlueprintSourceWiringTests(SimpleTestCase):
    def test_view_narrows_owned_blueprints_by_source(self) -> None:
        source = INDUSTRY_VIEW.read_text(encoding="utf-8")
        self.assertIn("describe_bpc_source(request.user, bpc_source_token)", source)
        self.assertIn('user_blueprints.filter(item_id__in=sorted(bpc_source["item_ids"]))', source)
        # The resolved item IDs stay server-side.
        self.assertIn('if key != "item_ids"', source)

    def test_configure_offers_the_selector(self) -> None:
        template = CRAFT_TEMPLATE.read_text(encoding="utf-8")
        for marker in (
            'id="bpcSourceSelect"',
            'id="applyBpcSourceBtn"',
            'id="bpcSourceStatus"',
            'id="buildStructureCacheNote"',
        ):
            self.assertIn(marker, template)


class PrivatePreferenceTests(SimpleTestCase):
    def test_last_build_location_is_a_validated_preference(self) -> None:
        state = normalize_preference_state(
            {"lastSystemId": "30000142", "lastStructureId": 1035466617946, "bpcSource": f"{STATION}:{CAN}"}
        )
        self.assertEqual(state["lastSystemId"], "30000142")
        self.assertEqual(state["lastStructureId"], "1035466617946")
        self.assertEqual(state["bpcSource"], f"{STATION}:{CAN}")
        self.assertNotIn("lastSystemId", normalize_preference_state({"lastSystemId": "Jita"}))
