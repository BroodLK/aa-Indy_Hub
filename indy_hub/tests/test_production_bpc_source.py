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
    list_asset_sources,
    parse_bpc_source,
    parse_bpc_source_scope,
)
from indy_hub.services.production_simulation_state import (
    normalize_preference_state,
)

INDUSTRY_VIEW = Path(__file__).resolve().parents[1] / "views" / "industry.py"
CRAFT_TEMPLATE = (
    Path(__file__).resolve().parents[1]
    / "templates"
    / "indy_hub"
    / "industry"
    / "Craft_BP_v2.html"
)

STATION = 60003760
OTHER_STATION = 60008494
CAN = 1_000_000_001
NESTED_CAN = 1_000_000_002
REGION_THE_FORGE = 10000002
REGION_DOMAIN = 10000043
SYSTEM_JITA = 30000142
SYSTEM_AMARR = 30002187


def _asset(
    user, item_id, *, location_id, raw_location_id=None, is_blueprint=True, type_id=1000
):
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
        self.assertEqual(parse_bpc_source(f"station:{STATION}"), (STATION, 0))
        self.assertEqual(parse_bpc_source(f"container:{STATION}:{CAN}"), (STATION, CAN))
        self.assertEqual(parse_bpc_source(f"region:{REGION_THE_FORGE}"), (0, 0))
        self.assertEqual(parse_bpc_source(f"system:{SYSTEM_JITA}"), (0, 0))
        for bad in ("abc", "-5", f"{STATION}:x", "0", f"{STATION}:-1"):
            self.assertEqual(parse_bpc_source(bad), (0, 0), bad)

    def test_parse_bpc_source_scope(self) -> None:
        self.assertEqual(parse_bpc_source_scope(""), ("all", 0, 0))
        self.assertEqual(parse_bpc_source_scope("all"), ("all", 0, 0))
        self.assertEqual(
            parse_bpc_source_scope(f"region:{REGION_THE_FORGE}"),
            ("region", REGION_THE_FORGE, 0),
        )
        self.assertEqual(
            parse_bpc_source_scope(f"system:{SYSTEM_JITA}"),
            ("system", SYSTEM_JITA, 0),
        )
        self.assertEqual(
            parse_bpc_source_scope(f"station:{STATION}"),
            ("station", STATION, 0),
        )
        self.assertEqual(
            parse_bpc_source_scope(f"container:{STATION}:{CAN}"),
            ("container", STATION, CAN),
        )
        self.assertEqual(
            parse_bpc_source_scope(str(STATION)),
            ("station", STATION, 0),
        )
        self.assertEqual(
            parse_bpc_source_scope(f"{STATION}:{CAN}"),
            ("container", STATION, CAN),
        )


class BlueprintSourceTests(TestCase):
    def setUp(self) -> None:
        # Alliance Auth (External Libs)
        from eve_sde.models import Constellation, NPCStation, Region, SolarSystem

        self.region_forge, _ = Region.objects.get_or_create(
            id=REGION_THE_FORGE, defaults={"name": "The Forge"}
        )
        self.region_domain, _ = Region.objects.get_or_create(
            id=REGION_DOMAIN, defaults={"name": "Domain"}
        )
        self.constellation_kimotoro, _ = Constellation.objects.get_or_create(
            id=20000020,
            defaults={"name": "Kimotoro", "region": self.region_forge},
        )
        self.constellation_throne, _ = Constellation.objects.get_or_create(
            id=20000322,
            defaults={"name": "Throne Worlds", "region": self.region_domain},
        )
        self.system_jita, _ = SolarSystem.objects.get_or_create(
            id=SYSTEM_JITA,
            defaults={"name": "Jita", "constellation": self.constellation_kimotoro},
        )
        self.system_amarr, _ = SolarSystem.objects.get_or_create(
            id=SYSTEM_AMARR,
            defaults={"name": "Amarr", "constellation": self.constellation_throne},
        )
        NPCStation.objects.get_or_create(
            id=STATION,
            defaults={
                "name": "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
                "solar_system": self.system_jita,
            },
        )
        NPCStation.objects.get_or_create(
            id=OTHER_STATION,
            defaults={
                "name": "Amarr VIII (Oris) - Emperor Family Academy",
                "solar_system": self.system_amarr,
            },
        )

        self.user = User.objects.create_user("bpc-source", password="x")
        self.other = User.objects.create_user("bpc-source-other", password="x")
        # Loose blueprint in the station hangar.
        _asset(self.user, 501, location_id=STATION)
        # A can, a nested can inside it, and a blueprint in the nested can.
        _asset(self.user, CAN, location_id=STATION, is_blueprint=False, type_id=3467)
        _asset(
            self.user,
            NESTED_CAN,
            location_id=STATION,
            raw_location_id=CAN,
            is_blueprint=False,
            type_id=3467,
        )
        _asset(self.user, 502, location_id=STATION, raw_location_id=NESTED_CAN)
        # Blueprint elsewhere, and another user's blueprint at the same station.
        _asset(self.user, 503, location_id=OTHER_STATION)
        _asset(self.other, 601, location_id=STATION)

    def test_location_narrows_to_that_location(self) -> None:
        self.assertEqual(
            blueprint_item_ids_at_source(self.user, location_id=STATION), {501, 502}
        )

    def test_region_narrows_to_locations_in_region(self) -> None:
        self.assertEqual(
            blueprint_item_ids_at_source(self.user, region_id=REGION_THE_FORGE),
            {501, 502},
        )
        self.assertEqual(
            blueprint_item_ids_at_source(self.user, region_id=REGION_DOMAIN),
            {503},
        )

    def test_system_narrows_to_locations_in_system(self) -> None:
        self.assertEqual(
            blueprint_item_ids_at_source(self.user, system_id=SYSTEM_JITA),
            {501, 502},
        )
        self.assertEqual(
            blueprint_item_ids_at_source(self.user, system_id=SYSTEM_AMARR),
            {503},
        )

    def test_container_includes_nested_containers(self) -> None:
        self.assertEqual(
            blueprint_item_ids_at_source(
                self.user, location_id=STATION, container_item_id=CAN
            ),
            {502},
        )

    def test_location_without_own_blueprints_is_not_a_source(self) -> None:
        self.assertIsNone(
            blueprint_item_ids_at_source(self.other, location_id=OTHER_STATION)
        )

    def test_describe_all_means_no_narrowing(self) -> None:
        result = describe_bpc_source(self.user, "all")
        self.assertIsNone(result["item_ids"])
        self.assertFalse(result["rejected"])
        self.assertEqual(result["scope"], "all")

    def test_describe_foreign_or_bogus_source_is_rejected_not_empty(self) -> None:
        # A forged location must fall back to "all" (no narrowing), never to
        # an empty set that would silently zero the user's coverage.
        for token in ("123456", "nonsense", "region:999999", "system:999999"):
            result = describe_bpc_source(self.other, token)
            self.assertIsNone(result["item_ids"], token)
            self.assertTrue(result["rejected"], token)

    def test_describe_valid_source(self) -> None:
        result = describe_bpc_source(self.user, f"{STATION}:{CAN}")
        self.assertEqual(result["item_ids"], {502})
        self.assertEqual(result["token"], f"{STATION}:{CAN}")
        self.assertEqual(result["container_item_id"], CAN)
        self.assertEqual(result["scope"], "container")
        self.assertTrue(result["location_name"])

    def test_describe_valid_region_and_system_sources(self) -> None:
        region_result = describe_bpc_source(self.user, f"region:{REGION_THE_FORGE}")
        self.assertEqual(region_result["item_ids"], {501, 502})
        self.assertEqual(region_result["scope"], "region")
        self.assertEqual(region_result["region_id"], REGION_THE_FORGE)
        self.assertEqual(region_result["region_name"], "The Forge")
        self.assertFalse(region_result["rejected"])

        system_result = describe_bpc_source(self.user, f"system:{SYSTEM_JITA}")
        self.assertEqual(system_result["item_ids"], {501, 502})
        self.assertEqual(system_result["scope"], "system")
        self.assertEqual(system_result["system_id"], SYSTEM_JITA)
        self.assertEqual(system_result["system_name"], "Jita")
        self.assertFalse(system_result["rejected"])

    def test_list_asset_sources_returns_regions_and_systems(self) -> None:
        data = list_asset_sources(self.user, blueprints=True)
        self.assertTrue(
            any(r["region_id"] == REGION_THE_FORGE for r in data["regions"])
        )
        self.assertTrue(any(s["system_id"] == SYSTEM_JITA for s in data["systems"]))

    def test_bp_model_in_cans_across_different_containers(self) -> None:
        from indy_hub.models import Blueprint

        bpo_can = 1_000_000_010
        bpc_can = 1_000_000_020
        # Register the two cans in CachedCharacterAsset at STATION
        _asset(self.user, bpo_can, location_id=STATION, is_blueprint=False, type_id=3467)
        _asset(self.user, bpc_can, location_id=STATION, is_blueprint=False, type_id=3467)

        # Create BPO in bpo_can and BPC in bpc_can via Blueprint model
        Blueprint.objects.create(
            owner_user=self.user,
            character_id=90000001,
            item_id=701,
            type_id=1001,
            location_id=bpo_can,
            location_flag="Hangar",
            quantity=-1,
            bp_type=Blueprint.BPType.ORIGINAL,
        )
        Blueprint.objects.create(
            owner_user=self.user,
            character_id=90000001,
            item_id=702,
            type_id=1002,
            location_id=bpc_can,
            location_flag="Hangar",
            quantity=-2,
            runs=10,
            bp_type=Blueprint.BPType.COPY,
        )

        # Selecting station should find both 701 and 702 (plus existing 501, 502)
        station_items = blueprint_item_ids_at_source(self.user, location_id=STATION)
        self.assertIn(701, station_items)
        self.assertIn(702, station_items)

        # Selecting only the BPC can should find 702 and NOT 701
        bpc_can_items = blueprint_item_ids_at_source(
            self.user, location_id=STATION, container_item_id=bpc_can
        )
        self.assertEqual(bpc_can_items, {702})

        # Selecting only the BPO can should find 701 and NOT 702
        bpo_can_items = blueprint_item_ids_at_source(
            self.user, location_id=STATION, container_item_id=bpo_can
        )
        self.assertEqual(bpo_can_items, {701})


class BlueprintSourceWiringTests(SimpleTestCase):
    def test_view_narrows_owned_blueprints_by_source(self) -> None:
        source = INDUSTRY_VIEW.read_text(encoding="utf-8")
        self.assertIn("describe_bpc_source(request.user, bpc_source_token)", source)
        self.assertIn('item_id__in=sorted(bpc_source["item_ids"])', source)
        # The resolved item IDs stay server-side.
        self.assertIn('if key != "item_ids"', source)

    def test_configure_offers_the_selector(self) -> None:
        template = CRAFT_TEMPLATE.read_text(encoding="utf-8")
        for marker in (
            'id="bpcSourceScopeSelect"',
            'id="bpcSourceRegionSelect"',
            'id="bpcSourceSystemInput"',
            'id="bpcSourceStationSelect"',
            'id="bpcSourceContainerSelect"',
            'id="bpcSourceSystemDatalist"',
            'id="bpcSourceSelect"',
            'id="applyBpcSourceBtn"',
            'id="bpcSourceStatus"',
            'id="buildStructureCacheNote"',
        ):
            self.assertIn(marker, template)


class PrivatePreferenceTests(SimpleTestCase):
    def test_last_build_location_is_a_validated_preference(self) -> None:
        state = normalize_preference_state(
            {
                "lastSystemId": "30000142",
                "lastStructureId": 1035466617946,
                "bpcSource": f"{STATION}:{CAN}",
            }
        )
        self.assertEqual(state["lastSystemId"], "30000142")
        self.assertEqual(state["lastStructureId"], "1035466617946")
        self.assertEqual(state["bpcSource"], f"{STATION}:{CAN}")
        self.assertNotIn(
            "lastSystemId", normalize_preference_state({"lastSystemId": "Jita"})
        )
