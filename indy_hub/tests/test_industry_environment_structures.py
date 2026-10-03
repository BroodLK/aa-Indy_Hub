from django.contrib.auth.models import User
from django.test import TestCase
from django.utils import timezone

from allianceauth.authentication.models import CharacterOwnership
from allianceauth.eveonline.models import EveAllianceInfo, EveCharacter, EveCorporationInfo
from indy_hub.models import Blueprint, CachedCorporationAsset, CachedStructureName
from indy_hub.services.industry_environment import (
    _get_user_alliance_corporation_ids,
    _load_corptools_engineering_structures,
)
from indy_hub.services.production_material_sources import list_asset_sources


class IndustryEnvironmentStructuresTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user("testuser", password="password")
        self.alliance_info = EveAllianceInfo.objects.create(
            alliance_id=3001,
            alliance_name="Test Alliance",
            alliance_ticker="TALLI",
            executor_corp_id=2001,
        )
        self.corp_info = EveCorporationInfo.objects.create(
            corporation_id=2001,
            corporation_name="Engineering Corp",
            corporation_ticker="ENG",
            member_count=10,
            alliance=self.alliance_info,
        )
        self.character = EveCharacter.objects.create(
            character_id=1001,
            character_name="Test Industrialist",
            corporation_id=2001,
            corporation_name="Engineering Corp",
            corporation_ticker="ENG",
            alliance_id=3001,
            alliance_name="Test Alliance",
        )
        CharacterOwnership.objects.create(
            user=self.user,
            character=self.character,
            owner_hash="hash1001",
        )

    def test_get_user_alliance_corporation_ids_includes_alliance_corps(self):
        corp_ids = _get_user_alliance_corporation_ids(self.user)
        self.assertIn(2001, corp_ids)

    def test_load_cached_assets_engineering_structures(self):
        CachedCorporationAsset.objects.create(
            corporation_id=2001,
            location_id=1000000000050,
            location_flag="Hangar",
            item_id=1000000000050,
            type_id=35825,  # Raitaru (Engineering Complex)
            synced_at=timezone.now(),
        )
        CachedStructureName.objects.create(
            structure_id=1000000000050,
            name="Alpha Engineering Complex",
        )
        structures = _load_corptools_engineering_structures([2001])
        structure_ids = [s["structure_id"] for s in structures]
        self.assertIn(1000000000050, structure_ids)


class BlueprintSourceLocationTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user("bpuser", password="password")
        self.alliance_info = EveAllianceInfo.objects.create(
            alliance_id=3002,
            alliance_name="Blueprint Alliance",
            alliance_ticker="BALLI",
            executor_corp_id=2002,
        )
        self.corp_info = EveCorporationInfo.objects.create(
            corporation_id=2002,
            corporation_name="Blueprint Corp",
            corporation_ticker="BPCORP",
            member_count=5,
            alliance=self.alliance_info,
        )
        self.character = EveCharacter.objects.create(
            character_id=1002,
            character_name="BP Industrialist",
            corporation_id=2002,
            corporation_name="Blueprint Corp",
            corporation_ticker="BPCORP",
            alliance_id=3002,
            alliance_name="Blueprint Alliance",
        )
        CharacterOwnership.objects.create(
            user=self.user,
            character=self.character,
            owner_hash="hash1002",
        )

    def test_list_asset_sources_finds_blueprints_from_blueprint_model(self):
        Blueprint.objects.create(
            owner_user=self.user,
            character_id=1002,
            item_id=99990001,
            type_id=688,
            type_name="Obelisk Blueprint",
            location_id=1000000000060,
            location_name="Remote Citadel",
            location_flag="Hangar",
            quantity=1,
            material_efficiency=10,
            time_efficiency=20,
            runs=-1,
            bp_type=Blueprint.BPType.ORIGINAL,
        )
        result = list_asset_sources(self.user, blueprints=True)
        sources = result.get("sources", [])
        location_ids = [s["location_id"] for s in sources]
        self.assertIn(1000000000060, location_ids)
