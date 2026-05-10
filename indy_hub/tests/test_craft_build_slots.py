"""Tests for craft build slot payload helpers."""

# Standard Library
from datetime import timedelta

# Django
from django.contrib.auth.models import User
from django.test import TestCase
from django.utils import timezone

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership
from allianceauth.eveonline.models import EveCharacter

# AA Example App
from indy_hub.models import Blueprint, IndustryJob, IndustrySkillSnapshot
from indy_hub.views.industry import _build_slot_overview_rows


class CraftBuildSlotOverviewTests(TestCase):
    def setUp(self) -> None:
        self.user = User.objects.create_user("buildslots", password="secret123")
        self.character_id = 9_100_001
        self.character = EveCharacter.objects.create(
            character_id=self.character_id,
            character_name="Build Slot Pilot",
            corporation_id=2_000_001,
            corporation_name="Test Corp",
            corporation_ticker="TEST",
            alliance_id=None,
            alliance_name="",
            alliance_ticker="",
            faction_id=None,
            faction_name="",
        )
        CharacterOwnership.objects.create(
            user=self.user,
            character=self.character,
            owner_hash=f"hash-{self.character_id}-{self.user.id}",
        )
        IndustrySkillSnapshot.objects.create(
            owner_user=self.user,
            character_id=self.character_id,
            mass_production_level=4,
            advanced_mass_production_level=1,
        )

        now = timezone.now()
        IndustryJob.objects.create(
            owner_user=self.user,
            character_id=self.character_id,
            corporation_id=None,
            corporation_name="",
            owner_kind=Blueprint.OwnerKind.CHARACTER,
            job_id=7000001,
            installer_id=self.character_id,
            station_id=60003760,
            location_name="Jita IV - Moon 4",
            activity_id=1,
            blueprint_id=8000001,
            blueprint_type_id=9000001,
            runs=1,
            cost=None,
            licensed_runs=None,
            probability=None,
            product_type_id=10000001,
            status="active",
            duration=3600,
            start_date=now,
            end_date=now + timedelta(hours=1),
            pause_date=None,
            completed_date=None,
            completed_character_id=None,
            successful_runs=None,
            activity_name="Manufacturing",
            blueprint_type_name="Build Blueprint",
            product_type_name="Build Product",
            character_name="Build Slot Pilot",
        )

    def test_refresh_false_uses_cached_snapshot_without_skill_scope(self) -> None:
        rows = _build_slot_overview_rows(self.user, refresh_skills=False)

        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]["skills_missing"])
        self.assertEqual(
            rows[0]["manufacturing"],
            {"total": 6, "available": 5, "used": 1, "percent_used": 17},
        )

    def test_refresh_true_hides_slots_without_skill_scope(self) -> None:
        rows = _build_slot_overview_rows(self.user)

        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["skills_missing"])
        self.assertEqual(
            rows[0]["manufacturing"],
            {"total": None, "available": None, "used": None, "percent_used": 0},
        )
