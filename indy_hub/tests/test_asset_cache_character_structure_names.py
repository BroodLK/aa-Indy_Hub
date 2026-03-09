"""Tests for character asset refresh caching structure names."""

# Standard Library
from unittest.mock import patch

# Django
from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import TestCase
from django.utils import timezone

# AA Example App
from indy_hub.models import CachedCharacterAsset, CachedCorporationAsset, CachedStructureName
from indy_hub.services import asset_cache


class _FakeToken:
    def __init__(self, *, character_id: int):
        self.character_id = character_id
        self.character = type("Char", (), {"corporation_id": None})()


class _FakeTokenQuerySet(list):
    def require_scopes(self, scopes):
        return self

    def require_valid(self):
        return self

    def exists(self):
        return True


class CharacterAssetRefreshStructureNameTests(TestCase):
    def test_refresh_character_assets_names_only_container_assets(self) -> None:
        user = User.objects.create_user("assets_user", password="secret123")

        character_id = 12345
        container_item_id = 1044300603008
        non_container_item_id = 1044300603009
        child_item_id = 1044300603010
        structure_id = 1042090993674

        fake_tokens = _FakeTokenQuerySet([_FakeToken(character_id=character_id)])

        assets_payload = [
            {
                "item_id": container_item_id,
                "location_id": structure_id,
                "location_flag": "Hangar",
                "type_id": 999,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            {
                "item_id": non_container_item_id,
                "location_id": structure_id,
                "location_flag": "Hangar",
                "type_id": 1001,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            {
                "item_id": child_item_id,
                "location_id": container_item_id,
                "location_flag": "Hangar",
                "type_id": 34,
                "quantity": 123,
                "is_singleton": False,
                "is_blueprint": False,
            }
        ]

        def resolve_side_effect(structure_ids, character_id=None, user=None, **kwargs):
            now = timezone.now()
            for sid in structure_ids:
                CachedStructureName.objects.update_or_create(
                    structure_id=int(sid),
                    defaults={"name": f"Structure {sid}", "last_resolved": now},
                )
            return {int(sid): f"Structure {sid}" for sid in structure_ids}

        with (
            patch.object(asset_cache.Token.objects, "filter", return_value=fake_tokens),
            patch.object(
                asset_cache.shared_client,
                "fetch_character_assets",
                return_value=assets_payload,
            ),
            patch.object(
                asset_cache.shared_client,
                "fetch_character_asset_names",
                return_value={container_item_id: "Example Production Container"},
            ) as mocked_fetch_names,
            patch.object(
                asset_cache,
                "resolve_structure_names",
                side_effect=resolve_side_effect,
            ) as mocked_resolve,
        ):
            refreshed_assets, scope_missing = asset_cache._refresh_character_assets(
                user
            )

        self.assertFalse(scope_missing)
        self.assertEqual(len(refreshed_assets), 3)
        by_item_id = {
            int(row["item_id"]): row
            for row in refreshed_assets
            if row.get("item_id") is not None
        }
        self.assertEqual(
            by_item_id[container_item_id].get("set_name"),
            "Example Production Container",
        )
        self.assertEqual(by_item_id[non_container_item_id].get("set_name"), "")

        # Ensure assets got written with new fields.
        container_row = CachedCharacterAsset.objects.get(
            user=user, item_id=container_item_id
        )
        self.assertEqual(container_row.raw_location_id, structure_id)
        self.assertEqual(container_row.location_id, structure_id)
        self.assertEqual(container_row.set_name, "Example Production Container")

        non_container_row = CachedCharacterAsset.objects.get(
            user=user, item_id=non_container_item_id
        )
        self.assertEqual(non_container_row.set_name, "")

        mocked_fetch_names.assert_called_once()
        fetch_kwargs = mocked_fetch_names.call_args.kwargs
        self.assertEqual(fetch_kwargs["character_id"], character_id)
        self.assertEqual(fetch_kwargs["item_ids"], [container_item_id])

        # Ensure we attempted to resolve/cache the structure name.
        self.assertTrue(mocked_resolve.called)
        self.assertTrue(
            CachedStructureName.objects.filter(structure_id=structure_id).exists()
        )


class CorporationAssetRefreshNameTests(TestCase):
    def test_refresh_corp_assets_names_only_container_assets(self) -> None:
        corporation_id = 98123456
        character_id = 70012345
        container_item_id = 1044300603008
        non_container_item_id = 1044300603009
        child_item_id = 1044300603010
        structure_id = 1042090993674

        assets_payload = [
            {
                "item_id": container_item_id,
                "location_id": structure_id,
                "location_flag": "Hangar",
                "type_id": 999,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            {
                "item_id": non_container_item_id,
                "location_id": structure_id,
                "location_flag": "Hangar",
                "type_id": 1001,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            {
                "item_id": child_item_id,
                "location_id": container_item_id,
                "location_flag": "Hangar",
                "type_id": 34,
                "quantity": 123,
                "is_singleton": False,
                "is_blueprint": False,
            },
        ]

        with (
            patch.object(
                asset_cache,
                "_get_character_for_scope",
                return_value=character_id,
            ),
            patch.object(
                asset_cache.shared_client,
                "fetch_corporation_assets",
                return_value=assets_payload,
            ),
            patch.object(
                asset_cache.shared_client,
                "fetch_corporation_asset_names",
                return_value={container_item_id: "Corp Container Name"},
            ) as mocked_fetch_names,
            patch.object(asset_cache, "_cache_corp_structure_names", return_value={}),
        ):
            refreshed_assets, scope_missing = asset_cache._refresh_corp_assets(
                corporation_id
            )

        self.assertFalse(scope_missing)
        self.assertEqual(len(refreshed_assets), 3)
        by_item_id = {
            int(row["item_id"]): row
            for row in refreshed_assets
            if row.get("item_id") is not None
        }
        self.assertEqual(
            by_item_id[container_item_id].get("set_name"),
            "Corp Container Name",
        )
        self.assertEqual(by_item_id[non_container_item_id].get("set_name"), "")

        mocked_fetch_names.assert_called_once()
        fetch_kwargs = mocked_fetch_names.call_args.kwargs
        self.assertEqual(fetch_kwargs["corporation_id"], corporation_id)
        self.assertEqual(fetch_kwargs["character_id"], character_id)
        self.assertEqual(fetch_kwargs["item_ids"], [container_item_id])

    def test_get_corp_assets_cached_includes_cached_set_name(self) -> None:
        corporation_id = 98123456
        item_id = 1044300603008
        cache.clear()

        CachedCorporationAsset.objects.create(
            corporation_id=corporation_id,
            item_id=item_id,
            location_id=1042090993674,
            location_flag="Hangar",
            type_id=23,
            quantity=1,
            is_singleton=True,
            is_blueprint=False,
        )

        cache.set(
            f"indy_hub:corp_asset_names:{corporation_id}:v1",
            {item_id: "Corp Named Can"},
            3600,
        )

        assets, scope_missing = asset_cache.get_corp_assets_cached(
            corporation_id,
            allow_refresh=False,
            max_age_minutes=60,
        )

        self.assertFalse(scope_missing)
        self.assertEqual(len(assets), 1)
        self.assertEqual(assets[0].get("set_name"), "Corp Named Can")

    def test_get_corp_assets_cached_backfills_set_names_when_missing(self) -> None:
        corporation_id = 98123456
        character_id = 70012345
        container_item_id = 1044300603008
        child_item_id = 1044300603010
        structure_id = 1042090993674
        cache.clear()

        CachedCorporationAsset.objects.create(
            corporation_id=corporation_id,
            item_id=container_item_id,
            location_id=structure_id,
            location_flag="Hangar",
            type_id=23,
            quantity=1,
            is_singleton=True,
            is_blueprint=False,
        )
        CachedCorporationAsset.objects.create(
            corporation_id=corporation_id,
            item_id=child_item_id,
            location_id=container_item_id,
            location_flag="Unlocked",
            type_id=34,
            quantity=10,
            is_singleton=False,
            is_blueprint=False,
        )

        with (
            patch.object(
                asset_cache,
                "_get_character_for_scope",
                return_value=character_id,
            ),
            patch.object(
                asset_cache.shared_client,
                "fetch_corporation_asset_names",
                return_value={container_item_id: "Backfilled Corp Container"},
            ) as mocked_fetch_names,
        ):
            assets, scope_missing = asset_cache.get_corp_assets_cached(
                corporation_id,
                allow_refresh=True,
                max_age_minutes=60,
            )

        self.assertFalse(scope_missing)
        by_item_id = {
            int(row["item_id"]): row
            for row in assets
            if row.get("item_id") is not None
        }
        self.assertEqual(
            by_item_id[container_item_id].get("set_name"),
            "Backfilled Corp Container",
        )
        self.assertEqual(by_item_id[child_item_id].get("set_name"), "")
        mocked_fetch_names.assert_called_once()
