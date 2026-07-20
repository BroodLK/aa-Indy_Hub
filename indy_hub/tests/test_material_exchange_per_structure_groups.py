# Standard Library
from unittest.mock import Mock, patch

# Django
from django.core.cache import cache
from django.test import TestCase

# AA Example App
from indy_hub.models import MaterialExchangeConfig
from indy_hub.views.material_exchange import (
    _find_sell_locations_for_type,
    _get_allowed_type_ids_for_config,
    _get_type_ids_for_market_group_branches,
)


class MaterialExchangePerStructureGroupTests(TestCase):
    def setUp(self):
        cache.clear()
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=60003760,
            structure_name="Primary Structure",
            sell_structure_ids=[60003760, 60003761],
            sell_structure_names=["Alpha", "Beta"],
            buy_structure_ids=[60003760, 60003761],
            buy_structure_names=["Alpha", "Beta"],
            allowed_market_groups_buy=[150],
            allowed_market_groups_buy_by_structure={
                "60003760": [300],
                "60003761": [400],
            },
            allowed_market_groups_sell=[100],
            allowed_market_groups_sell_by_structure={
                "60003760": None,
                "60003761": [200],
            },
            is_active=True,
        )

    @patch("indy_hub.views.material_exchange._get_market_group_parent_map")
    @patch("eve_sde.models.ItemType.objects.exclude")
    def test_market_group_branch_resolver_excludes_unselected_sibling_groups(
        self,
        mock_exclude,
        mock_parent_map,
    ):
        mock_parent_map.return_value = {
            211: 200,
            311: 211,
            411: 311,
            412: 311,
        }
        mock_queryset = Mock()
        mock_queryset.values_list.return_value = [
            (1000, 411),  # Raw Planetary Materials
            (1001, 412),  # Refined Planetary Materials
        ]
        mock_exclude.return_value = mock_queryset

        allowed_type_ids = _get_type_ids_for_market_group_branches({412})

        self.assertEqual(allowed_type_ids, {1001})

    @patch("indy_hub.views.material_exchange._expand_market_group_ids")
    @patch("indy_hub.views.material_exchange._get_type_ids_for_market_group_branches")
    def test_sell_structure_with_explicit_all_returns_no_filter(
        self,
        mock_get_type_ids,
        mock_expand_market_group_ids,
    ):
        allowed_type_ids = _get_allowed_type_ids_for_config(
            self.config,
            "sell",
            structure_id=60003760,
        )

        self.assertIsNone(allowed_type_ids)
        mock_get_type_ids.assert_not_called()
        mock_expand_market_group_ids.assert_not_called()

    @patch("indy_hub.views.material_exchange._expand_market_group_ids", return_value={200})
    @patch("indy_hub.views.material_exchange._get_type_ids_for_market_group_branches", return_value={34, 35})
    def test_sell_structure_specific_groups_override_global_groups(
        self,
        mock_get_type_ids,
        _mock_expand_market_group_ids,
    ):
        allowed_type_ids = _get_allowed_type_ids_for_config(
            self.config,
            "sell",
            structure_id=60003761,
        )

        self.assertEqual(allowed_type_ids, {34, 35})
        mock_get_type_ids.assert_called_once_with({200})

    @patch("indy_hub.views.material_exchange._expand_market_group_ids", return_value={100})
    @patch("indy_hub.views.material_exchange._get_type_ids_for_market_group_branches", return_value={36})
    def test_sell_structure_without_override_uses_global_groups(
        self,
        mock_get_type_ids,
        _mock_expand_market_group_ids,
    ):
        self.config.allowed_market_groups_sell_by_structure = {"60003760": [200]}
        self.config.save(update_fields=["allowed_market_groups_sell_by_structure"])

        allowed_type_ids = _get_allowed_type_ids_for_config(
            self.config,
            "sell",
            structure_id=60003761,
        )

        self.assertEqual(allowed_type_ids, {36})
        mock_get_type_ids.assert_called_once_with({100})

    @patch("indy_hub.views.material_exchange._expand_market_group_ids", return_value={500})
    @patch("indy_hub.views.material_exchange._get_type_ids_for_market_group_branches", return_value={37})
    def test_sell_structure_without_override_uses_default_profile_before_global_groups(
        self,
        mock_get_type_ids,
        _mock_expand_market_group_ids,
    ):
        self.config.allowed_market_groups_sell = [100]
        self.config.allowed_market_groups_sell_by_structure = {}
        self.config.sell_market_group_profiles = [
            {
                "name": "Materials",
                "is_default": True,
                "allow_all": False,
                "market_group_ids": [500],
            }
        ]
        self.config.save(
            update_fields=[
                "allowed_market_groups_sell",
                "allowed_market_groups_sell_by_structure",
                "sell_market_group_profiles",
            ]
        )

        allowed_type_ids = _get_allowed_type_ids_for_config(
            self.config,
            "sell",
            structure_id=60003761,
        )

        self.assertEqual(allowed_type_ids, {37})
        mock_get_type_ids.assert_called_once_with({500})

    @patch("indy_hub.views.material_exchange._expand_market_group_ids", return_value={300})
    @patch("indy_hub.views.material_exchange._get_type_ids_for_market_group_branches", return_value={44})
    def test_buy_structure_specific_groups_override_global_groups(
        self,
        mock_get_type_ids,
        _mock_expand_market_group_ids,
    ):
        allowed_type_ids = _get_allowed_type_ids_for_config(
            self.config,
            "buy",
            structure_id=60003760,
        )

        self.assertEqual(allowed_type_ids, {44})
        mock_get_type_ids.assert_called_once_with({300})

    @patch("indy_hub.views.material_exchange._expand_market_group_ids")
    @patch("indy_hub.views.material_exchange._get_type_ids_for_market_group_branches")
    def test_buy_structure_with_explicit_all_returns_no_filter(
        self,
        mock_get_type_ids,
        mock_expand_market_group_ids,
    ):
        self.config.allowed_market_groups_buy_by_structure = {"60003760": None}
        self.config.save(update_fields=["allowed_market_groups_buy_by_structure"])

        allowed_type_ids = _get_allowed_type_ids_for_config(
            self.config,
            "buy",
            structure_id=60003760,
        )

        self.assertIsNone(allowed_type_ids)
        mock_get_type_ids.assert_not_called()
        mock_expand_market_group_ids.assert_not_called()

    @patch("indy_hub.views.material_exchange._expand_market_group_ids", return_value={150})
    @patch("indy_hub.views.material_exchange._get_type_ids_for_market_group_branches", return_value={45})
    def test_buy_structure_without_override_uses_global_groups(
        self,
        mock_get_type_ids,
        _mock_expand_market_group_ids,
    ):
        self.config.allowed_market_groups_buy_by_structure = {"60003760": [300]}
        self.config.save(update_fields=["allowed_market_groups_buy_by_structure"])

        allowed_type_ids = _get_allowed_type_ids_for_config(
            self.config,
            "buy",
            structure_id=60003761,
        )

        self.assertEqual(allowed_type_ids, {45})
        mock_get_type_ids.assert_called_once_with({150})

    @patch("indy_hub.views.material_exchange._expand_market_group_ids", return_value={700})
    @patch("indy_hub.views.material_exchange._get_type_ids_for_market_group_branches", return_value={46})
    def test_buy_structure_without_override_uses_default_profile_before_global_groups(
        self,
        mock_get_type_ids,
        _mock_expand_market_group_ids,
    ):
        self.config.allowed_market_groups_buy = [150]
        self.config.allowed_market_groups_buy_by_structure = {}
        self.config.buy_market_group_profiles = [
            {
                "name": "Stock Desk",
                "is_default": True,
                "allow_all": False,
                "market_group_ids": [700],
            }
        ]
        self.config.save(
            update_fields=[
                "allowed_market_groups_buy",
                "allowed_market_groups_buy_by_structure",
                "buy_market_group_profiles",
            ]
        )

        allowed_type_ids = _get_allowed_type_ids_for_config(
            self.config,
            "buy",
            structure_id=60003761,
        )

        self.assertEqual(allowed_type_ids, {46})
        mock_get_type_ids.assert_called_once_with({700})

    @patch("indy_hub.views.material_exchange._get_allowed_type_ids_for_config")
    def test_find_sell_locations_for_type_returns_only_accepted_locations(
        self,
        mock_allowed_type_ids,
    ):
        def _allowed_for_location(_config, _mode, *, structure_id=None):
            if int(structure_id) == 60003760:
                return None
            if int(structure_id) == 60003761:
                return {35}
            return set()

        mock_allowed_type_ids.side_effect = _allowed_for_location

        matches = _find_sell_locations_for_type(
            config=self.config,
            sell_structure_ids=[60003760, 60003761],
            sell_structure_name_map={60003760: "Alpha", 60003761: "Beta"},
            user_assets_by_location={
                60003760: {34: 10},
                60003761: {34: 5},
            },
            type_id=34,
            exclude_location_id=None,
            allowed_type_ids_cache={},
        )

        self.assertEqual(
            matches,
            [
                {
                    "id": 60003760,
                    "name": "Alpha",
                    "quantity": 10,
                }
            ],
        )
