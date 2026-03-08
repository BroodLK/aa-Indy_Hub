# Standard Library
from unittest.mock import patch

# Django
from django.core.cache import cache
from django.test import TestCase

# AA Example App
from indy_hub.views.material_exchange_config import (
    MARKET_GROUP_CHOICE_DEPTH,
    _get_market_group_search_index_for_ids,
    _get_market_group_tree,
    _get_industry_market_group_choice_ids,
    _normalize_market_group_ids_for_choice_depth,
)


class MaterialExchangeConfigMarketGroupCoverageTests(TestCase):
    def setUp(self):
        cache.clear()

    @patch("indy_hub.views.material_exchange_config._build_market_group_index")
    @patch("indy_hub.views.material_exchange_config._get_industry_market_group_ids")
    def test_choice_ids_cover_non_industry_group_paths(
        self,
        mock_get_market_group_ids,
        mock_build_market_group_index,
    ):
        # Treat both IDs as valid item-type market groups, regardless of industry relevance.
        mock_get_market_group_ids.return_value = {111, 222}
        mock_build_market_group_index.return_value = {
            1: {"id": 1, "name": "Root", "parent_market_group_id": None},
            10: {"id": 10, "name": "Industry", "parent_market_group_id": 1},
            20: {"id": 20, "name": "Apparel", "parent_market_group_id": 1},
            111: {"id": 111, "name": "Minerals", "parent_market_group_id": 10},
            222: {"id": 222, "name": "Clothing", "parent_market_group_id": 20},
        }

        grouped_ids = _get_industry_market_group_choice_ids(depth_from_root=1)

        self.assertEqual(grouped_ids, {10, 20})

    @patch("indy_hub.views.material_exchange_config._build_market_group_index")
    @patch("indy_hub.views.material_exchange_config._get_itemtype_market_group_name_rows")
    def test_search_index_uses_itemtype_rows_for_item_names(
        self,
        mock_get_item_rows,
        mock_build_market_group_index,
    ):
        mock_get_item_rows.return_value = [
            (222, "Polar Glaze Matte - Unlimited"),
        ]
        mock_build_market_group_index.return_value = {
            1: {"id": 1, "name": "Root", "parent_market_group_id": None},
            20: {"id": 20, "name": "Apparel", "parent_market_group_id": 1},
            222: {"id": 222, "name": "Clothing", "parent_market_group_id": 20},
        }

        index = _get_market_group_search_index_for_ids({20})

        self.assertIn(20, index)
        self.assertIn("Polar Glaze Matte - Unlimited", index[20]["items"])

    @patch("indy_hub.views.material_exchange_config._build_market_group_index")
    def test_normalize_group_ids_maps_blueprint_leaf_to_blueprint_bucket(
        self,
        mock_build_market_group_index,
    ):
        mock_build_market_group_index.return_value = {
            1: {"id": 1, "name": "Root", "parent_market_group_id": None},
            30: {"id": 30, "name": "Blueprints & Reactions", "parent_market_group_id": 1},
            331: {"id": 331, "name": "Ship Blueprints", "parent_market_group_id": 30},
            777: {"id": 777, "name": "Frigate Blueprints", "parent_market_group_id": 331},
        }

        normalized = _normalize_market_group_ids_for_choice_depth(
            [777],
            depth_from_root=MARKET_GROUP_CHOICE_DEPTH,
        )

        self.assertEqual(normalized, [30])

    @patch("indy_hub.views.material_exchange_config._build_market_group_index")
    def test_non_expandable_groups_have_no_children_in_tree(self, mock_build_market_group_index):
        mock_build_market_group_index.return_value = {
            1: {"id": 1, "name": "Root", "parent_market_group_id": None},
            100: {"id": 100, "name": "Skills", "parent_market_group_id": 1},
            101: {"id": 101, "name": "Special Edition Assets", "parent_market_group_id": 1},
            102: {"id": 102, "name": "Structure Equipment", "parent_market_group_id": 1},
            103: {"id": 103, "name": "Ships", "parent_market_group_id": 1},
            200: {"id": 200, "name": "Spaceship Command", "parent_market_group_id": 100},
            201: {"id": 201, "name": "Special Edition Frigates", "parent_market_group_id": 101},
            202: {"id": 202, "name": "Structure Weapons", "parent_market_group_id": 102},
            203: {"id": 203, "name": "Frigates", "parent_market_group_id": 103},
        }

        tree = _get_market_group_tree()
        nodes_by_label = {node["label"]: node for node in tree}

        self.assertIn("Skills", nodes_by_label)
        self.assertEqual(nodes_by_label["Skills"]["children"], [])
        self.assertFalse(nodes_by_label["Skills"]["expandable"])

        self.assertIn("Special Edition Assets", nodes_by_label)
        self.assertEqual(nodes_by_label["Special Edition Assets"]["children"], [])
        self.assertFalse(nodes_by_label["Special Edition Assets"]["expandable"])

        self.assertIn("Structure Equipment", nodes_by_label)
        self.assertEqual(nodes_by_label["Structure Equipment"]["children"], [])
        self.assertFalse(nodes_by_label["Structure Equipment"]["expandable"])

    @patch("indy_hub.views.material_exchange_config._build_market_group_index")
    def test_manufacture_research_tree_includes_grandchildren(self, mock_build_market_group_index):
        mock_build_market_group_index.return_value = {
            1: {"id": 1, "name": "Root", "parent_market_group_id": None},
            200: {"id": 200, "name": "Manufacture & Research", "parent_market_group_id": 1},
            210: {"id": 210, "name": "Components", "parent_market_group_id": 200},
            211: {"id": 211, "name": "Materials", "parent_market_group_id": 200},
            212: {"id": 212, "name": "Research Equipment", "parent_market_group_id": 200},
            310: {"id": 310, "name": "Advanced Components", "parent_market_group_id": 210},
            311: {"id": 311, "name": "Minerals", "parent_market_group_id": 211},
            312: {"id": 312, "name": "Datacores", "parent_market_group_id": 212},
        }

        tree = _get_market_group_tree()
        manufacture_node = next(
            node for node in tree if node.get("label") == "Manufacture & Research"
        )
        components_node = next(
            child
            for child in (manufacture_node.get("children") or [])
            if child.get("label") == "Components"
        )
        materials_node = next(
            child
            for child in (manufacture_node.get("children") or [])
            if child.get("label") == "Materials"
        )
        research_node = next(
            child
            for child in (manufacture_node.get("children") or [])
            if child.get("label") == "Research Equipment"
        )

        self.assertTrue(components_node["expandable"])
        self.assertTrue(any(g["label"] == "Advanced Components" for g in components_node["children"]))
        self.assertTrue(any(g["label"] == "Minerals" for g in materials_node["children"]))
        self.assertTrue(any(g["label"] == "Datacores" for g in research_node["children"]))
