# Standard Library
from unittest.mock import patch

# Django
from django.core.cache import cache
from django.test import TestCase

# AA Example App
from indy_hub.views.material_exchange_config import (
    _get_industry_market_group_choice_ids,
    _get_industry_market_group_search_index,
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
    @patch("indy_hub.views.material_exchange_config._get_industry_market_group_choice_ids")
    def test_search_index_uses_itemtype_rows_for_item_names(
        self,
        mock_get_choice_ids,
        mock_get_item_rows,
        mock_build_market_group_index,
    ):
        mock_get_choice_ids.return_value = {20}
        mock_get_item_rows.return_value = [
            (222, "Polar Glaze Matte - Unlimited"),
        ]
        mock_build_market_group_index.return_value = {
            1: {"id": 1, "name": "Root", "parent_market_group_id": None},
            20: {"id": 20, "name": "Apparel", "parent_market_group_id": 1},
            222: {"id": 222, "name": "Clothing", "parent_market_group_id": 20},
        }

        index = _get_industry_market_group_search_index(depth_from_root=1)

        self.assertIn(20, index)
        self.assertIn("Polar Glaze Matte - Unlimited", index[20]["items"])
