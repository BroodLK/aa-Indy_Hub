# Standard Library
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

# Django
from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import TestCase

# AA Example App
from indy_hub.models import Blueprint, MaterialExchangeConfig, MaterialExchangeItemPriceOverride, MaterialExchangeStock
from indy_hub.services.asset_cache import make_managed_hangar_location_id
from indy_hub.tasks.material_exchange import _sync_stock_impl
from indy_hub.views.material_exchange import (
    _apply_reserved_quantities_to_buy_stock_snapshot,
    _build_buy_material_rows,
    _get_buy_browse_snapshot_cache_key,
    _get_buy_submission_snapshot_cache_key,
    _get_buy_location_scoped_corp_assets,
    _get_buy_stock_snapshot_for_submission,
    _get_buy_stock_blueprint_variant_map,
    _get_buy_stock_blueprint_variant_map_from_scoped_assets,
    _get_corp_blueprint_details_by_item_id,
    _selected_buy_stock_items_share_source_location,
    _store_buy_submission_snapshot,
    rebuild_material_exchange_buy_browse_snapshot_cache,
)


class MaterialExchangeStockMultiLocationTests(TestCase):
    def setUp(self):
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=1001,
            structure_name="Primary Structure",
            buy_structure_ids=[1001, 1002],
            buy_structure_names=["Structure Alpha", "Structure Beta"],
            buy_enabled=True,
            hangar_division=7,
            is_active=True,
        )

    @patch("indy_hub.tasks.material_exchange.sync_material_exchange_prices")
    @patch("indy_hub.tasks.material_exchange.get_type_name", return_value="Tritanium")
    @patch("indy_hub.tasks.material_exchange.get_corp_assets_cached")
    def test_sync_stock_aggregates_all_buy_locations(
        self,
        mock_get_corp_assets_cached,
        _mock_get_type_name,
        _mock_sync_prices,
    ):
        corp_assets = [
            # Structure 1001 represented via office-folder item id.
            {
                "item_id": 5001,
                "location_id": 1001,
                "location_flag": "OfficeFolder",
                "type_id": 27,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            {
                "item_id": 6001,
                "location_id": 5001,
                "location_flag": "CorpSAG7",
                "type_id": 34,
                "quantity": 10,
                "is_singleton": False,
                "is_blueprint": False,
            },
            # Structure 1002 represented directly by structure id.
            {
                "item_id": 6002,
                "location_id": 1002,
                "location_flag": "CorpSAG7",
                "type_id": 34,
                "quantity": 5,
                "is_singleton": False,
                "is_blueprint": False,
            },
        ]
        mock_get_corp_assets_cached.return_value = (corp_assets, False)

        _sync_stock_impl()

        stock = MaterialExchangeStock.objects.get(config=self.config, type_id=34)
        self.assertEqual(stock.quantity, 15)
        self.assertEqual(stock.source_structure_ids, [1001, 1002])
        self.assertEqual(stock.source_structure_names, ["Structure Alpha", "Structure Beta"])

    @patch("indy_hub.tasks.material_exchange.sync_material_exchange_prices")
    @patch("indy_hub.tasks.material_exchange.get_type_name", return_value="Tritanium")
    @patch("indy_hub.tasks.material_exchange.get_corp_assets_cached")
    def test_sync_stock_accepts_managed_hangar_location_context(
        self,
        mock_get_corp_assets_cached,
        _mock_get_type_name,
        _mock_sync_prices,
    ):
        office_folder_item_id = 5002
        managed_location_id = make_managed_hangar_location_id(office_folder_item_id, self.config.hangar_division)

        corp_assets = [
            {
                "item_id": office_folder_item_id,
                "location_id": 1002,
                "location_flag": "OfficeFolder",
                "type_id": 27,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            {
                "item_id": 7001,
                "location_id": managed_location_id,
                "location_flag": "Hangar",
                "type_id": 34,
                "quantity": 12,
                "is_singleton": False,
                "is_blueprint": False,
            },
        ]
        mock_get_corp_assets_cached.return_value = (corp_assets, False)

        _sync_stock_impl()

        stock = MaterialExchangeStock.objects.get(config=self.config, type_id=34)
        self.assertEqual(stock.quantity, 12)
        self.assertEqual(stock.source_structure_ids, [1002])
        self.assertEqual(stock.source_structure_names, ["Structure Beta"])

    @patch("indy_hub.tasks.material_exchange.sync_material_exchange_prices")
    @patch("indy_hub.tasks.material_exchange.get_type_name", return_value="Tritanium")
    @patch("indy_hub.tasks.material_exchange.get_corp_assets_cached")
    def test_sync_stock_accepts_station_hangar_fallback_for_container_children(
        self,
        mock_get_corp_assets_cached,
        _mock_get_type_name,
        _mock_sync_prices,
    ):
        """Items in containers should still count when station-style assets use Hangar flags."""
        self.config.buy_structure_ids = [1001]
        self.config.buy_structure_names = ["Structure Alpha"]
        self.config.hangar_division = 1
        self.config.save(update_fields=["buy_structure_ids", "buy_structure_names", "hangar_division"])

        corp_assets = [
            # Station-style parent container in hangar (no OfficeFolder row).
            {
                "item_id": 9001,
                "location_id": 1001,
                "location_flag": "Hangar",
                "type_id": 23,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            # Mineral stack inside the container.
            {
                "item_id": 9002,
                "location_id": 9001,
                "location_flag": "Unlocked",
                "type_id": 34,
                "quantity": 44,
                "is_singleton": False,
                "is_blueprint": False,
            },
        ]
        mock_get_corp_assets_cached.return_value = (corp_assets, False)

        _sync_stock_impl()

        stock = MaterialExchangeStock.objects.get(config=self.config, type_id=34)
        self.assertEqual(stock.quantity, 44)
        self.assertEqual(stock.source_structure_ids, [1001])
        self.assertEqual(stock.source_structure_names, ["Structure Alpha"])

    def test_rebuild_buy_browse_snapshot_cache_persists_flat_stock_rows(self):
        MaterialExchangeStock.objects.create(
            config=self.config,
            type_id=34,
            type_name="Tritanium",
            quantity=15,
            source_structure_ids=[1001, 1002],
            source_structure_names=["Structure Alpha", "Structure Beta"],
            jita_buy_price=Decimal("5.00"),
            jita_sell_price=Decimal("6.00"),
        )

        with patch("indy_hub.views.material_exchange._stock_item_is_allowed_for_buy", return_value=True):
            cached_snapshot = rebuild_material_exchange_buy_browse_snapshot_cache(config=self.config)
        cache_key = _get_buy_browse_snapshot_cache_key(self.config)

        self.assertEqual(cache.get(cache_key), cached_snapshot)
        self.assertEqual(len(cached_snapshot["stock_rows"]), 1)
        self.assertEqual(cached_snapshot["stock_rows"][0]["type_id"], 34)
        self.assertEqual(cached_snapshot["stock_rows"][0]["quantity"], 15)
        self.assertEqual(cached_snapshot["stock_meta_by_type"][34]["source_structure_ids"], [1001, 1002])

    def test_apply_reserved_quantities_to_buy_stock_snapshot_preserves_row_order_distribution(self):
        static_snapshot = {
            "stock_rows": [
                {
                    "row_kind": "item",
                    "row_index": 0,
                    "type_id": 34,
                    "quantity": 4,
                    "blueprint_variant": "",
                    "container_path": "",
                },
                {
                    "row_kind": "container",
                    "row_index": 1,
                    "container_key": "c9001",
                },
                {
                    "row_kind": "item",
                    "row_index": 2,
                    "type_id": 34,
                    "quantity": 6,
                    "blueprint_variant": "",
                    "container_path": "c9001",
                },
            ],
            "stock_meta_by_type": {
                34: {
                    "type_id": 34,
                    "quantity": 10,
                    "reserved_quantity": 0,
                    "available_quantity": 10,
                }
            },
            "pre_filter_stock_count": 1,
            "post_group_filter_count": 1,
        }

        reserved_snapshot = _apply_reserved_quantities_to_buy_stock_snapshot(
            buy_stock_snapshot=static_snapshot,
            reserved_quantities={34: 3},
        )

        self.assertEqual(reserved_snapshot["stock_meta_by_type"][34]["reserved_quantity"], 3)
        self.assertEqual(reserved_snapshot["stock_meta_by_type"][34]["available_quantity"], 7)
        self.assertEqual(reserved_snapshot["stock_rows"][0]["available_quantity"], 4)
        self.assertEqual(reserved_snapshot["stock_rows"][0]["reserved_quantity"], 0)
        self.assertEqual(reserved_snapshot["stock_rows"][2]["available_quantity"], 3)
        self.assertEqual(reserved_snapshot["stock_rows"][2]["reserved_quantity"], 3)
        self.assertEqual(reserved_snapshot["stock_rows"][0]["form_quantity_field_name"], "qty_34_std_root_0")
        self.assertEqual(reserved_snapshot["stock_rows"][2]["form_quantity_field_name"], "qty_34_std_incan_2")
        self.assertIn(0, reserved_snapshot["stock_row_by_index"])
        self.assertIn(2, reserved_snapshot["stock_row_by_index"])

    def test_get_buy_stock_snapshot_for_submission_reuses_cached_snapshot(self):
        static_snapshot = {
            "stock_rows": [
                {
                    "row_kind": "item",
                    "row_index": 0,
                    "type_id": 34,
                    "quantity": 10,
                    "blueprint_variant": "",
                    "container_path": "",
                }
            ],
            "stock_meta_by_type": {
                34: {
                    "type_id": 34,
                    "quantity": 10,
                    "reserved_quantity": 0,
                    "available_quantity": 10,
                }
            },
            "pre_filter_stock_count": 1,
            "post_group_filter_count": 1,
        }
        cache.set(_get_buy_browse_snapshot_cache_key(self.config), static_snapshot, 60)

        with patch("indy_hub.views.material_exchange.rebuild_material_exchange_buy_browse_snapshot_cache") as mock_rebuild:
            snapshot = _get_buy_stock_snapshot_for_submission(
                config=self.config,
                submitted_type_ids={34},
                reserved_quantities={34: 3},
            )

        mock_rebuild.assert_not_called()
        self.assertEqual(snapshot["stock_meta_by_type"][34]["reserved_quantity"], 3)
        self.assertEqual(snapshot["stock_meta_by_type"][34]["available_quantity"], 7)
        self.assertEqual(snapshot["stock_rows"][0]["available_quantity"], 7)
        self.assertEqual(snapshot["stock_rows"][0]["form_quantity_field_name"], "qty_34_std_root_0")
        self.assertIn(0, snapshot["stock_row_by_index"])

    def test_get_buy_stock_snapshot_for_submission_uses_submission_snapshot_token(self):
        static_snapshot = {
            "stock_rows": [
                {
                    "row_kind": "item",
                    "row_index": 0,
                    "type_id": 34,
                    "quantity": 10,
                    "available_quantity": 10,
                    "blueprint_variant": "",
                    "container_path": "",
                }
            ],
            "stock_meta_by_type": {
                34: {
                    "type_id": 34,
                    "quantity": 10,
                    "reserved_quantity": 0,
                    "available_quantity": 10,
                }
            },
            "pre_filter_stock_count": 1,
            "post_group_filter_count": 1,
        }
        viewer = User.objects.create_user("buy_snapshot_viewer")
        token = _store_buy_submission_snapshot(
            config=self.config,
            user_id=viewer.id,
            buy_stock_snapshot_static=static_snapshot,
        )

        cache.delete(_get_buy_browse_snapshot_cache_key(self.config))

        with patch("indy_hub.views.material_exchange.rebuild_material_exchange_buy_browse_snapshot_cache") as mock_rebuild:
            snapshot = _get_buy_stock_snapshot_for_submission(
                config=self.config,
                submitted_type_ids={34},
                reserved_quantities={34: 4},
                submission_snapshot_token=token,
                user_id=viewer.id,
            )

        mock_rebuild.assert_not_called()
        self.assertEqual(snapshot["stock_meta_by_type"][34]["reserved_quantity"], 4)
        self.assertEqual(snapshot["stock_meta_by_type"][34]["available_quantity"], 6)
        self.assertEqual(snapshot["stock_rows"][0]["available_quantity"], 6)
        self.assertEqual(
            cache.get(
                _get_buy_submission_snapshot_cache_key(
                    config=self.config,
                    user_id=viewer.id,
                    token=token,
                )
            ),
            static_snapshot,
        )


class MaterialExchangeBuyLocationCompatibilityTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user("bp_owner", password="secret123")
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=1001,
            structure_name="Primary Structure",
            buy_structure_ids=[1001],
            buy_structure_names=["Structure Alpha"],
            buy_enabled=True,
            hangar_division=1,
            is_active=True,
        )

    def test_selected_buy_stock_items_share_source_location_when_overlapping(self):
        selected_rows = [
            SimpleNamespace(source_structure_ids=[1001, 1002]),
            SimpleNamespace(source_structure_ids=[1002]),
        ]

        self.assertTrue(_selected_buy_stock_items_share_source_location(selected_rows))

    def test_selected_buy_stock_items_share_source_location_when_disjoint(self):
        selected_rows = [
            SimpleNamespace(source_structure_ids=[1001]),
            SimpleNamespace(source_structure_ids=[1002]),
        ]

        self.assertFalse(_selected_buy_stock_items_share_source_location(selected_rows))

    @patch("indy_hub.views.material_exchange.get_corp_assets_cached")
    def test_get_buy_location_scoped_corp_assets_does_not_force_refresh_by_default(
        self,
        mock_get_corp_assets_cached,
    ):
        mock_get_corp_assets_cached.return_value = ([], False)

        scoped_assets = _get_buy_location_scoped_corp_assets(config=self.config)

        self.assertEqual(scoped_assets, [])
        mock_get_corp_assets_cached.assert_called_once_with(
            int(self.config.corporation_id),
            allow_refresh=False,
        )

    def test_get_buy_location_scoped_corp_assets_resolves_nested_container_once(self):
        self.config.buy_structure_ids = [1001]
        self.config.buy_structure_names = ["Structure Alpha"]
        self.config.hangar_division = 1
        self.config.save(update_fields=["buy_structure_ids", "buy_structure_names", "hangar_division"])

        corp_assets = [
            {
                "item_id": 5001,
                "location_id": 1001,
                "location_flag": "OfficeFolder",
                "type_id": 27,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            {
                "item_id": 5002,
                "location_id": 5001,
                "location_flag": "CorpSAG1",
                "type_id": 23,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            },
            {
                "item_id": 5003,
                "location_id": 5002,
                "location_flag": "Unlocked",
                "type_id": 34,
                "quantity": 44,
                "is_singleton": False,
                "is_blueprint": False,
            },
        ]

        scoped_assets = _get_buy_location_scoped_corp_assets(
            config=self.config,
            corp_assets=corp_assets,
        )

        scoped_by_item_id = {int(asset["item_id"]): asset for asset in scoped_assets}
        self.assertEqual(scoped_by_item_id[5002]["source_structure_ids"], [1001])
        self.assertEqual(scoped_by_item_id[5003]["source_structure_ids"], [1001])

    @patch("indy_hub.views.material_exchange.get_corp_assets_cached")
    def test_buy_stock_blueprint_variant_map_detects_bpc(self, mock_get_corp_assets_cached):
        self.config.buy_structure_ids = [1001]
        self.config.hangar_division = 1
        self.config.save(update_fields=["buy_structure_ids", "hangar_division"])

        mock_get_corp_assets_cached.return_value = (
            [
                {
                    "item_id": 3001,
                    "location_id": 1001,
                    "location_flag": "CorpSAG1",
                    "type_id": 77777,
                    "quantity": -2,
                    "is_singleton": True,
                    "is_blueprint": True,
                }
            ],
            False,
        )

        variants = _get_buy_stock_blueprint_variant_map(
            config=self.config,
            type_ids={77777},
        )

        self.assertEqual(variants.get(77777), "bpc")

    def test_buy_stock_blueprint_variant_map_from_scoped_assets_reuses_prefetched_details(self):
        scoped_assets = [
            {
                "item_id": 3001,
                "location_id": 1001,
                "location_flag": "CorpSAG1",
                "type_id": 77777,
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
            }
        ]

        variants = _get_buy_stock_blueprint_variant_map_from_scoped_assets(
            scoped_assets=scoped_assets,
            type_ids={77777},
            blueprint_details_by_item_id={
                3001: {"variant": "bpc", "runs": 5},
            },
        )

        self.assertEqual(variants.get(77777), "bpc")

    @patch("indy_hub.views.material_exchange.get_corp_assets_cached")
    def test_buy_stock_blueprint_variant_map_prefers_corp_blueprint_copy_records(self, mock_get_corp_assets_cached):
        self.config.buy_structure_ids = [1001]
        self.config.hangar_division = 1
        self.config.save(update_fields=["buy_structure_ids", "hangar_division"])

        item_id = 4001
        Blueprint.objects.create(
            owner_user=self.user,
            owner_kind=Blueprint.OwnerKind.CORPORATION,
            corporation_id=self.config.corporation_id,
            corporation_name="Test Corp",
            item_id=item_id,
            blueprint_id=item_id,
            type_id=77777,
            location_id=1001,
            location_name="Structure Alpha",
            location_flag="CorpSAG1",
            quantity=1,
            bp_type=Blueprint.BPType.COPY,
            type_name="Capital Armor Plates Blueprint",
        )

        mock_get_corp_assets_cached.return_value = (
            [
                {
                    "item_id": item_id,
                    "location_id": 1001,
                    "location_flag": "CorpSAG1",
                    "type_id": 77777,
                    "quantity": 1,
                    "is_singleton": True,
                    "is_blueprint": False,
                }
            ],
            False,
        )

        variants = _get_buy_stock_blueprint_variant_map(
            config=self.config,
            type_ids={77777},
        )

        self.assertEqual(variants.get(77777), "bpc")

    @patch("indy_hub.views.material_exchange.get_corp_assets_cached")
    def test_buy_stock_blueprint_variant_map_uses_quantity_signal_when_bp_type_stale(
        self, mock_get_corp_assets_cached
    ):
        self.config.buy_structure_ids = [1001]
        self.config.hangar_division = 1
        self.config.save(update_fields=["buy_structure_ids", "hangar_division"])

        item_id = 4002
        Blueprint.objects.create(
            owner_user=self.user,
            owner_kind=Blueprint.OwnerKind.CORPORATION,
            corporation_id=self.config.corporation_id,
            corporation_name="Test Corp",
            item_id=item_id,
            blueprint_id=item_id,
            type_id=77778,
            location_id=1001,
            location_name="Structure Alpha",
            location_flag="CorpSAG1",
            quantity=-2,
            runs=0,
            # Simulate stale/inaccurate type persisted in DB.
            bp_type=Blueprint.BPType.ORIGINAL,
            type_name="Capital Construction Parts Blueprint",
        )

        mock_get_corp_assets_cached.return_value = (
            [
                {
                    "item_id": item_id,
                    "location_id": 1001,
                    "location_flag": "CorpSAG1",
                    "type_id": 77778,
                    "quantity": -2,
                    "is_singleton": True,
                    "is_blueprint": False,
                }
            ],
            False,
        )

        variants = _get_buy_stock_blueprint_variant_map(
            config=self.config,
            type_ids={77778},
        )

        self.assertEqual(variants.get(77778), "bpc")

    def test_get_corp_blueprint_details_backfills_bpc_runs_from_quantity(self):
        item_id = 4010
        Blueprint.objects.create(
            owner_user=self.user,
            owner_kind=Blueprint.OwnerKind.CORPORATION,
            corporation_id=self.config.corporation_id,
            corporation_name="Test Corp",
            item_id=item_id,
            blueprint_id=item_id,
            type_id=77779,
            location_id=1001,
            location_name="Structure Alpha",
            location_flag="CorpSAG1",
            quantity=10,
            runs=0,
            bp_type=Blueprint.BPType.COPY,
            type_name="Capital Construction Parts Blueprint",
        )

        details = _get_corp_blueprint_details_by_item_id(
            config=self.config,
            item_ids={item_id},
        )

        self.assertEqual(details[item_id]["variant"], "bpc")
        self.assertEqual(details[item_id]["runs"], 10)

    @patch("indy_hub.views.material_exchange.get_corp_assets_cached")
    def test_buy_stock_variant_map_uses_legacy_corp_row_even_if_owner_kind_stale(self, mock_get_corp_assets_cached):
        self.config.buy_structure_ids = [1001]
        self.config.hangar_division = 1
        self.config.save(update_fields=["buy_structure_ids", "hangar_division"])

        item_id = 4011
        Blueprint.objects.create(
            owner_user=self.user,
            # Legacy/stale row: owner_kind not set to corporation yet.
            owner_kind=Blueprint.OwnerKind.CHARACTER,
            corporation_id=self.config.corporation_id,
            corporation_name="Test Corp",
            item_id=item_id,
            blueprint_id=item_id,
            type_id=77780,
            location_id=1001,
            location_name="Structure Alpha",
            location_flag="CorpSAG1",
            quantity=-2,
            runs=10,
            bp_type=Blueprint.BPType.ORIGINAL,
            type_name="Capital Construction Parts Blueprint",
        )

        mock_get_corp_assets_cached.return_value = (
            [
                {
                    "item_id": item_id,
                    "location_id": 1001,
                    "location_flag": "CorpSAG1",
                    "type_id": 77780,
                    "quantity": -1,
                    "is_singleton": True,
                    "is_blueprint": False,
                }
            ],
            False,
        )

        variants = _get_buy_stock_blueprint_variant_map(
            config=self.config,
            type_ids={77780},
        )

        self.assertEqual(variants.get(77780), "bpc")

    @patch("indy_hub.views.material_exchange.get_corp_assets_cached")
    def test_buy_stock_blueprint_variant_map_detects_mixed(self, mock_get_corp_assets_cached):
        self.config.buy_structure_ids = [1001]
        self.config.hangar_division = 1
        self.config.save(update_fields=["buy_structure_ids", "hangar_division"])

        mock_get_corp_assets_cached.return_value = (
            [
                {
                    "item_id": 3001,
                    "location_id": 1001,
                    "location_flag": "CorpSAG1",
                    "type_id": 77777,
                    "quantity": -2,
                    "is_singleton": True,
                    "is_blueprint": True,
                },
                {
                    "item_id": 3002,
                    "location_id": 1001,
                    "location_flag": "CorpSAG1",
                    "type_id": 77777,
                    "quantity": -1,
                    "is_singleton": True,
                    "is_blueprint": True,
                },
            ],
            False,
        )

        variants = _get_buy_stock_blueprint_variant_map(
            config=self.config,
            type_ids={77777},
        )

        self.assertEqual(variants.get(77777), "mixed")

    @patch("indy_hub.views.material_exchange.get_corp_assets_cached")
    def test_buy_stock_blueprint_variant_map_uses_negative_quantity_when_flag_missing(
        self, mock_get_corp_assets_cached
    ):
        self.config.buy_structure_ids = [1001]
        self.config.hangar_division = 1
        self.config.save(update_fields=["buy_structure_ids", "hangar_division"])

        mock_get_corp_assets_cached.return_value = (
            [
                {
                    "item_id": 3001,
                    "location_id": 1001,
                    "location_flag": "CorpSAG1",
                    "type_id": 77777,
                    "quantity": -2,
                    "is_singleton": True,
                    "is_blueprint": False,
                }
            ],
            False,
        )

        variants = _get_buy_stock_blueprint_variant_map(
            config=self.config,
            type_ids={77777},
        )

        self.assertEqual(variants.get(77777), "bpc")

    def test_build_buy_material_rows_nests_container_children(self):
        container_item_id = 5001
        child_item_id = 5002
        scoped_assets = [
            {
                "item_id": container_item_id,
                "location_id": 1001,
                "location_flag": "CorpSAG1",
                "type_id": 23,
                "set_name": "Named Buy Container",
                "quantity": 1,
                "is_singleton": True,
                "is_blueprint": False,
                "source_structure_ids": [1001],
            },
            {
                "item_id": child_item_id,
                "location_id": container_item_id,
                "location_flag": "Unlocked",
                "type_id": 34,
                "quantity": 25,
                "is_singleton": False,
                "is_blueprint": False,
                "source_structure_ids": [1001],
            },
        ]
        stock_meta_by_type = {
            34: {
                "type_id": 34,
                "base_type_name": "Tritanium",
                "display_type_name": "Tritanium",
                "blueprint_variant": "",
                "display_sell_price_to_member": 1,
                "default_sell_price_to_member": 1,
                "has_buy_price_override": False,
                "quantity": 25,
                "reserved_quantity": 0,
                "available_quantity": 25,
                "source_structure_ids": [1001],
                "buy_location_label": "Structure Alpha",
            }
        }

        rows = _build_buy_material_rows(
            scoped_assets=scoped_assets,
            config=self.config,
            stock_meta_by_type=stock_meta_by_type,
            buy_override_map={},
            buy_market_group_override_map={},
            buy_name_map={1001: "Structure Alpha"},
            fallback_location_label="Structure Alpha",
            blueprint_variant_by_item_id={child_item_id: "bpc"},
            blueprint_runs_by_item_id={child_item_id: 7},
        )

        self.assertEqual(rows[0]["row_kind"], "container")
        self.assertEqual(rows[0]["container_name"], "Named Buy Container")
        item_rows = [row for row in rows if row.get("row_kind") == "item"]
        self.assertEqual(len(item_rows), 1)
        self.assertTrue(item_rows[0]["container_path"])
        self.assertEqual(item_rows[0]["blueprint_variant"], "bpc")
        self.assertEqual(item_rows[0]["bpc_runs"], 7)
        self.assertEqual(item_rows[0]["display_sell_price_to_member"], 0)

    def test_build_buy_material_rows_uses_profile_specific_price_for_matching_source_profile(self):
        MaterialExchangeItemPriceOverride.objects.create(
            config=self.config,
            type_id=34,
            type_name="Tritanium",
            buy_price_override=Decimal("6.10"),
        )
        self.config.buy_structure_ids = [1001, 1002]
        self.config.buy_structure_names = ["Structure Alpha", "Structure Beta"]
        self.config.allowed_market_groups_buy_by_structure = {
            "1001": [500],
            "1002": [600],
        }
        self.config.buy_market_group_profiles = [
            {"name": "Alpha Desk", "market_group_ids": [500]},
            {"name": "Beta Desk", "market_group_ids": [600]},
        ]
        self.config.profile_item_price_overrides = [
            {
                "type_id": 34,
                "type_name": "Tritanium",
                "buy_profile_name": "Beta Desk",
                "buy_price_override": "7.25",
            }
        ]
        self.config.save(
            update_fields=[
                "buy_structure_ids",
                "buy_structure_names",
                "allowed_market_groups_buy_by_structure",
                "buy_market_group_profiles",
                "profile_item_price_overrides",
            ]
        )

        scoped_assets = [
            {
                "item_id": 7001,
                "location_id": 1001,
                "location_flag": "CorpSAG1",
                "type_id": 34,
                "quantity": 10,
                "is_singleton": False,
                "is_blueprint": False,
                "source_structure_ids": [1001],
            },
            {
                "item_id": 7002,
                "location_id": 1002,
                "location_flag": "CorpSAG1",
                "type_id": 34,
                "quantity": 5,
                "is_singleton": False,
                "is_blueprint": False,
                "source_structure_ids": [1002],
            },
        ]
        stock_meta_by_type = {
            34: {
                "type_id": 34,
                "base_type_name": "Tritanium",
                "display_type_name": "Tritanium",
                "blueprint_variant": "",
                "display_sell_price_to_member": 6.10,
                "default_sell_price_to_member": 5.50,
                "has_buy_price_override": True,
                "jita_buy_price": 5,
                "jita_sell_price": 6,
                "quantity": 15,
                "reserved_quantity": 0,
                "available_quantity": 15,
                "source_structure_ids": [1001, 1002],
                "buy_location_label": "Structure Alpha, Structure Beta",
            }
        }

        rows = _build_buy_material_rows(
            scoped_assets=scoped_assets,
            config=self.config,
            stock_meta_by_type=stock_meta_by_type,
            buy_override_map={},
            buy_market_group_override_map={},
            buy_name_map={1001: "Structure Alpha", 1002: "Structure Beta"},
            fallback_location_label="Structure Alpha, Structure Beta",
        )

        item_rows_by_source = {
            tuple(row["source_structure_ids"]): row
            for row in rows
            if row.get("row_kind") == "item"
        }
        self.assertEqual(
            item_rows_by_source[(1001,)]["display_sell_price_to_member"],
            Decimal("6.10"),
        )
        self.assertEqual(
            item_rows_by_source[(1002,)]["display_sell_price_to_member"],
            Decimal("7.25"),
        )
        self.assertTrue(item_rows_by_source[(1001,)]["has_buy_price_override"])
        self.assertTrue(item_rows_by_source[(1002,)]["has_buy_price_override"])
