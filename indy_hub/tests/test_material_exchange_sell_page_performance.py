# Standard Library
from datetime import timedelta
from decimal import Decimal
from unittest.mock import Mock, patch

# Django
from django.contrib.auth.models import User
from django.core.cache import cache
from django.http import HttpResponse
from django.test import RequestFactory, TestCase
from django.utils import timezone

# AA Example App
from indy_hub.models import MaterialExchangeConfig
from indy_hub.views.material_exchange import (
    _fetch_fuzzwork_prices,
    material_exchange_sell,
)


class FuzzworkPriceCacheTests(TestCase):
    """`_fetch_fuzzwork_prices` must not hit the Fuzzwork API for cached types."""

    def setUp(self):
        cache.clear()

    def test_second_lookup_for_same_types_does_not_refetch(self):
        with patch("indy_hub.services.fuzzwork.fetch_fuzzwork_prices") as mock_fetch:
            mock_fetch.return_value = {
                34: {"buy": Decimal("5"), "sell": Decimal("6")},
                35: {"buy": Decimal("7"), "sell": Decimal("8")},
            }

            first = _fetch_fuzzwork_prices([34, 35])
            self.assertEqual(mock_fetch.call_count, 1)

            second = _fetch_fuzzwork_prices([34, 35])
            self.assertEqual(mock_fetch.call_count, 1)

        self.assertEqual(second, first)
        self.assertEqual(second[34]["buy"], Decimal("5"))
        self.assertEqual(second[35]["sell"], Decimal("8"))

    def test_only_uncached_types_are_fetched(self):
        with patch("indy_hub.services.fuzzwork.fetch_fuzzwork_prices") as mock_fetch:
            mock_fetch.return_value = {34: {"buy": Decimal("5"), "sell": Decimal("6")}}
            _fetch_fuzzwork_prices([34])

            mock_fetch.return_value = {36: {"buy": Decimal("9"), "sell": Decimal("10")}}
            prices = _fetch_fuzzwork_prices([34, 36])

            self.assertEqual(mock_fetch.call_count, 2)
            self.assertEqual(mock_fetch.call_args.args[0], [36])

        self.assertEqual(prices[34]["buy"], Decimal("5"))
        self.assertEqual(prices[36]["buy"], Decimal("9"))

    def test_unpriced_types_are_remembered_without_refetching(self):
        with patch("indy_hub.services.fuzzwork.fetch_fuzzwork_prices") as mock_fetch:
            mock_fetch.return_value = {34: {"buy": Decimal("0"), "sell": Decimal("0")}}

            first = _fetch_fuzzwork_prices([34])
            second = _fetch_fuzzwork_prices([34])

            self.assertEqual(mock_fetch.call_count, 1)

        self.assertEqual(first, {})
        self.assertEqual(second, {})

    def test_failed_batch_is_not_cached_as_unpriced(self):
        # Local
        from indy_hub.services.fuzzwork import FuzzworkError

        with patch("indy_hub.services.fuzzwork.fetch_fuzzwork_prices") as mock_fetch:
            mock_fetch.side_effect = FuzzworkError("boom")
            self.assertEqual(_fetch_fuzzwork_prices([34]), {})

        with patch("indy_hub.services.fuzzwork.fetch_fuzzwork_prices") as mock_fetch:
            mock_fetch.return_value = {34: {"buy": Decimal("5"), "sell": Decimal("6")}}
            prices = _fetch_fuzzwork_prices([34])
            self.assertEqual(mock_fetch.call_count, 1)

        self.assertEqual(prices[34]["buy"], Decimal("5"))


class SellPageStockSyncDispatchTests(TestCase):
    """The sell page must never run the corp stock sync inline."""

    def setUp(self):
        cache.clear()
        self.user = User.objects.create_user("seller_perf", password="secret123")
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=60003760,
            structure_name="Test Structure",
            sell_structure_ids=[60003760],
            sell_structure_names=["Test Structure"],
            is_active=True,
        )
        self.factory = RequestFactory()

    def _call_sell_view(self):
        request = self.factory.get("/material-exchange/sell/")
        request.user = self.user
        sell_view = material_exchange_sell.__wrapped__.__wrapped__.__wrapped__
        return sell_view(request, tokens=None)

    @patch("indy_hub.views.material_exchange.messages")
    @patch("indy_hub.views.material_exchange.build_nav_context", return_value={})
    @patch("indy_hub.views.material_exchange._build_nav_context", return_value={})
    @patch("indy_hub.views.material_exchange._get_corp_name_for_hub", return_value="Test Corp")
    @patch("indy_hub.views.material_exchange.get_user_assets_cached", return_value=([], False))
    @patch("indy_hub.views.material_exchange.sync_material_exchange_stock")
    @patch("indy_hub.views.material_exchange._get_material_exchange_config")
    @patch("indy_hub.views.material_exchange._is_material_exchange_enabled", return_value=True)
    @patch("indy_hub.views.material_exchange.render")
    def test_stale_stock_dispatches_sync_task_instead_of_running_it(
        self,
        mock_render,
        _mock_enabled,
        mock_get_config,
        mock_sync_stock,
        _mock_get_user_assets_cached,
        _mock_corp_name,
        _mock_build_nav,
        _mock_build_main_nav,
        _mock_messages,
    ) -> None:
        self.config.last_stock_sync = timezone.now() - timedelta(hours=5)
        self.config.save(update_fields=["last_stock_sync"])
        mock_get_config.return_value = self.config
        mock_render.return_value = HttpResponse("ok")
        mock_sync_stock.delay = Mock()

        response = self._call_sell_view()

        self.assertEqual(response.status_code, 200)
        mock_sync_stock.assert_not_called()
        self.assertEqual(mock_sync_stock.delay.call_count, 1)

    @patch("indy_hub.views.material_exchange.messages")
    @patch("indy_hub.views.material_exchange.build_nav_context", return_value={})
    @patch("indy_hub.views.material_exchange._build_nav_context", return_value={})
    @patch("indy_hub.views.material_exchange._get_corp_name_for_hub", return_value="Test Corp")
    @patch("indy_hub.views.material_exchange.get_user_assets_cached", return_value=([], False))
    @patch("indy_hub.views.material_exchange.sync_material_exchange_stock")
    @patch("indy_hub.views.material_exchange._get_material_exchange_config")
    @patch("indy_hub.views.material_exchange._is_material_exchange_enabled", return_value=True)
    @patch("indy_hub.views.material_exchange.render")
    def test_repeat_views_do_not_queue_duplicate_sync_tasks(
        self,
        mock_render,
        _mock_enabled,
        mock_get_config,
        mock_sync_stock,
        _mock_get_user_assets_cached,
        _mock_corp_name,
        _mock_build_nav,
        _mock_build_main_nav,
        _mock_messages,
    ) -> None:
        self.config.last_stock_sync = timezone.now() - timedelta(hours=5)
        self.config.save(update_fields=["last_stock_sync"])
        mock_get_config.return_value = self.config
        mock_render.return_value = HttpResponse("ok")
        mock_sync_stock.delay = Mock()

        self._call_sell_view()
        self._call_sell_view()

        self.assertEqual(mock_sync_stock.delay.call_count, 1)

    @patch("indy_hub.views.material_exchange.messages")
    @patch("indy_hub.views.material_exchange.build_nav_context", return_value={})
    @patch("indy_hub.views.material_exchange._build_nav_context", return_value={})
    @patch("indy_hub.views.material_exchange._get_corp_name_for_hub", return_value="Test Corp")
    @patch("indy_hub.views.material_exchange.get_user_assets_cached", return_value=([], False))
    @patch("indy_hub.views.material_exchange.sync_material_exchange_stock")
    @patch("indy_hub.views.material_exchange._get_material_exchange_config")
    @patch("indy_hub.views.material_exchange._is_material_exchange_enabled", return_value=True)
    @patch("indy_hub.views.material_exchange.render")
    def test_fresh_stock_does_not_dispatch_sync_task(
        self,
        mock_render,
        _mock_enabled,
        mock_get_config,
        mock_sync_stock,
        _mock_get_user_assets_cached,
        _mock_corp_name,
        _mock_build_nav,
        _mock_build_main_nav,
        _mock_messages,
    ) -> None:
        self.config.last_stock_sync = timezone.now()
        self.config.save(update_fields=["last_stock_sync"])
        mock_get_config.return_value = self.config
        mock_render.return_value = HttpResponse("ok")
        mock_sync_stock.delay = Mock()

        self._call_sell_view()

        mock_sync_stock.assert_not_called()
        mock_sync_stock.delay.assert_not_called()


class SellPageAssetReadTests(TestCase):
    """The sell page must materialize the user's cached assets only once per request."""

    def setUp(self):
        cache.clear()
        self.user = User.objects.create_user("seller_reads", password="secret123")
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=60003760,
            structure_name="Test Structure",
            sell_structure_ids=[60003760],
            sell_structure_names=["Test Structure"],
            allow_fitted_ships=False,
            is_active=True,
        )
        self.structure_id = 60003760
        self.factory = RequestFactory()

    @patch("indy_hub.views.material_exchange.build_nav_context", return_value={})
    @patch("indy_hub.views.material_exchange._build_nav_context", return_value={})
    @patch("indy_hub.views.material_exchange._get_corp_name_for_hub", return_value="Test Corp")
    @patch("indy_hub.views.material_exchange._get_allowed_type_ids_for_config", return_value=None)
    @patch("indy_hub.views.material_exchange._fetch_fuzzwork_prices")
    @patch("indy_hub.views.material_exchange.get_user_assets_cached")
    @patch("indy_hub.views.material_exchange._get_reserved_sell_quantities", return_value={})
    @patch(
        "indy_hub.views.material_exchange._get_item_price_override_maps",
        return_value=({}, {}),
    )
    @patch("indy_hub.views.material_exchange._get_material_exchange_config")
    @patch("indy_hub.views.material_exchange._is_material_exchange_enabled", return_value=True)
    @patch("indy_hub.views.material_exchange.render")
    def test_get_reads_cached_assets_once(
        self,
        mock_render,
        _mock_enabled,
        mock_get_config,
        _mock_override_maps,
        _mock_reserved_sell,
        mock_get_user_assets_cached,
        mock_fetch_prices,
        _mock_allowed_type_ids,
        _mock_corp_name,
        _mock_build_nav,
        _mock_build_main_nav,
    ) -> None:
        self.config.last_stock_sync = timezone.now()
        self.config.save(update_fields=["last_stock_sync"])
        mock_get_config.return_value = self.config
        mock_render.return_value = HttpResponse("ok")
        mock_fetch_prices.return_value = {34: {"buy": Decimal("10"), "sell": Decimal("11")}}
        mock_get_user_assets_cached.return_value = (
            [
                {
                    "character_id": 9001,
                    "item_id": 101,
                    "raw_location_id": self.structure_id,
                    "location_id": self.structure_id,
                    "type_id": 34,
                    "quantity": 5,
                    "is_singleton": False,
                }
            ],
            False,
        )

        request = self.factory.get("/material-exchange/sell/")
        request.user = self.user
        sell_view = material_exchange_sell.__wrapped__.__wrapped__.__wrapped__
        response = sell_view(request, tokens=None)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_get_user_assets_cached.call_count, 1)
        # The rows still render from that single read.
        self.assertTrue(mock_render.call_args.args[2]["materials"])
