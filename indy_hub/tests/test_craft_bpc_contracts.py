# Standard Library
import json
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

# Django
from django.contrib.auth.models import Permission, User
from django.core.cache import cache
from django.test import RequestFactory, SimpleTestCase, TestCase
from django.utils import timezone

# AA Example App
from indy_hub.models import PublicJitaContract, PublicJitaContractItem
from indy_hub.services import public_contracts_store
from indy_hub.services.public_contracts_store import (
    REFRESH_QUEUED_KEY,
    SYNC_FAILURE_KEY,
    SYNC_META_KEY,
    get_public_jita_bpc_offers,
    get_public_jita_contract_cache_meta,
    request_public_jita_contract_refresh,
    sync_public_jita_contract_cache,
)
from indy_hub.views.api import craft_bpc_contracts

CRAFT_JS = (
    Path(__file__).resolve().parents[1] / "static" / "indy_hub" / "js" / "craft_bp.js"
)
CRAFT_TEMPLATE = (
    Path(__file__).resolve().parents[1]
    / "templates"
    / "indy_hub"
    / "industry"
    / "Craft_BP_v2.html"
)

BLUEPRINT_TYPE_ID = 81100


def _make_contract(
    contract_id: int, *, expires_in: timedelta, issued_ago=timedelta(hours=2)
):
    now = timezone.now()
    contract = PublicJitaContract.objects.create(
        contract_id=contract_id,
        contract_type="item_exchange",
        status="outstanding",
        is_jita=True,
        is_active=True,
        price=1_000_000,
        date_issued=now - issued_ago,
        date_expired=now + expires_in,
    )
    PublicJitaContractItem.objects.create(
        contract=contract,
        record_id=contract_id * 10,
        type_id=BLUEPRINT_TYPE_ID,
        quantity=1,
        runs=10,
        is_included=True,
        is_blueprint_copy=True,
        material_efficiency=10,
        time_efficiency=20,
    )
    return contract


class PublicContractCacheMetaTests(TestCase):
    def setUp(self) -> None:
        cache.delete(SYNC_META_KEY)
        cache.delete(SYNC_FAILURE_KEY)
        cache.delete(REFRESH_QUEUED_KEY)

    def test_meta_reports_last_sync_and_age(self) -> None:
        synced_at = timezone.now() - timedelta(minutes=10)
        cache.set(SYNC_META_KEY, {"ok": True, "synced_at": synced_at.isoformat()})

        meta = get_public_jita_contract_cache_meta()

        self.assertEqual(meta["last_synced"], synced_at.isoformat())
        self.assertTrue(meta["is_cached"])
        self.assertGreaterEqual(meta["cache_age_seconds"], 590)
        self.assertFalse(meta["refresh_failed"])

    def test_stale_meta_is_not_reported_as_cached(self) -> None:
        synced_at = timezone.now() - timedelta(hours=3)
        cache.set(SYNC_META_KEY, {"ok": True, "synced_at": synced_at.isoformat()})

        meta = get_public_jita_contract_cache_meta()

        self.assertFalse(meta["is_cached"])
        self.assertEqual(meta["last_synced"], synced_at.isoformat())

    def test_failure_after_last_sync_is_surfaced_without_erasing_it(self) -> None:
        synced_at = timezone.now() - timedelta(hours=2)
        cache.set(SYNC_META_KEY, {"ok": True, "synced_at": synced_at.isoformat()})
        cache.set(
            SYNC_FAILURE_KEY,
            {"failed_at": timezone.now().isoformat(), "error": "HTTPError"},
        )

        meta = get_public_jita_contract_cache_meta()

        self.assertTrue(meta["refresh_failed"])
        self.assertEqual(meta["last_error"], "HTTPError")
        self.assertEqual(meta["last_synced"], synced_at.isoformat())

    def test_failure_older_than_last_sync_is_ignored(self) -> None:
        cache.set(
            SYNC_FAILURE_KEY,
            {
                "failed_at": (timezone.now() - timedelta(hours=5)).isoformat(),
                "error": "HTTPError",
            },
        )
        cache.set(SYNC_META_KEY, {"ok": True, "synced_at": timezone.now().isoformat()})

        self.assertFalse(get_public_jita_contract_cache_meta()["refresh_failed"])

    def test_sync_failure_records_error_class_only(self) -> None:
        with patch.object(
            public_contracts_store,
            "_collect_candidate_contracts",
            side_effect=RuntimeError("token=secret upstream exploded"),
        ):
            with self.assertRaises(RuntimeError):
                sync_public_jita_contract_cache()

        failure = cache.get(SYNC_FAILURE_KEY)
        self.assertEqual(failure["error"], "RuntimeError")
        self.assertNotIn("secret", json.dumps(failure))

    def test_expired_contracts_are_not_offered(self) -> None:
        _make_contract(5001, expires_in=timedelta(days=2))
        _make_contract(5002, expires_in=-timedelta(minutes=1))

        offers = get_public_jita_bpc_offers(blueprint_type_id=BLUEPRINT_TYPE_ID)

        self.assertEqual([offer["contract_id"] for offer in offers], [5001])
        self.assertTrue(offers[0]["issued_at"])
        self.assertTrue(offers[0]["expires_at"])


class RequestPublicContractRefreshTests(TestCase):
    def setUp(self) -> None:
        cache.delete(SYNC_META_KEY)
        cache.delete(SYNC_FAILURE_KEY)
        cache.delete(REFRESH_QUEUED_KEY)

    @patch("indy_hub.tasks.public_contracts.sync_public_jita_contracts.delay")
    def test_fresh_cache_does_not_queue(self, mock_delay) -> None:
        cache.set(SYNC_META_KEY, {"ok": True, "synced_at": timezone.now().isoformat()})

        self.assertFalse(request_public_jita_contract_refresh())
        mock_delay.assert_not_called()

    @patch("indy_hub.tasks.public_contracts.sync_public_jita_contracts.delay")
    def test_stale_cache_queues_once(self, mock_delay) -> None:
        cache.set(
            SYNC_META_KEY,
            {
                "ok": True,
                "synced_at": (timezone.now() - timedelta(hours=3)).isoformat(),
            },
        )

        self.assertTrue(request_public_jita_contract_refresh())
        self.assertFalse(request_public_jita_contract_refresh())
        mock_delay.assert_called_once_with()


class CraftBpcContractsEndpointTests(TestCase):
    def setUp(self) -> None:
        cache.delete(SYNC_META_KEY)
        cache.delete(SYNC_FAILURE_KEY)
        cache.delete(REFRESH_QUEUED_KEY)
        self.factory = RequestFactory()
        self.user = User.objects.create_user("bpc-contracts", password="secret123")
        self.user.user_permissions.add(
            Permission.objects.get(codename="can_access_indy_hub")
        )

    def _get(self, **params):
        view = craft_bpc_contracts
        while hasattr(view, "__wrapped__"):
            view = view.__wrapped__
        request = self.factory.get("/api/craft-bpc-contracts/", data=params)
        request.user = self.user
        return json.loads(view(request).content)

    def test_payload_includes_contract_and_cache_timestamps(self) -> None:
        _make_contract(6001, expires_in=timedelta(days=1))
        synced_at = timezone.now() - timedelta(minutes=5)
        cache.set(SYNC_META_KEY, {"ok": True, "synced_at": synced_at.isoformat()})

        payload = self._get(blueprint_type_ids=str(BLUEPRINT_TYPE_ID))

        offer = payload["contracts_by_blueprint"][str(BLUEPRINT_TYPE_ID)][0]
        self.assertTrue(offer["issued_at"])
        self.assertTrue(offer["expires_at"])
        self.assertEqual(payload["last_synced"], synced_at.isoformat())
        self.assertIsNotNone(payload["cache_age_seconds"])
        self.assertTrue(payload["is_cached"])
        self.assertFalse(payload["refresh_failed"])
        self.assertFalse(payload["refresh_queued"])

    def test_stale_failed_cache_is_not_reported_as_current(self) -> None:
        cache.set(
            SYNC_META_KEY,
            {
                "ok": True,
                "synced_at": (timezone.now() - timedelta(hours=4)).isoformat(),
            },
        )
        cache.set(
            SYNC_FAILURE_KEY,
            {"failed_at": timezone.now().isoformat(), "error": "Timeout"},
        )

        payload = self._get(blueprint_type_ids=str(BLUEPRINT_TYPE_ID))

        self.assertFalse(payload["is_cached"])
        self.assertTrue(payload["refresh_failed"])
        self.assertEqual(payload["last_error"], "Timeout")

    @patch("indy_hub.tasks.public_contracts.sync_public_jita_contracts.delay")
    def test_force_queues_background_sync_only_when_stale(self, mock_delay) -> None:
        cache.set(SYNC_META_KEY, {"ok": True, "synced_at": timezone.now().isoformat()})
        payload = self._get(blueprint_type_ids=str(BLUEPRINT_TYPE_ID), force="1")
        self.assertFalse(payload["refresh_queued"])
        mock_delay.assert_not_called()

        cache.set(
            SYNC_META_KEY,
            {
                "ok": True,
                "synced_at": (timezone.now() - timedelta(hours=2)).isoformat(),
            },
        )
        payload = self._get(blueprint_type_ids=str(BLUEPRINT_TYPE_ID), force="1")
        self.assertTrue(payload["refresh_queued"])
        mock_delay.assert_called_once_with()

    @patch("indy_hub.tasks.public_contracts.sync_public_jita_contracts.delay")
    def test_without_force_nothing_is_queued(self, mock_delay) -> None:
        self._get(blueprint_type_ids=str(BLUEPRINT_TYPE_ID))
        mock_delay.assert_not_called()


class BuyBpcsFreshnessUiTests(SimpleTestCase):
    def test_snapshot_states_are_rendered_distinctly(self) -> None:
        source = CRAFT_JS.read_text(encoding="utf-8")
        for marker in (
            "function getBuyBpcsSnapshotState()",
            "Current snapshot",
            "Cached snapshot (older than the refresh interval)",
            "Last refresh failed",
            "Last ESI refresh",
            "Local cache age",
            "Expired",
        ):
            self.assertIn(marker, source)
        self.assertIn(
            'id="buyBpcsFreshness"', CRAFT_TEMPLATE.read_text(encoding="utf-8")
        )

    def test_selected_contracts_are_revalidated(self) -> None:
        source = CRAFT_JS.read_text(encoding="utf-8")
        self.assertIn("function revalidateSelectedBpcContracts()", source)
        # A stored selection must not stand in for a live snapshot that no
        # longer lists it, nor for an expired offer.
        known_offer = source.split("function getKnownOfferForBlueprintContract", 1)[
            1
        ].split("\nfunction ", 1)[0]
        self.assertIn("isBpcOfferExpired(selectedOffer)", known_offer)
        self.assertIn("offersByBlueprintType.has(numericBlueprintTypeId)", known_offer)
