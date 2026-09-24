# Standard Library
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

# Django
from django.contrib.auth.models import Permission, User
from django.contrib.messages import get_messages
from django.contrib.messages.storage.fallback import FallbackStorage
from django.contrib.sessions.backends.db import SessionStore
from django.core.cache import cache
from django.test import RequestFactory, SimpleTestCase, TestCase
from django.utils import timezone

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership
from allianceauth.eveonline.models import EveCharacter

# AA Example App
from indy_hub.models import (
    MaterialExchangeBuyOrder,
    MaterialExchangeConfig,
    MaterialExchangeSettings,
    MaterialExchangeStock,
)
from indy_hub.services.material_exchange_buy_orders import create_buy_order
from indy_hub.views.material_exchange import (
    _get_buy_browse_snapshot_cache_key,
    material_exchange_buy,
)
from indy_hub.views.production_buyback import (
    production_buyback_availability,
    submit_production_buyback_order,
)

CRAFT_JS = Path(__file__).resolve().parents[1] / "static" / "indy_hub" / "js" / "craft_bp.js"
CRAFT_TEMPLATE = Path(__file__).resolve().parents[1] / "templates" / "indy_hub" / "industry" / "Craft_BP_v2.html"

TRITANIUM = 34
PYERITE = 35
CHARACTER_ID = 74000001


def _unwrap(view):
    while hasattr(view, "__wrapped__"):
        view = view.__wrapped__
    return view


class BuybackFixtureMixin:
    def setUp(self) -> None:
        cache.clear()
        self.factory = RequestFactory()
        self.user = User.objects.create_user("sim-buyback", password="secret123")
        self.user.user_permissions.add(Permission.objects.get(codename="can_access_indy_hub"))
        character, _created = EveCharacter.objects.get_or_create(
            character_id=CHARACTER_ID,
            defaults={
                "character_name": "Buyback Pilot",
                "corporation_id": 2000001,
                "corporation_name": "Test Corp",
                "corporation_ticker": "TEST",
            },
        )
        CharacterOwnership.objects.update_or_create(
            user=self.user,
            character=character,
            defaults={"owner_hash": f"hash-{self.user.id}-{CHARACTER_ID}"},
        )
        settings_obj = MaterialExchangeSettings.get_solo()
        settings_obj.is_enabled = True
        settings_obj.save(update_fields=["is_enabled"])
        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=1001,
            structure_name="Primary Structure",
            buy_structure_ids=[1001],
            buy_structure_names=["Structure Alpha"],
            buy_enabled=True,
            is_active=True,
            last_stock_sync=timezone.now(),
        )
        MaterialExchangeStock.objects.create(
            config=self.config,
            type_id=TRITANIUM,
            type_name="Tritanium",
            quantity=1000,
            source_structure_ids=[1001],
        )
        self._seed_snapshot()

    def _seed_snapshot(self, *, price="5.00", quantity=1000) -> None:
        # The buyback browse snapshot is what both the page and the simulator
        # validate submitted rows against.
        # The key embeds DB timestamps, so derive it from a fresh DB copy exactly
        # as the views will.
        cache.set(
            _get_buy_browse_snapshot_cache_key(MaterialExchangeConfig.objects.get(pk=self.config.pk)),
            {
                "stock_rows": [
                    {
                        "row_kind": "item",
                        "row_index": 0,
                        "type_id": TRITANIUM,
                        "display_type_name": "Tritanium",
                        "quantity": quantity,
                        "blueprint_variant": "",
                        "container_path": "",
                        "display_sell_price_to_member": price,
                        "has_buy_price_override": False,
                        "source_structure_ids": [1001],
                        "buy_location_label": "Structure Alpha",
                    },
                    {
                        "row_kind": "item",
                        "row_index": 1,
                        "type_id": TRITANIUM,
                        "display_type_name": "Tritanium",
                        "quantity": 50,
                        "blueprint_variant": "",
                        "container_path": "Some Can",
                        "display_sell_price_to_member": price,
                        "source_structure_ids": [1001],
                        "buy_location_label": "Structure Alpha",
                    },
                ],
                "stock_meta_by_type": {TRITANIUM: {"type_id": TRITANIUM, "quantity": quantity}},
            },
            600,
        )

    def _availability(self, **params):
        request = self.factory.get("/api/production-buyback/availability/", data=params)
        request.user = self.user
        return json.loads(_unwrap(production_buyback_availability)(request).content)

    def _submit(self, **overrides):
        payload = {
            "type_id": TRITANIUM,
            "quantity": 100,
            "row_index": 0,
            "expected_unit_price": "5.00",
            "recipient_character_id": CHARACTER_ID,
            "client_request_id": "req-abcdef12",
            **overrides,
        }
        request = self.factory.post(
            "/api/production-buyback/order/",
            data=json.dumps(payload),
            content_type="application/json",
        )
        request.user = self.user
        response = _unwrap(submit_production_buyback_order)(request, tokens=None)
        return response.status_code, json.loads(response.content)


class BuybackAvailabilityTests(BuybackFixtureMixin, TestCase):
    def test_reports_plain_row_price_location_and_recipients(self) -> None:
        payload = self._availability(type_ids=f"{TRITANIUM},{PYERITE}")

        self.assertTrue(payload["enabled"])
        item = payload["items"][str(TRITANIUM)]
        # The loose row, not the in-container one.
        self.assertEqual(item["row_index"], 0)
        self.assertEqual(item["available_quantity"], 1000)
        self.assertEqual(Decimal(item["unit_price"]), Decimal("5.00"))
        self.assertEqual(item["price_source"], "buyback_market")
        self.assertEqual(item["location_label"], "Structure Alpha")
        self.assertNotIn(str(PYERITE), payload["items"])
        self.assertEqual(payload["recipients"], [{"id": CHARACTER_ID, "name": "Buyback Pilot"}])
        self.assertFalse(payload["stock_stale"])

    def test_disabled_buyback_reports_reason(self) -> None:
        self.config.buy_enabled = False
        self.config.save(update_fields=["buy_enabled"])

        payload = self._availability(type_ids=str(TRITANIUM))

        self.assertFalse(payload["enabled"])
        self.assertEqual(payload["reason"], "buy_orders_disabled")
        self.assertEqual(payload["items"], {})

    def test_order_status_is_only_reported_for_own_orders(self) -> None:
        other = User.objects.create_user("someone-else", password="x")
        own = MaterialExchangeBuyOrder.objects.create(config=self.config, buyer=self.user)
        foreign = MaterialExchangeBuyOrder.objects.create(config=self.config, buyer=other)

        payload = self._availability(type_ids=str(TRITANIUM), order_ids=f"{own.id},{foreign.id}")

        self.assertEqual([order["id"] for order in payload["orders"]], [own.id])
        self.assertEqual(payload["orders"][0]["progress"], "pending")


class BuybackSubmitTests(BuybackFixtureMixin, TestCase):
    def test_successful_submission_creates_draft_order(self) -> None:
        status, body = self._submit()

        self.assertEqual(status, 200)
        self.assertTrue(body["success"])
        order = MaterialExchangeBuyOrder.objects.get(id=body["order"]["id"])
        self.assertEqual(order.buyer, self.user)
        self.assertEqual(order.status, MaterialExchangeBuyOrder.Status.DRAFT)
        self.assertEqual(order.recipient_character_id, CHARACTER_ID)
        item = order.items.get()
        self.assertEqual((item.type_id, item.quantity), (TRITANIUM, 100))
        self.assertEqual(item.unit_price, Decimal("5.00"))
        self.assertEqual(body["order"]["progress"], "pending")
        self.assertTrue(body["order"]["reference"])

    def test_duplicate_request_id_does_not_create_second_order(self) -> None:
        first_status, first = self._submit()
        second_status, second = self._submit()

        self.assertEqual((first_status, second_status), (200, 200))
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["order"]["id"], second["order"]["id"])
        self.assertEqual(MaterialExchangeBuyOrder.objects.count(), 1)

    def test_price_change_is_refused(self) -> None:
        self._seed_snapshot(price="6.50")

        status, body = self._submit()

        self.assertEqual(status, 409)
        self.assertEqual(body["error"], "price_changed")
        self.assertEqual(Decimal(body["current_price"]), Decimal("6.50"))
        self.assertFalse(MaterialExchangeBuyOrder.objects.exists())

    def test_quantity_above_unreserved_stock_is_refused(self) -> None:
        existing = MaterialExchangeBuyOrder.objects.create(config=self.config, buyer=self.user)
        existing.items.create(
            type_id=TRITANIUM,
            type_name="Tritanium",
            quantity=950,
            unit_price=Decimal("5"),
            total_price=Decimal("4750"),
        )

        status, body = self._submit(quantity=100, client_request_id="req-partial01")

        self.assertEqual(status, 409)
        self.assertEqual(body["error"], "insufficient_stock")
        self.assertEqual(body["available"], 50)
        self.assertEqual(MaterialExchangeBuyOrder.objects.count(), 1)

    def test_recipient_must_belong_to_user(self) -> None:
        status, body = self._submit(recipient_character_id=99999999)

        self.assertEqual(status, 400)
        self.assertEqual(body["error"], "invalid_recipient")
        self.assertFalse(MaterialExchangeBuyOrder.objects.exists())

    def test_failed_attempt_can_be_retried_with_same_key(self) -> None:
        self._submit(recipient_character_id=99999999)
        status, body = self._submit()

        self.assertEqual(status, 200)
        self.assertTrue(body["success"])

    def test_disabled_buyback_is_refused(self) -> None:
        settings_obj = MaterialExchangeSettings.get_solo()
        settings_obj.is_enabled = False
        settings_obj.save(update_fields=["is_enabled"])

        status, body = self._submit()

        self.assertEqual(status, 409)
        self.assertEqual(body["error"], "buyback_disabled")

    def test_invalid_payload_is_refused(self) -> None:
        status, body = self._submit(expected_unit_price="NaN")
        self.assertEqual((status, body["error"]), (400, "invalid_payload"))
        status, body = self._submit(client_request_id="x")
        self.assertEqual((status, body["error"]), (400, "invalid_payload"))

    def test_endpoint_requires_indy_hub_permission(self) -> None:
        stranger = User.objects.create_user("no-access", password="x")
        request = self.factory.post(
            "/api/production-buyback/order/",
            data=json.dumps({}),
            content_type="application/json",
        )
        request.user = stranger
        request.session = SessionStore()
        request._messages = FallbackStorage(request)
        # Only the login/permission layers, not the ESI token layer.
        view = submit_production_buyback_order.__wrapped__
        response = view(request)

        self.assertNotEqual(response.status_code, 200)
        self.assertFalse(MaterialExchangeBuyOrder.objects.exists())


class BuybackReconciliationTests(BuybackFixtureMixin, TestCase):
    def _order_with_status(self, status):
        order = MaterialExchangeBuyOrder.objects.create(config=self.config, buyer=self.user)
        MaterialExchangeBuyOrder.objects.filter(id=order.id).update(status=status)
        return order

    def test_progress_follows_order_workflow(self) -> None:
        pending = self._order_with_status(MaterialExchangeBuyOrder.Status.VALIDATED)
        delivered = self._order_with_status(MaterialExchangeBuyOrder.Status.COMPLETED)
        rejected = self._order_with_status(MaterialExchangeBuyOrder.Status.REJECTED)
        cancelled = self._order_with_status(MaterialExchangeBuyOrder.Status.CANCELLED)

        payload = self._availability(order_ids=",".join(str(o.id) for o in (pending, delivered, rejected, cancelled)))

        progress = {order["id"]: order["progress"] for order in payload["orders"]}
        self.assertEqual(progress[pending.id], "pending")
        self.assertEqual(progress[delivered.id], "delivered")
        self.assertEqual(progress[rejected.id], "failed")
        self.assertEqual(progress[cancelled.id], "failed")


class BuyPageRegressionTests(BuybackFixtureMixin, TestCase):
    """The buyback page still creates orders through the extracted service."""

    def _post_buy_page(self, data):
        request = self.factory.post("/material-exchange/buy/", data=data)
        request.user = self.user
        request.session = SessionStore()
        request._messages = FallbackStorage(request)
        with patch("indy_hub.views.material_exchange.emit_view_analytics_event"):
            response = _unwrap(material_exchange_buy)(request, tokens=None)
        return response, [str(message) for message in get_messages(request)]

    def test_form_post_creates_order(self) -> None:
        response, messages = self._post_buy_page(
            {"qty_34_std_root_0": "10", "recipient_character_id": str(CHARACTER_ID)}
        )

        self.assertEqual(response.status_code, 302)
        order = MaterialExchangeBuyOrder.objects.get()
        self.assertEqual(order.items.get().quantity, 10)
        self.assertTrue(any("Created buy order" in message for message in messages))

    def test_form_post_errors_become_messages(self) -> None:
        response, messages = self._post_buy_page(
            {"qty_34_std_root_0": "5000", "recipient_character_id": str(CHARACTER_ID)}
        )

        self.assertEqual(response.status_code, 302)
        self.assertFalse(MaterialExchangeBuyOrder.objects.exists())
        self.assertTrue(any("Insufficient unlocked stock" in message for message in messages))

    def test_service_rejects_mixed_blueprint_variant(self) -> None:
        result = create_buy_order(
            user=self.user,
            config=self.config,
            submitted_entries=[{"type_id": TRITANIUM, "quantity": 1, "row_index": 0, "blueprint_variant": "bpc"}],
            recipient_character_id=CHARACTER_ID,
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.errors[0].code, "stale_row")


class BuybackUiTests(SimpleTestCase):
    def test_modal_and_row_hooks_exist(self) -> None:
        template = CRAFT_TEMPLATE.read_text(encoding="utf-8")
        for marker in (
            'id="buybackOrderModal"',
            'id="buybackOrderQuantity"',
            'id="buybackOrderRecipient"',
            'id="buybackOrderConfirm"',
            'id="buybackOrderSubmit"',
            'id="neededBuybackStatus"',
        ):
            self.assertIn(marker, template)

    def test_pending_orders_never_count_as_owned(self) -> None:
        source = CRAFT_JS.read_text(encoding="utf-8")
        self.assertIn("function reconcileBuybackOrders(serverOrders)", source)
        self.assertIn("Reserved (pending)", source)
        self.assertIn("buybackOrders: collectBuybackOrdersSnapshot()", source)
        # The share allowlist must not pick buyback orders up.
        share_builder = source.split("function collectCraftShareState", 1)[1].split("\nfunction ", 1)[0]
        self.assertNotIn("buyback", share_builder.lower())
