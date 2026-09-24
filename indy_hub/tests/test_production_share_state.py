# Standard Library
import base64
import json
from pathlib import Path

# Django
from django.test import SimpleTestCase

# AA Example App
from indy_hub.services.production_simulation_state import (
    MAX_SHARE_ENCODED_LENGTH,
    SHARE_INPUT_ALLOWLIST,
    ShareStateError,
    decode_share_param,
    normalize_share_state,
)

CRAFT_JS = (
    Path(__file__).resolve().parents[1] / "static" / "indy_hub" / "js" / "craft_bp.js"
)


def _encode(payload) -> str:
    raw = json.dumps(payload).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _base_payload(**overrides):
    payload = {
        "v": 1,
        "blueprint_type_id": 23758,
        "runs": 10,
        "tab": "buy",
        "buy": [34, 35],
        "me_te": {
            "mainME": 10,
            "mainTE": 20,
            "blueprintConfigs": {"500": {"me": 8, "te": 16}},
        },
        "prices": [{"item_type_id": 34, "unit_price": 5.5, "is_sale_price": False}],
        "inputs": [
            {"id": "industryFeeEnabledInput", "type": "checkbox", "checked": True},
            {"id": "industryFeeFacilityTaxInput", "type": "number", "value": "1.5"},
        ],
        "shipping": {"route": 7},
        "display": {"tree_open": [True, False], "configure_open": ["collapse-500"]},
    }
    payload.update(overrides)
    return payload


class ShareStateAllowlistTests(SimpleTestCase):
    def test_round_trip_keeps_safe_scenario_fields(self) -> None:
        state = decode_share_param(_encode(_base_payload()))

        self.assertEqual(state["blueprint_type_id"], 23758)
        self.assertEqual(state["runs"], 10)
        self.assertEqual(state["tab"], "buy")
        self.assertEqual(state["buy"], [34, 35])
        self.assertEqual(
            state["me_te"]["blueprintConfigs"], {"500": {"me": 8, "te": 16}}
        )
        self.assertEqual(state["prices"][0]["unit_price"], 5.5)
        self.assertEqual(state["shipping"], {"route": 7})
        self.assertEqual(
            {entry["id"] for entry in state["inputs"]},
            {"industryFeeEnabledInput", "industryFeeFacilityTaxInput"},
        )

    def test_private_fields_never_survive(self) -> None:
        payload = _base_payload(
            characters=[90000001],
            buildEnvironment={"structureId": 1035466617946},
            materialsSourceLocationId="60003760",
            buybackOrders=[{"order_id": 1}],
        )
        payload["me_te"]["buildEnvironment"] = {"structureId": 1035466617946}
        payload["me_te"]["industryFee"] = {"system_id": 30000142}
        payload["me_te"]["blueprintConfigs"]["500"]["use"] = 1
        payload["inputs"].append(
            {"id": "industryFeeSystemIdInput", "type": "text", "value": "30000142"}
        )
        payload["inputs"].append(
            {"id": "industryFeeRigIdsInput", "type": "text", "value": "37180"}
        )

        state = decode_share_param(_encode(payload))
        serialized = json.dumps(state)

        for private in (
            "1035466617946",
            "30000142",
            "60003760",
            "characters",
            "buybackOrders",
            "37180",
        ):
            self.assertNotIn(private, serialized)
        self.assertNotIn("use", state["me_te"]["blueprintConfigs"]["500"])
        self.assertEqual(set(state["me_te"]), {"mainME", "mainTE", "blueprintConfigs"})

    def test_values_are_bounded_and_normalized(self) -> None:
        state = normalize_share_state(
            _base_payload(
                runs=10**12,
                tab="run_optimized",
                buy=[34, -1, "x", True],
                me_te={
                    "mainME": 99,
                    "mainTE": -5,
                    "blueprintConfigs": {"abc": {"me": 3}, "7": {"me": 50}},
                },
                prices=[
                    {"item_type_id": 34, "unit_price": "NaN"},
                    {"item_type_id": 35, "unit_price": -1},
                ],
                inputs=[{"id": "industryFeeFacilityTaxInput", "value": "1e99"}],
            )
        )

        self.assertEqual(state["runs"], 1_000_000)
        self.assertEqual(state["tab"], "plan")
        self.assertEqual(state["buy"], [34])
        self.assertEqual((state["me_te"]["mainME"], state["me_te"]["mainTE"]), (10, 0))
        self.assertEqual(state["me_te"]["blueprintConfigs"], {"7": {"me": 10}})
        self.assertEqual(state["prices"], [])
        self.assertEqual(state["inputs"], [])

    def test_unknown_fields_are_tolerated(self) -> None:
        state = decode_share_param(_encode(_base_payload(future_field={"x": 1})))
        self.assertNotIn("future_field", state)


class ShareStateErrorTests(SimpleTestCase):
    def test_malformed_input_is_a_recoverable_error(self) -> None:
        for bad in ("!!!", _encode("not a dict"), _encode({"v": 1}), "e30"):
            with self.assertRaises(ShareStateError):
                decode_share_param(bad)

    def test_oversized_input(self) -> None:
        with self.assertRaises(ShareStateError) as ctx:
            decode_share_param("A" * (MAX_SHARE_ENCODED_LENGTH + 1))
        self.assertEqual(str(ctx.exception), "oversized")

    def test_unsupported_version(self) -> None:
        with self.assertRaises(ShareStateError) as ctx:
            decode_share_param(_encode(_base_payload(v=2)))
        self.assertEqual(str(ctx.exception), "unsupported_version")

    def test_missing(self) -> None:
        with self.assertRaises(ShareStateError) as ctx:
            decode_share_param("")
        self.assertEqual(str(ctx.exception), "missing")


class ShareSchemaParityTests(SimpleTestCase):
    """The JS encoder/decoder and this mirror must agree on the allowlist."""

    def test_input_allowlist_matches_client(self) -> None:
        source = CRAFT_JS.read_text(encoding="utf-8")
        block = source.split("const CRAFT_SHARE_INPUT_ALLOWLIST = {", 1)[1].split(
            "};", 1
        )[0]
        client_ids = {
            line.strip().split(":", 1)[0] for line in block.splitlines() if ":" in line
        }
        self.assertEqual(client_ids, set(SHARE_INPUT_ALLOWLIST))

    def test_client_no_longer_shares_use_flag(self) -> None:
        source = CRAFT_JS.read_text(encoding="utf-8")
        block = source.split("function collectShareableMeTe", 1)[1].split(
            "\nfunction ", 1
        )[0]
        self.assertNotIn("safeEntry.use", block)

    def test_share_max_encoded_length_is_safe_for_http_request_line(self) -> None:
        source = CRAFT_JS.read_text(encoding="utf-8")
        block = source.split("const CRAFT_SHARE_MAX_ENCODED_LENGTH = ", 1)[1].split(
            ";", 1
        )[0]
        client_max = int(block.strip())
        self.assertLessEqual(client_max, 2500)

    def test_update_share_url_builds_clean_pathname_url(self) -> None:
        source = CRAFT_JS.read_text(encoding="utf-8")
        block = source.split("function updateCraftShareUrl()", 1)[1].split(
            "\nfunction ", 1
        )[0]
        self.assertIn("window.location.pathname", block)
        self.assertNotIn("window.location.href", block)

    def test_tree_open_trims_trailing_falses(self) -> None:
        state = normalize_share_state(
            _base_payload(
                display={
                    "tree_open": [True, False, False, True, False, False, False],
                    "configure_open": [],
                }
            )
        )
        self.assertEqual(state["display"]["tree_open"], [True, False, False, True])
