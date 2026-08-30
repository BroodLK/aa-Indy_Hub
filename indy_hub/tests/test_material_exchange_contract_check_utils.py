"""Regression tests for contract check parsing helpers."""

# Standard Library
from types import SimpleNamespace

# Django
from django.test import SimpleTestCase

# AA Example App
# Local
from indy_hub.utils.material_exchange_contract_check import (
    build_expected_items,
    parse_contract_export,
    parse_contract_items,
    split_quantity_and_remainder,
    summarize_counter,
)


class ContractCheckParsingTests(SimpleTestCase):
    def test_parse_contract_export_keeps_item_lines_separate(self) -> None:
        fields = parse_contract_export(
            "Items For Sale\tDamage Control II x 19\n"
            "1MN Afterburner I x 1\n"
            "9th Tier Overseer's Personal Effects x 1\n"
            "Items Required\t\n"
        )

        self.assertEqual(
            fields.get("Items For Sale", ""),
            "Damage Control II x 19\n" "1MN Afterburner I x 1\n" "9th Tier Overseer's Personal Effects x 1",
        )

    def test_parse_contract_items_keeps_leading_digits_and_ordinals(self) -> None:
        fields = parse_contract_export(
            "Contract Type\tItem Exchange\n"
            "Description\tINDY-SELL-CHECK\n"
            "Items For Sale\tDamage Control II x 19\n"
            "1MN Afterburner I x 1\n"
            "9th Tier Overseer's Personal Effects x 1\n"
            "Scourge Rage Torpedo x 2432\n"
            "Items Required\t\n"
        )

        parsed_items, parsed_labels = parse_contract_items(fields.get("Items For Sale"))
        summary = summarize_counter(parsed_items, parsed_labels)

        self.assertIn("1MN Afterburner I x 1", summary)
        self.assertIn("9th Tier Overseer's Personal Effects x 1", summary)
        self.assertIn("Damage Control II x 19", summary)
        self.assertIn("Scourge Rage Torpedo x 2432", summary)
        self.assertNotIn("MN Afterburner I x 1", summary)
        self.assertNotIn("th Tier Overseer's Personal Effects x 1", summary)

    def test_parse_contract_items_supports_tab_delimited_rows(self) -> None:
        parsed_items, parsed_labels = parse_contract_items(
            "1MN Afterburner I\t1\n" "9th Tier Overseer's Personal Effects\t1\n" "Scourge Rage Torpedo\t2,432\n"
        )
        summary = summarize_counter(parsed_items, parsed_labels)

        self.assertEqual(
            summary,
            [
                "1MN Afterburner I x 1",
                "9th Tier Overseer's Personal Effects x 1",
                "Scourge Rage Torpedo x 2432",
            ],
        )

    def test_parse_contract_items_supports_quantity_before_numeric_item_name(self) -> None:
        parsed_items, parsed_labels = parse_contract_items(
            "3 100K Bounty SCC Encrypted Bond\n"
            "5\t10K Bounty SCC Encrypted Bond\n"
            "12 10M Bounty SCC Encrypted Bond\n"
            "8 1M Bounty SCC Encrypted Bond\n"
        )
        self.assertEqual(
            summarize_counter(parsed_items, parsed_labels),
            [
                # summarize_counter sorts on the normalized key, so "100k..."
                # sorts before "10k..." ('0' < 'k').
                "100K Bounty SCC Encrypted Bond x 3",
                "10K Bounty SCC Encrypted Bond x 5",
                "10M Bounty SCC Encrypted Bond x 12",
                "1M Bounty SCC Encrypted Bond x 8",
            ],
        )

    def test_parse_contract_items_handles_compact_ingame_export_stream(self) -> None:
        compact_items = (
            "9th Tier Overseer's Personal Effects x 110MN Afterburner II x 1"
            "Antimatter Charge L x 100Acolyte I x 8Acolyte II x 810K Bounty SCC Encrypted Bond x 1"
            "'Arbalest' Heavy Missile Launcher x 35MN Cold-Gas Enduring Microwarpdrive x 1"
            "Alloyed Tritanium Bar x 1,640Accelerant Decryptor x 3"
            "'Integrated' Acolyte x 601600mm Steel Plates II x 11MN Afterburner I x 1"
        )
        parsed_items, parsed_labels = parse_contract_items(compact_items)
        summary = summarize_counter(parsed_items, parsed_labels)

        expected = {
            "9th Tier Overseer's Personal Effects x 1",
            "10MN Afterburner II x 1",
            "Antimatter Charge L x 100",
            "Acolyte I x 8",
            "Acolyte II x 8",
            "10K Bounty SCC Encrypted Bond x 1",
            "'Arbalest' Heavy Missile Launcher x 3",
            "5MN Cold-Gas Enduring Microwarpdrive x 1",
            "Alloyed Tritanium Bar x 1640",
            "Accelerant Decryptor x 3",
            "'Integrated' Acolyte x 60",
            "1600mm Steel Plates II x 1",
            "1MN Afterburner I x 1",
        }
        self.assertEqual(set(summary), expected)

    def test_parse_contract_items_keeps_10m_bounty_prefix_in_compact_stream(self) -> None:
        compact_items = "Intact Shield Emitter x 310M Bounty SCC Encrypted Bond x 3" "Trigger Unit x 7930"
        parsed_items, parsed_labels = parse_contract_items(compact_items)
        summary = summarize_counter(parsed_items, parsed_labels)

        self.assertIn("Intact Shield Emitter x 3", summary)
        self.assertIn("10M Bounty SCC Encrypted Bond x 3", summary)
        self.assertIn("Trigger Unit x 7930", summary)
        self.assertNotIn("M Bounty SCC Encrypted Bond x 3", summary)

    def test_parse_contract_items_splits_qty_before_numeric_item_in_stream(self) -> None:
        """A space between the quantity and a digit-leading item name is a
        boundary, not thousands grouping (``... x 17 100K Bounty ...``)."""
        compact_items = (
            "100K Bounty SCC Encrypted Bond x 17 10K Bounty SCC Encrypted Bond x 33 "
            "10M Bounty SCC Encrypted Bond x 13 1M Bounty SCC Encrypted Bond x 35 "
            "Alloyed Tritanium Bar x 440"
        )
        parsed_items, parsed_labels = parse_contract_items(compact_items)
        summary = summarize_counter(parsed_items, parsed_labels)

        self.assertEqual(
            summary,
            [
                "100K Bounty SCC Encrypted Bond x 17",
                "10K Bounty SCC Encrypted Bond x 33",
                "10M Bounty SCC Encrypted Bond x 13",
                "1M Bounty SCC Encrypted Bond x 35",
                "Alloyed Tritanium Bar x 440",
            ],
        )

    def test_split_quantity_keeps_thousands_grouping(self) -> None:
        """The boundary fix must not break genuine grouped quantities."""
        self.assertEqual(split_quantity_and_remainder(" 10 000 Tritanium x 5"), (10000, " Tritanium x 5"))
        self.assertEqual(split_quantity_and_remainder(" 1 234 567 Tritanium x 5"), (1234567, " Tritanium x 5"))

    def test_split_quantity_prefers_known_numeric_item_boundary(self) -> None:
        """Glued streams must not be split mid-name into ``1`` + ``7100K ...``."""
        self.assertEqual(
            split_quantity_and_remainder(" 17 100K Bounty SCC Encrypted Bond x 33"),
            (17, " 100K Bounty SCC Encrypted Bond x 33"),
        )
        self.assertEqual(
            split_quantity_and_remainder(" 17100K Bounty SCC Encrypted Bond x 33"),
            (17, "100K Bounty SCC Encrypted Bond x 33"),
        )

    def test_parse_contract_items_keeps_numeric_items_from_allowlist(self) -> None:
        compact_items = "Intact Shield Emitter x 310,000 Skill Points x 1" "Trigger Unit x 7930"
        parsed_items, parsed_labels = parse_contract_items(compact_items)
        summary = summarize_counter(parsed_items, parsed_labels)

        self.assertIn("Intact Shield Emitter x 3", summary)
        self.assertIn("10000 Skill Points x 1", summary)
        self.assertIn("Trigger Unit x 7930", summary)

    def test_item_key_normalization_matches_grouped_numeric_prefixes(self) -> None:
        parsed_items, _parsed_labels = parse_contract_items("10000 Skill Points x 1")
        expected_items, _expected_labels = build_expected_items(
            [SimpleNamespace(type_name="10,000 Skill Points", quantity=1)]
        )
        self.assertEqual(parsed_items, expected_items)
