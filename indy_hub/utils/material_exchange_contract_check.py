from __future__ import annotations

# Standard Library
import re
from collections import Counter
from collections.abc import Iterable

CONTRACT_EXPORT_LABELS = [
    "Contract Type",
    "Description",
    "Availability",
    "Location",
    "Expiration",
    "Sales Tax",
    "Broker's Fee",
    "Deposit",
    "I will pay",
    "I will receive",
    "Items For Sale",
    "Items Required",
]

MULTILINE_LABELS = {"Items For Sale", "Items Required"}
ITEM_LINE_SPLIT_RE = re.compile(r"\s*(?:,|;|\|)\s*")
ITEM_QTY_RE = re.compile(r"^(.+?)\s*(?:x|\*)\s*([0-9][0-9,.\s']*)$", re.IGNORECASE)
ITEM_QTY_FALLBACK_RE = re.compile(
    r"\s+[xX*]\s+([0-9][0-9,.\s']*)(?=(?:\s|$))"
)


def collapse_whitespace(value: str | None) -> str:
    return " ".join(str(value or "").split()).strip()


def normalize_text(value: str | None) -> str:
    return collapse_whitespace(value).casefold()


def parse_positive_quantity(raw_value: str | int | None) -> int | None:
    """Parse positive integer quantities from common exported formats."""

    text_value = str(raw_value or "").strip()
    if not text_value:
        return None

    normalized = (
        text_value.replace("\u00A0", " ")
        .replace("\u202F", " ")
        .replace("\u2009", " ")
        .replace("_", "")
        .replace("'", "")
    )
    compact = normalized.replace(" ", "")
    if compact.isdigit():
        parsed = int(compact)
        return parsed if parsed > 0 else None

    if re.match(r"^\d{1,3}(?:[.,]\d{3})+$", compact):
        parsed = int(compact.replace(",", "").replace(".", ""))
        return parsed if parsed > 0 else None

    return None


def parse_contract_export(raw_text: str) -> dict[str, str]:
    """Parse an in-game contract copy/paste export into labeled fields."""

    fields: dict[str, str] = {}
    current_label: str | None = None

    for raw_line in (raw_text or "").replace("\r", "").split("\n"):
        line = raw_line.strip()
        if not line:
            continue

        if "\t" in raw_line:
            parts = [part.strip() for part in raw_line.split("\t")]
            label = parts[0]
            if label in CONTRACT_EXPORT_LABELS:
                value = " ".join(part for part in parts[1:] if part).strip()
                fields[label] = (
                    value if label in MULTILINE_LABELS else collapse_whitespace(value)
                )
                current_label = label
                continue

        matched_label = next(
            (label for label in CONTRACT_EXPORT_LABELS if line.startswith(label)),
            None,
        )
        if matched_label is not None:
            value = line[len(matched_label) :].strip("\t :")
            fields[matched_label] = (
                value
                if matched_label in MULTILINE_LABELS
                else collapse_whitespace(value)
            )
            current_label = matched_label
            continue

        if current_label in MULTILINE_LABELS:
            previous = fields.get(current_label, "")
            fields[current_label] = f"{previous}\n{line}" if previous else line

    return fields


def parse_isk_amount(raw_value: str | None) -> int | None:
    """Parse the first ISK amount from a copied contract line."""

    value = collapse_whitespace(raw_value)
    if not value:
        return None

    head = value.split("ISK", 1)[0]
    digits = re.sub(r"[^0-9]", "", head)
    if not digits:
        return None
    return int(digits)


def parse_contract_items(raw_value: str | None) -> tuple[Counter[str], dict[str, str]]:
    """Parse pasted `Items For Sale` content into normalized item counters."""

    raw_text = str(raw_value or "").replace("\r", "")
    items: Counter[str] = Counter()
    labels: dict[str, str] = {}
    fallback_segments: list[str] = []

    def _record_item(raw_name: str, quantity: int | None) -> bool:
        clean_name = collapse_whitespace(raw_name)
        if not clean_name or quantity is None or quantity <= 0:
            return False

        key = normalize_text(clean_name)
        items[key] += quantity
        labels.setdefault(key, clean_name)
        return True

    for raw_line in raw_text.split("\n"):
        line = raw_line.strip()
        if not line:
            continue

        tab_parts = [part.strip() for part in line.split("\t") if part.strip()]
        if len(tab_parts) >= 2:
            tab_quantity = next(
                (
                    quantity
                    for part in tab_parts[1:]
                    if (quantity := parse_positive_quantity(part)) is not None
                ),
                None,
            )
            if _record_item(tab_parts[0], tab_quantity):
                continue

        segments = [segment for segment in ITEM_LINE_SPLIT_RE.split(line) if segment]
        parsed_segment = False
        for segment in segments:
            segment_match = ITEM_QTY_RE.match(collapse_whitespace(segment))
            if not segment_match:
                continue
            if _record_item(
                segment_match.group(1),
                parse_positive_quantity(segment_match.group(2)),
            ):
                parsed_segment = True
        if parsed_segment:
            continue

        fallback_segments.append(line)

    remaining = collapse_whitespace(" ".join(fallback_segments))
    while remaining:
        match = ITEM_QTY_FALLBACK_RE.search(remaining)
        if not match:
            break
        if not _record_item(
            remaining[: match.start()], parse_positive_quantity(match.group(1))
        ):
            break
        remaining = remaining[match.end() :].lstrip(" ,;|")

    return items, labels


def summarize_counter(
    counter: Counter[str], labels: dict[str, str] | None = None
) -> list[str]:
    labels = labels or {}
    summary: list[str] = []
    for key in sorted(counter.keys()):
        display = labels.get(key) or key
        summary.append(f"{display} x {counter[key]}")
    return summary


def build_expected_items(
    items: Iterable[object],
) -> tuple[Counter[str], dict[str, str]]:
    counter: Counter[str] = Counter()
    labels: dict[str, str] = {}

    for item in items:
        name = collapse_whitespace(getattr(item, "type_name", ""))
        if not name:
            continue
        key = normalize_text(name)
        counter[key] += int(getattr(item, "quantity", 0) or 0)
        labels.setdefault(key, name)

    return counter, labels
