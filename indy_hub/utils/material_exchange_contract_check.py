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
ITEM_LINE_SPLIT_RE = re.compile(r"\s*(?:;|\|)\s*")
ITEM_QTY_RE = re.compile(r"^(.+?)\s*(?:x|\*)\s*([0-9][0-9,.\s']*)$", re.IGNORECASE)
NEXT_NUMERIC_ITEM_START_RE = re.compile(r"^[1-9]\d*(?:MN|mm|K)\b", re.IGNORECASE)


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


def looks_like_item_start(value: str) -> bool:
    """Best-effort detector for compact item boundaries in pasted exports."""

    text = str(value or "")
    if not text:
        return False

    head = text[0]
    if head.isalpha() or head == "'":
        return True
    if NEXT_NUMERIC_ITEM_START_RE.match(text):
        return True
    return False


def split_quantity_and_remainder(raw_tail: str) -> tuple[int | None, str]:
    """Split `x <qty><next item...>` text into quantity and remaining stream."""

    tail = str(raw_tail or "")
    stripped_tail = tail.lstrip()
    if not stripped_tail or not stripped_tail[0].isdigit():
        return None, tail

    index = 1
    while index < len(stripped_tail):
        char = stripped_tail[index]
        if char.isdigit():
            index += 1
            continue
        if char in ",.' " and index + 1 < len(stripped_tail):
            if stripped_tail[index + 1].isdigit():
                index += 1
                continue
        break

    qty_token = stripped_tail[:index]
    remainder = stripped_tail[index:]
    full_quantity = parse_positive_quantity(qty_token)
    if full_quantity is None:
        return None, tail

    if not remainder:
        return full_quantity, ""

    if remainder[0].isspace() or remainder[0] in ",;|":
        return full_quantity, remainder

    # Grouped quantities such as `1,640` are explicit; keep as-is.
    if any(char in qty_token for char in ",. "):
        return full_quantity, remainder

    digits_only = re.sub(r"[^0-9]", "", qty_token)
    if not digits_only:
        return full_quantity, remainder

    for split_index in range(1, len(digits_only) + 1):
        candidate_qty = parse_positive_quantity(digits_only[:split_index])
        if candidate_qty is None:
            continue
        candidate_remainder = f"{digits_only[split_index:]}{remainder}"
        if not candidate_remainder or looks_like_item_start(candidate_remainder):
            return candidate_qty, candidate_remainder

    return full_quantity, remainder


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
            if re.search(r"\s+[xX*]\s+[0-9]", segment_match.group(1)):
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
        match = re.search(r"\s+[xX*]\s+", remaining)
        if not match:
            break
        quantity, remainder = split_quantity_and_remainder(remaining[match.end() :])
        if not _record_item(remaining[: match.start()], quantity):
            break
        remaining = remainder.lstrip(" ,;|")

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
