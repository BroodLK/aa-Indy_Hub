"""Helpers for EVERef industry cost API."""

from __future__ import annotations

# Standard Library
from decimal import Decimal

# Third Party
import requests

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

logger = get_extension_logger(__name__)

EVEREF_INDUSTRY_COST_URL = "https://api.everef.net/v1/industry/cost"


class EVERefError(Exception):
    """Raised when EVERef API request fails."""


def fetch_industry_cost(
    *,
    product_id: int,
    runs: int,
    query_params: dict[str, object] | None = None,
    timeout: int = 10,
) -> dict:
    """Fetch industry cost payload for one product/runs pair."""
    params: list[tuple[str, object]] = [
        ("product_id", int(product_id)),
        ("runs", max(1, int(runs))),
    ]
    if query_params:
        for key, value in query_params.items():
            if value is None:
                continue
            if isinstance(value, list):
                for item in value:
                    if item is None or item == "":
                        continue
                    params.append((str(key), item))
                continue
            if value == "":
                continue
            params.append((str(key), value))

    try:
        response = requests.get(
            EVEREF_INDUSTRY_COST_URL,
            params=params,
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "User-Agent": "aa-Indy_Hub/1.0 everef-industry",
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise EVERefError("Unexpected payload format")
        return payload
    except requests.HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        url = getattr(exc.response, "url", EVEREF_INDUSTRY_COST_URL)
        body = ""
        try:
            body = str(getattr(exc.response, "text", "") or "").strip()
        except Exception:
            body = ""
        body_preview = body[:600]
        logger.warning(
            "EVERef industry API request failed: status=%s url=%s body=%s",
            status_code,
            url,
            body_preview or "<empty>",
        )
        detail = f"HTTP {status_code} for url: {url}"
        if body_preview:
            detail = f"{detail} body={body_preview}"
        raise EVERefError(detail) from exc
    except requests.RequestException as exc:
        logger.warning("EVERef industry API request failed: %s", exc)
        raise EVERefError(str(exc)) from exc


def summarize_job_fees(
    payload: dict, *, included_sections: set[str] | None = None
) -> dict:
    """Extract job-fee costs from an EVERef industry response."""
    total_job_cost = Decimal("0")
    total_api_cost = Decimal("0")
    sections: dict[str, Decimal] = {}
    allowed_sections = {
        str(section_name).strip().lower()
        for section_name in (included_sections or set())
        if str(section_name).strip()
    } or None

    for section_name, section_data in payload.items():
        if not isinstance(section_data, dict):
            continue
        normalized_section_name = str(section_name or "").strip().lower()
        if allowed_sections is not None and normalized_section_name not in allowed_sections:
            continue
        section_job_cost = Decimal("0")
        section_total_cost = Decimal("0")

        for entry in section_data.values():
            if not isinstance(entry, dict):
                continue
            section_job_cost += Decimal(str(entry.get("total_job_cost") or 0))
            section_total_cost += Decimal(str(entry.get("total_cost") or 0))

        if section_job_cost != 0:
            sections[normalized_section_name] = section_job_cost
        total_job_cost += section_job_cost
        total_api_cost += section_total_cost

    return {
        "total_job_cost": float(total_job_cost),
        "total_api_cost": float(total_api_cost),
        "section_job_costs": {name: float(value) for name, value in sections.items()},
    }
