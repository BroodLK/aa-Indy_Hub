"""Helpers to load industry data from the EVE SDE into indy_hub tables."""

from __future__ import annotations

# Standard Library
import csv
import json
import logging
from pathlib import Path

# Alliance Auth (External Libs)
from eve_sde.sde_tasks import (
    SDE_FOLDER as DEFAULT_SDE_FOLDER,
    delete_sde_folder,
    delete_sde_zip,
    download_extract_sde,
)
from eve_sde.models import ItemType

# AA Example App
from indy_hub.models import (
    SdeIndustryActivityMaterial,
    SdeIndustryActivityProduct,
    SdeMarketGroup,
)

logger = logging.getLogger(__name__)


class SdeIndustryLoadError(RuntimeError):
    """Raised when SDE industry data could not be loaded."""


def _as_int(value, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _iter_jsonl_rows(file_path: Path):
    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _bulk_flush(model_cls, buffer: list, *, batch_size: int) -> None:
    if not buffer:
        return
    model_cls.objects.bulk_create(buffer, batch_size=batch_size)
    buffer.clear()


def _load_market_groups_from_jsonl(
    file_path: Path,
    *,
    batch_size: int,
    cleanup: bool,
) -> int:
    if cleanup:
        SdeMarketGroup.objects.all().delete()

    created = 0
    buffer: list[SdeMarketGroup] = []
    parent_updates: list[tuple[int, int]] = []
    group_ids: set[int] = set()
    for row in _iter_jsonl_rows(file_path):
        group_id = _as_int(row.get("_key"))
        if not group_id:
            continue
        name = (row.get("name") or {}).get("en") or ""
        parent_id = _as_int(row.get("parentGroupID"))
        group_ids.add(group_id)
        buffer.append(
            SdeMarketGroup(
                id=group_id,
                name=name,
            )
        )
        if parent_id:
            parent_updates.append((group_id, parent_id))
        created += 1
        if len(buffer) >= batch_size:
            _bulk_flush(SdeMarketGroup, buffer, batch_size=batch_size)

    _bulk_flush(SdeMarketGroup, buffer, batch_size=batch_size)

    if parent_updates:
        update_buffer: list[SdeMarketGroup] = []
        for group_id, parent_id in parent_updates:
            if parent_id not in group_ids:
                continue
            update_buffer.append(
                SdeMarketGroup(id=group_id, parent_id=parent_id)
            )
            if len(update_buffer) >= batch_size:
                SdeMarketGroup.objects.bulk_update(
                    update_buffer, ["parent_id"], batch_size=batch_size
                )
                update_buffer.clear()

        if update_buffer:
            SdeMarketGroup.objects.bulk_update(
                update_buffer, ["parent_id"], batch_size=batch_size
            )
    return created


def _load_industry_products_from_jsonl(
    file_path: Path,
    *,
    batch_size: int,
    cleanup: bool,
) -> int:
    if cleanup:
        SdeIndustryActivityProduct.objects.all().delete()

    existing_types = set(ItemType.objects.values_list("id", flat=True))
    created = 0
    skipped = 0
    buffer: list[SdeIndustryActivityProduct] = []

    for row in _iter_jsonl_rows(file_path):
        eve_type_id = _as_int(row.get("typeID"))
        product_type_id = _as_int(row.get("productTypeID"))
        if not eve_type_id or not product_type_id:
            continue
        if eve_type_id not in existing_types or product_type_id not in existing_types:
            skipped += 1
            continue
        activity_id = _as_int(row.get("activityID"), 0) or 0
        quantity = _as_int(row.get("quantity"), 0) or 0
        buffer.append(
            SdeIndustryActivityProduct(
                eve_type_id=eve_type_id,
                activity_id=activity_id,
                product_eve_type_id=product_type_id,
                quantity=quantity,
            )
        )
        created += 1
        if len(buffer) >= batch_size:
            _bulk_flush(SdeIndustryActivityProduct, buffer, batch_size=batch_size)

    _bulk_flush(SdeIndustryActivityProduct, buffer, batch_size=batch_size)
    if skipped:
        logger.warning("Skipped %s product rows due to missing ItemType entries", skipped)
    return created


def _load_industry_materials_from_jsonl(
    file_path: Path,
    *,
    batch_size: int,
    cleanup: bool,
) -> int:
    if cleanup:
        SdeIndustryActivityMaterial.objects.all().delete()

    existing_types = set(ItemType.objects.values_list("id", flat=True))
    created = 0
    skipped = 0
    buffer: list[SdeIndustryActivityMaterial] = []

    for row in _iter_jsonl_rows(file_path):
        eve_type_id = _as_int(row.get("typeID"))
        material_type_id = _as_int(row.get("materialTypeID"))
        if not eve_type_id or not material_type_id:
            continue
        if eve_type_id not in existing_types or material_type_id not in existing_types:
            skipped += 1
            continue
        activity_id = _as_int(row.get("activityID"), 0) or 0
        quantity = _as_int(row.get("quantity"), 0) or 0
        buffer.append(
            SdeIndustryActivityMaterial(
                eve_type_id=eve_type_id,
                activity_id=activity_id,
                material_eve_type_id=material_type_id,
                quantity=quantity,
            )
        )
        created += 1
        if len(buffer) >= batch_size:
            _bulk_flush(SdeIndustryActivityMaterial, buffer, batch_size=batch_size)

    _bulk_flush(SdeIndustryActivityMaterial, buffer, batch_size=batch_size)
    if skipped:
        logger.warning("Skipped %s material rows due to missing ItemType entries", skipped)
    return created


def _load_industry_products_from_csv(
    file_path: Path,
    *,
    batch_size: int,
    cleanup: bool,
) -> int:
    if cleanup:
        SdeIndustryActivityProduct.objects.all().delete()

    existing_types = set(ItemType.objects.values_list("id", flat=True))
    created = 0
    skipped = 0
    buffer: list[SdeIndustryActivityProduct] = []

    with file_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            eve_type_id = _as_int(row.get("typeID"))
            product_type_id = _as_int(row.get("productTypeID"))
            if not eve_type_id or not product_type_id:
                continue
            if eve_type_id not in existing_types or product_type_id not in existing_types:
                skipped += 1
                continue
            activity_id = _as_int(row.get("activityID"), 0) or 0
            quantity = _as_int(row.get("quantity"), 0) or 0
            buffer.append(
                SdeIndustryActivityProduct(
                    eve_type_id=eve_type_id,
                    activity_id=activity_id,
                    product_eve_type_id=product_type_id,
                    quantity=quantity,
                )
            )
            created += 1
            if len(buffer) >= batch_size:
                _bulk_flush(SdeIndustryActivityProduct, buffer, batch_size=batch_size)

    _bulk_flush(SdeIndustryActivityProduct, buffer, batch_size=batch_size)
    if skipped:
        logger.warning("Skipped %s product rows due to missing ItemType entries", skipped)
    return created


def load_industry_sde(
    *,
    sde_folder: str | None = None,
    download: bool = True,
    cleanup: bool = True,
    batch_size: int = 5000,
    keep_sde_folder: bool = False,
    load_market_groups: bool = True,
) -> dict[str, int]:
    """Load industry activity data (products/materials + market groups) from the SDE."""

    if not ItemType.objects.exists():
        raise SdeIndustryLoadError(
            "ItemType table is empty. Run `python manage.py esde_load_sde` first."
        )

    folder = Path(sde_folder or DEFAULT_SDE_FOLDER)

    if download:
        logger.info("Downloading EVE SDE JSONL bundle...")
        download_extract_sde()
        folder = Path(DEFAULT_SDE_FOLDER)

    results: dict[str, int] = {}

    market_groups_file = folder / "marketGroups.jsonl"
    if load_market_groups and market_groups_file.exists():
        results["market_groups"] = _load_market_groups_from_jsonl(
            market_groups_file,
            batch_size=batch_size,
            cleanup=cleanup,
        )
    elif load_market_groups:
        logger.warning("marketGroups.jsonl not found; skipping market group import.")

    products_file = folder / "industryActivityProducts.jsonl"
    if products_file.exists():
        results["industry_products"] = _load_industry_products_from_jsonl(
            products_file,
            batch_size=batch_size,
            cleanup=cleanup,
        )
    else:
        csv_fallback = Path("data") / "sde" / "industryActivityProducts.csv"
        if csv_fallback.exists():
            logger.warning(
                "industryActivityProducts.jsonl not found, using %s fallback.",
                csv_fallback,
            )
            results["industry_products"] = _load_industry_products_from_csv(
                csv_fallback,
                batch_size=batch_size,
                cleanup=cleanup,
            )
        else:
            raise SdeIndustryLoadError(
                "industryActivityProducts.jsonl not found and no CSV fallback present."
            )

    materials_file = folder / "industryActivityMaterials.jsonl"
    if materials_file.exists():
        results["industry_materials"] = _load_industry_materials_from_jsonl(
            materials_file,
            batch_size=batch_size,
            cleanup=cleanup,
        )
    else:
        logger.warning(
            "industryActivityMaterials.jsonl not found; material requirements will be missing."
        )

    if download and not keep_sde_folder:
        try:
            delete_sde_zip()
        except Exception:
            pass
        try:
            delete_sde_folder()
        except Exception:
            pass

    return results
