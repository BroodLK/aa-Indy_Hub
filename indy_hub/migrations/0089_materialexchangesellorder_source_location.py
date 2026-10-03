# Generated migration for order-level sell source location tracking

# Django
from django.db import migrations, models


def backfill_sell_order_source_location(apps, schema_editor):
    MaterialExchangeConfig = apps.get_model("indy_hub", "MaterialExchangeConfig")
    MaterialExchangeSellOrder = apps.get_model("indy_hub", "MaterialExchangeSellOrder")

    config_cache = {}

    for order in MaterialExchangeSellOrder.objects.filter(
        source_location_id__isnull=True
    ).iterator():
        config = config_cache.get(order.config_id)
        if config is None:
            config = MaterialExchangeConfig.objects.filter(id=order.config_id).first()
            config_cache[order.config_id] = config
        if not config:
            continue

        source_location_id = None
        source_location_name = ""

        raw_ids = list(getattr(config, "sell_structure_ids", None) or [])
        raw_names = list(getattr(config, "sell_structure_names", None) or [])
        for idx, raw_id in enumerate(raw_ids):
            try:
                sid = int(raw_id)
            except (TypeError, ValueError):
                continue
            if sid <= 0:
                continue
            source_location_id = sid
            try:
                source_location_name = str(raw_names[idx] or "").strip()
            except Exception:
                source_location_name = ""
            break

        if not source_location_id:
            try:
                primary_id = int(getattr(config, "structure_id", 0) or 0)
            except (TypeError, ValueError):
                primary_id = 0
            if primary_id > 0:
                source_location_id = primary_id

        if not source_location_name:
            source_location_name = str(
                getattr(config, "structure_name", None) or ""
            ).strip()
        if not source_location_name and source_location_id:
            source_location_name = f"Structure {source_location_id}"

        if not source_location_id and not source_location_name:
            continue

        order.source_location_id = source_location_id
        order.source_location_name = source_location_name
        order.save(update_fields=["source_location_id", "source_location_name"])


class Migration(migrations.Migration):

    dependencies = [
        ("indy_hub", "0088_material_exchange_multi_locations"),
    ]

    operations = [
        migrations.AddField(
            model_name="materialexchangesellorder",
            name="source_location_id",
            field=models.BigIntegerField(
                blank=True,
                help_text="Location ID selected by the seller when creating this order.",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="materialexchangesellorder",
            name="source_location_name",
            field=models.CharField(
                blank=True,
                help_text="Location name selected by the seller when creating this order.",
                max_length=255,
            ),
        ),
        migrations.RunPython(
            backfill_sell_order_source_location,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
