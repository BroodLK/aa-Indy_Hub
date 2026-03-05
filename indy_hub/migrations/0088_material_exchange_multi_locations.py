# Generated migration for multi-location Material Exchange configuration

# Django
from django.db import migrations, models


def seed_material_exchange_location_lists(apps, schema_editor):
    MaterialExchangeConfig = apps.get_model("indy_hub", "MaterialExchangeConfig")

    for config in MaterialExchangeConfig.objects.all():
        update_fields = []

        if not getattr(config, "sell_structure_ids", None):
            if getattr(config, "structure_id", None):
                config.sell_structure_ids = [int(config.structure_id)]
                update_fields.append("sell_structure_ids")
                if getattr(config, "structure_name", None):
                    config.sell_structure_names = [str(config.structure_name)]
                    update_fields.append("sell_structure_names")

        if not getattr(config, "buy_structure_ids", None):
            if getattr(config, "structure_id", None):
                config.buy_structure_ids = [int(config.structure_id)]
                update_fields.append("buy_structure_ids")
                if getattr(config, "structure_name", None):
                    config.buy_structure_names = [str(config.structure_name)]
                    update_fields.append("buy_structure_names")

        if not getattr(config, "location_match_mode", None):
            config.location_match_mode = "name_or_id"
            update_fields.append("location_match_mode")

        if update_fields:
            config.save(update_fields=update_fields)


class Migration(migrations.Migration):

    dependencies = [
        ("indy_hub", "0087_sde_industry_tables"),
    ]

    operations = [
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="sell_structure_ids",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text="List of structure IDs where members can SELL to the hub. Empty = use primary structure.",
            ),
        ),
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="sell_structure_names",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text="Cached structure names aligned with sell_structure_ids (same order).",
            ),
        ),
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="buy_structure_ids",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text="List of structure IDs where members can BUY from the hub. Empty = use primary structure when enabled.",
            ),
        ),
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="buy_structure_names",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text="Cached structure names aligned with buy_structure_ids (same order).",
            ),
        ),
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="buy_enabled",
            field=models.BooleanField(
                default=True,
                help_text="Enable/disable Material Exchange buy orders.",
            ),
        ),
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="location_match_mode",
            field=models.CharField(
                choices=[("name_or_id", "Match by name or ID"), ("strict_id", "Match by ID only")],
                default="name_or_id",
                help_text="How contract locations are matched during validation.",
                max_length=20,
            ),
        ),
        migrations.RunPython(
            seed_material_exchange_location_lists,
            reverse_code=migrations.RunPython.noop,
        ),
    ]

