# Generated migration for per-item buy stock source structures

# Django
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("indy_hub", "0091_materialexchangeconfig_allowed_market_groups_sell_by_structure"),
    ]

    operations = [
        migrations.AddField(
            model_name="materialexchangestock",
            name="source_structure_ids",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text="Buy structure IDs where this stock type currently exists.",
            ),
        ),
        migrations.AddField(
            model_name="materialexchangestock",
            name="source_structure_names",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text=(
                    "Cached buy structure names aligned with source_structure_ids (same order)."
                ),
            ),
        ),
    ]
