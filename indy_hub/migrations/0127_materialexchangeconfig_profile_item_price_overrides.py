from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("indy_hub", "0126_materialexchangeconfig_buy_groups_by_structure"),
    ]

    operations = [
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="profile_item_price_overrides",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text=(
                    "Per-item pricing overrides scoped to a saved sell or buy market-group profile. "
                    "Each row can define the target type, the side-specific profile name, and fixed "
                    "price and/or markup overrides for the matching side."
                ),
            ),
        ),
    ]
