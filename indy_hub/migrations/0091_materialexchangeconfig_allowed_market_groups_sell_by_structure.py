# Generated migration for per-structure sell market group filters

# Django
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("indy_hub", "0090_materialexchangeconfig_allow_fitted_ships"),
    ]

    operations = [
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="allowed_market_groups_sell_by_structure",
            field=models.JSONField(
                blank=True,
                default=dict,
                help_text=(
                    "Per-sell-location market group rules. Key = structure ID. "
                    "Value = list of market group IDs, or null to allow all groups."
                ),
            ),
        ),
    ]
