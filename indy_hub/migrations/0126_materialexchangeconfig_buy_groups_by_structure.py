from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("indy_hub", "0125_customprice_allow_sale_and_cost_per_item"),
    ]

    operations = [
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="allowed_market_groups_buy_by_structure",
            field=models.JSONField(
                blank=True,
                default=dict,
                help_text=(
                    "Per-buy-location market group rules. Key = structure ID. "
                    "Value = list of market group IDs allowed for that buy location."
                ),
            ),
        ),
    ]
