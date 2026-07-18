from decimal import Decimal

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("indy_hub", "0127_materialexchangeconfig_profile_item_price_overrides"),
    ]

    operations = [
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="use_refined_minerals_for_ore_pricing_sell",
            field=models.BooleanField(
                default=False,
                help_text=(
                    "When enabled, the SELL page prices ores by their refined mineral yield "
                    "(what the hub pays members for each yielded mineral) times the configured refine rate."
                ),
            ),
        ),
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="use_refined_minerals_for_ore_pricing_buy",
            field=models.BooleanField(
                default=False,
                help_text=(
                    "When enabled, the BUY page prices ores by their refined mineral yield "
                    "(what the hub charges members for each yielded mineral) times the configured refine rate."
                ),
            ),
        ),
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="ore_refine_rate_percent",
            field=models.DecimalField(
                decimal_places=2,
                default=Decimal("0"),
                help_text="Effective refine rate (0-100) used when refined-mineral ore pricing is enabled.",
                max_digits=5,
                validators=[
                    django.core.validators.MinValueValidator(Decimal("0")),
                    django.core.validators.MaxValueValidator(Decimal("100")),
                ],
            ),
        ),
    ]
