# Django
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("indy_hub", "0095_materialexchangeitempriceoverride_markup_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="market_group_price_overrides",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text=(
                    "Per-market-group pricing overrides. Each row can define fixed "
                    "price and/or markup overrides for sell and buy sides."
                ),
            ),
        ),
    ]
