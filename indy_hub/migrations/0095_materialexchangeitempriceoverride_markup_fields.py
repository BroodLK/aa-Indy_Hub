# Django
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("indy_hub", "0094_cachedcharacterasset_set_name"),
    ]

    operations = [
        migrations.AddField(
            model_name="materialexchangeitempriceoverride",
            name="buy_markup_base_override",
            field=models.CharField(
                blank=True,
                choices=[("buy", "Jita Buy"), ("sell", "Jita Sell")],
                max_length=8,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="materialexchangeitempriceoverride",
            name="buy_markup_percent_override",
            field=models.DecimalField(
                blank=True, decimal_places=2, max_digits=6, null=True
            ),
        ),
        migrations.AddField(
            model_name="materialexchangeitempriceoverride",
            name="sell_markup_base_override",
            field=models.CharField(
                blank=True,
                choices=[("buy", "Jita Buy"), ("sell", "Jita Sell")],
                max_length=8,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="materialexchangeitempriceoverride",
            name="sell_markup_percent_override",
            field=models.DecimalField(
                blank=True, decimal_places=2, max_digits=6, null=True
            ),
        ),
    ]
