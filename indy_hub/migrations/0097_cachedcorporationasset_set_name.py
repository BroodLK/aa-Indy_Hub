# Django
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("indy_hub", "0096_materialexchangeconfig_market_group_price_overrides"),
    ]

    operations = [
        migrations.AddField(
            model_name="cachedcorporationasset",
            name="set_name",
            field=models.CharField(blank=True, max_length=255),
        ),
    ]
