# Django
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("indy_hub", "0093_materialexchangeitempriceoverride"),
    ]

    operations = [
        migrations.AddField(
            model_name="cachedcharacterasset",
            name="set_name",
            field=models.CharField(blank=True, max_length=255),
        ),
    ]
