# Generated migration for material exchange sell asset filtering toggle

# Django
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("indy_hub", "0089_materialexchangesellorder_source_location"),
    ]

    operations = [
        migrations.AddField(
            model_name="materialexchangeconfig",
            name="allow_fitted_ships",
            field=models.BooleanField(
                default=False,
                help_text="When enabled, fitted ships and their fitted/cargo contents are allowed in sell listings.",
            ),
        ),
    ]
