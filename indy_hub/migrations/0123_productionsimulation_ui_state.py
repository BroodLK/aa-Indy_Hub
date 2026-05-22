from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("indy_hub", "0122_merge_20260521_2148"),
    ]

    operations = [
        migrations.AddField(
            model_name="productionsimulation",
            name="ui_state",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
