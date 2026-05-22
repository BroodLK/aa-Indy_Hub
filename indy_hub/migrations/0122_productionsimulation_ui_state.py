from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("indy_hub", "0121_merge_0116_weekly_mining_poll_crontab_reference_0120_materialexchangeconfig_market_group_profiles"),
    ]

    operations = [
        migrations.AddField(
            model_name="productionsimulation",
            name="ui_state",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
