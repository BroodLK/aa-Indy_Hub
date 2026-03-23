from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("indy_hub", "0104_reprocessing_services"),
    ]

    operations = [
        migrations.AddField(
            model_name="reprocessingserviceprofile",
            name="admin_force_unavailable",
            field=models.BooleanField(
                default=False,
                help_text="When enabled by a Material Exchange admin, the reprocessor cannot self-enable availability.",
            ),
        ),
    ]
