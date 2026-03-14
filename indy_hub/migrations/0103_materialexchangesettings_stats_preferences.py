from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("indy_hub", "0102_capitalshiporder_terminal_statuses"),
    ]

    operations = [
        migrations.AddField(
            model_name="materialexchangesettings",
            name="stats_selected_corporation_id",
            field=models.BigIntegerField(
                blank=True,
                help_text="Saved corporation selection for Material Exchange stats.",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="materialexchangesettings",
            name="stats_selected_wallet_division",
            field=models.IntegerField(
                blank=True,
                help_text="Saved wallet division selection for Material Exchange stats.",
                null=True,
                validators=[MinValueValidator(1), MaxValueValidator(7)],
            ),
        ),
    ]
