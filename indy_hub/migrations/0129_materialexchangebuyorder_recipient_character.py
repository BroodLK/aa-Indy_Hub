from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("indy_hub", "0128_materialexchangeconfig_refined_ore_pricing")]

    operations = [
        migrations.AddField(
            model_name="materialexchangebuyorder",
            name="recipient_character_id",
            field=models.BigIntegerField(blank=True, db_index=True, help_text="Validated character selected to receive the contract.", null=True),
        ),
        migrations.AddField(
            model_name="materialexchangebuyorder",
            name="recipient_character_name",
            field=models.CharField(blank=True, help_text="Character name captured when the order was created.", max_length=255),
        ),
    ]
