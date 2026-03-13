# Django
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        (
            "indy_hub",
            "0098_rename_indy_hub_sd_eve_typ_7a2e7e_idx_indy_hub_sd_eve_typ_0139d2_idx_and_more",
        ),
    ]

    operations = [
        migrations.AddField(
            model_name="materialexchangetransaction",
            name="jita_buy_total_value_snapshot",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text="quantity * Jita buy snapshot.",
                max_digits=20,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="materialexchangetransaction",
            name="jita_buy_unit_price_snapshot",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text="Jita buy unit price snapshot at completion time.",
                max_digits=20,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="materialexchangetransaction",
            name="jita_sell_total_value_snapshot",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text="quantity * Jita sell snapshot.",
                max_digits=20,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="materialexchangetransaction",
            name="jita_sell_unit_price_snapshot",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text="Jita sell unit price snapshot at completion time.",
                max_digits=20,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="materialexchangetransaction",
            name="jita_split_total_value_snapshot",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text="quantity * Jita split snapshot.",
                max_digits=20,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="materialexchangetransaction",
            name="jita_split_unit_price_snapshot",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text="Midpoint of Jita buy/sell at completion time.",
                max_digits=20,
                null=True,
            ),
        ),
    ]
