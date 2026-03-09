# Django
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("indy_hub", "0092_materialexchangestock_source_structures"),
    ]

    operations = [
        migrations.CreateModel(
            name="MaterialExchangeItemPriceOverride",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("type_id", models.IntegerField(help_text="EVE item type ID")),
                ("type_name", models.CharField(blank=True, db_index=True, max_length=255)),
                (
                    "sell_price_override",
                    models.DecimalField(
                        blank=True,
                        decimal_places=2,
                        help_text="Override payout price per unit when members sell to hub.",
                        max_digits=20,
                        null=True,
                    ),
                ),
                (
                    "buy_price_override",
                    models.DecimalField(
                        blank=True,
                        decimal_places=2,
                        help_text="Override purchase price per unit when members buy from hub.",
                        max_digits=20,
                        null=True,
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "config",
                    models.ForeignKey(
                        on_delete=models.deletion.CASCADE,
                        related_name="item_price_overrides",
                        to="indy_hub.materialexchangeconfig",
                    ),
                ),
            ],
            options={
                "verbose_name": "Material Exchange Item Price Override",
                "verbose_name_plural": "Material Exchange Item Price Overrides",
                "default_permissions": (),
                "unique_together": {("config", "type_id")},
            },
        ),
        migrations.AddIndex(
            model_name="materialexchangeitempriceoverride",
            index=models.Index(
                fields=["config", "type_id"], name="me_ovr_cfg_type_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="materialexchangeitempriceoverride",
            index=models.Index(fields=["type_name"], name="me_ovr_typename_idx"),
        ),
    ]
