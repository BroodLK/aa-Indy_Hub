# Django
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("indy_hub", "0108_add_missing_capital_config_fields"),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="materialexchangebuyorder",
            options={
                "default_permissions": (),
                "ordering": ["-created_at"],
                "verbose_name": "Buyback Buy Order",
                "verbose_name_plural": "Buyback Buy Orders",
            },
        ),
        migrations.AlterModelOptions(
            name="materialexchangebuyorderitem",
            options={
                "default_permissions": (),
                "ordering": ["created_at"],
                "verbose_name": "Buyback Buy Order Item",
                "verbose_name_plural": "Buyback Buy Order Items",
            },
        ),
        migrations.AlterModelOptions(
            name="materialexchangeconfig",
            options={
                "default_permissions": (),
                "verbose_name": "Buyback Configuration",
                "verbose_name_plural": "Buyback Configurations",
            },
        ),
        migrations.AlterModelOptions(
            name="materialexchangeitempriceoverride",
            options={
                "default_permissions": (),
                "verbose_name": "Buyback Item Price Override",
                "verbose_name_plural": "Buyback Item Price Overrides",
            },
        ),
        migrations.AlterModelOptions(
            name="materialexchangesellorder",
            options={
                "default_permissions": (),
                "ordering": ["-created_at"],
                "verbose_name": "Buyback Sell Order",
                "verbose_name_plural": "Buyback Sell Orders",
            },
        ),
        migrations.AlterModelOptions(
            name="materialexchangesellorderitem",
            options={
                "default_permissions": (),
                "ordering": ["created_at"],
                "verbose_name": "Buyback Sell Order Item",
                "verbose_name_plural": "Buyback Sell Order Items",
            },
        ),
        migrations.AlterModelOptions(
            name="materialexchangesettings",
            options={
                "default_permissions": (),
                "verbose_name": "Buyback Settings",
                "verbose_name_plural": "Buyback Settings",
            },
        ),
        migrations.AlterModelOptions(
            name="materialexchangestock",
            options={
                "default_permissions": (),
                "verbose_name": "Buyback Stock",
                "verbose_name_plural": "Buyback Stock",
            },
        ),
        migrations.AlterModelOptions(
            name="materialexchangetransaction",
            options={
                "default_permissions": (),
                "ordering": ["-completed_at"],
                "verbose_name": "Buyback Transaction",
                "verbose_name_plural": "Buyback Transactions",
            },
        ),
        migrations.AlterField(
            model_name="capitalshipordermessage",
            name="sender_role",
            field=models.CharField(
                choices=[
                    ("requester", "Requester"),
                    ("admin", "Buyback Admin"),
                    ("system", "System"),
                ],
                max_length=16,
            ),
        ),
        migrations.AlterField(
            model_name="materialexchangeconfig",
            name="buy_enabled",
            field=models.BooleanField(
                default=True,
                help_text="Enable/disable Buyback buy orders.",
            ),
        ),
        migrations.AlterField(
            model_name="materialexchangeconfig",
            name="is_active",
            field=models.BooleanField(
                default=True,
                help_text="Enable/disable the Buyback",
            ),
        ),
        migrations.AlterField(
            model_name="materialexchangesettings",
            name="is_enabled",
            field=models.BooleanField(
                default=True,
                help_text="Enable/disable the Buyback module",
            ),
        ),
        migrations.AlterField(
            model_name="materialexchangesettings",
            name="stats_selected_corporation_id",
            field=models.BigIntegerField(
                blank=True,
                help_text="Saved corporation selection for Buyback stats.",
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="materialexchangesettings",
            name="stats_selected_wallet_division",
            field=models.IntegerField(
                blank=True,
                help_text="Saved wallet division selection for Buyback stats.",
                null=True,
                validators=[MinValueValidator(1), MaxValueValidator(7)],
            ),
        ),
        migrations.AlterField(
            model_name="notificationwebhook",
            name="webhook_type",
            field=models.CharField(
                choices=[
                    ("material_exchange", "Buyback"),
                    ("blueprint_sharing", "Blueprint sharing"),
                ],
                max_length=32,
            ),
        ),
        migrations.AlterField(
            model_name="notificationwebhookmessage",
            name="webhook_type",
            field=models.CharField(
                choices=[
                    ("material_exchange", "Buyback"),
                    ("blueprint_sharing", "Blueprint sharing"),
                ],
                max_length=32,
            ),
        ),
        migrations.AlterField(
            model_name="reprocessingserviceprofile",
            name="admin_force_unavailable",
            field=models.BooleanField(
                default=False,
                help_text=(
                    "When enabled by a Buyback admin, the reprocessor cannot self-enable "
                    "availability."
                ),
            ),
        ),
    ]
