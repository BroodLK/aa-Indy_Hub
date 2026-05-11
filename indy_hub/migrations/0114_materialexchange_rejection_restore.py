from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("indy_hub", "0113_add_capital_order_builder_permission"),
    ]

    operations = [
        migrations.AddField(
            model_name="materialexchangebuyorder",
            name="status_before_rejection",
            field=models.CharField(
                blank=True,
                help_text="Previous status restored when an accidental rejection is reopened.",
                max_length=30,
            ),
        ),
        migrations.AddField(
            model_name="materialexchangesellorder",
            name="status_before_rejection",
            field=models.CharField(
                blank=True,
                help_text="Previous status restored when an accidental rejection is reopened.",
                max_length=30,
            ),
        ),
    ]
