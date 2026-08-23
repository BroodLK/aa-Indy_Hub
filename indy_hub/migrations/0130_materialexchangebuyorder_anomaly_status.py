from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("indy_hub", "0129_materialexchangebuyorder_recipient_character"),
    ]

    operations = [
        migrations.AlterField(
            model_name="materialexchangebuyorder",
            name="status",
            field=models.CharField(
                choices=[
                    ("draft", "Order Created - Awaiting Contract"),
                    ("awaiting_validation", "Awaiting Auth Validation"),
                    ("anomaly", "Anomaly - Waiting User/Admin Action"),
                    ("validated", "Validated - Awaiting User Accept"),
                    ("completed", "Completed"),
                    ("rejected", "Rejected"),
                    ("cancelled", "Cancelled"),
                ],
                default="draft",
                max_length=30,
            ),
        ),
    ]
