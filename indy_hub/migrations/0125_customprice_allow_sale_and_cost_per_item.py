from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("indy_hub", "0124_industry_skill_snapshot_skill_levels"),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name="productionsimulation",
            unique_together=set(),
        ),
        migrations.AlterUniqueTogether(
            name="customprice",
            unique_together={("simulation", "item_type_id", "is_sale_price")},
        ),
    ]
