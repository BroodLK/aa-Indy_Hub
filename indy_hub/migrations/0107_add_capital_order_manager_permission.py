# Django
from django.db import migrations


def add_capital_order_manager_permission(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    ContentType = apps.get_model("contenttypes", "ContentType")

    try:
        blueprint_ct = ContentType.objects.get(app_label="indy_hub", model="blueprint")
    except ContentType.DoesNotExist:
        return

    Permission.objects.update_or_create(
        content_type=blueprint_ct,
        codename="can_manage_capital_orders",
        defaults={"name": "can admin CapitalOrders"},
    )


def remove_capital_order_manager_permission(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    ContentType = apps.get_model("contenttypes", "ContentType")

    try:
        blueprint_ct = ContentType.objects.get(app_label="indy_hub", model="blueprint")
    except ContentType.DoesNotExist:
        return

    Permission.objects.filter(
        content_type=blueprint_ct,
        codename="can_manage_capital_orders",
    ).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("indy_hub", "0106_capital_order_enrichment_and_auto_cancel"),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="blueprint",
            options={
                "verbose_name": "Blueprint",
                "verbose_name_plural": "Blueprints",
                "db_table": "indy_hub_indyblueprint",
                "permissions": [
                    ("can_access_indy_hub", "can access Indy_Hub"),
                    ("can_manage_corp_bp_requests", "can admin Corp"),
                    ("can_manage_material_hub", "can admin MatExchange"),
                    ("can_manage_capital_orders", "can admin CapitalOrders"),
                ],
                "default_permissions": (),
            },
        ),
        migrations.RunPython(
            add_capital_order_manager_permission,
            remove_capital_order_manager_permission,
        ),
    ]
