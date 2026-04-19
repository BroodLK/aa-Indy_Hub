from django.test import TestCase

from indy_hub.models import MaterialExchangeConfig, MaterialExchangeSettings
from indy_hub.tasks.material_exchange import _collect_snapshot_corporation_configs


class MaterialExchangeSnapshotConfigSelectionTests(TestCase):
    def test_snapshot_uses_inactive_configs_when_module_is_enabled(self):
        settings_obj = MaterialExchangeSettings.get_solo()
        settings_obj.is_enabled = True
        settings_obj.save(update_fields=["is_enabled", "updated_at"])

        config = MaterialExchangeConfig.objects.create(
            corporation_id=98660859,
            structure_id=60000001,
            structure_name="SCI Zenith Hub",
            hangar_division=7,
            buy_structure_ids=[60000001],
            is_active=False,
            buy_enabled=True,
        )

        corp_configs = _collect_snapshot_corporation_configs(
            corporation_id=98660859
        )

        self.assertIn(98660859, corp_configs)
        self.assertEqual([item.id for item in corp_configs[98660859]], [config.id])

    def test_snapshot_skips_inactive_configs_when_module_is_disabled(self):
        settings_obj = MaterialExchangeSettings.get_solo()
        settings_obj.is_enabled = False
        settings_obj.save(update_fields=["is_enabled", "updated_at"])

        MaterialExchangeConfig.objects.create(
            corporation_id=98660859,
            structure_id=60000001,
            structure_name="SCI Zenith Hub",
            hangar_division=7,
            buy_structure_ids=[60000001],
            is_active=False,
            buy_enabled=True,
        )

        corp_configs = _collect_snapshot_corporation_configs(
            corporation_id=98660859
        )

        self.assertEqual(corp_configs, {})
