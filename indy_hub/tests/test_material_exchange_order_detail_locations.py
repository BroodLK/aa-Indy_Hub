# Django
from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership, UserProfile
from allianceauth.eveonline.models import EveCharacter

# AA Example App
from indy_hub.models import MaterialExchangeConfig, MaterialExchangeSellOrder


class SellOrderDetailLocationTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user("sellviewer", password="secret123")
        permission = Permission.objects.get(
            content_type__app_label="indy_hub",
            codename="can_access_indy_hub",
        )
        self.user.user_permissions.add(permission)
        character, _ = EveCharacter.objects.get_or_create(
            character_id=71000001,
            defaults={
                "character_name": "Sell Viewer Main",
                "corporation_id": 2000001,
                "corporation_name": "Test Corp",
                "corporation_ticker": "TEST",
            },
        )
        CharacterOwnership.objects.update_or_create(
            user=self.user,
            character=character,
            defaults={"owner_hash": f"hash-{self.user.id}-71000001"},
        )
        profile, _ = UserProfile.objects.get_or_create(user=self.user)
        profile.main_character = character
        profile.save(update_fields=["main_character"])

        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=123456789,
            structure_id=60003760,
            structure_name="Primary Trade Hub",
            sell_structure_ids=[70000001, 70000002],
            sell_structure_names=["Alpha Hub", "Beta Hub"],
            is_active=True,
        )

    def test_sell_order_detail_uses_order_source_location_name(self):
        order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.user,
            status=MaterialExchangeSellOrder.Status.DRAFT,
            source_location_id=70000002,
            source_location_name="Beta Hub",
        )

        self.client.force_login(self.user)
        response = self.client.get(
            reverse("indy_hub:sell_order_detail", kwargs={"order_id": order.id})
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Beta Hub")

    def test_sell_order_detail_falls_back_to_config_primary_location(self):
        order = MaterialExchangeSellOrder.objects.create(
            config=self.config,
            seller=self.user,
            status=MaterialExchangeSellOrder.Status.DRAFT,
            source_location_id=None,
            source_location_name="",
        )

        self.client.force_login(self.user)
        response = self.client.get(
            reverse("indy_hub:sell_order_detail", kwargs={"order_id": order.id})
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Primary Trade Hub")
