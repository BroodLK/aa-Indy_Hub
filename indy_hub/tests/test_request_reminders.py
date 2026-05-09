"""Focused tests for pending request reminder workflows."""

# Standard Library
from datetime import timedelta
from unittest.mock import patch

# Django
from django.contrib.auth.models import Permission, User
from django.core.cache import cache
from django.test import TestCase
from django.utils import timezone

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership, UserProfile
from allianceauth.eveonline.models import EveCharacter

# Local
from indy_hub.models import (
    Blueprint,
    BlueprintCopyOffer,
    BlueprintCopyRequest,
    CapitalShipOrder,
    CharacterSettings,
    MaterialExchangeConfig,
)
from indy_hub.tasks.material_exchange_contracts import (
    send_blueprint_copy_request_waiting_reminders,
    send_capital_order_waiting_reminders,
)

PUBLIC_STATION_ID = 60003760


def assign_main_character(user: User, *, character_id: int) -> EveCharacter:
    character, _ = EveCharacter.objects.get_or_create(
        character_id=character_id,
        defaults={
            "character_name": f"Pilot {character_id}",
            "corporation_id": 2_000_000,
            "corporation_name": "Test Corp",
            "corporation_ticker": "TEST",
        },
    )
    CharacterOwnership.objects.update_or_create(
        user=user,
        character=character,
        defaults={"owner_hash": f"hash-{character_id}-{user.id}"},
    )
    profile, _ = UserProfile.objects.get_or_create(user=user)
    profile.main_character = character
    profile.save(update_fields=["main_character"])
    return character


def grant_indy_permissions(user: User, *codenames: str) -> None:
    required = {"can_access_indy_hub"}
    required.update(codenames)
    permissions = Permission.objects.filter(codename__in=required)
    found = {perm.codename: perm for perm in permissions}
    missing = required - found.keys()
    if missing:
        raise AssertionError(f"Missing permissions: {sorted(missing)}")
    user.user_permissions.add(*found.values())


class CapitalWaitingReminderTests(TestCase):
    def setUp(self) -> None:
        cache.clear()
        self.manager = User.objects.create_user("capmanager", password="secret123")
        assign_main_character(self.manager, character_id=2025001)
        grant_indy_permissions(self.manager, "can_manage_capital_orders")

        self.requester = User.objects.create_user("caprequester", password="secret123")
        assign_main_character(self.requester, character_id=2025002)
        grant_indy_permissions(self.requester)

        self.config = MaterialExchangeConfig.objects.create(
            corporation_id=1234,
            structure_id=60003760,
            is_active=True,
        )

    def _create_order(self) -> CapitalShipOrder:
        return CapitalShipOrder.objects.create(
            config=self.config,
            requester=self.requester,
            ship_type_id=19720,
            ship_type_name="Revelation",
            ship_class=CapitalShipOrder.ShipClass.DREAD,
            reason=CapitalShipOrder.Reason.NO_CAP,
            status=CapitalShipOrder.Status.WAITING,
        )

    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_sends_24_hour_and_3_day_waiting_reminders(self, mock_notify_multi) -> None:
        first_order = self._create_order()
        second_order = self._create_order()
        first_order.created_at = timezone.now() - timedelta(hours=25)
        first_order.save(update_fields=["created_at"])
        second_order.created_at = timezone.now() - timedelta(days=3, minutes=5)
        second_order.save(update_fields=["created_at"])

        send_capital_order_waiting_reminders()
        send_capital_order_waiting_reminders()

        self.assertEqual(mock_notify_multi.call_count, 2)
        messages = [str(call.args[2]) for call in mock_notify_multi.call_args_list]
        self.assertTrue(any("24 hours" in message for message in messages))
        self.assertTrue(any("3 days" in message for message in messages))

    @patch("indy_hub.tasks.material_exchange_contracts.notify_multi")
    def test_skips_reminder_after_offer_activity(self, mock_notify_multi) -> None:
        order = self._create_order()
        order.offer_updated_at = timezone.now() - timedelta(hours=2)
        order.created_at = timezone.now() - timedelta(days=4)
        order.save(update_fields=["offer_updated_at", "created_at", "updated_at"])

        send_capital_order_waiting_reminders()

        mock_notify_multi.assert_not_called()


class BlueprintCopyWaitingReminderTests(TestCase):
    def setUp(self) -> None:
        cache.clear()
        self.requester = User.objects.create_user("bpbuyer", password="secret123")
        assign_main_character(self.requester, character_id=104101)
        grant_indy_permissions(self.requester, "can_manage_corp_bp_requests")

        self.owner = User.objects.create_user("bpowner", password="secret123")
        assign_main_character(self.owner, character_id=104102)
        CharacterSettings.objects.create(
            user=self.owner,
            character_id=0,
            allow_copy_requests=True,
            copy_sharing_scope=CharacterSettings.SCOPE_EVERYONE,
        )
        Blueprint.objects.create(
            owner_user=self.owner,
            character_id=104102,
            item_id=9054001,
            blueprint_id=9054002,
            type_id=905401,
            location_id=PUBLIC_STATION_ID,
            location_flag="hangar",
            quantity=-1,
            time_efficiency=14,
            material_efficiency=10,
            runs=0,
            character_name="Reminder Owner",
            type_name="Reminder Widget Blueprint",
        )

    @patch("indy_hub.views.industry.notify_user")
    def test_sends_24_hour_and_3_day_waiting_reminders(self, mock_notify_user) -> None:
        first_request = BlueprintCopyRequest.objects.create(
            type_id=905401,
            material_efficiency=10,
            time_efficiency=14,
            requested_by=self.requester,
            runs_requested=1,
            copies_requested=1,
        )
        second_request = BlueprintCopyRequest.objects.create(
            type_id=905401,
            material_efficiency=10,
            time_efficiency=14,
            requested_by=self.requester,
            runs_requested=2,
            copies_requested=2,
        )
        first_request.created_at = timezone.now() - timedelta(hours=25)
        first_request.save(update_fields=["created_at"])
        second_request.created_at = timezone.now() - timedelta(days=3, minutes=5)
        second_request.save(update_fields=["created_at"])

        send_blueprint_copy_request_waiting_reminders()
        send_blueprint_copy_request_waiting_reminders()

        self.assertEqual(mock_notify_user.call_count, 2)
        messages = [str(call.args[2]) for call in mock_notify_user.call_args_list]
        self.assertTrue(any("24 hours" in message for message in messages))
        self.assertTrue(any("3 days" in message for message in messages))

    @patch("indy_hub.views.industry.notify_user")
    def test_skips_reminder_after_provider_action(self, mock_notify_user) -> None:
        copy_request = BlueprintCopyRequest.objects.create(
            type_id=905401,
            material_efficiency=10,
            time_efficiency=14,
            requested_by=self.requester,
            runs_requested=1,
            copies_requested=1,
        )
        BlueprintCopyOffer.objects.create(
            request=copy_request,
            owner=self.owner,
            status="rejected",
        )
        copy_request.created_at = timezone.now() - timedelta(days=4)
        copy_request.save(update_fields=["created_at"])

        send_blueprint_copy_request_waiting_reminders()

        mock_notify_user.assert_not_called()
