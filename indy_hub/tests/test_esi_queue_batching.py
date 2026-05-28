"""Tests for bounded ESI fan-out queueing."""

# Standard Library
from unittest.mock import MagicMock, patch

# Django
from django.test import SimpleTestCase

# AA Example App
from indy_hub.tasks.industry import update_user_skill_snapshots
from indy_hub.tasks.location import cache_structure_names_bulk
from indy_hub.tasks.user import update_user_roles_snapshots


class ESIQueueBatchingTests(SimpleTestCase):
    @patch("indy_hub.tasks.industry.update_user_skill_snapshots.apply_async")
    @patch("indy_hub.tasks.industry.update_character_skill_snapshot_for_character.apply_async")
    @patch("indy_hub.tasks.industry.Token.objects.filter")
    @patch("indy_hub.tasks.industry._is_user_active", return_value=True)
    @patch("indy_hub.tasks.industry.User.objects.filter")
    def test_skill_snapshot_wrapper_queues_bounded_batches(
        self,
        user_filter_mock,
        _is_user_active_mock,
        token_filter_mock,
        queue_character_mock,
        queue_continuation_mock,
    ) -> None:
        user_qs = MagicMock()
        user_qs.first.return_value = MagicMock(id=77)
        user_filter_mock.return_value = user_qs

        token_qs = MagicMock()
        token_qs.require_scopes.return_value = token_qs
        token_qs.require_valid.return_value = token_qs
        token_qs.exclude.return_value = token_qs
        token_qs.order_by.return_value = token_qs
        token_values_qs = MagicMock()
        token_values_qs.distinct.return_value = token_values_qs
        token_values_qs.__bool__.return_value = True
        token_values_qs.__getitem__.return_value = [101, 202]
        token_qs.values_list.return_value = token_values_qs
        token_filter_mock.return_value = token_qs

        result = update_user_skill_snapshots(77, batch_size=2)

        self.assertEqual(result["queued"], 2)
        self.assertFalse(result["done"])
        self.assertEqual(queue_character_mock.call_count, 2)
        queue_character_mock.assert_any_call(args=(77, 101))
        queue_character_mock.assert_any_call(args=(77, 202))
        queue_continuation_mock.assert_called_once_with(
            kwargs={"user_id": 77, "last_character_id": 202, "batch_size": 2},
            countdown=1,
        )

    @patch("indy_hub.tasks.user.update_user_roles_snapshots.apply_async")
    @patch("indy_hub.tasks.user.update_character_roles_for_character.apply_async")
    @patch("indy_hub.tasks.user._is_user_active", return_value=True)
    @patch("indy_hub.tasks.user.CharacterOwnership.objects.filter")
    @patch("indy_hub.tasks.user.User.objects.filter")
    def test_role_snapshot_wrapper_queues_bounded_batches(
        self,
        user_filter_mock,
        ownership_filter_mock,
        _is_user_active_mock,
        queue_character_mock,
        queue_continuation_mock,
    ) -> None:
        user_qs = MagicMock()
        user_qs.first.return_value = MagicMock(id=88)
        user_filter_mock.return_value = user_qs

        ownership_qs = MagicMock()
        ownership_qs.exclude.return_value = ownership_qs
        ownership_qs.select_related.return_value = ownership_qs
        ownership_qs.order_by.return_value = ownership_qs
        ownership_values_qs = MagicMock()
        ownership_values_qs.distinct.return_value = ownership_values_qs
        ownership_values_qs.__getitem__.return_value = [11, 22]
        ownership_qs.values_list.return_value = ownership_values_qs
        ownership_filter_mock.return_value = ownership_qs

        result = update_user_roles_snapshots(88, batch_size=2)

        self.assertEqual(result["queued"], 2)
        self.assertFalse(result["done"])
        self.assertEqual(queue_character_mock.call_count, 2)
        queue_character_mock.assert_any_call(args=[88, 11])
        queue_character_mock.assert_any_call(args=[88, 22])
        queue_continuation_mock.assert_called_once_with(
            kwargs={"user_id": 88, "last_character_id": 22, "batch_size": 2},
            countdown=1,
        )

    @patch("indy_hub.tasks.location.cache_structure_names_bulk.apply_async")
    @patch("indy_hub.tasks.location.group")
    @patch("indy_hub.tasks.location.cache_structure_name.s")
    def test_structure_name_bulk_task_limits_fanout_per_run(
        self,
        signature_mock,
        group_mock,
        continuation_mock,
    ) -> None:
        signature = MagicMock()
        signature.set.return_value = signature
        signature_mock.return_value = signature

        group_result = MagicMock()
        group_mock.return_value = group_result

        result = cache_structure_names_bulk(
            [1, 2, 3, 4, 5],
            character_id=7,
            owner_user_id=9,
            batch_size=2,
        )

        self.assertEqual(result, {"total": 5, "queued": 2, "remaining": 3})
        self.assertEqual(signature_mock.call_count, 2)
        group_result.apply_async.assert_called_once_with()
        continuation_mock.assert_called_once_with(
            kwargs={
                "structure_ids": [3, 4, 5],
                "character_id": 7,
                "owner_user_id": 9,
                "batch_size": 2,
            },
            countdown=5,
            priority=6,
        )
