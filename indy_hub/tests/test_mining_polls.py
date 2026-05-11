"""Tests for recurring Discord mining upgrade polls."""

# Standard Library
from datetime import timedelta
from unittest.mock import patch

# Django
from django.test import TestCase
from django.utils import timezone

from indy_hub.models import WeeklyMiningPollConfig, WeeklyMiningPollRun
from indy_hub.services.mining_polls import create_main_poll_run, finalize_poll_run
from indy_hub.tasks.mining_polls import (
    dispatch_due_weekly_mining_polls,
    queue_closed_weekly_mining_polls_for_resolution,
)


class WeeklyMiningPollServiceTests(TestCase):
    def create_config(self, **overrides) -> WeeklyMiningPollConfig:
        defaults = {
            "system_name": "1DQ1-A",
            "poll_name": "Mining Upgrade Vote",
            "channel_id": 123456789012345678,
            "options_json": ["Ore Prospecting", "Ice Harvesting", "Moon Drills"],
            "cron_minute": "0",
            "cron_hour": "12",
            "cron_day_of_week": "*",
            "cron_day_of_month": "*",
            "cron_month_of_year": "*",
            "is_active": True,
        }
        defaults.update(overrides)
        return WeeklyMiningPollConfig.objects.create(**defaults)

    def test_main_poll_marks_current_winner_in_display_options(self) -> None:
        config = self.create_config(current_winner_option="Ice Harvesting")

        run = create_main_poll_run(config)

        self.assertEqual(
            run.display_option_labels,
            ["Ore Prospecting", "Ice Harvesting (current)", "Moon Drills"],
        )

    def test_finalize_poll_run_sets_direct_vote_winner(self) -> None:
        config = self.create_config(current_winner_option="Moon Drills")
        run = create_main_poll_run(config)

        result = finalize_poll_run(
            run.id,
            vote_counts={
                "Ore Prospecting": 7,
                "Ice Harvesting": 3,
                "Moon Drills": 1,
            },
        )

        run.refresh_from_db()
        config.refresh_from_db()
        self.assertEqual(result["winner"], "Ore Prospecting")
        self.assertEqual(run.winning_option, "Ore Prospecting")
        self.assertEqual(
            run.resolution_method,
            WeeklyMiningPollRun.ResolutionMethod.VOTE_WINNER,
        )
        self.assertEqual(config.current_winner_option, "Ore Prospecting")

    @patch("indy_hub.services.mining_polls.random.choice", return_value="Moon Drills")
    def test_no_votes_first_cycle_uses_random_winner(self, mock_choice) -> None:
        config = self.create_config(current_winner_option="")
        run = create_main_poll_run(config)

        result = finalize_poll_run(run.id, vote_counts={})

        run.refresh_from_db()
        config.refresh_from_db()
        self.assertEqual(result["winner"], "Moon Drills")
        self.assertEqual(run.winning_option, "Moon Drills")
        self.assertEqual(
            run.resolution_method,
            WeeklyMiningPollRun.ResolutionMethod.NO_VOTES_RANDOM,
        )
        self.assertEqual(config.current_winner_option, "Moon Drills")
        mock_choice.assert_called_once()

    def test_tie_creates_tiebreaker_with_only_tied_options(self) -> None:
        config = self.create_config(current_winner_option="Ice Harvesting")
        run = create_main_poll_run(config)

        result = finalize_poll_run(
            run.id,
            vote_counts={
                "Ore Prospecting": 4,
                "Ice Harvesting": 4,
                "Moon Drills": 1,
            },
        )

        run.refresh_from_db()
        self.assertEqual(result["winner"], None)
        self.assertIsNotNone(result["tiebreak_run_id"])
        self.assertEqual(
            run.resolution_method,
            WeeklyMiningPollRun.ResolutionMethod.TIEBREAKER,
        )
        tiebreak = WeeklyMiningPollRun.objects.get(pk=result["tiebreak_run_id"])
        self.assertEqual(tiebreak.kind, WeeklyMiningPollRun.Kind.TIEBREAKER)
        self.assertEqual(tiebreak.tiebreak_round, 1)
        self.assertEqual(tiebreak.option_labels, ["Ore Prospecting", "Ice Harvesting"])
        self.assertEqual(
            tiebreak.display_option_labels,
            ["Ore Prospecting", "Ice Harvesting"],
        )

    def test_max_tiebreak_round_falls_back_to_previous_winner(self) -> None:
        config = self.create_config(current_winner_option="Moon Drills")
        run = WeeklyMiningPollRun.objects.create(
            config=config,
            kind=WeeklyMiningPollRun.Kind.TIEBREAKER,
            status=WeeklyMiningPollRun.Status.PENDING_RESOLUTION,
            tiebreak_round=3,
            duration_hours=8,
            discord_channel_id=config.channel_id,
            question_text=config.build_question_text(tiebreak_round=3),
            option_labels=["Ore Prospecting", "Ice Harvesting"],
            display_option_labels=["Ore Prospecting", "Ice Harvesting"],
            previous_winner_option=config.current_winner_option,
        )

        result = finalize_poll_run(
            run.id,
            vote_counts={"Ore Prospecting": 5, "Ice Harvesting": 5},
        )

        run.refresh_from_db()
        config.refresh_from_db()
        self.assertEqual(result["winner"], "Moon Drills")
        self.assertEqual(run.winning_option, "Moon Drills")
        self.assertEqual(
            run.resolution_method,
            WeeklyMiningPollRun.ResolutionMethod.FALLBACK_PREVIOUS,
        )
        self.assertEqual(config.current_winner_option, "Moon Drills")


class WeeklyMiningPollTaskTests(TestCase):
    def create_due_config(self, *, now) -> WeeklyMiningPollConfig:
        return WeeklyMiningPollConfig.objects.create(
            system_name="MJ-5F9",
            poll_name="Mining Upgrade Vote",
            channel_id=987654321098765432,
            options_json=["Ore Prospecting", "Ice Harvesting", "Moon Drills"],
            cron_minute=str(now.minute),
            cron_hour=str(now.hour),
            cron_day_of_week="*",
            cron_day_of_month="*",
            cron_month_of_year="*",
            last_scheduled_post_at=now - timedelta(days=1),
            is_active=True,
        )

    @patch("indy_hub.tasks.mining_polls._queue_bot_task", return_value=True)
    def test_dispatch_due_weekly_mining_polls_creates_and_queues_run(
        self, mock_queue
    ) -> None:
        now = timezone.now().replace(second=0, microsecond=0)
        config = self.create_due_config(now=now)

        posted = dispatch_due_weekly_mining_polls(now=now)

        config.refresh_from_db()
        self.assertEqual(posted, 1)
        run = WeeklyMiningPollRun.objects.get(config=config)
        self.assertEqual(run.status, WeeklyMiningPollRun.Status.PENDING_POST)
        self.assertEqual(run.kind, WeeklyMiningPollRun.Kind.MAIN)
        self.assertEqual(config.last_scheduled_post_at, now)
        mock_queue.assert_called_once()

    @patch("indy_hub.tasks.mining_polls._queue_bot_task", return_value=True)
    def test_queue_closed_weekly_mining_polls_marks_runs_pending_resolution(
        self, mock_queue
    ) -> None:
        now = timezone.now()
        config = WeeklyMiningPollConfig.objects.create(
            system_name="Y-2ANO",
            poll_name="Mining Upgrade Vote",
            channel_id=111111111111111111,
            options_json=["Ore Prospecting", "Ice Harvesting", "Moon Drills"],
            is_active=True,
        )
        run = WeeklyMiningPollRun.objects.create(
            config=config,
            kind=WeeklyMiningPollRun.Kind.MAIN,
            status=WeeklyMiningPollRun.Status.OPEN,
            duration_hours=24,
            discord_channel_id=config.channel_id,
            discord_message_id=222222222222222222,
            question_text=config.build_question_text(),
            option_labels=config.options,
            display_option_labels=config.options,
            resolve_after=now - timedelta(minutes=1),
        )

        queued = queue_closed_weekly_mining_polls_for_resolution(now=now)

        run.refresh_from_db()
        self.assertEqual(queued, 1)
        self.assertEqual(run.status, WeeklyMiningPollRun.Status.PENDING_RESOLUTION)
        mock_queue.assert_called_once()
