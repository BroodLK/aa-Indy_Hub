"""Business logic for recurring Discord mining upgrade polls."""

# Standard Library
import random
from datetime import timedelta

# Django
from django.db import transaction
from django.utils import timezone

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

from ..models import WeeklyMiningPollConfig, WeeklyMiningPollRun

CURRENT_WINNER_SUFFIX = " (current)"
MAIN_POLL_DURATION_HOURS = 24
TIEBREAKER_DURATION_HOURS = 8
MAX_TIEBREAKER_ROUNDS = 3
POLL_RESOLUTION_GRACE = timedelta(minutes=5)
MAX_RESOLUTION_ATTEMPTS = 12

logger = get_extension_logger(__name__)


def build_display_options(
    options: list[str], *, current_winner: str = "", annotate_current: bool = False
) -> list[str]:
    display_options: list[str] = []
    for option in options:
        if annotate_current and current_winner and option == current_winner:
            display_options.append(f"{option}{CURRENT_WINNER_SUFFIX}")
        else:
            display_options.append(option)
    return display_options


def create_main_poll_run(config: WeeklyMiningPollConfig, *, scheduled_at=None) -> WeeklyMiningPollRun:
    scheduled_at = scheduled_at or timezone.now()
    options = list(config.options)
    run = WeeklyMiningPollRun.objects.create(
        config=config,
        kind=WeeklyMiningPollRun.Kind.MAIN,
        status=WeeklyMiningPollRun.Status.PENDING_POST,
        duration_hours=MAIN_POLL_DURATION_HOURS,
        discord_channel_id=config.channel_id,
        ping_role_id=config.ping_role_id,
        question_text=config.build_question_text(),
        option_labels=options,
        display_option_labels=build_display_options(
            options,
            current_winner=config.current_winner_option,
            annotate_current=True,
        ),
        previous_winner_option=config.current_winner_option,
    )
    config.last_scheduled_post_at = scheduled_at
    config.save(update_fields=["last_scheduled_post_at", "updated_at"])
    return run


def create_tiebreaker_poll_run(
    *,
    config: WeeklyMiningPollConfig,
    parent_run: WeeklyMiningPollRun,
    tied_options: list[str],
) -> WeeklyMiningPollRun:
    root_run = parent_run.root_run or parent_run
    run = WeeklyMiningPollRun.objects.create(
        config=config,
        parent_run=parent_run,
        root_run=root_run,
        kind=WeeklyMiningPollRun.Kind.TIEBREAKER,
        status=WeeklyMiningPollRun.Status.PENDING_POST,
        tiebreak_round=parent_run.tiebreak_round + 1,
        duration_hours=TIEBREAKER_DURATION_HOURS,
        discord_channel_id=config.channel_id,
        ping_role_id=config.ping_role_id,
        question_text=config.build_question_text(tiebreak_round=parent_run.tiebreak_round + 1),
        option_labels=list(tied_options),
        display_option_labels=list(tied_options),
        previous_winner_option=config.current_winner_option,
    )
    logger.info(
        "Created tie-breaker run %s for config %s with options %s",
        run.id,
        config.id,
        tied_options,
    )
    return run


def finalize_poll_run(
    run_id: int,
    *,
    vote_counts: dict[str, int],
    finalized_at=None,
) -> dict[str, int | str | None]:
    finalized_at = finalized_at or timezone.now()
    with transaction.atomic():
        run = WeeklyMiningPollRun.objects.select_for_update().select_related("config").get(pk=run_id)
        config = WeeklyMiningPollConfig.objects.select_for_update().get(pk=run.config_id)

        normalized_counts: list[int] = []
        for option in run.option_labels or []:
            count = vote_counts.get(option, 0)
            try:
                normalized_counts.append(max(int(count), 0))
            except (TypeError, ValueError):
                normalized_counts.append(0)

        total_votes = sum(normalized_counts)
        run.total_votes = total_votes
        run.finalized_at = finalized_at

        if total_votes == 0:
            if config.current_winner_option:
                winner = config.current_winner_option
                method = WeeklyMiningPollRun.ResolutionMethod.NO_VOTES_PREVIOUS
            else:
                winner = random.choice(list(run.option_labels))
                method = WeeklyMiningPollRun.ResolutionMethod.NO_VOTES_RANDOM
            _finalize_winner(run, config, winner=winner, method=method)
            return {
                "run_id": run.id,
                "winner": winner,
                "total_votes": total_votes,
                "tiebreak_run_id": None,
            }

        highest_vote_count = max(normalized_counts)
        tied_options = [
            option for option, count in zip(run.option_labels, normalized_counts) if count == highest_vote_count
        ]

        if len(tied_options) == 1:
            winner = tied_options[0]
            _finalize_winner(
                run,
                config,
                winner=winner,
                method=WeeklyMiningPollRun.ResolutionMethod.VOTE_WINNER,
            )
            return {
                "run_id": run.id,
                "winner": winner,
                "total_votes": total_votes,
                "tiebreak_run_id": None,
            }

        if run.tiebreak_round < MAX_TIEBREAKER_ROUNDS:
            run.status = WeeklyMiningPollRun.Status.COMPLETED
            run.resolution_method = WeeklyMiningPollRun.ResolutionMethod.TIEBREAKER
            run.save(
                update_fields=[
                    "status",
                    "resolution_method",
                    "total_votes",
                    "finalized_at",
                    "updated_at",
                ]
            )
            tiebreak_run = create_tiebreaker_poll_run(
                config=config,
                parent_run=run,
                tied_options=tied_options,
            )
            return {
                "run_id": run.id,
                "winner": None,
                "total_votes": total_votes,
                "tiebreak_run_id": tiebreak_run.id,
            }

        if config.current_winner_option:
            winner = config.current_winner_option
            method = WeeklyMiningPollRun.ResolutionMethod.FALLBACK_PREVIOUS
        else:
            winner = random.choice(tied_options)
            method = WeeklyMiningPollRun.ResolutionMethod.FALLBACK_RANDOM
        _finalize_winner(run, config, winner=winner, method=method)
        return {
            "run_id": run.id,
            "winner": winner,
            "total_votes": total_votes,
            "tiebreak_run_id": None,
        }


def _finalize_winner(
    run: WeeklyMiningPollRun,
    config: WeeklyMiningPollConfig,
    *,
    winner: str,
    method: str,
) -> None:
    config.current_winner_option = winner
    config.save(update_fields=["current_winner_option", "updated_at"])

    run.status = WeeklyMiningPollRun.Status.COMPLETED
    run.winning_option = winner
    run.resolution_method = method
    run.save(
        update_fields=[
            "status",
            "winning_option",
            "resolution_method",
            "total_votes",
            "finalized_at",
            "updated_at",
        ]
    )


def open_run_after_post(run: WeeklyMiningPollRun, *, posted_at=None) -> None:
    posted_at = posted_at or timezone.now()
    run.status = WeeklyMiningPollRun.Status.OPEN
    run.posted_at = posted_at
    run.closes_at = posted_at + timedelta(hours=run.duration_hours)
    run.resolve_after = run.closes_at + POLL_RESOLUTION_GRACE
    run.failure_reason = ""
    run.save(
        update_fields=[
            "status",
            "posted_at",
            "closes_at",
            "resolve_after",
            "failure_reason",
            "updated_at",
        ]
    )


def mark_run_failed(run: WeeklyMiningPollRun, reason: str) -> None:
    run.status = WeeklyMiningPollRun.Status.FAILED
    run.failure_reason = reason
    run.save(update_fields=["status", "failure_reason", "updated_at"])


def delay_resolution(run: WeeklyMiningPollRun, *, delay_minutes: int = 5, reason: str = "") -> None:
    run.resolution_attempts += 1
    if run.resolution_attempts >= MAX_RESOLUTION_ATTEMPTS:
        run.status = WeeklyMiningPollRun.Status.FAILED
        run.resolve_after = None
    else:
        run.status = WeeklyMiningPollRun.Status.OPEN
        run.resolve_after = timezone.now() + timedelta(minutes=delay_minutes)
    if reason:
        run.failure_reason = reason
    run.save(
        update_fields=[
            "status",
            "resolution_attempts",
            "resolve_after",
            "failure_reason",
            "updated_at",
        ]
    )
