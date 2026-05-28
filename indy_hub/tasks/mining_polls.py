"""Celery tasks for recurring Discord mining upgrade polls."""

# Third Party
# Celery
from celery import shared_task

# Django
from django.utils import timezone

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

from ..models import WeeklyMiningPollConfig, WeeklyMiningPollRun
from ..services.mining_polls import create_main_poll_run

logger = get_extension_logger(__name__)

BOT_TASK_POST = "indy_hub.bot_tasks.post_weekly_mining_poll"
BOT_TASK_RESOLVE = "indy_hub.bot_tasks.resolve_weekly_mining_poll"


def _queue_bot_task(function_name: str, run_id: int) -> bool:
    try:
        # Third Party
        from aadiscordbot.tasks import run_task_function
    except Exception:
        logger.exception("aadiscordbot task queue is unavailable for poll run %s", run_id)
        return False

    run_task_function.apply_async(
        kwargs={
            "function": function_name,
            "task_args": [run_id],
            "task_kwargs": {},
        }
    )
    return True


def queue_weekly_mining_poll_post(run_id: int) -> bool:
    return _queue_bot_task(BOT_TASK_POST, run_id)


def queue_weekly_mining_poll_resolution(run_id: int) -> bool:
    return _queue_bot_task(BOT_TASK_RESOLVE, run_id)


@shared_task
def run_weekly_mining_poll_cycle() -> dict[str, int]:
    """Dispatch due weekly polls and resolve completed poll runs."""

    posted = dispatch_due_weekly_mining_polls()
    resolving = queue_closed_weekly_mining_polls_for_resolution()
    return {"posted": posted, "resolving": resolving}


def dispatch_due_weekly_mining_polls(*, now=None) -> int:
    now = now or timezone.now()
    posted = 0

    configs = WeeklyMiningPollConfig.objects.filter(is_active=True).order_by("id")
    for config in configs:
        last_run_at = config.last_scheduled_post_at or config.created_at or now
        if not config.build_crontab().is_due(last_run_at).is_due:
            continue

        has_active_run = config.runs.filter(
            status__in=[
                WeeklyMiningPollRun.Status.PENDING_POST,
                WeeklyMiningPollRun.Status.OPEN,
                WeeklyMiningPollRun.Status.PENDING_RESOLUTION,
            ]
        ).exists()
        if has_active_run:
            logger.warning(
                "Skipping weekly mining poll config %s because an earlier run is still active.",
                config.id,
            )
            config.last_scheduled_post_at = now
            config.save(update_fields=["last_scheduled_post_at", "updated_at"])
            continue

        run = create_main_poll_run(config, scheduled_at=now)
        if queue_weekly_mining_poll_post(run.id):
            posted += 1
        else:
            run.status = WeeklyMiningPollRun.Status.FAILED
            run.failure_reason = "aadiscordbot task queue unavailable"
            run.save(update_fields=["status", "failure_reason", "updated_at"])

    return posted


def queue_closed_weekly_mining_polls_for_resolution(*, now=None) -> int:
    now = now or timezone.now()
    queued = 0
    due_runs = WeeklyMiningPollRun.objects.filter(
        status=WeeklyMiningPollRun.Status.OPEN,
        resolve_after__isnull=False,
        resolve_after__lte=now,
        discord_message_id__isnull=False,
    ).order_by("resolve_after", "id")

    for run in due_runs:
        updated = WeeklyMiningPollRun.objects.filter(
            pk=run.pk,
            status=WeeklyMiningPollRun.Status.OPEN,
        ).update(
            status=WeeklyMiningPollRun.Status.PENDING_RESOLUTION,
            updated_at=now,
        )
        if not updated:
            continue
        if queue_weekly_mining_poll_resolution(run.id):
            queued += 1
            continue
        WeeklyMiningPollRun.objects.filter(pk=run.pk).update(
            status=WeeklyMiningPollRun.Status.FAILED,
            failure_reason="aadiscordbot task queue unavailable",
            updated_at=timezone.now(),
        )

    return queued
