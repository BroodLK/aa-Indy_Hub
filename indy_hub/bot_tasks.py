"""Discord bot task helpers for recurring mining upgrade polls."""

# Django
from django.utils import timezone

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

from .models import WeeklyMiningPollRun
from .services.mining_polls import (
    delay_resolution,
    finalize_poll_run,
    mark_run_failed,
    open_run_after_post,
)

logger = get_extension_logger(__name__)


async def post_weekly_mining_poll(bot, run_id: int) -> None:
    """Post a native Discord poll for the given poll run."""

    # Third Party
    import discord

    try:
        run = WeeklyMiningPollRun.objects.select_related("config").get(pk=run_id)
    except WeeklyMiningPollRun.DoesNotExist:
        logger.warning("Weekly mining poll run %s no longer exists.", run_id)
        return

    if run.status != WeeklyMiningPollRun.Status.PENDING_POST:
        logger.info(
            "Skipping poll run %s post because status is %s.",
            run.id,
            run.status,
        )
        return

    try:
        channel = bot.get_channel(run.discord_channel_id)
        if channel is None:
            channel = await bot.fetch_channel(run.discord_channel_id)

        poll = discord.Poll(
            question=run.question_text,
            answers=[discord.PollAnswer(text=option) for option in run.display_option_labels],
            duration=run.duration_hours,
            allow_multiselect=False,
        )
        content = f"<@&{run.ping_role_id}>" if run.ping_role_id else None
        allowed_mentions = None
        if run.ping_role_id:
            allowed_mentions = discord.AllowedMentions(
                everyone=False,
                users=False,
                roles=True,
            )

        message = await channel.send(
            content=content,
            poll=poll,
            allowed_mentions=allowed_mentions,
        )

        run.discord_message_id = message.id
        run.save(update_fields=["discord_message_id", "updated_at"])
        open_run_after_post(run, posted_at=timezone.now())
    except Exception as exc:
        logger.exception("Failed to post weekly mining poll run %s", run_id)
        mark_run_failed(run, str(exc))


async def resolve_weekly_mining_poll(bot, run_id: int) -> None:
    """Fetch a Discord poll message and finalize the run outcome."""

    try:
        run = WeeklyMiningPollRun.objects.select_related("config").get(pk=run_id)
    except WeeklyMiningPollRun.DoesNotExist:
        logger.warning("Weekly mining poll run %s no longer exists.", run_id)
        return

    if run.status != WeeklyMiningPollRun.Status.PENDING_RESOLUTION:
        logger.info(
            "Skipping poll run %s resolution because status is %s.",
            run.id,
            run.status,
        )
        return

    try:
        channel = bot.get_channel(run.discord_channel_id)
        if channel is None:
            channel = await bot.fetch_channel(run.discord_channel_id)

        message = await channel.fetch_message(run.discord_message_id)
        poll = getattr(message, "poll", None)
        if poll is None:
            raise RuntimeError("Discord message does not contain a poll payload.")

        if not poll.results or not poll.has_ended():
            delay_resolution(
                run,
                reason="Poll results were not finalized by Discord yet.",
            )
            return

        display_to_base = {display: base for base, display in zip(run.option_labels, run.display_option_labels)}
        vote_counts = {option: 0 for option in run.option_labels}
        for answer in poll.answers:
            option_label = display_to_base.get(answer.text, answer.text)
            if option_label in vote_counts:
                vote_counts[option_label] = answer.count or 0

        result = finalize_poll_run(
            run.id,
            vote_counts=vote_counts,
            finalized_at=timezone.now(),
        )
        tiebreak_run_id = result.get("tiebreak_run_id")
        if tiebreak_run_id:
            await post_weekly_mining_poll(bot, int(tiebreak_run_id))
    except Exception as exc:
        logger.exception("Failed to resolve weekly mining poll run %s", run_id)
        delay_resolution(run, reason=str(exc))
