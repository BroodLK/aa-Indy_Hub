"""Celery tasks for the compressed-ore price cache."""

from __future__ import annotations

# Third Party
from celery import shared_task

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger
from allianceauth.services.tasks import QueueOnce

logger = get_extension_logger(__name__)


@shared_task(
    base=QueueOnce,
    once={"graceful": True},
    bind=True,
    max_retries=2,
    default_retry_delay=300,
)
def refresh_compressed_ore_prices(self) -> dict:
    """Refresh CompressedOreCache prices out of band.

    Previously the only caller of ``_update_compressed_ore_prices`` was a
    management command, so the ore-conversion request path had no choice but to
    fetch inline: a single Fuzzwork call with a per-socket (not total) timeout
    followed by one UPDATE per cached ore. That work belongs here.

    QueueOnce keeps concurrent conversions from stacking duplicate refreshes.
    """
    # AA Example App
    from indy_hub.services.reprocessing import _update_compressed_ore_prices

    updated, message = _update_compressed_ore_prices()
    if not updated:
        logger.warning("Compressed ore price refresh did not update: %s", message)
    return {"updated": bool(updated), "message": str(message or "")}
