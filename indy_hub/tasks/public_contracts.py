"""Periodic tasks for DB-backed public Jita contract cache."""

# Third Party
from celery import shared_task

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

# Local
from indy_hub.services.public_contracts_store import sync_public_jita_contract_cache

logger = get_extension_logger(__name__)


@shared_task(name="indy_hub.tasks.public_contracts.sync_public_jita_contracts")
def sync_public_jita_contracts(*, force: bool = False, max_pages: int = 2000):
    """Sync public Jita contracts into local DB cache."""
    safe_max_pages = max(1, int(max_pages or 2000))
    result = sync_public_jita_contract_cache(force=bool(force), max_pages=safe_max_pages)
    logger.info(
        "Public Jita contracts sync task completed force=%s max_pages=%s result=%s",
        bool(force),
        safe_max_pages,
        result,
    )
    return result
