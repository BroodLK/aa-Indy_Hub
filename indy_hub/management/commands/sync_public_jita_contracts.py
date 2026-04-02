# Standard Library
import os

# Django
from django.core.management.base import BaseCommand
from django.db import connection

# Local
from indy_hub.models import PublicJitaContract, PublicJitaContractItem
from indy_hub.services.public_contracts_store import (
    get_public_jita_contract_cache_meta,
    sync_public_jita_contract_cache,
)


def _format_bytes(size_bytes: int) -> str:
    size = float(max(0, int(size_bytes or 0)))
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    return f"{size:.2f} {units[idx]}"


def _estimate_contract_cache_size_bytes() -> int:
    table_names = [
        PublicJitaContract._meta.db_table,
        PublicJitaContractItem._meta.db_table,
    ]

    vendor = str(connection.vendor or "").lower()
    if vendor == "postgresql":
        total = 0
        with connection.cursor() as cursor:
            for table_name in table_names:
                cursor.execute("SELECT COALESCE(pg_total_relation_size(%s), 0)", [table_name])
                row = cursor.fetchone() or [0]
                total += int(row[0] or 0)
        return total

    if vendor == "mysql":
        total = 0
        with connection.cursor() as cursor:
            for table_name in table_names:
                cursor.execute(
                    """
                    SELECT COALESCE(SUM(data_length + index_length), 0)
                    FROM information_schema.tables
                    WHERE table_schema = DATABASE() AND table_name = %s
                    """,
                    [table_name],
                )
                row = cursor.fetchone() or [0]
                total += int(row[0] or 0)
        return total

    if vendor == "sqlite":
        db_path = str(connection.settings_dict.get("NAME") or "").strip()
        if db_path and db_path != ":memory:" and os.path.exists(db_path):
            return int(os.path.getsize(db_path))

    return 0


class Command(BaseCommand):
    help = "Sync public Jita contracts into DB cache and print cache size/stats."

    def add_arguments(self, parser):
        parser.add_argument(
            "--force",
            action="store_true",
            help="Force a sync run (still lock-protected).",
        )
        parser.add_argument(
            "--max-pages",
            type=int,
            default=2000,
            help="Maximum public contract pages to scan (default: 2000).",
        )

    def handle(self, *args, **options):
        force = bool(options.get("force"))
        max_pages = max(1, int(options.get("max_pages") or 2000))

        self.stdout.write(
            f"Syncing Public Jita contracts (force={force}, max_pages={max_pages})..."
        )

        result = sync_public_jita_contract_cache(force=force, max_pages=max_pages)
        meta = get_public_jita_contract_cache_meta()

        contracts_count = PublicJitaContract.objects.count()
        items_count = PublicJitaContractItem.objects.count()
        size_bytes = _estimate_contract_cache_size_bytes()

        if result.get("ok"):
            self.stdout.write(self.style.SUCCESS("Public Jita contract sync completed."))
        elif str(result.get("skipped")) == "locked":
            self.stdout.write(self.style.WARNING("Sync skipped: another sync run is in progress."))
        else:
            self.stdout.write(self.style.WARNING("Sync completed with warnings."))

        self.stdout.write(f"Result: {result}")
        self.stdout.write(
            "Cache Meta: "
            f"cached_at={meta.get('cached_at') or ''} "
            f"expires_at={meta.get('expires_at') or ''} "
            f"is_cached={bool(meta.get('is_cached'))}"
        )
        self.stdout.write(f"Rows: contracts={contracts_count} items={items_count}")
        self.stdout.write(f"Approx size: {size_bytes} bytes ({_format_bytes(size_bytes)})")
