# Django
from django.core.cache import cache
from django.core.management.base import BaseCommand
from django.db import transaction

# Local
from indy_hub.models import PublicJitaContract, PublicJitaContractItem
from indy_hub.services.public_contracts_store import SYNC_META_KEY


class Command(BaseCommand):
    help = "Delete all DB-cached public Jita contracts and items."

    def handle(self, *args, **options):
        before_contracts = PublicJitaContract.objects.count()
        before_items = PublicJitaContractItem.objects.count()
        self.stdout.write(
            f"Clearing public Jita contract cache rows "
            f"(contracts={before_contracts}, items={before_items})..."
        )

        with transaction.atomic():
            deleted_items, _ = PublicJitaContractItem.objects.all().delete()
            deleted_contracts, _ = PublicJitaContract.objects.all().delete()

        cache.delete(SYNC_META_KEY)

        self.stdout.write(
            self.style.SUCCESS(
                "Public Jita contract cache cleared "
                f"(contracts_deleted={deleted_contracts}, items_deleted={deleted_items})."
            )
        )
