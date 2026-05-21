# Django
from django.core.management.base import BaseCommand, CommandError

# Local
from indy_hub.services.reprocessing import (
    _populate_compressed_ore_cache,
    _update_compressed_ore_prices,
    clear_compressed_ore_cache,
)


class Command(BaseCommand):
    help = "Populate the compressed ore cache from loaded EVE SDE data."

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip-price-update",
            action="store_true",
            help="Populate ore data only and skip the initial market price refresh.",
        )
        parser.add_argument(
            "--clear-only",
            action="store_true",
            help="Delete existing compressed ore cache rows and exit.",
        )

    def handle(self, *args, **options):
        if options.get("clear_only"):
            cleared = clear_compressed_ore_cache()
            self.stdout.write(
                self.style.SUCCESS(f"Cleared {cleared} compressed ore cache rows.")
            )
            return

        success, message = _populate_compressed_ore_cache()
        if not success:
            raise CommandError(message)
        self.stdout.write(self.style.SUCCESS(message))

        if options.get("skip_price_update"):
            self.stdout.write("Skipped price refresh.")
            return

        price_success, price_message = _update_compressed_ore_prices()
        if price_success:
            self.stdout.write(self.style.SUCCESS(price_message))
        else:
            self.stderr.write(self.style.WARNING(price_message))
