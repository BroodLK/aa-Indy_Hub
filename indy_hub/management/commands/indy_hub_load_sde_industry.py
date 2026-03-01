# Django
from django.core.management.base import BaseCommand, CommandError

# AA Example App
from indy_hub.sde_loader import SdeIndustryLoadError, load_industry_sde


class Command(BaseCommand):
    help = "Load industry activity data (products/materials/market groups) from the EVE SDE."

    def add_arguments(self, parser):
        parser.add_argument(
            "--no-download",
            action="store_true",
            help="Do not download the SDE bundle; use existing folder.",
        )
        parser.add_argument(
            "--sde-folder",
            default=None,
            help="Path to an extracted SDE folder (JSONL).",
        )
        parser.add_argument(
            "--keep-sde-folder",
            action="store_true",
            help="Keep the downloaded SDE folder after import.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=5000,
            help="Batch size for bulk inserts (default: 5000).",
        )
        parser.add_argument(
            "--no-cleanup",
            action="store_true",
            help="Do not delete existing industry tables before import.",
        )
        parser.add_argument(
            "--skip-market-groups",
            action="store_true",
            help="Skip importing market groups.",
        )

    def handle(self, *args, **options):
        try:
            results = load_industry_sde(
                sde_folder=options.get("sde_folder"),
                download=not options.get("no_download"),
                cleanup=not options.get("no_cleanup"),
                batch_size=int(options.get("batch_size") or 5000),
                keep_sde_folder=bool(options.get("keep_sde_folder")),
                load_market_groups=not options.get("skip_market_groups"),
            )
        except SdeIndustryLoadError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS("Industry SDE import complete."))
        for key, value in results.items():
            self.stdout.write(f"- {key}: {value}")
