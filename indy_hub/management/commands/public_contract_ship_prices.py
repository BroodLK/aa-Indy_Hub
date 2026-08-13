"""Print ship public-contract prices in F9-FUV, LXQ2-T and 9WVY-F using AA's ESI client."""

from django.core.management.base import BaseCommand
from indy_hub.services.public_contract_scanner import scan_public_contracts

class Command(BaseCommand):
    help = "Print ship prices from public contracts in F9-FUV, LXQ2-T and 9WVY-F."

    def add_arguments(self, parser):
        parser.add_argument("--max-pages", type=int, default=2000)
        parser.add_argument("--character-id", type=int, default=0)

    def handle(self, *args, **options):
        max_pages = max(1, options["max_pages"])
        character_id = int(options.get("character_id") or 0)

        def progress_callback(msg):
            if msg.startswith("FOUND: "):
                pass  # We'll print results at the end
            else:
                self.stdout.write(msg)

        scan_result = scan_public_contracts(
            character_id=character_id,
            max_pages=max_pages,
            progress_callback=progress_callback
        )

        results = scan_result["results"]
        diagnostics = scan_result["diagnostics"]

        self.stdout.write("\nFinal list:")
        if diagnostics["structure_failures"] > 0 and not scan_result["esi_token_used"]:
            self.stdout.write("Note: Some structures could not be resolved. Providing --character-id may help resolve 'Location <ID>' entries to system names.")
        
        self.stdout.write(
            f"Diagnostics: target contracts={diagnostics['target_matches']}, "
            f"total contracts={diagnostics['total_contracts']}, "
            f"structure lookup failures={diagnostics['structure_failures']}, "
            f"unresolved locations={diagnostics['unresolved_locations']}, "
            f"item rows={diagnostics['item_rows']}, ship hulls={diagnostics['ship_hulls']}"
        )
        self.stdout.write("\n".join(results) if results else "No matching ship contracts found.")
