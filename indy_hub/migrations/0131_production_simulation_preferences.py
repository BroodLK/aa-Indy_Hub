# Django
from django.db import migrations, models


def _normalize(value):
    normalized = " ".join((value or "").split()).casefold()
    return normalized or None


def populate_normalized_names(apps, schema_editor):
    """Backfill normalized names, renaming pre-existing per-user duplicates.

    The unique constraint added below would abort the deploy on any account
    that already holds two simulations whose names differ only by case or
    whitespace, so collisions are suffixed here instead.
    """
    ProductionSimulation = apps.get_model("indy_hub", "ProductionSimulation")
    seen_by_user: dict[int, set[str]] = {}
    for simulation in ProductionSimulation.objects.order_by("user_id", "id").iterator():
        normalized = _normalize(simulation.simulation_name)
        if normalized is None:
            simulation.simulation_name_normalized = None
            simulation.save(update_fields=["simulation_name_normalized"])
            continue

        seen = seen_by_user.setdefault(simulation.user_id, set())
        if normalized in seen:
            suffix = 2
            while _normalize(f"{simulation.simulation_name} ({suffix})") in seen:
                suffix += 1
            simulation.simulation_name = f"{simulation.simulation_name} ({suffix})"
            normalized = _normalize(simulation.simulation_name)
        seen.add(normalized)
        simulation.simulation_name_normalized = normalized
        simulation.save(update_fields=["simulation_name", "simulation_name_normalized"])


class Migration(migrations.Migration):
    dependencies = [("indy_hub", "0130_materialexchangebuyorder_anomaly_status")]

    operations = [
        migrations.AddField(
            model_name="productionsimulation",
            name="simulation_name_normalized",
            field=models.CharField(blank=True, default=None, max_length=255, null=True),
        ),
        migrations.CreateModel(
            name="ProductionSimulationPreference",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("state", models.JSONField(blank=True, default=dict)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "user",
                    models.OneToOneField(
                        on_delete=models.deletion.CASCADE,
                        related_name="production_simulation_preferences",
                        to="auth.user",
                    ),
                ),
            ],
            options={"default_permissions": ()},
        ),
        migrations.RunPython(populate_normalized_names, migrations.RunPython.noop),
        migrations.AddConstraint(
            model_name="productionsimulation",
            constraint=models.UniqueConstraint(
                fields=("user", "simulation_name_normalized"),
                name="indy_sim_user_name_normalized_uniq",
            ),
        ),
    ]
