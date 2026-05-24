"""Helpers for craft planner material quantity calculations."""

from __future__ import annotations

# Standard Library
from math import ceil


def calculate_job_material_quantity(
    base_per_run_quantity: int,
    runs: int,
    *,
    material_efficiency: int | float = 0,
    structure_bonus: float = 0.0,
    rig_bonus: float = 0.0,
) -> int:
    """Return the total input quantity for a job using per-run rounding.

    EVE rounds the adjusted material quantity for each run before totaling the
    job. That means a material required ``1`` per run stays ``1`` for every run,
    even when ME/structure/rig reductions would otherwise push the aggregated
    total below the run count.
    """

    per_run_quantity = max(0, int(base_per_run_quantity or 0))
    run_count = max(0, int(runs or 0))
    if per_run_quantity <= 0 or run_count <= 0:
        return 0

    me_multiplier = max(0.0, (100.0 - float(material_efficiency or 0)) / 100.0)
    structure_multiplier = max(0.0, 1.0 - float(structure_bonus or 0.0))
    rig_multiplier = max(0.0, 1.0 - float(rig_bonus or 0.0))

    adjusted_per_run_quantity = ceil(
        per_run_quantity * me_multiplier * structure_multiplier * rig_multiplier
    )
    return adjusted_per_run_quantity * run_count
