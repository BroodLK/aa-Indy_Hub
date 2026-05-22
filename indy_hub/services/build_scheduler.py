"""
Build scheduling and time optimization service.

Calculates manufacturing times, handles dependencies, and optimizes slot usage
for minimal completion time.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from allianceauth.services.hooks import get_extension_logger

logger = get_extension_logger(__name__)


# EVE Online Industry Activity IDs
ACTIVITY_MANUFACTURING = 1
ACTIVITY_RESEARCH_TIME = 3
ACTIVITY_RESEARCH_MATERIAL = 4
ACTIVITY_COPYING = 5
ACTIVITY_INVENTION = 8
ACTIVITY_REACTION = 11


@dataclass
class ManufacturingJob:
    """Represents a single manufacturing job."""

    job_id: int
    item_type_id: int
    item_name: str
    blueprint_type_id: int
    quantity_needed: int
    quantity_per_run: int
    runs_required: int
    base_time_seconds: int
    adjusted_time_seconds: int
    total_time_seconds: int
    activity_id: int = ACTIVITY_MANUFACTURING
    material_efficiency: int = 0
    time_efficiency: int = 0
    dependencies: List[int] = field(default_factory=list)
    chunk_index: int = 1
    chunk_count: int = 1

    # Scheduling info
    assigned_slot: Optional[int] = None
    start_time_seconds: int = 0
    end_time_seconds: int = 0

    def __post_init__(self):
        """Calculate total time after initialization."""
        if self.job_id <= 0:
            self.job_id = self.item_type_id
        if self.total_time_seconds == 0:
            self.total_time_seconds = self.adjusted_time_seconds * self.runs_required

    @property
    def activity_name(self) -> str:
        """Get human-readable activity name."""
        if self.activity_id == ACTIVITY_REACTION:
            return "Reaction"
        if self.activity_id == ACTIVITY_MANUFACTURING:
            return "Manufacturing"
        return f"Activity {self.activity_id}"

    @property
    def display_name(self) -> str:
        """Return a label that distinguishes split jobs for the same item."""
        if self.chunk_count > 1:
            return f"{self.item_name} ({self.chunk_index}/{self.chunk_count})"
        return self.item_name


@dataclass
class IndustrySlot:
    """Represents an available industry slot lane."""

    slot_id: int
    character_id: int
    character_name: str
    slot_name: str = ""
    max_concurrent_jobs: int = 1

    # Scheduling state
    jobs: List[ManufacturingJob] = field(default_factory=list)
    available_at_seconds: int = 0

    def add_job(self, job: ManufacturingJob, start_time: int):
        """Assign a job to this slot."""
        job.assigned_slot = self.slot_id
        job.start_time_seconds = start_time
        job.end_time_seconds = start_time + job.total_time_seconds
        self.jobs.append(job)
        self.available_at_seconds = job.end_time_seconds


@dataclass
class BuildSchedule:
    """Complete build schedule with time estimates and slot assignments."""

    jobs: List[ManufacturingJob]
    slots: List[IndustrySlot]
    total_sequential_time_seconds: int
    total_parallel_time_seconds: int
    critical_path: List[int] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        jobs_by_id = {job.job_id: job for job in self.jobs}
        critical_path_job_ids = list(self.critical_path)
        critical_path_item_ids = list(
            dict.fromkeys(
                jobs_by_id[job_id].item_type_id
                for job_id in critical_path_job_ids
                if job_id in jobs_by_id
            )
        )

        return {
            "total_sequential_time_seconds": self.total_sequential_time_seconds,
            "total_parallel_time_seconds": self.total_parallel_time_seconds,
            "total_sequential_time_formatted": format_time_duration(
                self.total_sequential_time_seconds
            ),
            "total_parallel_time_formatted": format_time_duration(
                self.total_parallel_time_seconds
            ),
            "time_saved_seconds": (
                self.total_sequential_time_seconds - self.total_parallel_time_seconds
            ),
            "time_saved_formatted": format_time_duration(
                self.total_sequential_time_seconds - self.total_parallel_time_seconds
            ),
            "efficiency_percent": round(
                (
                    1
                    - self.total_parallel_time_seconds
                    / max(self.total_sequential_time_seconds, 1)
                )
                * 100,
                1,
            ),
            "jobs": [
                {
                    "job_id": job.job_id,
                    "item_type_id": job.item_type_id,
                    "item_name": job.item_name,
                    "job_label": job.display_name,
                    "runs_required": job.runs_required,
                    "quantity_needed": job.quantity_needed,
                    "chunk_index": job.chunk_index,
                    "chunk_count": job.chunk_count,
                    "total_time_seconds": job.total_time_seconds,
                    "total_time_formatted": format_time_duration(job.total_time_seconds),
                    "assigned_slot": job.assigned_slot,
                    "start_time_seconds": job.start_time_seconds,
                    "start_time_formatted": format_time_duration(job.start_time_seconds),
                    "end_time_seconds": job.end_time_seconds,
                    "end_time_formatted": format_time_duration(job.end_time_seconds),
                    "dependencies": job.dependencies,
                    "activity_id": job.activity_id,
                    "activity_name": job.activity_name,
                }
                for job in self.jobs
            ],
            "slots": [
                {
                    "slot_id": slot.slot_id,
                    "character_id": slot.character_id,
                    "character_name": slot.character_name,
                    "slot_name": slot.slot_name or slot.character_name,
                    "jobs_count": len(slot.jobs),
                    "utilization_percent": round(
                        (
                            slot.available_at_seconds
                            / max(self.total_parallel_time_seconds, 1)
                        )
                        * 100,
                        1,
                    ),
                    "completion_time_seconds": slot.available_at_seconds,
                    "completion_time_formatted": format_time_duration(
                        slot.available_at_seconds
                    ),
                }
                for slot in self.slots
            ],
            "critical_path": critical_path_item_ids,
            "critical_path_job_ids": critical_path_job_ids,
            "recommendations": self.recommendations,
        }


def format_time_duration(seconds: int) -> str:
    """Format seconds into human-readable duration."""
    if seconds < 0:
        return "0s"

    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")

    return " ".join(parts)


def calculate_manufacturing_time(
    *,
    base_time_seconds: int,
    time_efficiency: int = 0,
    structure_bonus: float = 0.0,
    rig_bonus: float = 0.0,
    skill_industry: int = 0,
    skill_advanced_industry: int = 0,
) -> int:
    """
    Calculate adjusted manufacturing time using EVE's formula.

    Formula: Adjusted Time = Base Time * (1 - TE/100) * Structure Modifier
    * Rig Modifier * Skill Modifier
    """
    te_modifier = 1.0 - (time_efficiency / 100.0)
    structure_modifier = 1.0 - structure_bonus
    rig_modifier = 1.0 - rig_bonus

    industry_reduction = skill_industry * 0.04
    advanced_industry_reduction = skill_advanced_industry * 0.03
    skill_modifier = 1.0 - industry_reduction - advanced_industry_reduction

    adjusted = (
        base_time_seconds
        * te_modifier
        * structure_modifier
        * rig_modifier
        * skill_modifier
    )
    return max(1, math.ceil(adjusted))


def detect_blueprint_activity_type(blueprint_type_id: int) -> int:
    """
    Detect whether a blueprint is for manufacturing (1) or reactions (11).
    """
    try:
        from indy_hub.models import SdeIndustryActivityProduct

        has_reaction = SdeIndustryActivityProduct.objects.filter(
            eve_type_id=blueprint_type_id,
            activity_id=ACTIVITY_REACTION,
        ).exists()
        if has_reaction:
            return ACTIVITY_REACTION

        has_manufacturing = SdeIndustryActivityProduct.objects.filter(
            eve_type_id=blueprint_type_id,
            activity_id=ACTIVITY_MANUFACTURING,
        ).exists()
        if has_manufacturing:
            return ACTIVITY_MANUFACTURING

        return ACTIVITY_MANUFACTURING
    except Exception as exc:
        logger.warning("Error detecting activity type for %s: %s", blueprint_type_id, exc)
        return ACTIVITY_MANUFACTURING


def get_base_manufacturing_time(
    blueprint_type_id: int,
    activity_id: Optional[int] = None,
) -> Tuple[int, int]:
    """
    Get base manufacturing/reaction time for a blueprint from eve_sde.
    """
    try:
        import eve_sde.models as sde_models
    except ImportError:
        logger.warning("eve_sde not available for time lookup")
        return (0, ACTIVITY_MANUFACTURING)

    if activity_id is None:
        activity_id = detect_blueprint_activity_type(blueprint_type_id)

    try:
        item_type = sde_models.ItemType.objects.filter(id=blueprint_type_id).first()
        if not item_type:
            return (0, activity_id)

        time_attr = sde_models.TypeDogma.objects.filter(
            item_type_id=blueprint_type_id,
            dogma_attribute_id=1210,
        ).first()
        if time_attr and time_attr.value:
            return (int(time_attr.value), activity_id)

        if activity_id == ACTIVITY_REACTION:
            return (1800, activity_id)
        return (3600, activity_id)
    except Exception as exc:
        logger.error(
            "Error getting time for blueprint %s: %s",
            blueprint_type_id,
            exc,
        )
        return (0, activity_id)


def build_dependency_tree(jobs_data: List[dict]) -> Dict[int, List[int]]:
    """
    Build dependency tree for production items.

    Returns a mapping of item_type_id -> dependent item_type_ids.
    """
    from indy_hub.models import SdeIndustryActivityMaterial

    dependencies: Dict[int, List[int]] = {}
    producing_items = {job["item_type_id"] for job in jobs_data}

    for job in jobs_data:
        item_type_id = job["item_type_id"]
        blueprint_type_id = job.get("blueprint_type_id")

        if not blueprint_type_id:
            dependencies[item_type_id] = []
            continue

        activity_id = detect_blueprint_activity_type(blueprint_type_id)
        materials = SdeIndustryActivityMaterial.objects.filter(
            eve_type_id=blueprint_type_id,
            activity_id=activity_id,
        ).values_list("material_eve_type_id", flat=True)

        dependencies[item_type_id] = [
            material_type_id
            for material_type_id in materials
            if material_type_id in producing_items
        ]

    return dependencies


def split_runs_evenly(total_runs: int, chunk_count: int) -> List[int]:
    """Split runs into near-equal chunks."""
    normalized_runs = max(0, int(total_runs))
    normalized_chunks = max(1, int(chunk_count))
    if normalized_runs <= 0:
        return []

    active_chunks = min(normalized_runs, normalized_chunks)
    base_runs = normalized_runs // active_chunks
    remainder = normalized_runs % active_chunks
    return [
        base_runs + (1 if index < remainder else 0)
        for index in range(active_chunks)
    ]


def split_jobs_evenly_across_slots(
    jobs: List[ManufacturingJob],
    total_slot_count: int,
) -> List[ManufacturingJob]:
    """
    Split each item into balanced jobs so available slot lanes can work in parallel.

    Dependencies remain conservative: any downstream split job waits until all
    split jobs for each dependency item have completed.
    """
    normalized_slot_count = max(1, int(total_slot_count))
    expanded_jobs: List[ManufacturingJob] = []
    jobs_by_item_type: Dict[int, List[ManufacturingJob]] = {}
    next_job_id = 1

    for job in jobs:
        run_chunks = split_runs_evenly(job.runs_required, normalized_slot_count)
        if not run_chunks:
            continue

        chunk_count = len(run_chunks)
        remaining_quantity = max(0, int(job.quantity_needed))
        split_jobs: List[ManufacturingJob] = []

        for chunk_index, chunk_runs in enumerate(run_chunks, start=1):
            chunk_capacity = chunk_runs * max(1, int(job.quantity_per_run))
            chunk_quantity = min(remaining_quantity, chunk_capacity)
            remaining_quantity = max(0, remaining_quantity - chunk_quantity)

            split_job = ManufacturingJob(
                job_id=next_job_id,
                item_type_id=job.item_type_id,
                item_name=job.item_name,
                blueprint_type_id=job.blueprint_type_id,
                quantity_needed=chunk_quantity,
                quantity_per_run=job.quantity_per_run,
                runs_required=chunk_runs,
                base_time_seconds=job.base_time_seconds,
                adjusted_time_seconds=job.adjusted_time_seconds,
                total_time_seconds=job.adjusted_time_seconds * chunk_runs,
                activity_id=job.activity_id,
                material_efficiency=job.material_efficiency,
                time_efficiency=job.time_efficiency,
                dependencies=[],
                chunk_index=chunk_index,
                chunk_count=chunk_count,
            )
            next_job_id += 1
            split_jobs.append(split_job)
            expanded_jobs.append(split_job)

        jobs_by_item_type[job.item_type_id] = split_jobs

    for original_job in jobs:
        dependency_job_ids = list(
            dict.fromkeys(
                dependency_job.job_id
                for dependency_item_type_id in original_job.dependencies
                for dependency_job in jobs_by_item_type.get(
                    dependency_item_type_id, []
                )
            )
        )
        for split_job in jobs_by_item_type.get(original_job.item_type_id, []):
            split_job.dependencies = dependency_job_ids.copy()

    return expanded_jobs


def schedule_jobs_critical_path(
    jobs: List[ManufacturingJob],
    slots: List[IndustrySlot],
) -> BuildSchedule:
    """
    Schedule jobs using a dependency-aware critical-path approach.
    """
    if not jobs or not slots:
        return BuildSchedule(
            jobs=[],
            slots=slots,
            total_sequential_time_seconds=0,
            total_parallel_time_seconds=0,
        )

    dep_map: Dict[int, Set[int]] = {
        job.job_id: set(job.dependencies) for job in jobs
    }
    job_map: Dict[int, ManufacturingJob] = {job.job_id: job for job in jobs}
    earliest_start: Dict[int, int] = {}

    def calc_earliest_start(job_id: int) -> int:
        if job_id in earliest_start:
            return earliest_start[job_id]

        deps = dep_map.get(job_id, set())
        if not deps:
            earliest_start[job_id] = 0
            return 0

        max_dep_end = max(
            calc_earliest_start(dep_id) + job_map[dep_id].total_time_seconds
            for dep_id in deps
            if dep_id in job_map
        )
        earliest_start[job_id] = max_dep_end
        return max_dep_end

    for job in jobs:
        calc_earliest_start(job.job_id)

    sorted_jobs = sorted(
        jobs,
        key=lambda job: (
            earliest_start.get(job.job_id, 0),
            -job.total_time_seconds,
            job.job_id,
        ),
    )

    for slot in slots:
        slot.jobs = []
        slot.available_at_seconds = 0

    for job in sorted_jobs:
        earliest = earliest_start.get(job.job_id, 0)
        best_slot = min(slots, key=lambda slot: max(slot.available_at_seconds, earliest))
        start_time = max(best_slot.available_at_seconds, earliest)
        best_slot.add_job(job, start_time)

    total_sequential = sum(job.total_time_seconds for job in jobs)
    total_parallel = max(slot.available_at_seconds for slot in slots) if slots else 0
    critical_path = find_critical_path(jobs, dep_map, job_map)
    recommendations = generate_recommendations(
        jobs,
        slots,
        total_parallel,
        critical_path,
        job_map,
    )

    return BuildSchedule(
        jobs=sorted_jobs,
        slots=slots,
        total_sequential_time_seconds=total_sequential,
        total_parallel_time_seconds=total_parallel,
        critical_path=critical_path,
        recommendations=recommendations,
    )


def find_critical_path(
    jobs: List[ManufacturingJob],
    dep_map: Dict[int, Set[int]],
    job_map: Dict[int, ManufacturingJob],
) -> List[int]:
    """Find the critical path (longest dependency chain) in the job graph."""

    def longest_path_from(job_id: int, visited: Set[int]) -> Tuple[int, List[int]]:
        if job_id in visited:
            return (0, [])

        visited.add(job_id)
        job = job_map.get(job_id)
        if not job:
            return (0, [])

        deps = dep_map.get(job_id, set())
        if not deps:
            return (job.total_time_seconds, [job_id])

        best_length = 0
        best_path: List[int] = []
        for dep_id in deps:
            if dep_id not in job_map:
                continue
            dep_length, dep_path = longest_path_from(dep_id, visited.copy())
            if dep_length > best_length:
                best_length = dep_length
                best_path = dep_path

        return (best_length + job.total_time_seconds, best_path + [job_id])

    max_length = 0
    critical_path: List[int] = []
    for job in jobs:
        length, path = longest_path_from(job.job_id, set())
        if length > max_length:
            max_length = length
            critical_path = path

    return critical_path


def generate_recommendations(
    jobs: List[ManufacturingJob],
    slots: List[IndustrySlot],
    total_time: int,
    critical_path: List[int],
    job_map: Dict[int, ManufacturingJob],
) -> List[str]:
    """Generate optimization recommendations based on schedule analysis."""
    recommendations: List[str] = []

    if slots:
        avg_utilization = sum(slot.available_at_seconds for slot in slots) / len(slots)
        utilization_pct = (avg_utilization / max(total_time, 1)) * 100

        if utilization_pct < 50:
            recommendations.append(
                f"Low slot utilization ({utilization_pct:.0f}%). "
                "Consider using fewer slots or adding more jobs."
            )

        idle_slots = [slot for slot in slots if len(slot.jobs) == 0]
        if idle_slots:
            recommendations.append(
                f"{len(idle_slots)} slot(s) have no jobs assigned. Consider removing unused slots."
            )

    if critical_path and len(critical_path) > 1:
        critical_items = list(
            dict.fromkeys(
                job_map[job_id].item_name
                for job_id in critical_path
                if job_id in job_map
            )
        )
        recommendations.append(
            f"Critical path: {' -> '.join(critical_items)}. "
            "Optimizing these items will reduce total time."
        )

    if jobs:
        longest_job = max(jobs, key=lambda job: job.total_time_seconds)
        if longest_job.total_time_seconds > total_time * 0.3:
            recommendations.append(
                f"'{longest_job.item_name}' takes "
                f"{format_time_duration(longest_job.total_time_seconds)} "
                f"({(longest_job.total_time_seconds / max(total_time, 1) * 100):.0f}% "
                "of total time). Consider splitting runs across multiple jobs or improving TE."
            )

    return recommendations
