"""
Build scheduling and time optimization service.

Calculates manufacturing times, handles dependencies, and optimizes slot usage
for minimal completion time.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple
import math

from django.db.models import Q

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

    item_type_id: int
    item_name: str
    blueprint_type_id: int
    quantity_needed: int
    quantity_per_run: int
    runs_required: int
    base_time_seconds: int  # Base manufacturing time per run
    adjusted_time_seconds: int  # After TE, skills, structure bonuses
    total_time_seconds: int  # Total for all runs
    activity_id: int = ACTIVITY_MANUFACTURING  # 1=manufacturing, 11=reactions
    material_efficiency: int = 0
    time_efficiency: int = 0
    dependencies: List[int] = field(default_factory=list)  # List of item_type_ids that must be built first

    # Scheduling info
    assigned_slot: Optional[int] = None
    start_time_seconds: int = 0
    end_time_seconds: int = 0

    def __post_init__(self):
        """Calculate total time after initialization."""
        if self.total_time_seconds == 0:
            self.total_time_seconds = self.adjusted_time_seconds * self.runs_required

    @property
    def activity_name(self) -> str:
        """Get human-readable activity name."""
        if self.activity_id == ACTIVITY_REACTION:
            return "Reaction"
        elif self.activity_id == ACTIVITY_MANUFACTURING:
            return "Manufacturing"
        else:
            return f"Activity {self.activity_id}"


@dataclass
class IndustrySlot:
    """Represents an available industry slot (character)."""

    slot_id: int
    character_id: int
    character_name: str
    max_concurrent_jobs: int = 1  # Most characters have 1 job slot

    # Scheduling state
    jobs: List[ManufacturingJob] = field(default_factory=list)
    available_at_seconds: int = 0  # When this slot becomes free

    def add_job(self, job: ManufacturingJob, start_time: int):
        """Assign a job to this slot."""
        job.assigned_slot = self.slot_id
        job.start_time_seconds = start_time
        job.end_time_seconds = start_time + job.total_time_seconds
        self.jobs.append(job)
        self.available_at_seconds = job.end_time_seconds

    def can_start_job_at(self, time: int) -> bool:
        """Check if slot is available at given time."""
        return self.available_at_seconds <= time


@dataclass
class BuildSchedule:
    """Complete build schedule with time estimates and slot assignments."""

    jobs: List[ManufacturingJob]
    slots: List[IndustrySlot]
    total_sequential_time_seconds: int  # If run on single slot sequentially
    total_parallel_time_seconds: int  # Actual completion time with all slots
    critical_path: List[int] = field(default_factory=list)  # Job IDs on critical path
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            'total_sequential_time_seconds': self.total_sequential_time_seconds,
            'total_parallel_time_seconds': self.total_parallel_time_seconds,
            'total_sequential_time_formatted': format_time_duration(self.total_sequential_time_seconds),
            'total_parallel_time_formatted': format_time_duration(self.total_parallel_time_seconds),
            'time_saved_seconds': self.total_sequential_time_seconds - self.total_parallel_time_seconds,
            'time_saved_formatted': format_time_duration(
                self.total_sequential_time_seconds - self.total_parallel_time_seconds
            ),
            'efficiency_percent': round(
                (1 - self.total_parallel_time_seconds / max(self.total_sequential_time_seconds, 1)) * 100,
                1
            ),
            'jobs': [
                {
                    'item_type_id': job.item_type_id,
                    'item_name': job.item_name,
                    'runs_required': job.runs_required,
                    'quantity_needed': job.quantity_needed,
                    'total_time_seconds': job.total_time_seconds,
                    'total_time_formatted': format_time_duration(job.total_time_seconds),
                    'assigned_slot': job.assigned_slot,
                    'start_time_seconds': job.start_time_seconds,
                    'start_time_formatted': format_time_duration(job.start_time_seconds),
                    'end_time_seconds': job.end_time_seconds,
                    'end_time_formatted': format_time_duration(job.end_time_seconds),
                    'dependencies': job.dependencies,
                    'activity_id': job.activity_id,
                    'activity_name': job.activity_name,
                }
                for job in self.jobs
            ],
            'slots': [
                {
                    'slot_id': slot.slot_id,
                    'character_id': slot.character_id,
                    'character_name': slot.character_name,
                    'jobs_count': len(slot.jobs),
                    'utilization_percent': round(
                        (slot.available_at_seconds / max(self.total_parallel_time_seconds, 1)) * 100,
                        1
                    ),
                    'completion_time_seconds': slot.available_at_seconds,
                    'completion_time_formatted': format_time_duration(slot.available_at_seconds),
                }
                for slot in self.slots
            ],
            'critical_path': self.critical_path,
            'recommendations': self.recommendations,
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

    Formula: Adjusted Time = Base Time × (1 - TE/100) × Structure Modifier × Rig Modifier × Skill Modifier

    Args:
        base_time_seconds: Base manufacturing time from blueprint
        time_efficiency: TE level (0-20, usually)
        structure_bonus: Structure time bonus (e.g., 0.25 for 25% reduction in Sotiyo)
        rig_bonus: Rig time bonus (e.g., 0.20 for T2 rig)
        skill_industry: Industry skill level (0-5), 4% per level
        skill_advanced_industry: Advanced Industry skill level (0-5), 3% per level

    Returns:
        Adjusted manufacturing time in seconds
    """
    # TE modifier
    te_modifier = 1.0 - (time_efficiency / 100.0)

    # Structure modifier (bonuses reduce time)
    structure_modifier = 1.0 - structure_bonus

    # Rig modifier (bonuses reduce time)
    rig_modifier = 1.0 - rig_bonus

    # Skill modifier: Industry (4% per level) + Advanced Industry (3% per level)
    industry_reduction = skill_industry * 0.04
    advanced_industry_reduction = skill_advanced_industry * 0.03
    skill_modifier = 1.0 - industry_reduction - advanced_industry_reduction

    # Calculate adjusted time
    adjusted = base_time_seconds * te_modifier * structure_modifier * rig_modifier * skill_modifier

    # Round up to nearest second
    return max(1, math.ceil(adjusted))


def detect_blueprint_activity_type(blueprint_type_id: int) -> int:
    """
    Detect whether a blueprint is for manufacturing (1) or reactions (11).

    Args:
        blueprint_type_id: Blueprint type ID

    Returns:
        Activity ID (1 for manufacturing, 11 for reactions), defaults to 1
    """
    try:
        from indy_hub.models import SdeIndustryActivityProduct

        # Check if this blueprint has products for reaction activity
        has_reaction = SdeIndustryActivityProduct.objects.filter(
            eve_type_id=blueprint_type_id,
            activity_id=ACTIVITY_REACTION
        ).exists()

        if has_reaction:
            return ACTIVITY_REACTION

        # Check if it has manufacturing products
        has_manufacturing = SdeIndustryActivityProduct.objects.filter(
            eve_type_id=blueprint_type_id,
            activity_id=ACTIVITY_MANUFACTURING
        ).exists()

        if has_manufacturing:
            return ACTIVITY_MANUFACTURING

        # Default to manufacturing
        return ACTIVITY_MANUFACTURING

    except Exception as e:
        logger.warning(f"Error detecting activity type for {blueprint_type_id}: {e}")
        return ACTIVITY_MANUFACTURING


def get_base_manufacturing_time(blueprint_type_id: int, activity_id: int = None) -> Tuple[int, int]:
    """
    Get base manufacturing/reaction time for a blueprint from eve_sde.

    Args:
        blueprint_type_id: Blueprint type ID
        activity_id: Activity ID (1=manufacturing, 11=reactions), auto-detected if None

    Returns:
        Tuple of (base_time_seconds, activity_id)
    """
    try:
        import eve_sde.models as sde_models
    except ImportError:
        logger.warning("eve_sde not available for time lookup")
        return (0, ACTIVITY_MANUFACTURING)

    if activity_id is None:
        activity_id = detect_blueprint_activity_type(blueprint_type_id)

    try:
        # Try to get manufacturing time from blueprint attributes
        # In eve_sde, this might be in the IndustryActivity or ItemType table
        item_type = sde_models.ItemType.objects.filter(id=blueprint_type_id).first()

        if not item_type:
            return (0, activity_id)

        # Check for time attribute in dogma attributes
        # Manufacturing time attribute ID is typically 1210 for manufacturing
        # Reaction time attribute ID is typically 1210 as well
        time_attr = sde_models.TypeDogma.objects.filter(
            item_type_id=blueprint_type_id,
            dogma_attribute_id=1210  # Manufacturing/reaction time attribute
        ).first()

        if time_attr and time_attr.value:
            return (int(time_attr.value), activity_id)

        # Fallback: estimate based on activity type
        # Reactions are typically faster than manufacturing
        if activity_id == ACTIVITY_REACTION:
            return (1800, activity_id)  # Default 30 minutes for reactions
        else:
            return (3600, activity_id)  # Default 1 hour for manufacturing

    except Exception as e:
        logger.error(f"Error getting time for blueprint {blueprint_type_id}: {e}")
        return (0, activity_id)


def build_dependency_tree(jobs_data: List[dict]) -> Dict[int, List[int]]:
    """
    Build dependency tree for production items.

    Args:
        jobs_data: List of job dicts with item_type_id and materials

    Returns:
        Dict mapping item_type_id to list of item_type_ids it depends on
    """
    from indy_hub.models import SdeIndustryActivityMaterial

    dependencies: Dict[int, List[int]] = {}
    producing_items = {job['item_type_id'] for job in jobs_data}

    for job in jobs_data:
        item_type_id = job['item_type_id']
        blueprint_type_id = job.get('blueprint_type_id')

        if not blueprint_type_id:
            dependencies[item_type_id] = []
            continue

        # Detect activity type (manufacturing vs reactions)
        activity_id = detect_blueprint_activity_type(blueprint_type_id)

        # Get materials required for this blueprint
        materials = SdeIndustryActivityMaterial.objects.filter(
            eve_type_id=blueprint_type_id,
            activity_id=activity_id
        ).values_list('material_eve_type_id', flat=True)

        # Only include dependencies that are also being produced in this plan
        item_dependencies = [
            mat_id for mat_id in materials
            if mat_id in producing_items
        ]

        dependencies[item_type_id] = item_dependencies

    return dependencies


def schedule_jobs_critical_path(
    jobs: List[ManufacturingJob],
    slots: List[IndustrySlot]
) -> BuildSchedule:
    """
    Schedule jobs using critical path method to minimize total completion time.

    This algorithm:
    1. Identifies dependencies between jobs
    2. Calculates earliest start time for each job based on dependencies
    3. Assigns jobs to slots to minimize total completion time
    4. Identifies the critical path (longest dependency chain)

    Args:
        jobs: List of manufacturing jobs to schedule
        slots: Available industry slots

    Returns:
        Complete build schedule with optimized slot assignments
    """
    if not jobs or not slots:
        return BuildSchedule(
            jobs=[],
            slots=slots,
            total_sequential_time_seconds=0,
            total_parallel_time_seconds=0
        )

    # Build dependency map
    dep_map: Dict[int, Set[int]] = {job.item_type_id: set(job.dependencies) for job in jobs}
    job_map: Dict[int, ManufacturingJob] = {job.item_type_id: job for job in jobs}

    # Calculate earliest start times based on dependencies
    earliest_start: Dict[int, int] = {}

    def calc_earliest_start(item_id: int) -> int:
        """Recursively calculate earliest start time."""
        if item_id in earliest_start:
            return earliest_start[item_id]

        deps = dep_map.get(item_id, set())
        if not deps:
            earliest_start[item_id] = 0
            return 0

        # Must wait for all dependencies to complete
        max_dep_end = max(
            calc_earliest_start(dep_id) + job_map[dep_id].total_time_seconds
            for dep_id in deps
            if dep_id in job_map
        )

        earliest_start[item_id] = max_dep_end
        return max_dep_end

    # Calculate earliest start for all jobs
    for job in jobs:
        calc_earliest_start(job.item_type_id)

    # Sort jobs by earliest start time (topological sort with time priority)
    sorted_jobs = sorted(jobs, key=lambda j: (earliest_start.get(j.item_type_id, 0), -j.total_time_seconds))

    # Reset slot state
    for slot in slots:
        slot.jobs = []
        slot.available_at_seconds = 0

    # Assign jobs to slots
    for job in sorted_jobs:
        earliest = earliest_start.get(job.item_type_id, 0)

        # Find slot that can start this job earliest
        best_slot = min(slots, key=lambda s: max(s.available_at_seconds, earliest))
        start_time = max(best_slot.available_at_seconds, earliest)

        best_slot.add_job(job, start_time)

    # Calculate totals
    total_sequential = sum(job.total_time_seconds for job in jobs)
    total_parallel = max(slot.available_at_seconds for slot in slots) if slots else 0

    # Find critical path (longest chain of dependencies)
    critical_path = find_critical_path(jobs, dep_map, job_map)

    # Generate recommendations
    recommendations = generate_recommendations(jobs, slots, total_parallel, critical_path, job_map)

    return BuildSchedule(
        jobs=sorted_jobs,
        slots=slots,
        total_sequential_time_seconds=total_sequential,
        total_parallel_time_seconds=total_parallel,
        critical_path=critical_path,
        recommendations=recommendations
    )


def find_critical_path(
    jobs: List[ManufacturingJob],
    dep_map: Dict[int, Set[int]],
    job_map: Dict[int, ManufacturingJob]
) -> List[int]:
    """Find the critical path (longest dependency chain) in the job graph."""

    def longest_path_from(item_id: int, visited: Set[int]) -> Tuple[int, List[int]]:
        """Find longest path from this item."""
        if item_id in visited:
            return 0, []

        visited.add(item_id)
        job = job_map.get(item_id)

        if not job:
            return 0, []

        deps = dep_map.get(item_id, set())
        if not deps:
            return job.total_time_seconds, [item_id]

        # Find longest path through dependencies
        best_length = 0
        best_path = []

        for dep_id in deps:
            if dep_id in job_map:
                dep_length, dep_path = longest_path_from(dep_id, visited.copy())
                if dep_length > best_length:
                    best_length = dep_length
                    best_path = dep_path

        return best_length + job.total_time_seconds, best_path + [item_id]

    # Find the longest path overall
    max_length = 0
    critical_path = []

    for job in jobs:
        length, path = longest_path_from(job.item_type_id, set())
        if length > max_length:
            max_length = length
            critical_path = path

    return critical_path


def generate_recommendations(
    jobs: List[ManufacturingJob],
    slots: List[IndustrySlot],
    total_time: int,
    critical_path: List[int],
    job_map: Dict[int, ManufacturingJob]
) -> List[str]:
    """Generate optimization recommendations based on schedule analysis."""
    recommendations = []

    # Check slot utilization
    if slots:
        avg_utilization = sum(slot.available_at_seconds for slot in slots) / len(slots)
        utilization_pct = (avg_utilization / max(total_time, 1)) * 100

        if utilization_pct < 50:
            recommendations.append(
                f"Low slot utilization ({utilization_pct:.0f}%). Consider using fewer slots or adding more jobs."
            )

        # Check for idle slots
        idle_slots = [slot for slot in slots if len(slot.jobs) == 0]
        if idle_slots:
            recommendations.append(
                f"{len(idle_slots)} slot(s) have no jobs assigned. Consider removing unused slots."
            )

    # Analyze critical path
    if critical_path and len(critical_path) > 1:
        critical_items = [job_map[item_id].item_name for item_id in critical_path if item_id in job_map]
        recommendations.append(
            f"Critical path: {' → '.join(critical_items)}. Optimizing these items will reduce total time."
        )

    # Check for long-running jobs
    if jobs:
        longest_job = max(jobs, key=lambda j: j.total_time_seconds)
        if longest_job.total_time_seconds > total_time * 0.3:
            recommendations.append(
                f"'{longest_job.item_name}' takes {format_time_duration(longest_job.total_time_seconds)} "
                f"({(longest_job.total_time_seconds/max(total_time,1)*100):.0f}% of total time). "
                f"Consider splitting runs across multiple jobs or improving TE."
            )

    return recommendations
