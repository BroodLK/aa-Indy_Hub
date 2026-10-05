"""
Build scheduling and time optimization service.

Calculates manufacturing times, handles dependencies, and optimizes slot usage
for minimal completion time.
"""

from __future__ import annotations

# Standard Library
import math
from dataclasses import dataclass, field
from functools import lru_cache

# Alliance Auth
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
    dependencies: list[int] = field(default_factory=list)
    required_skills: list[dict] = field(default_factory=list)
    chunk_index: int = 1
    chunk_count: int = 1

    # Scheduling info
    assigned_slot: int | None = None
    start_time_seconds: int = 0
    end_time_seconds: int = 0

    # Multi-product metadata
    is_final_product: bool = False
    parent_product_type_ids: list[int] = field(default_factory=list)

    def __post_init__(self):
        """Calculate total time after initialization."""
        if self.job_id <= 0:
            self.job_id = self.item_type_id
        if self.total_time_seconds == 0:
            self.total_time_seconds = self.adjusted_time_seconds * self.runs_required

    @property
    def is_shared(self) -> bool:
        """Whether this intermediate job supplies multiple final products."""
        return len(self.parent_product_type_ids) > 1

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
    skill_levels: dict[int, int] = field(default_factory=dict)

    # Scheduling state
    jobs: list[ManufacturingJob] = field(default_factory=list)
    available_at_seconds: int = 0

    def add_job(self, job: ManufacturingJob, start_time: int):
        """Assign a job to this slot."""
        job.assigned_slot = self.slot_id
        job.start_time_seconds = start_time
        job.end_time_seconds = start_time + job.total_time_seconds
        self.jobs.append(job)
        self.available_at_seconds = job.end_time_seconds

    def meets_skill_requirements(self, requirements: list[dict]) -> bool:
        if not requirements:
            return True

        for requirement in requirements:
            skill_type_id = int(requirement.get("skill_type_id") or 0)
            required_level = int(requirement.get("level") or 0)
            if skill_type_id <= 0 or required_level <= 0:
                continue
            if int(self.skill_levels.get(skill_type_id) or 0) < required_level:
                return False

        return True


@dataclass
class BuildSchedule:
    """Complete build schedule with time estimates and slot assignments."""

    jobs: list[ManufacturingJob]
    slots: list[IndustrySlot]
    total_sequential_time_seconds: int
    total_parallel_time_seconds: int
    critical_path: list[int] = field(default_factory=list)
    recommendations: list[dict] = field(default_factory=list)
    schedule_mode: str = "fastest"
    requested_slot_count: int = 0
    used_slot_count: int = 0
    final_product_item_type_id: int | None = None
    final_product_item_type_ids: list[int] = field(default_factory=list)
    final_assembly_priority: str = "parallel"
    component_completion_time_seconds: int | None = None
    component_target_time_seconds: int | None = None

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
        critical_path_duration_seconds = sum(
            jobs_by_id[job_id].total_time_seconds
            for job_id in critical_path_job_ids
            if job_id in jobs_by_id
        )

        total_busy = sum(slot_busy_seconds(slot) for slot in self.slots)
        total_idle = sum(slot_idle_seconds(slot) for slot in self.slots)
        total_capacity = len(self.slots) * self.total_parallel_time_seconds
        overall_utilization_pct = round(
            (total_busy / max(total_capacity, 1)) * 100, 1
        )

        all_final_type_ids = set(self.final_product_item_type_ids)
        if self.final_product_item_type_id:
            all_final_type_ids.add(self.final_product_item_type_id)

        final_jobs = [
            job
            for job in self.jobs
            if job.is_final_product or job.item_type_id in all_final_type_ids
        ]
        final_assembly_start = (
            min((job.start_time_seconds for job in final_jobs), default=None)
            if final_jobs
            else None
        )
        final_assembly_end = (
            max((job.end_time_seconds for job in final_jobs), default=None)
            if final_jobs
            else None
        )

        # Per-product schedule breakdown
        product_type_ids: list[int] = list(self.final_product_item_type_ids)
        if (
            self.final_product_item_type_id
            and self.final_product_item_type_id not in product_type_ids
        ):
            product_type_ids.append(self.final_product_item_type_id)
        if not product_type_ids:
            product_type_ids = list(
                dict.fromkeys(
                    job.item_type_id for job in self.jobs if job.is_final_product
                )
            )

        products_summary: list[dict] = []
        for pid in product_type_ids:
            p_final_jobs = [j for j in self.jobs if j.item_type_id == pid]
            p_all_jobs = [
                j
                for j in self.jobs
                if pid in getattr(j, "parent_product_type_ids", [])
                or j.item_type_id == pid
            ]
            if not p_all_jobs:
                continue

            p_name = (
                p_final_jobs[0].item_name
                if p_final_jobs
                else p_all_jobs[0].item_name
            )
            p_runs = (
                sum(j.runs_required for j in p_final_jobs)
                if p_final_jobs
                else sum(j.runs_required for j in p_all_jobs)
            )
            p_start = min(j.start_time_seconds for j in p_all_jobs)
            p_end = max(
                (j.end_time_seconds for j in p_final_jobs),
                default=max(j.end_time_seconds for j in p_all_jobs),
            )
            p_seq_time = sum(j.total_time_seconds for j in p_all_jobs)

            p_job_ids = {j.job_id for j in p_all_jobs}
            p_dep_map = {
                jid: {d for d in jobs_by_id[jid].dependencies if d in p_job_ids}
                for jid in p_job_ids
                if jid in jobs_by_id
            }
            p_crit_job_ids = find_critical_path(p_all_jobs, p_dep_map, jobs_by_id)
            p_crit_items = list(
                dict.fromkeys(
                    jobs_by_id[jid].item_type_id
                    for jid in p_crit_job_ids
                    if jid in jobs_by_id
                )
            )

            products_summary.append(
                {
                    "product_type_id": pid,
                    "product_name": p_name,
                    "runs_required": p_runs,
                    "start_time_seconds": p_start,
                    "start_time_formatted": format_time_duration(p_start),
                    "completion_time_seconds": p_end,
                    "completion_time_formatted": format_time_duration(p_end),
                    "duration_seconds": max(0, p_end - p_start),
                    "duration_formatted": format_time_duration(max(0, p_end - p_start)),
                    "sequential_time_seconds": p_seq_time,
                    "sequential_time_formatted": format_time_duration(p_seq_time),
                    "critical_path": p_crit_items,
                    "critical_path_job_ids": p_crit_job_ids,
                }
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
                    "total_time_formatted": format_time_duration(
                        job.total_time_seconds
                    ),
                    "assigned_slot": job.assigned_slot,
                    "start_time_seconds": job.start_time_seconds,
                    "start_time_formatted": format_time_duration(
                        job.start_time_seconds
                    ),
                    "end_time_seconds": job.end_time_seconds,
                    "end_time_formatted": format_time_duration(job.end_time_seconds),
                    "dependencies": job.dependencies,
                    "required_skills": job.required_skills,
                    "activity_id": job.activity_id,
                    "activity_name": job.activity_name,
                    "is_final_product": job.is_final_product,
                    "parent_product_type_ids": list(job.parent_product_type_ids),
                    "is_shared": job.is_shared,
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
                    "runs_assigned": sum(job.runs_required for job in slot.jobs),
                    "utilization_percent": round(
                        (
                            slot_busy_seconds(slot)
                            / max(self.total_parallel_time_seconds, 1)
                        )
                        * 100,
                        1,
                    ),
                    "span_percent": round(
                        (
                            slot.available_at_seconds
                            / max(self.total_parallel_time_seconds, 1)
                        )
                        * 100,
                        1,
                    ),
                    "busy_time_seconds": slot_busy_seconds(slot),
                    "busy_time_formatted": format_time_duration(
                        slot_busy_seconds(slot)
                    ),
                    "idle_time_seconds": slot_idle_seconds(slot),
                    "idle_time_formatted": format_time_duration(
                        slot_idle_seconds(slot)
                    ),
                    "completion_time_seconds": slot.available_at_seconds,
                    "completion_time_formatted": format_time_duration(
                        slot.available_at_seconds
                    ),
                }
                for slot in self.slots
            ],
            "slot_metrics": {
                "total_slots": len(self.slots),
                "total_busy_time_seconds": total_busy,
                "total_busy_time_formatted": format_time_duration(total_busy),
                "total_idle_time_seconds": total_idle,
                "total_idle_time_formatted": format_time_duration(total_idle),
                "total_capacity_seconds": total_capacity,
                "overall_utilization_percent": overall_utilization_pct,
            },
            "products": products_summary,
            "critical_path": critical_path_item_ids,
            "critical_path_job_ids": critical_path_job_ids,
            "critical_path_duration_seconds": critical_path_duration_seconds,
            "critical_path_duration_formatted": format_time_duration(
                critical_path_duration_seconds
            ),
            "recommendations": self.recommendations,
            "schedule_mode": self.schedule_mode,
            "requested_slot_count": self.requested_slot_count,
            "used_slot_count": self.used_slot_count or len(self.slots),
            "final_product_item_type_id": self.final_product_item_type_id,
            "final_product_item_type_ids": self.final_product_item_type_ids,
            "final_assembly_priority": self.final_assembly_priority,
            "final_assembly_start_time_seconds": final_assembly_start,
            "final_assembly_start_time_formatted": (
                format_time_duration(final_assembly_start)
                if final_assembly_start is not None
                else None
            ),
            "final_assembly_completion_time_seconds": final_assembly_end,
            "final_assembly_completion_time_formatted": (
                format_time_duration(final_assembly_end)
                if final_assembly_end is not None
                else None
            ),
            "component_completion_time_seconds": self.component_completion_time_seconds,
            "component_completion_time_formatted": (
                format_time_duration(self.component_completion_time_seconds)
                if self.component_completion_time_seconds is not None
                else None
            ),
            "component_target_time_seconds": self.component_target_time_seconds,
            "component_target_time_formatted": (
                format_time_duration(self.component_target_time_seconds)
                if self.component_target_time_seconds is not None
                else None
            ),
            "component_target_met": (
                self.component_target_time_seconds is None
                or (
                    self.component_completion_time_seconds is not None
                    and self.component_completion_time_seconds
                    <= self.component_target_time_seconds
                )
            ),
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
        # AA Example App
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
        logger.warning(
            "Error detecting activity type for %s: %s", blueprint_type_id, exc
        )
        return ACTIVITY_MANUFACTURING


def get_base_manufacturing_time(
    blueprint_type_id: int,
    activity_id: int | None = None,
) -> tuple[int, int]:
    """
    Get base manufacturing/reaction time for a blueprint from eve_sde.
    """
    try:
        # Alliance Auth (External Libs)
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


@lru_cache(maxsize=512)
def get_blueprint_skill_requirements(
    blueprint_type_id: int,
    activity_id: int | None = None,
) -> tuple[tuple[int, int, str], ...]:
    """Return the blueprint skill requirements for the requested activity."""
    if blueprint_type_id <= 0:
        return ()

    if activity_id is None:
        activity_id = detect_blueprint_activity_type(blueprint_type_id)

    try:
        # Third Party
        from eveuniverse.models import EveIndustryActivitySkill
    except ImportError:
        logger.warning("eveuniverse not available for blueprint skill lookup")
        return ()

    try:
        requirements = (
            EveIndustryActivitySkill.objects.filter(
                eve_type_id=blueprint_type_id,
                activity_id=activity_id,
            )
            .select_related("skill_eve_type")
            .order_by("skill_eve_type_id")
        )
        normalized: list[tuple[int, int, str]] = []
        for requirement in requirements:
            skill_name = (
                str(
                    getattr(getattr(requirement, "skill_eve_type", None), "name", "")
                ).strip()
                or f"Skill {int(requirement.skill_eve_type_id)}"
            )
            normalized.append(
                (
                    int(requirement.skill_eve_type_id),
                    int(requirement.level or 0),
                    skill_name,
                )
            )
        return tuple(normalized)
    except Exception as exc:
        logger.warning(
            "Error getting skill requirements for blueprint %s: %s",
            blueprint_type_id,
            exc,
        )
        return ()


def format_skill_requirements(
    requirements: list[dict] | tuple[tuple[int, int, str], ...],
) -> str:
    """Return a compact human-readable requirements string."""
    parts: list[str] = []
    for requirement in requirements or []:
        if isinstance(requirement, dict):
            skill_name = str(requirement.get("skill_name") or "").strip()
            level = int(requirement.get("level") or 0)
        else:
            _skill_type_id, level, skill_name = requirement
            skill_name = str(skill_name or "").strip()
        if not skill_name or level <= 0:
            continue
        parts.append(f"{skill_name} {level}")
    return ", ".join(parts)


def build_dependency_tree(jobs_data: list[dict]) -> dict[int, list[int]]:
    """
    Build dependency tree for production items.

    Returns a mapping of item_type_id -> dependent item_type_ids.
    """
    # AA Example App
    from indy_hub.models import SdeIndustryActivityMaterial

    dependencies: dict[int, list[int]] = {}
    producing_items = {job["item_type_id"] for job in jobs_data}

    for job in jobs_data:
        item_type_id = job["item_type_id"]
        # If job already specified dependencies explicitly
        if "dependencies" in job and job["dependencies"] is not None:
            dependencies[item_type_id] = [
                dep for dep in job["dependencies"] if dep in producing_items
            ]
            continue

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


def split_runs_evenly(total_runs: int, chunk_count: int) -> list[int]:
    """Split runs into near-equal chunks."""
    normalized_runs = max(0, int(total_runs))
    normalized_chunks = max(1, int(chunk_count))
    if normalized_runs <= 0:
        return []

    active_chunks = min(normalized_runs, normalized_chunks)
    base_runs = normalized_runs // active_chunks
    remainder = normalized_runs % active_chunks
    return [
        base_runs + (1 if index < remainder else 0) for index in range(active_chunks)
    ]


def split_jobs_evenly_across_slots(
    jobs: list[ManufacturingJob],
    total_slot_count: int,
) -> list[ManufacturingJob]:
    """
    Split each item into balanced jobs so available slot lanes can work in parallel.

    Dependencies remain conservative: any downstream split job waits until all
    split jobs for each dependency item have completed.
    """
    normalized_slot_count = max(1, int(total_slot_count))
    expanded_jobs: list[ManufacturingJob] = []
    jobs_by_item_type: dict[int, list[ManufacturingJob]] = {}
    next_job_id = 1

    for job in jobs:
        run_chunks = split_runs_evenly(job.runs_required, normalized_slot_count)
        if not run_chunks:
            continue

        chunk_count = len(run_chunks)
        remaining_quantity = max(0, int(job.quantity_needed))
        split_jobs: list[ManufacturingJob] = []

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
                required_skills=list(job.required_skills or []),
                chunk_index=chunk_index,
                chunk_count=chunk_count,
                is_final_product=job.is_final_product,
                parent_product_type_ids=list(job.parent_product_type_ids or []),
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
                for dependency_job in jobs_by_item_type.get(dependency_item_type_id, [])
            )
        )
        for split_job in jobs_by_item_type.get(original_job.item_type_id, []):
            split_job.dependencies = dependency_job_ids.copy()

    return expanded_jobs


def clone_slot(slot: IndustrySlot) -> IndustrySlot:
    """Create a fresh slot instance for a scheduling attempt."""
    return IndustrySlot(
        slot_id=slot.slot_id,
        character_id=slot.character_id,
        character_name=slot.character_name,
        slot_name=slot.slot_name,
        max_concurrent_jobs=slot.max_concurrent_jobs,
        skill_levels=dict(slot.skill_levels or {}),
    )


def component_completion_time_seconds(
    jobs: list[ManufacturingJob],
    final_product_item_type_id: int | None = None,
    final_product_item_type_ids: list[int] | None = None,
) -> int:
    """Return when all non-final-product jobs have completed."""
    if not jobs:
        return 0

    all_final_type_ids = set()
    if final_product_item_type_ids:
        all_final_type_ids.update(final_product_item_type_ids)
    if final_product_item_type_id:
        all_final_type_ids.add(final_product_item_type_id)

    if not all_final_type_ids:
        all_final_type_ids = {
            job.item_type_id for job in jobs if job.is_final_product
        }

    if not all_final_type_ids:
        return max((job.end_time_seconds for job in jobs), default=0)

    component_jobs = [
        job
        for job in jobs
        if job.item_type_id not in all_final_type_ids and not job.is_final_product
    ]
    if not component_jobs:
        return 0

    return max((job.end_time_seconds for job in component_jobs), default=0)


def _slot_capability_score(
    slot: IndustrySlot, jobs: list[ManufacturingJob]
) -> tuple[int, int]:
    """Score how broadly a slot can satisfy the selected job set."""
    eligible_jobs = [
        job for job in jobs if slot.meets_skill_requirements(job.required_skills)
    ]
    eligible_count = len(eligible_jobs)
    distinct_skills = len(
        {
            int(requirement.get("skill_type_id") or 0)
            for job in eligible_jobs
            for requirement in (job.required_skills or [])
            if int(requirement.get("skill_type_id") or 0) > 0
        }
    )
    return (eligible_count, distinct_skills)


def select_slot_subset(
    slots: list[IndustrySlot],
    jobs: list[ManufacturingJob],
    slot_limit: int,
) -> list[IndustrySlot]:
    """Pick the most capable subset of slots while preserving display order."""
    normalized_limit = max(0, min(int(slot_limit), len(slots)))
    if normalized_limit <= 0:
        return []

    slot_scores = {slot.slot_id: _slot_capability_score(slot, jobs) for slot in slots}
    ranked_slots = sorted(
        slots,
        key=lambda slot: (
            -slot_scores.get(slot.slot_id, (0, 0))[0],
            -slot_scores.get(slot.slot_id, (0, 0))[1],
            slot.slot_id,
        ),
    )
    selected_ids = {slot.slot_id for slot in ranked_slots[:normalized_limit]}
    return [clone_slot(slot) for slot in slots if slot.slot_id in selected_ids]


def _build_schedule_with_slot_limit(
    original_jobs: list[ManufacturingJob],
    available_slots: list[IndustrySlot],
    slot_limit: int,
    *,
    schedule_mode: str,
    final_product_item_type_id: int | None = None,
    final_product_item_type_ids: list[int] | None = None,
    final_assembly_priority: str = "parallel",
    product_priorities: list[int] | None = None,
    component_target_time_seconds: int | None = None,
) -> BuildSchedule:
    """Build a schedule variant for a specific number of active slots."""
    selected_slots = select_slot_subset(available_slots, original_jobs, slot_limit)
    split_jobs = split_jobs_evenly_across_slots(original_jobs, len(selected_slots))
    schedule = schedule_jobs_critical_path(
        split_jobs,
        selected_slots,
        preferred_item_type_id=final_product_item_type_id,
        preferred_item_type_ids=final_product_item_type_ids,
        final_product_item_type_ids=final_product_item_type_ids,
        final_assembly_priority=final_assembly_priority,
        product_priorities=product_priorities,
    )
    schedule.schedule_mode = schedule_mode
    schedule.requested_slot_count = len(available_slots)
    schedule.used_slot_count = len(selected_slots)
    schedule.final_product_item_type_id = final_product_item_type_id
    schedule.final_product_item_type_ids = (
        final_product_item_type_ids
        or ([final_product_item_type_id] if final_product_item_type_id else [])
    )
    schedule.final_assembly_priority = final_assembly_priority
    schedule.component_completion_time_seconds = component_completion_time_seconds(
        schedule.jobs,
        final_product_item_type_id=final_product_item_type_id,
        final_product_item_type_ids=schedule.final_product_item_type_ids,
    )
    schedule.component_target_time_seconds = component_target_time_seconds
    return schedule


def calculate_schedule_for_mode(
    original_jobs: list[ManufacturingJob],
    available_slots: list[IndustrySlot],
    *,
    schedule_mode: str = "fastest",
    final_product_item_type_id: int | None = None,
    final_product_item_type_ids: list[int] | None = None,
    final_assembly_priority: str = "parallel",
    product_priorities: list[int] | None = None,
    component_target_time_seconds: int | None = None,
) -> BuildSchedule:
    """Calculate a schedule using the requested optimization mode."""
    normalized_mode = str(schedule_mode or "fastest").strip().lower() or "fastest"
    slot_count = len(available_slots)

    if normalized_mode == "fewest_slots":
        last_error: ValueError | None = None
        for candidate_slot_count in range(1, slot_count + 1):
            try:
                return _build_schedule_with_slot_limit(
                    original_jobs,
                    available_slots,
                    candidate_slot_count,
                    schedule_mode=normalized_mode,
                    final_product_item_type_id=final_product_item_type_id,
                    final_product_item_type_ids=final_product_item_type_ids,
                    final_assembly_priority=final_assembly_priority,
                    product_priorities=product_priorities,
                )
            except ValueError as exc:
                last_error = exc
        if last_error:
            raise last_error

    if normalized_mode == "component_target":
        if component_target_time_seconds is None or component_target_time_seconds <= 0:
            raise ValueError("Enter a valid component completion target in days.")

        best_fallback: BuildSchedule | None = None
        last_error: ValueError | None = None

        for candidate_slot_count in range(1, slot_count + 1):
            try:
                schedule = _build_schedule_with_slot_limit(
                    original_jobs,
                    available_slots,
                    candidate_slot_count,
                    schedule_mode=normalized_mode,
                    final_product_item_type_id=final_product_item_type_id,
                    final_product_item_type_ids=final_product_item_type_ids,
                    final_assembly_priority=final_assembly_priority,
                    product_priorities=product_priorities,
                    component_target_time_seconds=component_target_time_seconds,
                )
            except ValueError as exc:
                last_error = exc
                continue

            best_fallback = schedule
            if (
                schedule.component_completion_time_seconds is not None
                and schedule.component_completion_time_seconds
                <= component_target_time_seconds
            ):
                schedule.recommendations.insert(
                    0,
                    {
                        "code": "component_target_met",
                        # Good news; it was previously rendered as a warning.
                        "severity": "success",
                        "message": (
                            "Components can be ready in "
                            + format_time_duration(
                                schedule.component_completion_time_seconds
                            )
                        ),
                        "params": {
                            "seconds": schedule.component_completion_time_seconds,
                            "slots_used": candidate_slot_count,
                        },
                        "target": None,
                    },
                )
                return schedule

        if best_fallback is not None:
            best_fallback.recommendations.insert(
                0,
                {
                    "code": "component_target_missed",
                    "severity": "warning",
                    "message": (
                        "Component target not met. Fastest component completion with "
                        "the selected slots is "
                        f"{format_time_duration(best_fallback.component_completion_time_seconds or 0)}."
                    ),
                    "params": {
                        "seconds": best_fallback.component_completion_time_seconds or 0,
                        "target_seconds": component_target_time_seconds,
                    },
                    "target": {"element_id": "buildScheduleTargetDays"},
                },
            )
            return best_fallback

        if last_error:
            raise last_error

    return _build_schedule_with_slot_limit(
        original_jobs,
        available_slots,
        slot_count,
        schedule_mode="fastest",
        final_product_item_type_id=final_product_item_type_id,
        final_product_item_type_ids=final_product_item_type_ids,
        final_assembly_priority=final_assembly_priority,
        product_priorities=product_priorities,
    )


def schedule_jobs_critical_path(
    jobs: list[ManufacturingJob],
    slots: list[IndustrySlot],
    *,
    preferred_item_type_id: int | None = None,
    preferred_item_type_ids: list[int] | None = None,
    final_product_item_type_ids: list[int] | None = None,
    final_assembly_priority: str = "parallel",
    product_priorities: list[int] | None = None,
) -> BuildSchedule:
    """
    Schedule jobs using a dependency-aware critical-path approach for single or multi-root DAGs.
    """
    if not jobs or not slots:
        return BuildSchedule(
            jobs=[],
            slots=slots,
            total_sequential_time_seconds=0,
            total_parallel_time_seconds=0,
            final_product_item_type_id=preferred_item_type_id,
            final_product_item_type_ids=final_product_item_type_ids or [],
            final_assembly_priority=final_assembly_priority,
        )

    dep_map: dict[int, set[int]] = {job.job_id: set(job.dependencies) for job in jobs}
    job_map: dict[int, ManufacturingJob] = {job.job_id: job for job in jobs}
    reverse_dep_map: dict[int, set[int]] = {job.job_id: set() for job in jobs}
    for job in jobs:
        for dep_id in dep_map.get(job.job_id, set()):
            if dep_id in reverse_dep_map:
                reverse_dep_map[dep_id].add(job.job_id)

    # Determine final product IDs in priority order
    final_ids: list[int] = []
    if product_priorities:
        final_ids = [int(pid) for pid in product_priorities if int(pid or 0) > 0]
    elif preferred_item_type_ids:
        final_ids = [int(pid) for pid in preferred_item_type_ids if int(pid or 0) > 0]
    elif final_product_item_type_ids:
        final_ids = [int(pid) for pid in final_product_item_type_ids if int(pid or 0) > 0]
    elif preferred_item_type_id:
        final_ids = [int(preferred_item_type_id)]

    all_dep_target_ids = {dep_id for deps in dep_map.values() for dep_id in deps}
    if not final_ids:
        final_ids = list(
            dict.fromkeys(
                job.item_type_id
                for job in jobs
                if job.is_final_product or job.job_id not in all_dep_target_ids
            )
        )

    # Calculate reaching products for each job
    product_job_ids: dict[int, set[int]] = {
        pid: {job.job_id for job in jobs if job.item_type_id == pid}
        for pid in final_ids
    }

    reaches_cache: dict[tuple[int, int], bool] = {}

    def job_reaches_product(job_id: int, product_type_id: int) -> bool:
        key = (job_id, product_type_id)
        if key in reaches_cache:
            return reaches_cache[key]
        if job_id in product_job_ids.get(product_type_id, set()):
            reaches_cache[key] = True
            return True
        reaches = any(
            job_reaches_product(child_id, product_type_id)
            for child_id in reverse_dep_map.get(job_id, set())
        )
        reaches_cache[key] = reaches
        return reaches

    # Tag jobs with parent_product_type_ids and is_final_product
    for job in jobs:
        reached = [pid for pid in final_ids if job_reaches_product(job.job_id, pid)]
        if reached:
            job.parent_product_type_ids = reached
        if job.item_type_id in final_ids:
            job.is_final_product = True

    generic_tail_cache: dict[int, int] = {}
    product_tail_cache: dict[tuple[int, int], int] = {}

    def calc_generic_tail(job_id: int) -> int:
        if job_id in generic_tail_cache:
            return generic_tail_cache[job_id]

        child_ids = reverse_dep_map.get(job_id, set())
        tail_seconds = job_map[job_id].total_time_seconds
        if child_ids:
            tail_seconds += max(calc_generic_tail(child_id) for child_id in child_ids)
        generic_tail_cache[job_id] = tail_seconds
        return tail_seconds

    def calc_product_tail(job_id: int, product_type_id: int) -> int:
        key = (job_id, product_type_id)
        if key in product_tail_cache:
            return product_tail_cache[key]

        if not job_reaches_product(job_id, product_type_id):
            product_tail_cache[key] = 0
            return 0

        child_tails = [
            calc_product_tail(child_id, product_type_id)
            for child_id in reverse_dep_map.get(job_id, set())
            if job_reaches_product(child_id, product_type_id)
        ]
        tail_seconds = job_map[job_id].total_time_seconds
        if child_tails:
            tail_seconds += max(child_tails)
        product_tail_cache[key] = tail_seconds
        return tail_seconds

    # Scheduling state
    scheduled_jobs: list[ManufacturingJob] = []
    completed_job_times: dict[int, int] = {}

    for slot in slots:
        slot.jobs = []
        slot.available_at_seconds = 0

    unscheduled_job_ids: set[int] = {job.job_id for job in jobs}

    is_prioritized = (
        str(final_assembly_priority).strip().lower() in ("prioritized", "sequential")
        or (preferred_item_type_id is not None and not final_product_item_type_ids)
    )

    while unscheduled_job_ids:
        best_choice_key: tuple[int, int, int, int, int, int, int] | None = None
        best_choice_slot: IndustrySlot | None = None
        best_choice_job: ManufacturingJob | None = None
        blocked_job_ids: set[int] = set()

        for job_id in list(unscheduled_job_ids):
            deps = dep_map.get(job_id, set())
            unresolved_deps = {
                dep_id
                for dep_id in deps
                if dep_id in job_map and dep_id not in completed_job_times
            }
            if unresolved_deps:
                blocked_job_ids.add(job_id)
                continue

            job = job_map[job_id]
            release_time = max(
                (
                    completed_job_times.get(dep_id, 0)
                    for dep_id in deps
                    if dep_id in job_map
                ),
                default=0,
            )
            eligible_slots = [
                slot
                for slot in slots
                if slot.meets_skill_requirements(job.required_skills)
            ]
            if not eligible_slots:
                requirements_text = format_skill_requirements(job.required_skills)
                if requirements_text:
                    raise ValueError(
                        f"No selected characters meet the skill requirements for "
                        f"{job.item_name}: {requirements_text}."
                    )
                raise ValueError(
                    f"No eligible industry slots are available for {job.item_name}."
                )

            best_slot = min(
                eligible_slots,
                key=lambda slot: (
                    max(slot.available_at_seconds, release_time),
                    slot.available_at_seconds,
                    slot.slot_id,
                ),
            )
            candidate_start = max(best_slot.available_at_seconds, release_time)

            if is_prioritized and final_ids:
                matching_indices = [
                    idx
                    for idx, pid in enumerate(final_ids)
                    if job_reaches_product(job_id, pid)
                ]
                if matching_indices:
                    best_product_idx = min(matching_indices)
                    priority_rank = best_product_idx
                    product_tail = calc_product_tail(job_id, final_ids[best_product_idx])
                else:
                    priority_rank = 999999
                    product_tail = 0
            else:
                priority_rank = 0
                product_tail = 0

            candidate_sort = (
                candidate_start,
                priority_rank,
                -product_tail,
                -calc_generic_tail(job_id),
                -job.total_time_seconds,
                best_slot.slot_id,
                job_id,
            )

            if best_choice_key is None or candidate_sort < best_choice_key:
                best_choice_key = candidate_sort
                best_choice_slot = best_slot
                best_choice_job = job

        if (
            best_choice_key is None
            or best_choice_slot is None
            or best_choice_job is None
        ):
            blocked_job_names = ", ".join(
                sorted(
                    job_map[job_id].item_name
                    for job_id in blocked_job_ids
                    if job_id in job_map
                )
            )
            raise ValueError(
                "Unable to resolve build dependencies while scheduling"
                + (f": {blocked_job_names}" if blocked_job_names else ".")
            )

        selected_slot = best_choice_slot
        selected_job = best_choice_job
        selected_start_time = best_choice_key[0]
        selected_slot.add_job(selected_job, selected_start_time)
        completed_job_times[selected_job.job_id] = selected_job.end_time_seconds
        scheduled_jobs.append(selected_job)
        unscheduled_job_ids.discard(selected_job.job_id)

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

    primary_final_id = (
        final_ids[0]
        if final_ids
        else (preferred_item_type_id or None)
    )

    comp_time = component_completion_time_seconds(
        scheduled_jobs,
        final_product_item_type_id=primary_final_id,
        final_product_item_type_ids=final_ids,
    )

    return BuildSchedule(
        jobs=scheduled_jobs,
        slots=slots,
        total_sequential_time_seconds=total_sequential,
        total_parallel_time_seconds=total_parallel,
        critical_path=critical_path,
        recommendations=recommendations,
        final_product_item_type_id=primary_final_id,
        final_product_item_type_ids=final_ids,
        final_assembly_priority=final_assembly_priority,
        component_completion_time_seconds=comp_time,
    )


def find_critical_path(
    jobs: list[ManufacturingJob],
    dep_map: dict[int, set[int]],
    job_map: dict[int, ManufacturingJob],
) -> list[int]:
    """Find the critical path (longest dependency chain) in the job graph."""

    def longest_path_from(job_id: int, visited: set[int]) -> tuple[int, list[int]]:
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
        best_path: list[int] = []
        for dep_id in deps:
            if dep_id not in job_map:
                continue
            dep_length, dep_path = longest_path_from(dep_id, visited.copy())
            if dep_length > best_length:
                best_length = dep_length
                best_path = dep_path

        return (best_length + job.total_time_seconds, best_path + [job_id])

    max_length = 0
    critical_path: list[int] = []
    for job in jobs:
        length, path = longest_path_from(job.job_id, set())
        if length > max_length:
            max_length = length
            critical_path = path

    return critical_path


def slot_busy_seconds(slot: IndustrySlot) -> int:
    """Time this lane actually spends running jobs."""
    return sum(job.total_time_seconds for job in slot.jobs)


def slot_idle_seconds(slot: IndustrySlot) -> int:
    """Time this lane spends waiting, before and between its jobs."""
    return max(0, slot.available_at_seconds - slot_busy_seconds(slot))


def generate_recommendations(
    jobs: list[ManufacturingJob],
    slots: list[IndustrySlot],
    total_time: int,
    critical_path: list[int],
    job_map: dict[int, ManufacturingJob],
) -> list[dict]:
    """Actionable schedule findings, each with a measured effect and a target.

    Returns structured records rather than sentences so the client can localize
    them and attach a focus affordance:

        {"code", "severity", "message", "params", "target"}

    Every number quoted here is measured from the schedule that was actually
    built. Where an effect is an estimate it is stated as an upper bound, not a
    promised saving -- nothing here re-runs the allocator.
    """
    recommendations: list[dict] = []
    span = max(total_time, 1)

    if slots:
        busy_total = sum(slot_busy_seconds(slot) for slot in slots)
        utilization_pct = (busy_total / (len(slots) * span)) * 100

        # Idle lanes, ranked. The old "low utilization" message used a
        # completion timestamp as if it were busy time, so it under-triggered.
        idle_ranked = sorted(slots, key=slot_idle_seconds, reverse=True)
        worst_idle = idle_ranked[0] if idle_ranked else None
        if worst_idle is not None and slot_idle_seconds(worst_idle) > 0:
            recommendations.append(
                {
                    "code": "lane_idle",
                    "severity": "info",
                    "message": (
                        f"{worst_idle.slot_name or worst_idle.character_name} is idle "
                        f"{format_time_duration(slot_idle_seconds(worst_idle))} of the "
                        f"{format_time_duration(span)} plan, waiting on earlier jobs. "
                        "Overall lane utilization is "
                        f"{utilization_pct:.0f}%."
                    ),
                    "params": {
                        "slot_id": worst_idle.slot_id,
                        "idle_seconds": slot_idle_seconds(worst_idle),
                        "utilization_pct": round(utilization_pct, 1),
                    },
                    "target": {"character_id": worst_idle.character_id},
                }
            )

        # Rebalancing: state the bound, not a promise. Moving work can only
        # close half the gap between the last and first lane to finish, and
        # cannot beat the per-run cost of the job being moved.
        busiest = max(slots, key=lambda slot: slot.available_at_seconds)
        lightest = min(slots, key=lambda slot: slot.available_at_seconds)
        gap = busiest.available_at_seconds - lightest.available_at_seconds
        if gap > 0 and busiest.jobs and busiest is not lightest:
            movable = max(busiest.jobs, key=lambda job: job.runs_required)
            per_run = max(1, movable.adjusted_time_seconds)
            runs_to_move = max(1, min(movable.runs_required, (gap // 2) // per_run))
            saving_bound = min(runs_to_move * per_run, gap // 2)
            if saving_bound > 0:
                recommendations.append(
                    {
                        "code": "rebalance_lanes",
                        "severity": "info",
                        "message": (
                            f"Move up to {runs_to_move} run(s) of {movable.item_name} from "
                            f"{busiest.slot_name or busiest.character_name} to "
                            f"{lightest.slot_name or lightest.character_name}: "
                            f"{busiest.slot_name or busiest.character_name} finishes "
                            f"{format_time_duration(gap)} after it, so this removes at most "
                            f"{format_time_duration(saving_bound)} from the total."
                        ),
                        "params": {
                            "from_slot_id": busiest.slot_id,
                            "to_slot_id": lightest.slot_id,
                            "runs": runs_to_move,
                            "max_saving_seconds": saving_bound,
                        },
                        "target": {"character_id": lightest.character_id},
                    }
                )

        empty_slots = [slot for slot in slots if not slot.jobs]
        if empty_slots:
            recommendations.append(
                {
                    "code": "empty_lanes",
                    "severity": "info",
                    "message": (
                        f"{len(empty_slots)} selected lane(s) received no work. "
                        "Reducing the slot count for those characters will not change "
                        "the finish time."
                    ),
                    "params": {
                        "slot_ids": [slot.slot_id for slot in empty_slots],
                    },
                    "target": {"character_id": empty_slots[0].character_id},
                }
            )

    if critical_path and len(critical_path) > 1:
        critical_items = list(
            dict.fromkeys(
                job_map[job_id].item_name
                for job_id in critical_path
                if job_id in job_map
            )
        )
        critical_seconds = sum(
            job_map[job_id].total_time_seconds
            for job_id in critical_path
            if job_id in job_map
        )
        recommendations.append(
            {
                "code": "critical_path",
                "severity": "info",
                "message": (
                    f"The critical path is {' -> '.join(critical_items)}, "
                    f"{format_time_duration(critical_seconds)} of the "
                    f"{format_time_duration(span)} plan. Only changes to these items "
                    "can shorten the total."
                ),
                "params": {
                    "items": critical_items,
                    "critical_seconds": critical_seconds,
                },
                "target": None,
            }
        )

    if jobs:
        longest_job = max(jobs, key=lambda job: job.total_time_seconds)
        share = longest_job.total_time_seconds / span * 100
        if share > 30:
            # TE is the lever that actually exists here: the runs are already
            # split across every selected lane, so "split it up" is not
            # actionable advice.
            recommendations.append(
                {
                    "code": "dominant_job",
                    "severity": "warning",
                    "message": (
                        f"{longest_job.item_name} takes "
                        f"{format_time_duration(longest_job.total_time_seconds)}, "
                        f"{share:.0f}% of the plan, at TE {longest_job.time_efficiency}. "
                        "Raising its TE or adding a lane for it is the only way to "
                        "shorten this."
                    ),
                    "params": {
                        "item_type_id": longest_job.item_type_id,
                        "seconds": longest_job.total_time_seconds,
                        "share_pct": round(share, 1),
                        "time_efficiency": longest_job.time_efficiency,
                    },
                    "target": {"element_id": "buildSkillAdvanced"},
                }
            )

    return recommendations
