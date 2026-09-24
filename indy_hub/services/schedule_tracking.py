"""Conservative reconciliation of planned manufacturing chunks with ESI jobs."""

# Future
from __future__ import annotations

# Standard Library
from datetime import datetime, timedelta
from typing import Any, Iterable

# Django
from django.utils import timezone

# Tolerance applied when comparing a planned window with an observed one. The
# planner works from estimated durations, so exact equality is never expected.
MATCH_WINDOW = timedelta(minutes=15)

# ESI job states that mean the chunk will never progress further.
TERMINAL_FAILED_STATUSES = {"cancelled", "canceled", "reverted"}
TERMINAL_DONE_STATUSES = {"delivered", "ready", "completed"}

# Cached job data older than this is reported as stale rather than current.
# Industry jobs are synced by a periodic task, so a gap this long means the
# sync is not reaching ESI for these characters.
TRACKING_STALE_AFTER = timedelta(hours=2)


def _datetime(value: Any):
    """Normalize browser ISO timestamps and model datetimes for matching."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if timezone.is_naive(parsed):
        return timezone.make_aware(parsed, timezone.get_current_timezone())
    return parsed


def _number(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _seconds(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _overlaps(left_start, left_end, right_start, right_end) -> bool:
    if not all((left_start, left_end, right_start, right_end)):
        return False
    return (
        left_start <= right_end + MATCH_WINDOW
        and right_start <= left_end + MATCH_WINDOW
    )


def _slot_characters(schedule: dict[str, Any]) -> dict[int, int]:
    """Map slot_id to character_id.

    BuildSchedule.to_dict records the character on the slot, not on the chunk,
    so a chunk's assigned_slot is the only link back to a character. Without
    this the single strongest match key would be unavailable.
    """
    mapping: dict[int, int] = {}
    slots = schedule.get("slots") if isinstance(schedule, dict) else None
    for slot in slots if isinstance(slots, list) else []:
        if not isinstance(slot, dict):
            continue
        slot_id = _number(slot.get("slot_id"))
        character_id = _number(slot.get("character_id"))
        if slot_id and character_id:
            mapping[slot_id] = character_id
    return mapping


def normalize_planned_chunks(
    schedule: dict[str, Any], *, anchor=None
) -> list[dict[str, Any]]:
    """Flatten a schedule payload into comparable planned chunks.

    start_time_seconds and end_time_seconds are offsets from the start of the
    plan, so an absolute anchor is required before they can be compared with
    ESI timestamps. Chunks that already carry absolute times are used as-is.
    """
    if not isinstance(schedule, dict):
        return []
    # Callers pass either a datetime or the ISO string the simulation stores,
    # so normalize here rather than relying on every call site to parse first.
    anchor = _datetime(anchor)
    slot_characters = _slot_characters(schedule)
    raw_jobs = schedule.get("jobs")
    chunks: list[dict[str, Any]] = []

    for index, chunk in enumerate(raw_jobs if isinstance(raw_jobs, list) else []):
        if not isinstance(chunk, dict):
            chunks.append({"index": index})
            continue

        character_id = _number(
            chunk.get("character_id") or chunk.get("installer_id")
        ) or slot_characters.get(_number(chunk.get("assigned_slot")) or 0)

        planned_start = _datetime(chunk.get("planned_start") or chunk.get("start"))
        planned_end = _datetime(chunk.get("planned_end") or chunk.get("end"))
        start_offset = _seconds(chunk.get("start_time_seconds"))
        end_offset = _seconds(chunk.get("end_time_seconds"))
        duration = _seconds(chunk.get("total_time_seconds"))
        if planned_start is None and anchor is not None and start_offset is not None:
            planned_start = anchor + timedelta(seconds=start_offset)
        if planned_end is None and anchor is not None:
            if end_offset is not None:
                planned_end = anchor + timedelta(seconds=end_offset)
            elif planned_start is not None and duration is not None:
                planned_end = planned_start + timedelta(seconds=duration)

        chunks.append(
            {
                "index": index,
                "label": str(chunk.get("job_label") or chunk.get("item_name") or ""),
                "character_id": character_id,
                "product_type_id": _number(
                    chunk.get("product_type_id") or chunk.get("item_type_id")
                ),
                "blueprint_type_id": _number(chunk.get("blueprint_type_id")),
                "station_id": _number(chunk.get("station_id")),
                "activity_id": _number(chunk.get("activity_id")),
                "runs": _number(chunk.get("runs_required") or chunk.get("runs")),
                "planned_start": planned_start,
                "planned_end": planned_end,
                "planned_duration_seconds": duration,
            }
        )
    return chunks


def planned_window(chunks: Iterable[dict[str, Any]]):
    """Return the (earliest start, latest end) covered by planned chunks."""
    materialized = list(chunks)
    starts = [c.get("planned_start") for c in materialized if c.get("planned_start")]
    ends = [c.get("planned_end") for c in materialized if c.get("planned_end")]
    if not starts or not ends:
        return None, None
    return min(starts) - MATCH_WINDOW, max(ends) + MATCH_WINDOW


def _job_status(job, observed_end, refreshed_at, planned_end=None) -> str:
    raw_status = str(getattr(job, "status", "") or "").strip().lower()
    if raw_status in TERMINAL_FAILED_STATUSES:
        return "cancelled"
    if raw_status in TERMINAL_DONE_STATUSES:
        return "completed"
    if observed_end and observed_end <= refreshed_at:
        return "completed"
    # Still running, but it will end (or should already have ended) later than
    # planned by more than the matching tolerance.
    if planned_end is not None:
        late_by_end = (
            observed_end is not None and observed_end > planned_end + MATCH_WINDOW
        )
        late_by_clock = refreshed_at > planned_end + MATCH_WINDOW
        if late_by_end or late_by_clock:
            return "delayed"
    return "running"


def _is_candidate(chunk: dict[str, Any], job) -> bool:
    """Every identifier present on both sides must agree; none may be guessed."""
    job_character = {
        _number(getattr(job, "character_id", None)),
        _number(getattr(job, "installer_id", None)),
    }
    if chunk["character_id"] not in job_character:
        return False

    job_activity = _number(getattr(job, "activity_id", None))
    if chunk["activity_id"] and job_activity and chunk["activity_id"] != job_activity:
        return False

    job_product = _number(getattr(job, "product_type_id", None))
    job_blueprint = _number(getattr(job, "blueprint_type_id", None))
    if chunk["product_type_id"]:
        if job_product:
            if chunk["product_type_id"] != job_product:
                return False
        elif not (
            chunk["blueprint_type_id"] and chunk["blueprint_type_id"] == job_blueprint
        ):
            # The job exposes no product, so only an explicit blueprint match is
            # acceptable. Comparing a product ID to a blueprint ID is not.
            return False

    if (
        chunk["blueprint_type_id"]
        and job_blueprint
        and chunk["blueprint_type_id"] != job_blueprint
    ):
        return False

    job_station = _number(getattr(job, "station_id", None))
    if chunk["station_id"] and job_station and chunk["station_id"] != job_station:
        return False

    job_runs = _number(getattr(job, "runs", None))
    if chunk["runs"] and job_runs and chunk["runs"] != job_runs:
        return False

    if chunk["planned_start"] and chunk["planned_end"]:
        if not _overlaps(
            chunk["planned_start"],
            chunk["planned_end"],
            _datetime(getattr(job, "start_date", None)),
            _datetime(getattr(job, "end_date", None)),
        ):
            return False
    return True


def reconcile_schedule_jobs(
    schedule: dict[str, Any], jobs: Iterable[Any], *, refreshed_at=None, anchor=None
) -> dict[str, Any]:
    """Match schedule chunks using multiple identifying fields.

    A character plus a product or blueprint is the minimum evidence accepted;
    item names alone are never sufficient. Each job is consumed once so a single
    live ESI row cannot satisfy multiple planned chunks.
    """
    refreshed_at = refreshed_at or timezone.now()
    anchor = _datetime(anchor)
    chunks = normalize_planned_chunks(schedule, anchor=anchor)
    available = list(jobs)
    consumed: set[int] = set()
    results = []

    for chunk in chunks:
        index = chunk.get("index", 0)
        if not chunk.get("character_id") or not (
            chunk.get("product_type_id") or chunk.get("blueprint_type_id")
        ):
            results.append(
                {
                    "index": index,
                    "status": "unmatched",
                    "reason": "insufficient identifying fields",
                }
            )
            continue

        candidates = []
        for job in available:
            job_id = _number(getattr(job, "job_id", None))
            if not job_id or job_id in consumed:
                continue
            if _is_candidate(chunk, job):
                candidates.append(job)

        if len(candidates) != 1:
            results.append(
                {
                    "index": index,
                    "status": "unmatched",
                    "reason": (
                        "multiple conservative matches"
                        if candidates
                        else "no conservative match"
                    ),
                }
            )
            continue

        job = candidates[0]
        consumed.add(_number(job.job_id))
        observed_start = _datetime(getattr(job, "start_date", None))
        observed_end = _datetime(getattr(job, "end_date", None))
        observed_duration = (
            int((observed_end - observed_start).total_seconds())
            if observed_start and observed_end
            else None
        )
        planned_duration = chunk.get("planned_duration_seconds")
        results.append(
            {
                "index": index,
                "status": _job_status(
                    job, observed_end, refreshed_at, chunk.get("planned_end")
                ),
                "job_id": int(job.job_id),
                "character_id": chunk["character_id"],
                "observed_start": (
                    observed_start.isoformat() if observed_start else None
                ),
                "observed_end": observed_end.isoformat() if observed_end else None,
                "planned_start": (
                    chunk["planned_start"].isoformat()
                    if chunk["planned_start"]
                    else None
                ),
                "planned_end": (
                    chunk["planned_end"].isoformat() if chunk["planned_end"] else None
                ),
                "planned_duration_seconds": planned_duration,
                "observed_duration_seconds": observed_duration,
                "duration_delta_seconds": (
                    observed_duration - planned_duration
                    if observed_duration is not None and planned_duration is not None
                    else None
                ),
            }
        )

    counts: dict[str, int] = {}
    for row in results:
        counts[row["status"]] = counts.get(row["status"], 0) + 1

    return {
        "refreshed_at": refreshed_at.isoformat(),
        "anchor": anchor.isoformat() if anchor else None,
        "results": results,
        "counts": counts,
        "matched": sum(1 for row in results if row.get("job_id")),
    }


def tracking_freshness(
    jobs_last_synced,
    *,
    has_scope: bool,
    now=None,
    stale_after: timedelta = TRACKING_STALE_AFTER,
) -> dict[str, Any]:
    """How far the reconciled job data can be trusted.

    ``unavailable``: no tracked character has a token with the industry jobs
    scope, so ESI cannot be asked at all. ``stale``: data exists but is older
    than ``stale_after`` (or was never synced). ``fresh`` otherwise. The planned
    schedule is never altered by any of these states.
    """
    now = now or timezone.now()
    last = _datetime(jobs_last_synced)
    if not has_scope:
        return {
            "state": "unavailable",
            "reason": "missing_scope",
            "jobs_last_synced": last.isoformat() if last else None,
        }
    if last is None:
        return {"state": "stale", "reason": "never_synced", "jobs_last_synced": None}
    if now - last > stale_after:
        return {
            "state": "stale",
            "reason": "sync_overdue",
            "jobs_last_synced": last.isoformat(),
        }
    return {"state": "fresh", "reason": "", "jobs_last_synced": last.isoformat()}
