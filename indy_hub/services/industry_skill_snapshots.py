"""Reusable industry skill snapshot and slot overview helpers."""

from __future__ import annotations

# Standard Library
from datetime import timedelta

# Django
from django.contrib.auth.models import User
from django.db.models import Count
from django.utils import timezone

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership
from allianceauth.services.hooks import get_extension_logger
from esi.models import Token

# AA Example App
from indy_hub.models import Blueprint, IndustryJob, IndustrySkillSnapshot
from indy_hub.services.esi_client import (
    ESITokenError,
    ESIUnmodifiedError,
    shared_client,
)
from indy_hub.utils.eve import get_character_name

logger = get_extension_logger(__name__)

SKILLS_SCOPE = "esi-skills.read_skills.v1"
SKILL_CACHE_TTL = timedelta(hours=1)
SKILL_TYPE_IDS = {
    "mass_production": 3387,
    "advanced_mass_production": 3388,
    "laboratory_operation": 3406,
    "advanced_laboratory_operation": 24624,
    "mass_reactions": 45746,
    "advanced_mass_reactions": 45748,
}
MANUFACTURING_ACTIVITY_IDS = {1}
RESEARCH_ACTIVITY_IDS = {3, 4, 5, 8}
REACTION_ACTIVITY_IDS = {9, 11}

_SKILLS_OPERATION_UNAVAILABLE = False

try:
    # Alliance Auth
    from esi.exceptions import HTTPNotModified
except ImportError:  # pragma: no cover - older django-esi
    HTTPNotModified = None


def fetch_character_skill_levels(
    character_id: int,
    *,
    force_refresh: bool = False,
) -> dict[int, dict[str, int]]:
    """Fetch raw skill levels for one character from ESI."""
    global _SKILLS_OPERATION_UNAVAILABLE

    if _SKILLS_OPERATION_UNAVAILABLE:
        raise ESIUnmodifiedError("ESI skills operation unavailable")

    token = Token.get_token(character_id, SKILLS_SCOPE)
    client = shared_client.client
    skills_resource = getattr(client, "Skills", None)
    operation_fn = None
    if skills_resource is not None:
        operation_fn = getattr(
            skills_resource,
            "get_characters_character_id_skills",
            None,
        ) or getattr(skills_resource, "GetCharactersCharacterIdSkills", None)
    if operation_fn is None:
        character_resource = client.Character
        operation_fn = getattr(
            character_resource,
            "get_characters_character_id_skills",
            None,
        ) or getattr(character_resource, "GetCharactersCharacterIdSkills", None)
    if not callable(operation_fn):
        _SKILLS_OPERATION_UNAVAILABLE = True
        raise ESIUnmodifiedError("ESI skills operation unavailable")

    request_kwargs = {"If-None-Match": ""} if force_refresh else {}
    try:
        result_obj = operation_fn(
            character_id=character_id,
            token=token,
            **request_kwargs,
        )
        payload = result_obj.results()
    except Exception as exc:
        if HTTPNotModified and isinstance(exc, HTTPNotModified):
            try:
                result_obj = operation_fn(
                    character_id=character_id,
                    token=token,
                    **request_kwargs,
                )
                payload = result_obj.results(use_cache=True)
            except Exception as cache_exc:
                logger.debug(
                    "Failed to retrieve cached skills for character %s after 304: %s",
                    character_id,
                    cache_exc,
                )
                raise ESIUnmodifiedError("ESI skills not modified and no cache available") from cache_exc
        else:
            exc_text = str(exc)
            if "GetCharactersCharacterIdSkills" in exc_text and "not found" in exc_text:
                _SKILLS_OPERATION_UNAVAILABLE = True
                raise ESIUnmodifiedError("ESI skills operation unavailable") from exc
            if "is not of type 'string'" in exc_text:
                access_token = token.valid_access_token()
                payload = operation_fn(
                    character_id=character_id,
                    token=access_token,
                    **request_kwargs,
                ).results()
            else:
                raise

    skills = payload.get("skills", []) if payload else []
    levels: dict[int, dict[str, int]] = {}
    for skill in skills:
        if not isinstance(skill, dict):
            continue
        skill_id = skill.get("skill_id")
        if not skill_id:
            continue
        active_level = int(skill.get("active_skill_level") or 0)
        trained_level = int(skill.get("trained_skill_level") or 0)
        levels[int(skill_id)] = {
            "active": active_level,
            "trained": trained_level,
        }
    return levels


def update_skill_snapshot(
    user: User,
    character_id: int,
    levels: dict[int, dict[str, int]],
) -> IndustrySkillSnapshot:
    """Persist the skill snapshot used by the slot overview and planner."""
    serialized_levels = {
        str(int(skill_id)): {
            "active": int((value or {}).get("active") or 0),
            "trained": int((value or {}).get("trained") or 0),
        }
        for skill_id, value in (levels or {}).items()
        if int(skill_id or 0) > 0
    }

    def _extract_levels(skill_id: int) -> tuple[int, int]:
        entry = levels.get(skill_id, 0)
        if isinstance(entry, dict):
            active_level = int(entry.get("active") or 0)
            trained_level = int(entry.get("trained") or 0)
        else:
            active_level = int(entry or 0)
            trained_level = active_level
        return active_level, trained_level

    mass_active, mass_trained = _extract_levels(SKILL_TYPE_IDS["mass_production"])
    adv_mass_active, adv_mass_trained = _extract_levels(SKILL_TYPE_IDS["advanced_mass_production"])
    lab_active, lab_trained = _extract_levels(SKILL_TYPE_IDS["laboratory_operation"])
    adv_lab_active, adv_lab_trained = _extract_levels(SKILL_TYPE_IDS["advanced_laboratory_operation"])
    react_active, react_trained = _extract_levels(SKILL_TYPE_IDS["mass_reactions"])
    adv_react_active, adv_react_trained = _extract_levels(SKILL_TYPE_IDS["advanced_mass_reactions"])

    return IndustrySkillSnapshot.objects.update_or_create(
        owner_user=user,
        character_id=character_id,
        defaults={
            "skill_levels": serialized_levels,
            "mass_production_level": mass_active,
            "advanced_mass_production_level": adv_mass_active,
            "laboratory_operation_level": lab_active,
            "advanced_laboratory_operation_level": adv_lab_active,
            "mass_reactions_level": react_active,
            "advanced_mass_reactions_level": adv_react_active,
            "trained_mass_production_level": mass_trained,
            "trained_advanced_mass_production_level": adv_mass_trained,
            "trained_laboratory_operation_level": lab_trained,
            "trained_advanced_laboratory_operation_level": adv_lab_trained,
            "trained_mass_reactions_level": react_trained,
            "trained_advanced_mass_reactions_level": adv_react_trained,
        },
    )[0]


def _skill_snapshot_stale(snapshot: IndustrySkillSnapshot | None) -> bool:
    if not snapshot:
        return True
    return timezone.now() - snapshot.last_updated > SKILL_CACHE_TTL


def build_slot_overview_rows(user: User, *, refresh_skills: bool = True) -> list[dict[str, object]]:
    """Return slot overview rows for the user's owned characters."""
    ownerships = CharacterOwnership.objects.filter(user=user).select_related("character")
    character_ids = [ownership.character.character_id for ownership in ownerships if ownership.character]
    now = timezone.now()

    snapshots = {
        snapshot.character_id: snapshot
        for snapshot in IndustrySkillSnapshot.objects.filter(
            owner_user=user,
            character_id__in=character_ids,
        )
    }
    skill_token_ids = set(
        Token.objects.filter(user=user, character_id__in=character_ids)
        .require_scopes([SKILLS_SCOPE])
        .require_valid()
        .values_list("character_id", flat=True)
    )

    active_job_rows = (
        IndustryJob.objects.filter(
            owner_user=user,
            owner_kind=Blueprint.OwnerKind.CHARACTER,
            status="active",
            end_date__gt=now,
            character_id__in=character_ids,
        )
        .values("character_id", "activity_id")
        .annotate(total=Count("id"))
    )
    used_counts: dict[int, dict[str, int]] = {
        char_id: {"manufacturing": 0, "research": 0, "reactions": 0} for char_id in character_ids
    }
    for row in active_job_rows:
        char_id = int(row.get("character_id") or 0)
        activity_id = int(row.get("activity_id") or 0)
        total = int(row.get("total") or 0)
        if char_id not in used_counts:
            continue
        if activity_id in MANUFACTURING_ACTIVITY_IDS:
            used_counts[char_id]["manufacturing"] += total
        elif activity_id in RESEARCH_ACTIVITY_IDS:
            used_counts[char_id]["research"] += total
        elif activity_id in REACTION_ACTIVITY_IDS:
            used_counts[char_id]["reactions"] += total

    def _slots_payload(total_value: int | None, used_value: int) -> dict[str, int | None]:
        if total_value is None:
            return {"total": None, "available": None, "used": None, "percent_used": 0}
        used_clamped = min(max(used_value, 0), total_value)
        available = max(total_value - used_clamped, 0)
        percent_used = int(round((used_clamped / total_value) * 100)) if total_value else 0
        return {
            "total": total_value,
            "available": available,
            "used": used_clamped,
            "percent_used": percent_used,
        }

    character_rows: list[dict[str, object]] = []
    for ownership in ownerships:
        char = ownership.character
        if not char:
            continue
        character_id = char.character_id
        snapshot = snapshots.get(character_id)
        skills_missing = snapshot is None

        if refresh_skills:
            skills_missing = character_id not in skill_token_ids

        if refresh_skills and not skills_missing:
            if snapshot is None:
                try:
                    levels = fetch_character_skill_levels(
                        character_id,
                        force_refresh=True,
                    )
                    snapshot = update_skill_snapshot(user, character_id, levels)
                except ESIUnmodifiedError:
                    skills_missing = True
                except ESITokenError:
                    skills_missing = True
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.warning("Failed to refresh skills for %s: %s", character_id, exc)
                    skills_missing = True
            elif _skill_snapshot_stale(snapshot):
                try:
                    levels = fetch_character_skill_levels(character_id)
                    snapshot = update_skill_snapshot(user, character_id, levels)
                except ESIUnmodifiedError:
                    pass
                except ESITokenError as exc:
                    logger.warning("Failed to refresh skills for %s: %s", character_id, exc)
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.warning("Failed to refresh skills for %s: %s", character_id, exc)
        elif snapshot is None:
            skills_missing = True

        if skills_missing and (refresh_skills or snapshot is None):
            snapshot = None

        totals = {
            "manufacturing": snapshot.manufacturing_slots if snapshot else None,
            "research": snapshot.research_slots if snapshot else None,
            "reactions": snapshot.reaction_slots if snapshot else None,
        }
        used = used_counts.get(
            character_id,
            {"manufacturing": 0, "research": 0, "reactions": 0},
        )

        character_rows.append(
            {
                "character_id": character_id,
                "name": get_character_name(character_id),
                "skills_missing": skills_missing,
                "manufacturing": _slots_payload(totals["manufacturing"], used["manufacturing"]),
                "research": _slots_payload(totals["research"], used["research"]),
                "reactions": _slots_payload(totals["reactions"], used["reactions"]),
            }
        )

    return character_rows
