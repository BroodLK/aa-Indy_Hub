"""Reprocessing Services views."""

from __future__ import annotations

# Standard Library
from decimal import Decimal, InvalidOperation
import hashlib
import re
import unicodedata

# Django
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import Permission, User
from django.core.cache import cache
from django.db import connection, transaction
from django.db.models import Q
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from django.views.decorators.http import require_http_methods

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership, UserProfile
from allianceauth.eveonline.models import EveCharacter, EveCorporationInfo
from allianceauth.services.hooks import get_extension_logger
from esi.models import Token
from esi.views import sso_redirect

# Local
from ..decorators import indy_hub_access_required, indy_hub_permission_required
from ..models import (
    CachedStructureName,
    ESIContract,
    NotificationWebhook,
    ReprocessingServiceProfile,
    ReprocessingServiceRequest,
    ReprocessingServiceRequestItem,
    ReprocessingServiceRequestOutput,
)
from ..notifications import notify_multi, notify_user, send_discord_webhook
from ..services.asset_cache import get_corp_assets_cached
from ..services.esi_client import shared_client
from ..services.reprocessing import (
    REPROCESSING_CLONES_SCOPE,
    REPROCESSING_RIG_PROFILES,
    REPROCESSING_SKILL_TYPE_IDS,
    REPROCESSING_SKILLS_SCOPE,
    STRUCTURE_BONUS_BY_TYPE_ID,
    STRUCTURE_LABEL_BY_TYPE_ID,
    SUPPORTED_STRUCTURE_TYPE_IDS,
    aggregate_contract_items_by_type,
    build_reprocessing_estimate,
    build_reprocessing_skill_snapshot,
    compute_estimated_yield_percent,
    contract_items_match_exact,
    contract_items_match_with_tolerance,
    fetch_character_clone_options,
    fetch_character_skill_levels,
    infer_security_modifier,
    resolve_processing_skill_level_for_item,
)
from ..utils.analytics import emit_view_analytics_event
from ..utils.eve import get_character_name, get_corporation_name, get_type_name
from .navigation import build_nav_context

logger = get_extension_logger(__name__)

REPROCESSING_SERVICES_SCOPE_SET = sorted(
    {
        REPROCESSING_SKILLS_SCOPE,
        REPROCESSING_CLONES_SCOPE,
        "esi-universe.read_structures.v1",
        "esi-corporations.read_structures.v1",
        "esi-characters.read_corporation_roles.v1",
    }
)

_REQUEST_ITEM_LINE_SPLIT_RE = re.compile(r"\s*(?:,|;|\|)\s*")
_REQUEST_ITEM_QTY_RE = re.compile(r"^(.+?)\s*(?:x|\*)\s*([0-9][0-9,.\s']*)$", re.IGNORECASE)
_TYPE_TEXT_LOOKUP_CACHE: dict[str, int | None] = {}
_REPROCESSING_ESTIMATE_CACHE_TTL_SECONDS = 15 * 60

_RIG_PROFILE_KEY_BY_NAME_PATTERN: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"standup\s+m-set\s+moon\s+ore\s+grading\s+processor\s+ii", re.IGNORECASE), "moon_t2"),
    (re.compile(r"standup\s+m-set\s+moon\s+ore\s+grading\s+processor\s+i$", re.IGNORECASE), "moon_t1"),
    (re.compile(r"standup\s+m-set\s+ore\s+grading\s+processor\s+ii", re.IGNORECASE), "ore_t2"),
    (re.compile(r"standup\s+m-set\s+ore\s+grading\s+processor\s+i$", re.IGNORECASE), "ore_t1"),
]

_RIG_LOCATION_FLAG_HINTS = (
    "rigslot",
    "serviceslot",
    "service",
    "fitting",
)

_ASSET_STRUCTURE_FLAGS = {
    "OfficeFolder",
    "StructureFuel",
    "MoonMaterialBay",
    "QuantumCoreRoom",
    "ServiceSlot0",
    "CorpDeliveries",
}


def _infer_supported_structure_type(
    *,
    structure_type_id: int | None,
    structure_name: str | None,
    structure_type_name: str | None,
    structure_flags: set[str] | None = None,
) -> tuple[int, str] | None:
    type_id = int(structure_type_id or 0)
    if type_id in SUPPORTED_STRUCTURE_TYPE_IDS:
        return (
            int(type_id),
            str(
                structure_type_name
                or STRUCTURE_LABEL_BY_TYPE_ID.get(int(type_id))
                or get_type_name(int(type_id))
                or f"Type {int(type_id)}"
            ),
        )
    label_probe = f"{structure_type_name or ''} {structure_name or ''}".lower()
    if "athanor" in label_probe:
        return (35835, STRUCTURE_LABEL_BY_TYPE_ID[35835])
    if "tatara" in label_probe:
        return (35836, STRUCTURE_LABEL_BY_TYPE_ID[35836])
    if any(str(flag).lower() == "moonmaterialbay" for flag in (structure_flags or set())):
        # Refineries expose MoonMaterialBay; when type is unavailable we infer Athanor conservatively.
        return (35835, _("Athanor (inferred)"))
    return None


def _build_nav_context(user, *, active_tab: str | None = None) -> dict:
    return build_nav_context(
        user,
        active_tab=active_tab,
        can_manage_corp=user.has_perm("indy_hub.can_manage_corp_bp_requests"),
        can_manage_material_hub=user.has_perm("indy_hub.can_manage_material_hub"),
        can_access_indy_hub=user.has_perm("indy_hub.can_access_indy_hub"),
    )


def _to_decimal(value, *, fallback: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return fallback


def _rig_bonus_for_key(profile_key: str) -> Decimal:
    normalized_key = str(profile_key or "").strip().lower()
    for profile in REPROCESSING_RIG_PROFILES:
        if str(profile.get("key") or "").strip().lower() == normalized_key:
            return _to_decimal(profile.get("bonus_percent"), fallback=Decimal("0.000"))
    return Decimal("0.000")


def _get_material_exchange_admin_users() -> list[User]:
    try:
        permission = Permission.objects.get(
            codename="can_manage_material_hub",
            content_type__app_label="indy_hub",
        )
    except Permission.DoesNotExist:
        return []
    return list(
        User.objects.filter(
            Q(groups__permissions=permission) | Q(user_permissions=permission),
            is_active=True,
        ).distinct()
    )


def _notify_material_exchange_admins(
    *,
    title: str,
    message: str,
    level: str = "info",
    link: str | None = None,
) -> None:
    webhook = NotificationWebhook.get_material_exchange_webhook()
    if webhook and webhook.webhook_url:
        sent = send_discord_webhook(
            webhook.webhook_url,
            title,
            message,
            level=level,
            link=link,
            mention_everyone=bool(getattr(webhook, "ping_here", False)),
            embed_title=f"[Reprocessing] {title}",
        )
        if sent:
            return
    notify_multi(
        _get_material_exchange_admin_users(),
        title,
        message,
        level=level,
        link=link,
    )


def _get_user_main_character(user) -> tuple[int | None, str]:
    try:
        profile = UserProfile.objects.select_related("main_character").get(user=user)
        character = getattr(profile, "main_character", None)
        if character and getattr(character, "character_id", None):
            return int(character.character_id), str(character.character_name or "")
    except Exception:
        pass
    return None, ""


def _get_user_character_rows(user) -> list[dict[str, object]]:
    ownerships = CharacterOwnership.objects.filter(user=user).select_related("character")
    rows: list[dict[str, object]] = []
    for ownership in ownerships:
        character = ownership.character
        if not character:
            continue
        try:
            character_id = int(character.character_id)
        except (TypeError, ValueError):
            continue
        corp_id = getattr(character, "corporation_id", None)
        corp_name = getattr(character, "corporation_name", "") or get_corporation_name(corp_id)
        rows.append(
            {
                "character_id": character_id,
                "character_name": str(character.character_name or get_character_name(character_id)),
                "corporation_id": int(corp_id) if corp_id else None,
                "corporation_name": str(corp_name or ""),
            }
        )
    rows.sort(key=lambda row: str(row.get("character_name", "")).lower())
    return rows


def _get_user_corporation_rows(user) -> list[dict[str, object]]:
    rows = _get_user_character_rows(user)
    seen: set[int] = set()
    corporations: list[dict[str, object]] = []
    for row in rows:
        corp_id = row.get("corporation_id")
        if not corp_id:
            continue
        corp_id_int = int(corp_id)
        if corp_id_int in seen:
            continue
        seen.add(corp_id_int)
        corporations.append(
            {
                "corporation_id": corp_id_int,
                "corporation_name": str(row.get("corporation_name") or get_corporation_name(corp_id_int)),
            }
        )
    corporations.sort(key=lambda row: str(row.get("corporation_name", "")).lower())
    return corporations


def _get_token_for_corp_scope(user, corp_id: int, scope: str):
    tokens = Token.objects.filter(user=user).require_scopes([scope]).require_valid()
    for token in tokens:
        try:
            character = getattr(token, "character", None)
            if character and int(getattr(character, "corporation_id", 0) or 0) == int(corp_id):
                return token
        except Exception:
            continue
        try:
            stored_character = EveCharacter.objects.get_character_by_id(int(token.character_id))
            if stored_character is None:
                stored_character = EveCharacter.objects.create_character(int(token.character_id))
            if stored_character and int(getattr(stored_character, "corporation_id", 0) or 0) == int(corp_id):
                return token
        except Exception:
            continue
    return None


def _resolve_corp_and_alliance_names(corporation_id: int | None) -> tuple[str, int | None, str]:
    if not corporation_id:
        return "", None, ""
    corp_name = get_corporation_name(int(corporation_id))
    alliance_id: int | None = None
    alliance_name = ""
    try:
        corp_obj = EveCorporationInfo.objects.filter(corporation_id=int(corporation_id)).first()
        if not corp_obj:
            corp_obj = EveCorporationInfo.objects.create_corporation(int(corporation_id))
        if corp_obj:
            corp_name = str(getattr(corp_obj, "corporation_name", "") or corp_name or "")
            raw_alliance_id = getattr(corp_obj, "alliance_id", None)
            alliance_id = int(raw_alliance_id) if raw_alliance_id else None
    except Exception:
        alliance_id = None

    if alliance_id:
        try:
            # Alliance Auth
            from allianceauth.eveonline.models import EveAllianceInfo

            alliance_obj = EveAllianceInfo.objects.filter(alliance_id=alliance_id).first()
            if alliance_obj:
                alliance_name = str(getattr(alliance_obj, "alliance_name", "") or "")
        except Exception:
            alliance_name = ""

    return str(corp_name or ""), alliance_id, alliance_name


def _get_alliance_corporation_ids(selected_corporation_id: int) -> list[int]:
    corp_ids: set[int] = {int(selected_corporation_id)}
    try:
        corp_obj = EveCorporationInfo.objects.filter(
            corporation_id=int(selected_corporation_id)
        ).first()
        if not corp_obj:
            corp_obj = EveCorporationInfo.objects.create_corporation(int(selected_corporation_id))
        alliance_id = getattr(corp_obj, "alliance_id", None) if corp_obj else None
        if alliance_id:
            alliance_corps = EveCorporationInfo.objects.filter(
                alliance_id=int(alliance_id)
            ).values_list("corporation_id", flat=True)
            corp_ids.update(int(corp_id) for corp_id in alliance_corps if corp_id)
    except Exception:
        pass
    return sorted(corp_ids)


def _resolve_system_security_modifiers(system_ids: list[int]) -> dict[int, Decimal]:
    system_ids = [int(x) for x in system_ids if int(x) > 0]
    if not system_ids:
        return {}
    try:
        # Alliance Auth (External Libs)
        import eve_sde.models as sde_models
    except Exception:
        return {}

    solar_system_model = getattr(sde_models, "SolarSystem", None)
    if solar_system_model is None:
        return {}
    try:
        rows = solar_system_model.objects.filter(id__in=system_ids).values_list("id", "security_status")
    except Exception:
        return {}

    modifiers: dict[int, Decimal] = {}
    for system_id, security_status in rows:
        try:
            modifiers[int(system_id)] = infer_security_modifier(_to_decimal(security_status))
        except Exception:
            modifiers[int(system_id)] = Decimal("0.000")
    return modifiers


def _infer_rig_profile_key_from_type_name(type_name: str) -> str | None:
    normalized = str(type_name or "").strip()
    if not normalized:
        return None
    for pattern, profile_key in _RIG_PROFILE_KEY_BY_NAME_PATTERN:
        if pattern.search(normalized):
            return profile_key
    return None


def _pick_best_rig_profile_key(current_key: str | None, candidate_key: str | None) -> str | None:
    if not candidate_key:
        return current_key
    if not current_key:
        return candidate_key
    if _rig_bonus_for_key(candidate_key) > _rig_bonus_for_key(current_key):
        return candidate_key
    return current_key


def _load_corptools_structure_rows(corporation_ids: list[int]) -> list[dict[str, object]]:
    try:
        # AA Example App
        from corptools.models.audits import CorporationAudit
        from corptools.models.structures import Structure
    except Exception:
        return []

    corp_ids = [int(corp_id) for corp_id in corporation_ids if int(corp_id) > 0]
    if not corp_ids:
        return []
    try:
        corp_audits = CorporationAudit.objects.filter(
            corporation__corporation_id__in=corp_ids
        ).select_related("corporation")
        structures = (
            Structure.objects.filter(
                corporation__in=corp_audits,
                type_id__in=SUPPORTED_STRUCTURE_TYPE_IDS,
            )
            .select_related("corporation__corporation", "system_name")
            .order_by("structure_id")
        )
    except Exception:
        return []

    rows: list[dict[str, object]] = []
    for structure in structures:
        try:
            structure_id = int(getattr(structure, "structure_id", 0) or 0)
            type_id = int(getattr(structure, "type_id", 0) or 0)
        except (TypeError, ValueError):
            continue
        if structure_id <= 0 or type_id not in SUPPORTED_STRUCTURE_TYPE_IDS:
            continue

        try:
            location_id = int(getattr(structure, "system_id", 0) or 0)
        except (TypeError, ValueError):
            location_id = 0
        location_name = ""
        system_name = getattr(structure, "system_name", None)
        if system_name is not None:
            location_name = str(getattr(system_name, "name", "") or "").strip()

        owner_corp_id = None
        owner_corp = getattr(structure, "corporation", None)
        owner_corp_eve = getattr(owner_corp, "corporation", None) if owner_corp else None
        try:
            owner_corp_id = int(getattr(owner_corp_eve, "corporation_id", 0) or 0) or None
        except (TypeError, ValueError):
            owner_corp_id = None

        rows.append(
            {
                "structure_id": structure_id,
                "structure_name": str(getattr(structure, "name", "") or "").strip(),
                "structure_type_id": type_id,
                "structure_type_name": (
                    STRUCTURE_LABEL_BY_TYPE_ID.get(type_id)
                    or get_type_name(type_id)
                    or f"Type {type_id}"
                ),
                "location_id": location_id if location_id > 0 else None,
                "location_name": location_name,
                "owner_corporation_id": owner_corp_id,
                "structure_bonus_percent": STRUCTURE_BONUS_BY_TYPE_ID.get(type_id, Decimal("0.000")),
                "security_bonus_percent": Decimal("0.000"),
            }
        )
    return rows


def _infer_structure_rigs_from_corptools(
    corporation_ids: list[int],
    structure_ids: list[int],
) -> dict[int, str]:
    try:
        # AA Example App
        from corptools.models.assets import CorpAsset
        from corptools.models.audits import CorporationAudit
    except Exception:
        return {}

    corp_ids = [int(corp_id) for corp_id in corporation_ids if int(corp_id) > 0]
    structure_ids = [int(structure_id) for structure_id in structure_ids if int(structure_id) > 0]
    if not corp_ids or not structure_ids:
        return {}

    try:
        corp_audits = CorporationAudit.objects.filter(corporation__corporation_id__in=corp_ids)
        rows = CorpAsset.objects.filter(
            corporation__in=corp_audits,
            location_id__in=structure_ids,
        ).values_list("location_id", "location_flag", "type_id", "type_name__name")
    except Exception:
        return {}

    rig_key_by_structure: dict[int, str] = {}
    for location_id, location_flag, type_id, type_name in rows:
        try:
            structure_id = int(location_id)
            type_id_int = int(type_id)
        except (TypeError, ValueError):
            continue
        flag_text = str(location_flag or "").strip().lower()
        if flag_text and not any(hint in flag_text for hint in _RIG_LOCATION_FLAG_HINTS):
            continue
        type_name_text = str(type_name or get_type_name(type_id_int) or "")
        candidate_key = _infer_rig_profile_key_from_type_name(type_name_text)
        if not candidate_key:
            continue
        rig_key_by_structure[structure_id] = _pick_best_rig_profile_key(
            rig_key_by_structure.get(structure_id),
            candidate_key,
        ) or candidate_key
    return rig_key_by_structure


def _infer_structure_rigs_from_cached_assets(
    corporation_ids: list[int],
    structure_ids: list[int],
) -> dict[int, str]:
    rig_key_by_structure: dict[int, str] = {}
    structure_id_set = {int(structure_id) for structure_id in structure_ids if int(structure_id) > 0}
    if not structure_id_set:
        return rig_key_by_structure

    for corp_id in corporation_ids:
        try:
            assets_qs, _ = get_corp_assets_cached(
                int(corp_id),
                allow_refresh=False,
                as_queryset=True,
                values_fields=["location_id", "location_flag", "type_id"],
            )
        except Exception:
            continue
        try:
            asset_rows = assets_qs.filter(location_id__in=list(structure_id_set))
        except Exception:
            continue
        for asset in asset_rows:
            try:
                structure_id = int(asset.get("location_id") or 0)
                type_id = int(asset.get("type_id") or 0)
            except (TypeError, ValueError, AttributeError):
                continue
            if structure_id <= 0 or type_id <= 0:
                continue
            flag_text = str(asset.get("location_flag") or "").strip().lower()
            if flag_text and not any(hint in flag_text for hint in _RIG_LOCATION_FLAG_HINTS):
                continue
            candidate_key = _infer_rig_profile_key_from_type_name(get_type_name(type_id))
            if not candidate_key:
                continue
            rig_key_by_structure[structure_id] = _pick_best_rig_profile_key(
                rig_key_by_structure.get(structure_id),
                candidate_key,
            ) or candidate_key
    return rig_key_by_structure


def _extract_structure_flag_map_from_corp_assets(corp_id: int) -> dict[int, set[str]]:
    try:
        assets_qs, _ = get_corp_assets_cached(
            int(corp_id),
            allow_refresh=False,
            as_queryset=True,
        )
    except Exception:
        return {}

    try:
        office_folder_map = {
            int(item_id): int(location_id)
            for item_id, location_id in assets_qs.filter(location_flag="OfficeFolder")
            .exclude(item_id__isnull=True)
            .values_list("item_id", "location_id")
            .distinct()
        }
    except Exception:
        office_folder_map = {}

    structure_flags: dict[int, set[str]] = {}
    try:
        for location_id, location_flag in assets_qs.filter(
            location_flag__in=_ASSET_STRUCTURE_FLAGS
        ).values_list("location_id", "location_flag"):
            if location_id:
                structure_id = int(location_id)
                structure_flags.setdefault(structure_id, set()).add(str(location_flag or ""))
    except Exception:
        pass

    try:
        sag_rows = assets_qs.filter(location_flag__startswith="CorpSAG").values_list(
            "location_id",
            "location_flag",
        )
    except Exception:
        sag_rows = []
    for office_folder_item_id, _flag in sag_rows:
        if not office_folder_item_id:
            continue
        structure_id = office_folder_map.get(int(office_folder_item_id))
        if not structure_id:
            structure_id = int(office_folder_item_id)
        if structure_id:
            structure_flags.setdefault(int(structure_id), set()).add("CorpSAG")
    return structure_flags


def _get_cached_structure_name_map(structure_ids: list[int]) -> dict[int, str]:
    ids = [int(structure_id) for structure_id in structure_ids if int(structure_id) > 0]
    if not ids:
        return {}
    try:
        rows = CachedStructureName.objects.filter(structure_id__in=ids).values_list(
            "structure_id",
            "name",
        )
    except Exception:
        return {}
    return {
        int(structure_id): str(name or "").strip()
        for structure_id, name in rows
        if structure_id and str(name or "").strip()
    }


def _fetch_reprocessing_structures(user, selected_corporation_id: int) -> list[dict[str, object]]:
    """Return Athanor/Tatara choices from selected corp alliance scope (best-effort)."""
    _ = user
    structures_by_id: dict[int, dict[str, object]] = {}
    corp_ids = _get_alliance_corporation_ids(int(selected_corporation_id))
    location_ids: set[int] = set()

    # 1) Corptools cache first.
    corptools_rows = _load_corptools_structure_rows(corp_ids)
    corptools_by_structure_id: dict[int, dict[str, object]] = {}
    for row in corptools_rows:
        structure_id_int = int(row.get("structure_id") or 0)
        if structure_id_int <= 0:
            continue
        corptools_by_structure_id[structure_id_int] = dict(row)
        location_id = int(row.get("location_id") or 0)
        if location_id > 0:
            location_ids.add(location_id)

        existing = structures_by_id.get(structure_id_int)
        if existing:
            existing_name = str(existing.get("structure_name", "") or "").strip()
            if not existing_name or existing_name.startswith("Structure "):
                existing["structure_name"] = str(row.get("structure_name") or existing_name or "").strip()
            if not existing.get("location_id") and location_id > 0:
                existing["location_id"] = location_id
            if not existing.get("location_name"):
                existing["location_name"] = str(row.get("location_name") or "")
            if int(existing.get("owner_corporation_id") or 0) <= 0:
                existing["owner_corporation_id"] = int(row.get("owner_corporation_id") or 0) or None
            continue

        merged_row = dict(row)
        merged_row["data_source"] = "corptools"
        structures_by_id[structure_id_int] = merged_row

    # 2) Material Exchange-style cached corp assets (selected corp only) for fast non-ESI path.
    for corp_id in [int(selected_corporation_id)]:
        structure_flags_by_id = _extract_structure_flag_map_from_corp_assets(int(corp_id))
        structure_ids_from_assets = set(structure_flags_by_id.keys())
        if not structure_ids_from_assets:
            continue
        structure_name_map = _get_cached_structure_name_map(
            sorted(structure_ids_from_assets)
        )

        for structure_id in structure_ids_from_assets:
            if structure_id <= 0:
                continue
            corptools_row = corptools_by_structure_id.get(int(structure_id), {})
            existing = structures_by_id.get(int(structure_id))
            structure_name = str(structure_name_map.get(int(structure_id)) or "").strip()
            if not structure_name:
                structure_name = str(corptools_row.get("structure_name") or "").strip()
            if not structure_name:
                structure_name = f"Structure {int(structure_id)}"

            if existing:
                if str(existing.get("structure_name", "")).startswith("Structure "):
                    existing["structure_name"] = structure_name
                if int(existing.get("owner_corporation_id") or 0) <= 0:
                    existing["owner_corporation_id"] = int(corp_id)
                continue

            inferred_type = _infer_supported_structure_type(
                structure_type_id=int(corptools_row.get("structure_type_id") or 0),
                structure_name=structure_name,
                structure_type_name=str(corptools_row.get("structure_type_name") or ""),
                structure_flags=structure_flags_by_id.get(int(structure_id), set()),
            )
            if inferred_type is None:
                continue
            structure_type_id, structure_type_name = inferred_type
            location_id = int(corptools_row.get("location_id") or 0)
            if location_id > 0:
                location_ids.add(location_id)
            structures_by_id[int(structure_id)] = {
                "structure_id": int(structure_id),
                "structure_name": structure_name,
                "structure_type_id": structure_type_id or None,
                "structure_type_name": structure_type_name,
                "location_id": location_id if location_id > 0 else None,
                "location_name": str(corptools_row.get("location_name") or ""),
                "owner_corporation_id": int(corp_id),
                "structure_bonus_percent": STRUCTURE_BONUS_BY_TYPE_ID.get(
                    structure_type_id,
                    Decimal("0.000"),
                ),
                "security_bonus_percent": Decimal("0.000"),
                "data_source": "assets_cache",
            }

    filtered_rows: list[dict[str, object]] = []
    for row in structures_by_id.values():
        inferred_type = _infer_supported_structure_type(
            structure_type_id=int(row.get("structure_type_id") or 0),
            structure_name=str(row.get("structure_name") or ""),
            structure_type_name=str(row.get("structure_type_name") or ""),
        )
        if inferred_type is None:
            continue
        type_id, type_name = inferred_type
        row["structure_type_id"] = int(type_id)
        row["structure_type_name"] = str(type_name)
        row["structure_bonus_percent"] = STRUCTURE_BONUS_BY_TYPE_ID.get(
            int(type_id),
            Decimal("0.000"),
        )
        filtered_rows.append(row)

    if not filtered_rows:
        return []

    if location_ids:
        try:
            location_name_map = shared_client.resolve_ids_to_names(sorted(location_ids))
        except Exception:
            location_name_map = {}
        security_modifier_map = _resolve_system_security_modifiers(sorted(location_ids))
        for row in filtered_rows:
            location_id = int(row.get("location_id") or 0)
            row["location_name"] = str(location_name_map.get(location_id, "") or "")
            row["security_bonus_percent"] = _to_decimal(
                security_modifier_map.get(location_id, Decimal("0.000"))
            )

    structure_ids = sorted(int(row.get("structure_id") or 0) for row in filtered_rows)
    rig_key_hints = _infer_structure_rigs_from_corptools(corp_ids, structure_ids)
    cached_rig_hints = _infer_structure_rigs_from_cached_assets(corp_ids, structure_ids)
    for structure_id, profile_key in cached_rig_hints.items():
        rig_key_hints[structure_id] = _pick_best_rig_profile_key(
            rig_key_hints.get(structure_id),
            profile_key,
        ) or profile_key

    for row in filtered_rows:
        structure_id = int(row.get("structure_id") or 0)
        rig_profile_key = rig_key_hints.get(structure_id)
        row["suggested_rig_profile_key"] = rig_profile_key or "none"
        rig_profile = _get_rig_profile(rig_profile_key or "none")
        row["suggested_rig_profile_name"] = str(rig_profile.get("label") or "")

    rows = list(filtered_rows)
    rows.sort(key=lambda row: (str(row.get("structure_name", "")).lower(), int(row.get("structure_id", 0))))
    return rows

def _get_rig_profile(profile_key: str) -> dict[str, object]:
    normalized_key = str(profile_key or "").strip().lower()
    for profile in REPROCESSING_RIG_PROFILES:
        if str(profile.get("key")) == normalized_key:
            return dict(profile)
    return dict(REPROCESSING_RIG_PROFILES[0])


def _user_can_access_request(user, service_request: ReprocessingServiceRequest) -> bool:
    if user.id == service_request.requester_id:
        return True
    if user.id == service_request.processor_user_id:
        return True
    return bool(user.has_perm("indy_hub.can_manage_material_hub"))


def _build_expected_item_map(service_request: ReprocessingServiceRequest) -> dict[int, int]:
    return {
        int(item.type_id): int(item.quantity)
        for item in service_request.items.all()
    }


def _build_expected_output_map(service_request: ReprocessingServiceRequest) -> dict[int, int]:
    return {
        int(output.type_id): int(output.expected_quantity)
        for output in service_request.expected_outputs.all()
    }


def _contract_title_contains_request_reference(
    *,
    contract_title: str | None,
    request_reference: str | None,
) -> bool:
    title = str(contract_title or "").strip().lower()
    reference = str(request_reference or "").strip().lower()
    if not reference:
        return False
    return reference in title


def _normalize_items_text_for_cache(raw_text: str) -> str:
    lines = [str(line or "").strip() for line in str(raw_text or "").splitlines()]
    return "\n".join(line for line in lines if line)


def _build_estimate_cache_token(
    *,
    profile_id: int,
    requester_character_id: int | None,
    items_text: str,
    profile_updated_at,
) -> str:
    fingerprint = (
        f"{int(profile_id)}|"
        f"{int(requester_character_id or 0)}|"
        f"{int(getattr(profile_updated_at, 'timestamp', lambda: 0)() or 0)}|"
        f"{_normalize_items_text_for_cache(items_text)}"
    )
    return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:24]


def _build_estimate_cache_key(*, user_id: int, profile_id: int, token: str) -> str:
    return f"indy_hub:reproc_estimate:{int(user_id)}:{int(profile_id)}:{str(token)}"


def _verify_inbound_contract(service_request: ReprocessingServiceRequest) -> tuple[bool, str]:
    contract_id = int(service_request.inbound_contract_id or 0)
    if contract_id <= 0:
        return False, _("Inbound contract ID is not set.")
    contract = ESIContract.objects.filter(contract_id=contract_id).prefetch_related("items").first()
    if not contract:
        return False, _("Inbound contract was not found in cached ESI contracts.")

    if str(contract.contract_type or "").lower() != "item_exchange":
        return False, _("Inbound contract must be an Item Exchange contract.")
    if not _contract_title_contains_request_reference(
        contract_title=str(contract.title or ""),
        request_reference=str(service_request.request_reference or ""),
    ):
        return (
            False,
            _(
                "Inbound contract title/description must include request reference %(reference)s."
            )
            % {"reference": service_request.request_reference},
        )

    contract_price = Decimal(str(contract.price or 0)).quantize(Decimal("0.01"))
    contract_reward = Decimal(str(contract.reward or 0)).quantize(Decimal("0.01"))
    if contract_price != Decimal("0.00") or contract_reward != Decimal("0.00"):
        return False, _("Inbound contract must be created with 0 ISK price and 0 reward.")

    expected_assignee_ids = {
        int(service_request.processor_character_id or 0),
        int(service_request.processor_profile.corporation_id or 0),
    }
    expected_assignee_ids = {value for value in expected_assignee_ids if value > 0}
    if expected_assignee_ids and int(contract.assignee_id or 0) not in expected_assignee_ids:
        return False, _("Inbound contract assignee does not match the selected reprocessor.")

    requester_character_id = int(service_request.requester_character_id or 0)
    if requester_character_id > 0 and int(contract.issuer_id or 0) != requester_character_id:
        return False, _("Inbound contract issuer does not match the requester character.")

    expected_by_type = _build_expected_item_map(service_request)
    if not contract_items_match_exact(
        contract_items=contract.items.filter(is_included=True),
        expected_by_type=expected_by_type,
    ):
        return False, _("Inbound contract items do not exactly match the submitted request.")

    return True, _("Inbound contract verified.")


def _verify_return_contract(service_request: ReprocessingServiceRequest) -> tuple[bool, str]:
    contract_id = int(service_request.return_contract_id or 0)
    if contract_id <= 0:
        return False, _("Return contract ID is not set.")
    contract = ESIContract.objects.filter(contract_id=contract_id).prefetch_related("items").first()
    if not contract:
        return False, _("Return contract was not found in cached ESI contracts.")

    if str(contract.contract_type or "").lower() != "item_exchange":
        return False, _("Return contract must be an Item Exchange contract.")
    if not _contract_title_contains_request_reference(
        contract_title=str(contract.title or ""),
        request_reference=str(service_request.request_reference or ""),
    ):
        return (
            False,
            _(
                "Return contract title/description must include request reference %(reference)s."
            )
            % {"reference": service_request.request_reference},
        )

    expected_issuer_id = int(service_request.processor_character_id or 0)
    if expected_issuer_id > 0 and int(contract.issuer_id or 0) != expected_issuer_id:
        return False, _("Return contract issuer does not match the selected reprocessor.")

    requester_character_id = int(service_request.requester_character_id or 0)
    if requester_character_id > 0 and int(contract.assignee_id or 0) != requester_character_id:
        return False, _("Return contract assignee does not match the requester character.")

    expected_reward = Decimal(str(service_request.reward_isk or 0)).quantize(Decimal("0.01"))
    contract_price = Decimal(str(contract.price or 0)).quantize(Decimal("0.01"))
    if contract_price != expected_reward:
        return (
            False,
            _(
                "Return contract price mismatch. Expected %(expected)s ISK, got %(actual)s ISK."
            )
            % {
                "expected": f"{expected_reward:,.2f}",
                "actual": f"{contract_price:,.2f}",
            },
        )

    expected_by_type = _build_expected_output_map(service_request)
    matches, errors = contract_items_match_with_tolerance(
        contract_items=contract.items.filter(is_included=True),
        expected_by_type=expected_by_type,
        tolerance_percent=Decimal(str(service_request.tolerance_percent or Decimal("1.00"))),
    )
    if not matches:
        return False, "\n".join(errors)

    return True, _("Return contract verified.")


def _character_has_required_scopes(user, character_id: int) -> bool:
    return (
        Token.objects.filter(user=user, character_id=character_id)
        .require_scopes([REPROCESSING_SKILLS_SCOPE, REPROCESSING_CLONES_SCOPE])
        .require_valid()
        .exists()
    )


def _parse_margin_percent(raw_value: str | None) -> Decimal:
    margin = _to_decimal((raw_value or "").strip() or "0")
    if margin < Decimal("0"):
        margin = Decimal("0")
    if margin > Decimal("100"):
        margin = Decimal("100")
    return margin.quantize(Decimal("0.01"))


def _avatar_url(character_id: int, *, size: int = 128) -> str:
    return f"https://images.evetech.net/characters/{int(character_id)}/portrait?size={int(size)}"


def _beancounter_implants(implant_names: list[str] | None) -> list[str]:
    names = [str(name or "").strip() for name in (implant_names or [])]
    return [name for name in names if name and ("beancounter" in name.lower() or "rx-80" in name.lower())]


def _build_reprocessing_skill_rows(
    skill_levels: dict[int, dict[str, int]] | None,
) -> list[dict[str, object]]:
    skill_levels = skill_levels or {}
    core_skill_ids = [
        int(REPROCESSING_SKILL_TYPE_IDS["reprocessing"]),
        int(REPROCESSING_SKILL_TYPE_IDS["reprocessing_efficiency"]),
        int(REPROCESSING_SKILL_TYPE_IDS["scrapmetal_processing"]),
    ]
    core_sort_index = {skill_id: idx for idx, skill_id in enumerate(core_skill_ids)}

    def _extract_levels(raw_row: object) -> tuple[int, int]:
        if isinstance(raw_row, dict):
            return int(raw_row.get("active") or 0), int(raw_row.get("trained") or 0)
        level = int(raw_row or 0)
        return level, level

    rows_by_skill_id: dict[int, dict[str, object]] = {}
    for skill_id in core_skill_ids:
        active_level, trained_level = _extract_levels(skill_levels.get(skill_id, {}))
        rows_by_skill_id[skill_id] = {
            "skill_id": int(skill_id),
            "skill_name": str(get_type_name(int(skill_id)) or f"Skill {int(skill_id)}"),
            "active_level": int(active_level),
            "trained_level": int(trained_level),
        }

    for raw_skill_id, raw_row in skill_levels.items():
        try:
            skill_id = int(raw_skill_id)
        except (TypeError, ValueError):
            continue
        if skill_id <= 0:
            continue
        skill_name = str(get_type_name(skill_id) or "").strip()
        normalized = skill_name.lower()
        if "processing" not in normalized and skill_id not in core_sort_index:
            continue
        active_level, trained_level = _extract_levels(raw_row)
        rows_by_skill_id[skill_id] = {
            "skill_id": int(skill_id),
            "skill_name": skill_name or f"Skill {skill_id}",
            "active_level": int(active_level),
            "trained_level": int(trained_level),
        }

    rows = list(rows_by_skill_id.values())
    rows.sort(
        key=lambda row: (
            0 if int(row.get("skill_id") or 0) in core_sort_index else 1,
            core_sort_index.get(int(row.get("skill_id") or 0), 99),
            str(row.get("skill_name", "")).lower(),
        )
    )
    return rows


def _compute_character_proficiency(
    skill_rows: list[dict[str, object]],
) -> dict[str, object]:
    levels = [
        int(row.get("active_level") or 0)
        for row in (skill_rows or [])
        if int(row.get("skill_id") or 0) > 0
    ]
    if not levels:
        return {"percent": Decimal("0.0"), "label": _("No data")}

    percent = (
        Decimal(str(sum(levels)))
        / Decimal(str(max(len(levels), 1) * 5))
        * Decimal("100")
    ).quantize(Decimal("0.1"))
    if percent >= Decimal("85.0"):
        label = _("Expert")
    elif percent >= Decimal("60.0"):
        label = _("Advanced")
    elif percent >= Decimal("35.0"):
        label = _("Intermediate")
    else:
        label = _("Basic")
    return {"percent": percent, "label": label}


def _resolve_type_id_from_text(type_text: str) -> int | None:
    def _normalize_text(value: str | None) -> str:
        text = unicodedata.normalize("NFKC", str(value or ""))
        text = (
            text.replace("\u00A0", " ")
            .replace("\u202F", " ")
            .replace("\u2009", " ")
            .replace("\u2010", "-")
            .replace("\u2011", "-")
            .replace("\u2012", "-")
            .replace("\u2013", "-")
            .replace("\u2014", "-")
            .replace("\u2212", "-")
        )
        text = re.sub(r"\s*-\s*", "-", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    normalized = _normalize_text(type_text)
    if not normalized:
        return None
    if normalized.isdigit():
        value = int(normalized)
        return value if value > 0 else None

    cache_key = normalized.casefold()
    if cache_key in _TYPE_TEXT_LOOKUP_CACHE:
        return _TYPE_TEXT_LOOKUP_CACHE[cache_key]

    candidates: list[str] = [normalized]
    if "-" in normalized:
        candidates.append(normalized.replace("-", " "))
    grade_dash = re.sub(r"(?i)\b([ivx]+)\s+grade\b", r"\1-Grade", normalized)
    grade_space = re.sub(r"(?i)\b([ivx]+)-grade\b", r"\1 Grade", normalized)
    candidates.extend([grade_dash, grade_space])
    deduped_candidates: list[str] = []
    seen_candidate_keys: set[str] = set()
    for candidate in candidates:
        key = _normalize_text(candidate).casefold()
        if not key or key in seen_candidate_keys:
            continue
        seen_candidate_keys.add(key)
        deduped_candidates.append(_normalize_text(candidate))

    resolved_type_id: int | None = None
    try:
        # Alliance Auth (External Libs)
        from eve_sde.models import ItemType

        for candidate in deduped_candidates:
            exact_match = (
                ItemType.objects.filter(name__iexact=candidate)
                .values_list("id", flat=True)
                .first()
            )
            if exact_match:
                resolved_type_id = int(exact_match)
                break
    except Exception:
        resolved_type_id = None

    if not resolved_type_id:
        try:
            with connection.cursor() as cursor:
                for candidate in deduped_candidates:
                    cursor.execute(
                        "SELECT id FROM eve_sde_itemtype WHERE lower(name)=lower(%s) LIMIT 1",
                        [candidate],
                    )
                    row = cursor.fetchone()
                    if row and row[0]:
                        resolved_type_id = int(row[0])
                        break
                    cursor.execute(
                        """
                        SELECT id
                        FROM eve_sde_itemtype
                        WHERE lower(
                            replace(
                                replace(
                                    replace(
                                        replace(name, %s, '-'),
                                        %s,
                                        '-'
                                    ),
                                    %s,
                                    '-'
                                ),
                                %s,
                                '-'
                            )
                        ) = lower(%s)
                        LIMIT 1
                        """,
                        ["\u2011", "\u2013", "\u2014", "\u2212", candidate],
                    )
                    row = cursor.fetchone()
                    if row and row[0]:
                        resolved_type_id = int(row[0])
                        break
        except Exception:
            resolved_type_id = None

    if resolved_type_id:
        _TYPE_TEXT_LOOKUP_CACHE[cache_key] = resolved_type_id
    return resolved_type_id


def _parse_request_item_lines(raw_text: str) -> tuple[list[dict[str, int]], list[str]]:
    def _parse_positive_quantity(raw_value: str | int | None) -> int | None:
        text_value = str(raw_value or "").strip()
        if not text_value:
            return None
        normalized = (
            text_value.replace("\u00A0", " ")
            .replace("\u202F", " ")
            .replace("\u2009", " ")
            .replace("_", "")
            .replace("'", "")
        )
        compact = normalized.replace(" ", "")
        if compact.isdigit():
            parsed = int(compact)
            return parsed if parsed > 0 else None
        if re.match(r"^\d{1,3}(?:[.,]\d{3})+$", compact):
            parsed = int(compact.replace(",", "").replace(".", ""))
            return parsed if parsed > 0 else None
        return None

    rows_by_type: dict[int, int] = {}
    errors: list[str] = []
    lines = [str(line or "").strip() for line in str(raw_text or "").splitlines()]
    for line in lines:
        if not line:
            continue
        type_part = ""
        quantity: int | None = None

        tab_parts = [str(part or "").strip() for part in line.split("\t") if str(part or "").strip()]
        if len(tab_parts) >= 2:
            type_part = tab_parts[0]
            for quantity_candidate in tab_parts[1:]:
                quantity = _parse_positive_quantity(quantity_candidate)
                if quantity is not None:
                    break

        if not type_part or quantity is None:
            normalized_line = (
                line.replace("\u00A0", " ")
                .replace("\u202F", " ")
                .replace("\u2009", " ")
                .strip()
            )
            split_parts = _REQUEST_ITEM_LINE_SPLIT_RE.split(normalized_line, maxsplit=1)
            if len(split_parts) == 2:
                type_part = split_parts[0]
                quantity = _parse_positive_quantity(split_parts[1])
            else:
                match = _REQUEST_ITEM_QTY_RE.match(normalized_line)
                if match:
                    type_part = str(match.group(1))
                    quantity = _parse_positive_quantity(match.group(2))
                else:
                    space_parts = normalized_line.rsplit(" ", 1)
                    if len(space_parts) == 2:
                        type_part = space_parts[0]
                        quantity = _parse_positive_quantity(space_parts[1])

        type_part = str(type_part or "").strip()
        if not type_part or quantity is None:
            errors.append(line)
            continue

        type_id = _resolve_type_id_from_text(type_part)
        if not type_id:
            errors.append(line)
            continue

        rows_by_type[type_id] = rows_by_type.get(type_id, 0) + quantity

    rows = [
        {"type_id": int(type_id), "quantity": int(quantity)}
        for type_id, quantity in sorted(rows_by_type.items(), key=lambda x: get_type_name(int(x[0])).lower())
    ]
    return rows, errors


def _build_request_timeline(service_request: ReprocessingServiceRequest) -> list[dict[str, object]]:
    status = service_request.status
    completed = {
        ReprocessingServiceRequest.Status.REQUEST_SUBMITTED: [ReprocessingServiceRequest.Status.REQUEST_SUBMITTED],
        ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT: [
            ReprocessingServiceRequest.Status.REQUEST_SUBMITTED,
            ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT,
        ],
        ReprocessingServiceRequest.Status.INBOUND_CONTRACT_VERIFIED: [
            ReprocessingServiceRequest.Status.REQUEST_SUBMITTED,
            ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT,
            ReprocessingServiceRequest.Status.INBOUND_CONTRACT_VERIFIED,
        ],
        ReprocessingServiceRequest.Status.PROCESSING: [
            ReprocessingServiceRequest.Status.REQUEST_SUBMITTED,
            ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT,
            ReprocessingServiceRequest.Status.INBOUND_CONTRACT_VERIFIED,
            ReprocessingServiceRequest.Status.PROCESSING,
        ],
        ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT: [
            ReprocessingServiceRequest.Status.REQUEST_SUBMITTED,
            ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT,
            ReprocessingServiceRequest.Status.INBOUND_CONTRACT_VERIFIED,
            ReprocessingServiceRequest.Status.PROCESSING,
            ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT,
        ],
        ReprocessingServiceRequest.Status.COMPLETED: [
            ReprocessingServiceRequest.Status.REQUEST_SUBMITTED,
            ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT,
            ReprocessingServiceRequest.Status.INBOUND_CONTRACT_VERIFIED,
            ReprocessingServiceRequest.Status.PROCESSING,
            ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT,
            ReprocessingServiceRequest.Status.COMPLETED,
        ],
    }.get(status, [ReprocessingServiceRequest.Status.REQUEST_SUBMITTED])

    timeline = [
        {
            "key": ReprocessingServiceRequest.Status.REQUEST_SUBMITTED,
            "label": _("Request submitted"),
            "icon": "fa-paper-plane",
            "done": ReprocessingServiceRequest.Status.REQUEST_SUBMITTED in completed,
            "timestamp": service_request.created_at,
        },
        {
            "key": ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT,
            "label": _("Awaiting inbound contract"),
            "icon": "fa-file-import",
            "done": ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT in completed,
            "timestamp": service_request.updated_at
            if status != ReprocessingServiceRequest.Status.REQUEST_SUBMITTED
            else None,
        },
        {
            "key": ReprocessingServiceRequest.Status.INBOUND_CONTRACT_VERIFIED,
            "label": _("Inbound contract verified"),
            "icon": "fa-check-circle",
            "done": ReprocessingServiceRequest.Status.INBOUND_CONTRACT_VERIFIED in completed,
            "timestamp": service_request.inbound_contract_verified_at,
        },
        {
            "key": ReprocessingServiceRequest.Status.PROCESSING,
            "label": _("Processing"),
            "icon": "fa-industry",
            "done": ReprocessingServiceRequest.Status.PROCESSING in completed,
            "timestamp": service_request.updated_at
            if status in {
                ReprocessingServiceRequest.Status.PROCESSING,
                ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT,
                ReprocessingServiceRequest.Status.COMPLETED,
            }
            else None,
        },
        {
            "key": ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT,
            "label": _("Awaiting return contract"),
            "icon": "fa-file-export",
            "done": ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT in completed,
            "timestamp": service_request.updated_at
            if status in {
                ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT,
                ReprocessingServiceRequest.Status.COMPLETED,
            }
            else None,
        },
        {
            "key": ReprocessingServiceRequest.Status.COMPLETED,
            "label": _("Completed"),
            "icon": "fa-flag-checkered",
            "done": ReprocessingServiceRequest.Status.COMPLETED in completed,
            "timestamp": service_request.completed_at,
        },
    ]

    if status == ReprocessingServiceRequest.Status.DISPUTED:
        timeline.append(
            {
                "key": ReprocessingServiceRequest.Status.DISPUTED,
                "label": _("Disputed"),
                "icon": "fa-triangle-exclamation",
                "done": True,
                "timestamp": service_request.updated_at,
            }
        )
    if status == ReprocessingServiceRequest.Status.CANCELLED:
        timeline.append(
            {
                "key": ReprocessingServiceRequest.Status.CANCELLED,
                "label": _("Cancelled"),
                "icon": "fa-ban",
                "done": True,
                "timestamp": service_request.cancelled_at or service_request.updated_at,
            }
        )
    return timeline


def _request_status_badge_class(status: str) -> str:
    return {
        ReprocessingServiceRequest.Status.REQUEST_SUBMITTED: "bg-primary-subtle text-primary",
        ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT: "bg-info-subtle text-info",
        ReprocessingServiceRequest.Status.INBOUND_CONTRACT_VERIFIED: "bg-success-subtle text-success",
        ReprocessingServiceRequest.Status.PROCESSING: "bg-warning-subtle text-warning",
        ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT: "bg-warning-subtle text-warning",
        ReprocessingServiceRequest.Status.COMPLETED: "bg-success",
        ReprocessingServiceRequest.Status.DISPUTED: "bg-danger",
        ReprocessingServiceRequest.Status.CANCELLED: "bg-secondary",
    }.get(str(status or ""), "bg-secondary")


@indy_hub_access_required
@login_required
def reprocessing_services_index(request):
    emit_view_analytics_event(view_name="reprocessing_services.index", request=request)
    return redirect("indy_hub:reprocessing_browse")


@indy_hub_access_required
@login_required
def reprocessing_authorize_scopes(request):
    emit_view_analytics_event(view_name="reprocessing_services.authorize", request=request)
    return sso_redirect(
        request,
        scopes=" ".join(REPROCESSING_SERVICES_SCOPE_SET),
        return_to="indy_hub:reprocessing_become",
    )

@indy_hub_access_required
@login_required
@require_http_methods(["GET", "POST"])
def reprocessing_become(request):
    emit_view_analytics_event(view_name="reprocessing_services.become", request=request)
    character_rows = _get_user_character_rows(request.user)
    corporation_rows = _get_user_corporation_rows(request.user)
    profile_qs = ReprocessingServiceProfile.objects.filter(user=request.user).order_by("character_name")
    existing_profiles = list(profile_qs)
    profile_by_character = {int(profile.character_id): profile for profile in existing_profiles}

    main_character_id, _main_character_name = _get_user_main_character(request.user)
    selected_character_id_raw = request.POST.get("character_id") or request.GET.get("character_id")
    selected_character_id: int | None = None
    try:
        selected_character_id = int(selected_character_id_raw or 0)
    except (TypeError, ValueError):
        selected_character_id = None
    if not selected_character_id:
        if main_character_id:
            selected_character_id = int(main_character_id)
        elif character_rows:
            selected_character_id = int(character_rows[0]["character_id"])

    selected_character_row = next(
        (
            row
            for row in character_rows
            if int(row.get("character_id", 0)) == int(selected_character_id or 0)
        ),
        None,
    )
    selected_profile = profile_by_character.get(int(selected_character_id or 0))

    selected_corporation_id_raw = (
        request.POST.get("selected_corporation_id")
        or request.GET.get("selected_corporation_id")
        or (getattr(selected_profile, "selected_corporation_id", None) if selected_profile else None)
        or (selected_character_row.get("corporation_id") if selected_character_row else None)
    )
    selected_corporation_id: int | None = None
    try:
        selected_corporation_id = int(selected_corporation_id_raw or 0) or None
    except (TypeError, ValueError):
        selected_corporation_id = None

    has_required_scopes = True

    skill_snapshot = {
        "reprocessing": 0,
        "reprocessing_efficiency": 0,
        "processing": 0,
        "scrapmetal_processing": 0,
    }
    selected_character_skill_levels: dict[int, dict[str, int]] = {}
    reprocessing_skill_rows: list[dict[str, object]] = []
    character_proficiency = {"percent": Decimal("0.0"), "label": _("No data")}
    clone_options: list[dict[str, object]] = []
    structure_options: list[dict[str, object]] = []
    scope_error = ""

    if selected_character_id:
        skills_error = False
        clones_error = False
        try:
            selected_character_skill_levels = fetch_character_skill_levels(int(selected_character_id))
            skill_snapshot = build_reprocessing_skill_snapshot(selected_character_skill_levels)
            reprocessing_skill_rows = _build_reprocessing_skill_rows(selected_character_skill_levels)
            character_proficiency = _compute_character_proficiency(reprocessing_skill_rows)
        except Exception as exc:
            logger.warning(
                "Unable to load reprocessing skills for %s: %s",
                selected_character_id,
                exc,
            )
            skills_error = True
        try:
            clone_options = fetch_character_clone_options(int(selected_character_id))
        except Exception as exc:
            logger.warning(
                "Unable to load reprocessing clones for %s: %s",
                selected_character_id,
                exc,
            )
            clones_error = True

        if skills_error or clones_error:
            scope_error = _(
                "Unable to read character skills/clones from corptools cache. "
                "Confirm corptools character sync has completed for this character."
            )

    if selected_corporation_id:
        try:
            structure_options = _fetch_reprocessing_structures(request.user, int(selected_corporation_id))
        except Exception as exc:
            logger.warning(
                "Unable to load reprocessing structures for corp %s: %s",
                selected_corporation_id,
                exc,
            )
            structure_options = []

    selected_clone_id_raw = (
        request.POST.get("selected_clone_id")
        or request.GET.get("selected_clone_id")
        or (getattr(selected_profile, "selected_clone_id", None) if selected_profile else None)
    )
    try:
        selected_clone_id = int(selected_clone_id_raw or 0)
    except (TypeError, ValueError):
        selected_clone_id = 0
    if selected_clone_id <= 0 and clone_options:
        selected_clone_id = int(clone_options[0].get("clone_id") or 0)
    selected_clone_row = next(
        (
            row
            for row in clone_options
            if int(row.get("clone_id") or 0) == int(selected_clone_id or 0)
        ),
        clone_options[0] if clone_options else None,
    )
    selected_clone_beancounter_implants = _beancounter_implants(
        list((selected_clone_row or {}).get("implant_names") or [])
    )
    if not selected_clone_beancounter_implants:
        selected_clone_beancounter_implants = [
            str(name)
            for name in ((selected_clone_row or {}).get("beancounter_implants") or [])
            if str(name or "").strip()
        ]

    selected_structure_id_raw = (
        request.POST.get("structure_id")
        or request.GET.get("structure_id")
        or (getattr(selected_profile, "structure_id", None) if selected_profile else None)
    )
    try:
        selected_structure_id = int(selected_structure_id_raw or 0)
    except (TypeError, ValueError):
        selected_structure_id = 0
    if selected_structure_id <= 0 and structure_options:
        selected_structure_id = int(structure_options[0].get("structure_id") or 0)
    selected_structure_row = next(
        (
            row
            for row in structure_options
            if int(row.get("structure_id") or 0) == int(selected_structure_id or 0)
        ),
        structure_options[0] if structure_options else None,
    )

    preview_rig_profile = _get_rig_profile(
        str((selected_structure_row or {}).get("suggested_rig_profile_key") or "none")
    )
    estimated_yield_preview = None
    if selected_clone_row and selected_structure_row:
        estimated_yield_preview = compute_estimated_yield_percent(
            skill_snapshot=skill_snapshot,
            implant_bonus_percent=_to_decimal(
                (selected_clone_row or {}).get("beancounter_bonus_percent")
            ),
            structure_bonus_percent=_to_decimal(
                (selected_structure_row or {}).get("structure_bonus_percent")
            ),
            rig_bonus_percent=_to_decimal(preview_rig_profile.get("bonus_percent")),
            security_bonus_percent=_to_decimal(
                (selected_structure_row or {}).get("security_bonus_percent")
            ),
        )

    if request.method == "POST":
        action = str(request.POST.get("action") or "save_profile").strip().lower()
        if action in {"save_profile", "submit_application"}:
            if not selected_character_row:
                messages.error(request, _("Please select one of your characters."))
                return redirect("indy_hub:reprocessing_become")
            if not clone_options:
                messages.error(request, _("No clone information available for this character."))
                return redirect("indy_hub:reprocessing_become")
            if not selected_corporation_id:
                messages.error(request, _("Please choose a corporation context for structure discovery."))
                return redirect("indy_hub:reprocessing_become")
            if not structure_options:
                messages.error(
                    request,
                    _(
                        "No Athanor/Tatara structures were found in corptools/cached assets for that corporation/alliance scope."
                    ),
                )
                return redirect("indy_hub:reprocessing_become")

            selected_clone_id = int(request.POST.get("selected_clone_id") or 0)
            selected_structure_id = int(request.POST.get("structure_id") or 0)
            margin_percent = _parse_margin_percent(request.POST.get("margin_percent"))
            requested_available = request.POST.get("is_available") == "on"

            clone_row = next(
                (
                    row
                    for row in clone_options
                    if int(row.get("clone_id") or 0) == selected_clone_id
                ),
                None,
            )
            if not clone_row:
                clone_row = clone_options[0]

            structure_row = next(
                (
                    row
                    for row in structure_options
                    if int(row.get("structure_id") or 0) == selected_structure_id
                ),
                None,
            )
            if not structure_row:
                messages.error(request, _("Please choose a valid structure."))
                return redirect("indy_hub:reprocessing_become")
            rig_profile = _get_rig_profile(
                str(structure_row.get("suggested_rig_profile_key") or "none")
            )

            try:
                skill_levels = fetch_character_skill_levels(int(selected_character_id))
            except Exception:
                messages.error(
                    request,
                    _(
                        "Unable to refresh character skills from corptools cache. "
                        "Ensure corptools has synced this character."
                    ),
                )
                return redirect("indy_hub:reprocessing_become")
            skill_snapshot = build_reprocessing_skill_snapshot(skill_levels)
            reprocessing_skill_rows = _build_reprocessing_skill_rows(skill_levels)
            character_proficiency = _compute_character_proficiency(reprocessing_skill_rows)
            active_skill_levels_by_id: dict[str, int] = {}
            for raw_skill_id, row in (skill_levels or {}).items():
                try:
                    skill_id = int(raw_skill_id)
                    active_level = int((row or {}).get("active") or 0)
                except (TypeError, ValueError, AttributeError):
                    continue
                if skill_id > 0 and active_level > 0:
                    active_skill_levels_by_id[str(skill_id)] = active_level
            estimated_yield = compute_estimated_yield_percent(
                skill_snapshot=skill_snapshot,
                implant_bonus_percent=_to_decimal(clone_row.get("beancounter_bonus_percent")),
                structure_bonus_percent=_to_decimal(structure_row.get("structure_bonus_percent")),
                rig_bonus_percent=_to_decimal(rig_profile.get("bonus_percent")),
                security_bonus_percent=_to_decimal(structure_row.get("security_bonus_percent")),
            )

            character_corp_id = int(selected_character_row.get("corporation_id") or 0) or None
            character_corp_name = str(selected_character_row.get("corporation_name") or "")
            _scope_corp_name, alliance_id, alliance_name = _resolve_corp_and_alliance_names(selected_corporation_id)

            with transaction.atomic():
                profile = profile_by_character.get(int(selected_character_id))
                is_new_profile = profile is None
                if profile is None:
                    profile = ReprocessingServiceProfile(
                        user=request.user,
                        character_id=int(selected_character_id),
                    )

                profile.character_name = str(selected_character_row.get("character_name") or get_character_name(int(selected_character_id)))
                profile.corporation_id = character_corp_id
                profile.corporation_name = character_corp_name
                profile.alliance_id = alliance_id
                profile.alliance_name = alliance_name
                profile.selected_corporation_id = int(selected_corporation_id)

                profile.margin_percent = margin_percent
                profile.selected_clone_id = int(clone_row.get("clone_id") or 0)
                profile.selected_clone_label = str(clone_row.get("clone_label") or "")
                profile.selected_implant_type_ids = [int(tid) for tid in (clone_row.get("implant_type_ids") or []) if int(tid) > 0]
                profile.selected_implant_names = [str(name) for name in (clone_row.get("implant_names") or []) if str(name or "").strip()]
                profile.beancounter_bonus_percent = _to_decimal(clone_row.get("beancounter_bonus_percent")).quantize(Decimal("0.001"))

                profile.reprocessing_skill_level = int(skill_snapshot.get("reprocessing") or 0)
                profile.reprocessing_efficiency_level = int(skill_snapshot.get("reprocessing_efficiency") or 0)
                profile.processing_skill_level = int(skill_snapshot.get("processing") or 0)
                profile.skill_levels = {
                    "reprocessing": int(skill_snapshot.get("reprocessing") or 0),
                    "reprocessing_efficiency": int(skill_snapshot.get("reprocessing_efficiency") or 0),
                    "processing": int(skill_snapshot.get("processing") or 0),
                    "scrapmetal_processing": int(skill_snapshot.get("scrapmetal_processing") or 0),
                    "security_bonus_percent": str(
                        _to_decimal(structure_row.get("security_bonus_percent")).quantize(
                            Decimal("0.001")
                        )
                    ),
                    "skill_levels_by_id": active_skill_levels_by_id,
                }

                profile.structure_id = int(structure_row.get("structure_id") or 0)
                profile.structure_name = str(structure_row.get("structure_name") or "")
                profile.structure_type_id = int(structure_row.get("structure_type_id") or 0) or None
                profile.structure_type_name = str(structure_row.get("structure_type_name") or "")
                profile.structure_location_name = str(structure_row.get("location_name") or "")
                profile.structure_bonus_percent = _to_decimal(structure_row.get("structure_bonus_percent")).quantize(Decimal("0.001"))
                profile.rig_profile_key = str(rig_profile.get("key") or "")
                profile.rig_profile_name = str(rig_profile.get("label") or "")
                profile.rig_bonus_percent = _to_decimal(rig_profile.get("bonus_percent")).quantize(Decimal("0.001"))
                profile.estimated_yield_percent = _to_decimal(estimated_yield).quantize(Decimal("0.001"))

                approval_resubmitted = False
                if profile.approval_status != ReprocessingServiceProfile.ApprovalStatus.APPROVED or action == "submit_application":
                    if profile.approval_status != ReprocessingServiceProfile.ApprovalStatus.PENDING:
                        approval_resubmitted = True
                    profile.approval_status = ReprocessingServiceProfile.ApprovalStatus.PENDING
                    profile.reviewed_by = None
                    profile.reviewed_at = None
                    profile.review_notes = ""
                admin_forced_unavailable = bool(profile.admin_force_unavailable)
                profile.is_available = bool(
                    requested_available
                    and profile.approval_status == ReprocessingServiceProfile.ApprovalStatus.APPROVED
                    and not admin_forced_unavailable
                )
                profile.save()

                if is_new_profile or approval_resubmitted:
                    profile_link = request.build_absolute_uri(
                        reverse("indy_hub:reprocessing_admin_applications")
                    )
                    _notify_material_exchange_admins(
                        title="Reprocessing application submitted",
                        message=(
                            f"{profile.character_name} submitted a reprocessing profile for review.\n"
                            f"Yield: {profile.estimated_yield_percent}% | Margin: {profile.margin_percent}%\n"
                            f"Structure: {profile.structure_name}"
                        ),
                        level="info",
                        link=profile_link,
                    )
                    notify_user(
                        request.user,
                        _("Reprocessing application submitted"),
                        _(
                            "Your reprocessing profile for %(character)s has been submitted and is awaiting Material Exchange admin approval."
                        )
                        % {"character": profile.character_name},
                        level="info",
                        link=reverse("indy_hub:reprocessing_become"),
                    )

                if profile.approval_status == ReprocessingServiceProfile.ApprovalStatus.APPROVED:
                    if requested_available and profile.admin_force_unavailable:
                        messages.warning(
                            request,
                            _(
                                "Availability is currently disabled by a Material Exchange admin and cannot be self-enabled."
                            ),
                        )
                    messages.success(request, _("Reprocessing profile updated."))
                else:
                    messages.success(request, _("Reprocessing profile saved and queued for approval."))
                return redirect("indy_hub:reprocessing_become")

    for profile in existing_profiles:
        profile.portrait_url = _avatar_url(int(profile.character_id), size=64)
        profile.beancounter_implants = _beancounter_implants(profile.selected_implant_names)

    context = {
        "character_rows": character_rows,
        "corporation_rows": corporation_rows,
        "existing_profiles": existing_profiles,
        "selected_profile": selected_profile,
        "selected_character_id": int(selected_character_id or 0),
        "selected_corporation_id": int(selected_corporation_id or 0),
        "selected_clone_id": int(selected_clone_id or 0),
        "selected_structure_id": int(selected_structure_id or 0),
        "selected_clone_row": selected_clone_row or {},
        "selected_clone_beancounter_implants": selected_clone_beancounter_implants,
        "selected_structure_row": selected_structure_row or {},
        "estimated_yield_preview": estimated_yield_preview,
        "preview_rig_profile": preview_rig_profile,
        "reprocessing_skill_rows": reprocessing_skill_rows,
        "character_proficiency": character_proficiency,
        "has_required_scopes": has_required_scopes,
        "scope_error": scope_error,
        "authorize_url": reverse("indy_hub:reprocessing_authorize_scopes"),
        "clone_options": clone_options,
        "structure_options": structure_options,
        "skill_snapshot": skill_snapshot,
    }
    context.update(_build_nav_context(request.user, active_tab="reprocessing"))
    return render(request, "indy_hub/reprocessing_services/become.html", context)


@indy_hub_access_required
@login_required
def reprocessing_browse(request):
    emit_view_analytics_event(view_name="reprocessing_services.browse", request=request)
    sort_key = str(request.GET.get("sort") or "yield_desc").strip().lower()
    order_map = {
        "yield_desc": ["-estimated_yield_percent", "margin_percent", "character_name"],
        "yield_asc": ["estimated_yield_percent", "margin_percent", "character_name"],
        "margin_asc": ["margin_percent", "-estimated_yield_percent", "character_name"],
        "margin_desc": ["-margin_percent", "-estimated_yield_percent", "character_name"],
        "location": ["structure_location_name", "-estimated_yield_percent", "character_name"],
        "character": ["character_name"],
    }
    queryset = ReprocessingServiceProfile.objects.filter(
        approval_status=ReprocessingServiceProfile.ApprovalStatus.APPROVED,
        is_available=True,
    ).order_by(*order_map.get(sort_key, order_map["yield_desc"]))

    profile_cards = []
    for profile in queryset:
        beancounter_names = _beancounter_implants(profile.selected_implant_names)
        profile_cards.append(
            {
                "profile": profile,
                "portrait_url": _avatar_url(int(profile.character_id), size=128),
                "beancounter_implants": beancounter_names,
                "skills": {
                    "reprocessing": int(profile.reprocessing_skill_level or 0),
                    "efficiency": int(profile.reprocessing_efficiency_level or 0),
                    "processing": int(profile.processing_skill_level or 0),
                },
            }
        )

    context = {
        "profile_cards": profile_cards,
        "sort_key": sort_key,
        "sort_options": [
            ("yield_desc", _("Highest yield")),
            ("yield_asc", _("Lowest yield")),
            ("margin_asc", _("Lowest margin")),
            ("margin_desc", _("Highest margin")),
            ("location", _("Location")),
            ("character", _("Character name")),
        ],
    }
    context.update(_build_nav_context(request.user, active_tab="reprocessing"))
    return render(request, "indy_hub/reprocessing_services/browse.html", context)


@indy_hub_access_required
@login_required
def reprocessing_my_requests(request):
    emit_view_analytics_event(
        view_name="reprocessing_services.my_requests",
        request=request,
    )
    requester_qs = (
        ReprocessingServiceRequest.objects.select_related("processor_user")
        .filter(requester=request.user)
        .order_by("-updated_at", "-created_at")
    )
    processor_qs = (
        ReprocessingServiceRequest.objects.select_related("requester")
        .filter(processor_user=request.user)
        .order_by("-updated_at", "-created_at")
    )

    requester_rows = [
        {
            "request": service_request,
            "status_badge_class": _request_status_badge_class(service_request.status),
            "counterparty_name": str(
                service_request.processor_character_name
                or getattr(service_request.processor_user, "username", "")
                or "-"
            ),
            "counterparty_character_id": int(service_request.processor_character_id or 0),
        }
        for service_request in requester_qs
    ]
    processor_rows = [
        {
            "request": service_request,
            "status_badge_class": _request_status_badge_class(service_request.status),
            "counterparty_name": str(
                service_request.requester_character_name
                or getattr(service_request.requester, "username", "")
                or "-"
            ),
            "counterparty_character_id": int(service_request.requester_character_id or 0),
        }
        for service_request in processor_qs
    ]

    context = {
        "requester_rows": requester_rows,
        "processor_rows": processor_rows,
        "requester_count": len(requester_rows),
        "processor_count": len(processor_rows),
    }
    context.update(_build_nav_context(request.user, active_tab="reprocessing"))
    return render(request, "indy_hub/reprocessing_services/my_requests.html", context)


@indy_hub_permission_required("can_manage_material_hub")
@login_required
def reprocessing_admin_applications(request):
    emit_view_analytics_event(
        view_name="reprocessing_services.admin_applications",
        request=request,
    )
    pending_profiles = ReprocessingServiceProfile.objects.filter(
        approval_status=ReprocessingServiceProfile.ApprovalStatus.PENDING
    ).order_by("created_at")
    reviewed_profiles = ReprocessingServiceProfile.objects.exclude(
        approval_status=ReprocessingServiceProfile.ApprovalStatus.PENDING
    ).select_related("reviewed_by").order_by("-reviewed_at", "-updated_at")[:100]

    context = {
        "pending_profiles": pending_profiles,
        "reviewed_profiles": reviewed_profiles,
    }
    context.update(_build_nav_context(request.user, active_tab="reprocessing"))
    return render(request, "indy_hub/reprocessing_services/admin_applications.html", context)


@indy_hub_permission_required("can_manage_material_hub")
@login_required
@require_http_methods(["POST"])
def reprocessing_admin_review(request, profile_id: int):
    emit_view_analytics_event(view_name="reprocessing_services.admin_review", request=request)
    profile = get_object_or_404(ReprocessingServiceProfile, pk=int(profile_id))
    action = str(request.POST.get("action") or "").strip().lower()
    review_notes = str(request.POST.get("review_notes") or "").strip()
    set_available = bool(request.POST.get("is_available") == "on")

    if action not in {"approve", "reject", "admin_enable", "admin_disable"}:
        messages.error(request, _("Invalid review action."))
        return redirect("indy_hub:reprocessing_admin_applications")

    if action in {"admin_enable", "admin_disable"}:
        if profile.approval_status != ReprocessingServiceProfile.ApprovalStatus.APPROVED:
            messages.error(
                request,
                _("Only approved reprocessors can be manually enabled or disabled."),
            )
            return redirect("indy_hub:reprocessing_admin_applications")

        profile.reviewed_by = request.user
        profile.reviewed_at = timezone.now()
        if review_notes:
            profile.review_notes = review_notes

        if action == "admin_disable":
            profile.admin_force_unavailable = True
            profile.is_available = False
            profile.save()
            notify_user(
                profile.user,
                _("Reprocessing availability disabled by admin"),
                _(
                    "A Material Exchange admin disabled new contracts for %(character)s."
                )
                % {"character": profile.character_name},
                level="warning",
                link=reverse("indy_hub:reprocessing_become"),
            )
            messages.warning(request, _("Reprocessor disabled by admin."))
        else:
            profile.admin_force_unavailable = False
            profile.is_available = True
            profile.save()
            notify_user(
                profile.user,
                _("Reprocessing availability enabled by admin"),
                _(
                    "A Material Exchange admin enabled new contracts for %(character)s."
                )
                % {"character": profile.character_name},
                level="success",
                link=reverse("indy_hub:reprocessing_become"),
            )
            messages.success(request, _("Reprocessor enabled by admin."))
        return redirect("indy_hub:reprocessing_admin_applications")

    profile.reviewed_by = request.user
    profile.reviewed_at = timezone.now()
    profile.review_notes = review_notes

    if action == "approve":
        profile.approval_status = ReprocessingServiceProfile.ApprovalStatus.APPROVED
        profile.admin_force_unavailable = False
        profile.is_available = set_available
        profile.save()
        notify_user(
            profile.user,
            _("Reprocessing application approved"),
            _(
                "Your reprocessing profile for %(character)s has been approved."
            )
            % {"character": profile.character_name},
            level="success",
            link=reverse("indy_hub:reprocessing_become"),
        )
        messages.success(request, _("Application approved."))
    else:
        profile.approval_status = ReprocessingServiceProfile.ApprovalStatus.REJECTED
        profile.admin_force_unavailable = False
        profile.is_available = False
        profile.save()
        notify_user(
            profile.user,
            _("Reprocessing application rejected"),
            _(
                "Your reprocessing profile for %(character)s was rejected. Review notes and resubmit when ready."
            )
            % {"character": profile.character_name},
            level="warning",
            link=reverse("indy_hub:reprocessing_become"),
        )
        messages.warning(request, _("Application rejected."))
    return redirect("indy_hub:reprocessing_admin_applications")

@indy_hub_access_required
@login_required
@require_http_methods(["GET", "POST"])
def reprocessing_request_create(request, profile_id: int):
    emit_view_analytics_event(view_name="reprocessing_services.request_create", request=request)
    profile = get_object_or_404(
        ReprocessingServiceProfile.objects.select_related("user"),
        pk=int(profile_id),
        approval_status=ReprocessingServiceProfile.ApprovalStatus.APPROVED,
    )
    if not profile.is_available:
        messages.error(request, _("This reprocessor is currently unavailable for new requests."))
        return redirect("indy_hub:reprocessing_browse")

    requester_character_rows = _get_user_character_rows(request.user)
    main_character_id, main_character_name = _get_user_main_character(request.user)
    selected_requester_character_id_raw = (
        request.POST.get("requester_character_id")
        or request.GET.get("requester_character_id")
        or (main_character_id or 0)
        or (requester_character_rows[0]["character_id"] if requester_character_rows else 0)
    )
    try:
        selected_requester_character_id = int(selected_requester_character_id_raw or 0)
    except (TypeError, ValueError):
        selected_requester_character_id = int(main_character_id or 0)
    selected_requester_row = next(
        (
            row
            for row in requester_character_rows
            if int(row.get("character_id", 0)) == int(selected_requester_character_id)
        ),
        None,
    )
    selected_requester_character_name = (
        str(selected_requester_row.get("character_name"))
        if selected_requester_row
        else (str(main_character_name) if main_character_name else request.user.username)
    )

    items_text = str(request.POST.get("items_text") or "").strip()
    action = str(request.POST.get("action") or "").strip().lower()
    parsed_items: list[dict[str, int]] = []
    parse_errors: list[str] = []
    estimate_payload: dict[str, object] | None = None
    unsupported_inputs: list[dict[str, int]] = []
    estimate_cache_token = ""
    estimate_cache_key = ""
    submit_blocked_by_estimate_state = False

    if request.method == "POST":
        if requester_character_rows and not selected_requester_row:
            messages.error(request, _("Please choose one of your owned characters as requester."))
            context = {
                "profile": profile,
                "profile_portrait_url": _avatar_url(int(profile.character_id), size=128),
                "requester_character_rows": requester_character_rows,
                "selected_requester_character_id": selected_requester_character_id,
                "selected_requester_character_name": selected_requester_character_name,
                "items_text": items_text,
                "parsed_item_rows": [],
                "parse_errors": [],
                "estimate_payload": None,
                "unsupported_rows": [],
                "estimate_ready": False,
                "estimate_cache_key": "",
                "estimate_token": "",
            }
            context.update(_build_nav_context(request.user, active_tab="reprocessing"))
            return render(request, "indy_hub/reprocessing_services/request_create.html", context)

        estimate_cache_token = _build_estimate_cache_token(
            profile_id=int(profile.id),
            requester_character_id=int(selected_requester_character_id or 0),
            items_text=items_text,
            profile_updated_at=profile.updated_at,
        )
        estimate_cache_key = _build_estimate_cache_key(
            user_id=int(request.user.id),
            profile_id=int(profile.id),
            token=estimate_cache_token,
        )

        parsed_items, parse_errors = _parse_request_item_lines(items_text)
        if not parsed_items:
            if parse_errors:
                messages.error(
                    request,
                    _("No valid item lines were parsed. Invalid lines: %(lines)s")
                    % {"lines": "; ".join(parse_errors[:5])},
                )
            else:
                messages.error(request, _("Enter at least one valid item line."))
        else:
            if parse_errors:
                messages.warning(
                    request,
                    _("Some item lines are invalid and were ignored: %(lines)s")
                    % {"lines": "; ".join(parse_errors[:5])},
                )
            used_cached_estimate = False
            if action == "submit_request":
                submitted_cache_key = str(request.POST.get("estimate_cache_key") or "").strip()
                submitted_cache_token = str(request.POST.get("estimate_token") or "").strip()
                if not submitted_cache_key or not submitted_cache_token:
                    submit_blocked_by_estimate_state = True
                    messages.error(
                        request,
                        _("Estimate data is missing. Click Estimate again before submitting."),
                    )
                elif (
                    submitted_cache_token != estimate_cache_token
                    or submitted_cache_key != estimate_cache_key
                ):
                    submit_blocked_by_estimate_state = True
                    messages.error(
                        request,
                        _("Estimate data is out of date. Click Estimate again before submitting."),
                    )
                else:
                    cached = cache.get(submitted_cache_key)
                    if isinstance(cached, dict) and str(cached.get("token") or "") == estimate_cache_token:
                        cached_parsed_items = list(cached.get("parsed_items") or [])
                        if cached_parsed_items == parsed_items:
                            cached_estimate = cached.get("estimate_payload")
                            if isinstance(cached_estimate, dict):
                                estimate_payload = cached_estimate
                                unsupported_inputs = list(cached.get("unsupported_inputs") or [])
                                used_cached_estimate = True
                    if not used_cached_estimate:
                        submit_blocked_by_estimate_state = True
                        messages.error(
                            request,
                            _(
                                "Estimate expired or no longer matches your input. Click Estimate again before submitting."
                            ),
                        )

            if not used_cached_estimate and action != "submit_request":
                profile_skill_map = (
                    profile.skill_levels
                    if isinstance(profile.skill_levels, dict)
                    else {}
                )
                raw_skill_levels_by_id = profile_skill_map.get("skill_levels_by_id") or {}
                skill_levels_by_id: dict[int, int] = {}
                if isinstance(raw_skill_levels_by_id, dict):
                    for raw_skill_id, raw_level in raw_skill_levels_by_id.items():
                        try:
                            skill_id = int(raw_skill_id)
                            level = int(raw_level or 0)
                        except (TypeError, ValueError):
                            continue
                        if skill_id > 0 and level > 0:
                            skill_levels_by_id[skill_id] = level

                base_skill_snapshot = {
                    "reprocessing": int(profile.reprocessing_skill_level or 0),
                    "reprocessing_efficiency": int(profile.reprocessing_efficiency_level or 0),
                    "processing": int(profile.processing_skill_level or 0),
                }
                yield_percent_by_type: dict[int, Decimal] = {}
                security_bonus_raw = profile_skill_map.get("security_bonus_percent")
                if security_bonus_raw is not None:
                    security_bonus_percent = _to_decimal(security_bonus_raw)
                    for row in parsed_items:
                        source_type_id = int(row.get("type_id") or 0)
                        if source_type_id <= 0:
                            continue
                        processing_level = resolve_processing_skill_level_for_item(
                            type_id=source_type_id,
                            skill_levels_by_id=skill_levels_by_id,
                            fallback_level=int(base_skill_snapshot.get("processing") or 0),
                        )
                        yield_percent_by_type[source_type_id] = compute_estimated_yield_percent(
                            skill_snapshot={
                                "reprocessing": int(base_skill_snapshot["reprocessing"]),
                                "reprocessing_efficiency": int(
                                    base_skill_snapshot["reprocessing_efficiency"]
                                ),
                                "processing": int(processing_level),
                            },
                            implant_bonus_percent=_to_decimal(profile.beancounter_bonus_percent),
                            structure_bonus_percent=_to_decimal(profile.structure_bonus_percent),
                            rig_bonus_percent=_to_decimal(profile.rig_bonus_percent),
                            security_bonus_percent=security_bonus_percent,
                        )

                estimate_payload = build_reprocessing_estimate(
                    input_items=parsed_items,
                    yield_percent=Decimal(str(profile.estimated_yield_percent or 0)),
                    margin_percent=Decimal(str(profile.margin_percent or 0)),
                    yield_percent_by_type=yield_percent_by_type,
                )
                unsupported_inputs = list(estimate_payload.get("unsupported_inputs") or [])

                if estimate_payload and not parse_errors:
                    cache.set(
                        estimate_cache_key,
                        {
                            "token": estimate_cache_token,
                            "parsed_items": parsed_items,
                            "estimate_payload": estimate_payload,
                            "unsupported_inputs": unsupported_inputs,
                        },
                        timeout=_REPROCESSING_ESTIMATE_CACHE_TTL_SECONDS,
                    )

            if estimate_payload and unsupported_inputs:
                unsupported_names = ", ".join(
                    f"{get_type_name(int(row['type_id']))} x{int(row['quantity'])}"
                    for row in unsupported_inputs[:8]
                )
                messages.error(
                    request,
                    _("Some items have no SDE reprocessing outputs and are unsupported: %(items)s")
                    % {"items": unsupported_names},
                )
            elif estimate_payload and not (estimate_payload.get("outputs") or []):
                messages.error(request, _("No reprocessing outputs were produced from the submitted inputs."))

        if (
            action == "submit_request"
            and parsed_items
            and not parse_errors
            and estimate_payload
            and not unsupported_inputs
        ):
            with transaction.atomic():
                service_request = ReprocessingServiceRequest.objects.create(
                    requester=request.user,
                    requester_character_id=(
                        int(selected_requester_character_id) if selected_requester_character_id > 0 else None
                    ),
                    requester_character_name=selected_requester_character_name,
                    processor_profile=profile,
                    processor_user=profile.user,
                    processor_character_id=int(profile.character_id),
                    processor_character_name=profile.character_name,
                    status=ReprocessingServiceRequest.Status.REQUEST_SUBMITTED,
                    structure_id=int(profile.structure_id or 0),
                    structure_name=profile.structure_name,
                    structure_type_name=profile.structure_type_name,
                    structure_location_name=profile.structure_location_name,
                    margin_percent_snapshot=Decimal(str(profile.margin_percent or 0)).quantize(Decimal("0.01")),
                    estimated_yield_percent_snapshot=Decimal(str(profile.estimated_yield_percent or 0)).quantize(Decimal("0.001")),
                    estimated_output_value=Decimal(str(estimate_payload.get("total_output_value") or 0)).quantize(Decimal("0.01")),
                    reward_isk=Decimal(str(estimate_payload.get("reward_isk") or 0)).quantize(Decimal("0.01")),
                    tolerance_percent=Decimal("1.00"),
                )
                ReprocessingServiceRequestItem.objects.bulk_create(
                    [
                        ReprocessingServiceRequestItem(
                            request=service_request,
                            type_id=int(row["type_id"]),
                            type_name=get_type_name(int(row["type_id"])),
                            quantity=int(row["quantity"]),
                        )
                        for row in parsed_items
                    ]
                )
                ReprocessingServiceRequestOutput.objects.bulk_create(
                    [
                        ReprocessingServiceRequestOutput(
                            request=service_request,
                            type_id=int(output_row["type_id"]),
                            type_name=str(output_row.get("type_name") or get_type_name(int(output_row["type_id"]))),
                            expected_quantity=int(output_row["expected_quantity"]),
                            estimated_unit_price=Decimal(str(output_row.get("unit_price") or 0)).quantize(Decimal("0.01")),
                            estimated_total_value=Decimal(str(output_row.get("total_value") or 0)).quantize(Decimal("0.01")),
                        )
                        for output_row in (estimate_payload.get("outputs") or [])
                    ]
                )
                service_request.status = ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT
                service_request.save(update_fields=["status", "updated_at"])

            detail_link = reverse("indy_hub:reprocessing_request_detail", args=[service_request.id])
            notify_user(
                request.user,
                _("Reprocessing request submitted"),
                _(
                    "Your request %(reference)s has been submitted. Follow the inbound contract instructions on the detail page."
                )
                % {"reference": service_request.request_reference},
                level="success",
                link=detail_link,
            )
            notify_user(
                profile.user,
                _("Incoming reprocessing request"),
                _(
                    "%(character)s sent a new reprocessing request %(reference)s.\n"
                    "Create guidance is now available on the request detail page."
                )
                % {
                    "character": selected_requester_character_name,
                    "reference": service_request.request_reference,
                },
                level="info",
                link=detail_link,
            )
            messages.success(
                request,
                _(
                    "Request submitted. Create an inbound Item Exchange contract to %(processor)s with 0 ISK reward, 14-day completion, and title %(reference)s."
                )
                % {
                    "processor": profile.character_name,
                    "reference": service_request.request_reference,
                },
            )
            if estimate_cache_key:
                cache.delete(estimate_cache_key)
            return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)
        elif action == "submit_request" and not submit_blocked_by_estimate_state:
            messages.error(
                request,
                _("Generate a valid estimate first, then submit the request."),
            )

    parsed_item_rows = [
        {
            "type_id": int(row["type_id"]),
            "type_name": get_type_name(int(row["type_id"])),
            "quantity": int(row["quantity"]),
        }
        for row in parsed_items
    ]
    unsupported_rows = [
        {
            "type_id": int(row["type_id"]),
            "type_name": get_type_name(int(row["type_id"])),
            "quantity": int(row["quantity"]),
        }
        for row in unsupported_inputs
    ]
    estimate_ready = bool(
        estimate_payload
        and (estimate_payload.get("outputs") or [])
        and not unsupported_inputs
        and parsed_items
        and not parse_errors
    )

    context = {
        "profile": profile,
        "profile_portrait_url": _avatar_url(int(profile.character_id), size=128),
        "requester_character_rows": requester_character_rows,
        "selected_requester_character_id": selected_requester_character_id,
        "selected_requester_character_name": selected_requester_character_name,
        "items_text": items_text,
        "parsed_item_rows": parsed_item_rows,
        "parse_errors": parse_errors,
        "estimate_payload": estimate_payload,
        "unsupported_rows": unsupported_rows,
        "estimate_ready": estimate_ready,
        "estimate_cache_key": estimate_cache_key,
        "estimate_token": estimate_cache_token,
    }
    context.update(_build_nav_context(request.user, active_tab="reprocessing"))
    return render(request, "indy_hub/reprocessing_services/request_create.html", context)


@indy_hub_access_required
@login_required
def reprocessing_request_detail(request, request_id: int):
    emit_view_analytics_event(view_name="reprocessing_services.request_detail", request=request)
    service_request = get_object_or_404(
        ReprocessingServiceRequest.objects.select_related("requester", "processor_user", "processor_profile").prefetch_related("items", "expected_outputs"),
        pk=int(request_id),
    )
    if not _user_can_access_request(request.user, service_request):
        messages.error(request, _("You do not have access to this reprocessing request."))
        return redirect("indy_hub:reprocessing_browse")

    is_requester = request.user.id == service_request.requester_id
    is_processor = request.user.id == service_request.processor_user_id
    is_admin = request.user.has_perm("indy_hub.can_manage_material_hub")
    status = service_request.status

    can_submit_inbound = is_requester and status in {
        ReprocessingServiceRequest.Status.REQUEST_SUBMITTED,
        ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT,
    }
    can_verify_inbound = (is_requester or is_processor or is_admin) and status == ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT
    can_mark_processing = (is_processor or is_admin) and status == ReprocessingServiceRequest.Status.INBOUND_CONTRACT_VERIFIED
    can_mark_awaiting_return = (is_processor or is_admin) and status == ReprocessingServiceRequest.Status.PROCESSING
    can_submit_return = (is_processor or is_admin) and status in {
        ReprocessingServiceRequest.Status.PROCESSING,
        ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT,
    }
    can_verify_return = (is_requester or is_processor or is_admin) and status == ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT
    can_cancel = (is_requester or is_admin) and not service_request.is_terminal
    can_dispute = (is_requester or is_processor or is_admin) and not service_request.is_terminal

    inbound_contract = (
        ESIContract.objects.filter(contract_id=int(service_request.inbound_contract_id or 0))
        .first()
        if service_request.inbound_contract_id
        else None
    )
    return_contract = (
        ESIContract.objects.filter(contract_id=int(service_request.return_contract_id or 0))
        .first()
        if service_request.return_contract_id
        else None
    )

    context = {
        "service_request": service_request,
        "timeline": _build_request_timeline(service_request),
        "is_requester": is_requester,
        "is_processor": is_processor,
        "is_admin": is_admin,
        "can_submit_inbound": can_submit_inbound,
        "can_verify_inbound": can_verify_inbound,
        "can_mark_processing": can_mark_processing,
        "can_mark_awaiting_return": can_mark_awaiting_return,
        "can_submit_return": can_submit_return,
        "can_verify_return": can_verify_return,
        "can_cancel": can_cancel,
        "can_dispute": can_dispute,
        "inbound_contract": inbound_contract,
        "return_contract": return_contract,
        "processor_portrait_url": _avatar_url(int(service_request.processor_character_id), size=128),
    }
    context.update(_build_nav_context(request.user, active_tab="reprocessing"))
    return render(request, "indy_hub/reprocessing_services/request_detail.html", context)


def _get_request_with_access_check(request, request_id: int) -> ReprocessingServiceRequest | None:
    service_request = get_object_or_404(
        ReprocessingServiceRequest.objects.select_related("requester", "processor_user", "processor_profile").prefetch_related("items", "expected_outputs"),
        pk=int(request_id),
    )
    if not _user_can_access_request(request.user, service_request):
        messages.error(request, _("You do not have access to this reprocessing request."))
        return None
    return service_request


@indy_hub_access_required
@login_required
@require_http_methods(["POST"])
def reprocessing_request_submit_inbound(request, request_id: int):
    emit_view_analytics_event(
        view_name="reprocessing_services.request_submit_inbound",
        request=request,
    )
    service_request = _get_request_with_access_check(request, request_id)
    if service_request is None:
        return redirect("indy_hub:reprocessing_browse")

    if request.user.id != service_request.requester_id and not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Only the requester can submit the inbound contract."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    try:
        inbound_contract_id = int(request.POST.get("inbound_contract_id") or 0)
    except (TypeError, ValueError):
        inbound_contract_id = 0
    if inbound_contract_id <= 0:
        messages.error(request, _("Enter a valid inbound contract ID."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    if service_request.is_terminal:
        messages.error(request, _("This request is already closed."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)
    if service_request.status not in {
        ReprocessingServiceRequest.Status.REQUEST_SUBMITTED,
        ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT,
    }:
        messages.error(request, _("Inbound contract cannot be changed in the current status."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    service_request.inbound_contract_id = inbound_contract_id
    service_request.inbound_contract_verified_at = None
    service_request.status = ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT
    service_request.save(
        update_fields=[
            "inbound_contract_id",
            "inbound_contract_verified_at",
            "status",
            "updated_at",
        ]
    )
    messages.success(request, _("Inbound contract submitted. Run verification once the contract is cached."))
    return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)


@indy_hub_access_required
@login_required
@require_http_methods(["POST"])
def reprocessing_request_verify_inbound(request, request_id: int):
    emit_view_analytics_event(
        view_name="reprocessing_services.request_verify_inbound",
        request=request,
    )
    service_request = _get_request_with_access_check(request, request_id)
    if service_request is None:
        return redirect("indy_hub:reprocessing_browse")

    if service_request.status != ReprocessingServiceRequest.Status.AWAITING_INBOUND_CONTRACT:
        messages.error(request, _("Inbound verification is not available in the current status."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    matches, reason = _verify_inbound_contract(service_request)
    if not matches:
        messages.error(request, reason)
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    service_request.status = ReprocessingServiceRequest.Status.INBOUND_CONTRACT_VERIFIED
    service_request.inbound_contract_verified_at = timezone.now()
    service_request.save(update_fields=["status", "inbound_contract_verified_at", "updated_at"])

    detail_link = reverse("indy_hub:reprocessing_request_detail", args=[service_request.id])
    notify_user(
        service_request.processor_user,
        _("Inbound contract verified"),
        _(
            "Inbound contract for request %(reference)s is verified. You can now start processing and prepare the return contract."
        )
        % {"reference": service_request.request_reference},
        level="success",
        link=detail_link,
    )
    notify_user(
        service_request.requester,
        _("Inbound contract verified"),
        _(
            "Inbound contract for request %(reference)s has been verified."
        )
        % {"reference": service_request.request_reference},
        level="success",
        link=detail_link,
    )
    messages.success(request, _("Inbound contract verified successfully."))
    return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)


@indy_hub_access_required
@login_required
@require_http_methods(["POST"])
def reprocessing_request_mark_processing(request, request_id: int):
    emit_view_analytics_event(
        view_name="reprocessing_services.request_mark_processing",
        request=request,
    )
    service_request = _get_request_with_access_check(request, request_id)
    if service_request is None:
        return redirect("indy_hub:reprocessing_browse")

    if request.user.id != service_request.processor_user_id and not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Only the assigned reprocessor can start processing."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)
    if service_request.status != ReprocessingServiceRequest.Status.INBOUND_CONTRACT_VERIFIED:
        messages.error(request, _("Request must have a verified inbound contract before processing starts."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    service_request.status = ReprocessingServiceRequest.Status.PROCESSING
    service_request.save(update_fields=["status", "updated_at"])
    messages.success(request, _("Request moved to processing."))
    return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)


@indy_hub_access_required
@login_required
@require_http_methods(["POST"])
def reprocessing_request_mark_awaiting_return(request, request_id: int):
    emit_view_analytics_event(
        view_name="reprocessing_services.request_mark_awaiting_return",
        request=request,
    )
    service_request = _get_request_with_access_check(request, request_id)
    if service_request is None:
        return redirect("indy_hub:reprocessing_browse")

    if request.user.id != service_request.processor_user_id and not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Only the assigned reprocessor can update this status."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)
    if service_request.status != ReprocessingServiceRequest.Status.PROCESSING:
        messages.error(request, _("Request must be in processing before awaiting return contract."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    service_request.status = ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT
    service_request.save(update_fields=["status", "updated_at"])
    messages.success(request, _("Request moved to awaiting return contract."))
    return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

@indy_hub_access_required
@login_required
@require_http_methods(["POST"])
def reprocessing_request_submit_return(request, request_id: int):
    emit_view_analytics_event(
        view_name="reprocessing_services.request_submit_return",
        request=request,
    )
    service_request = _get_request_with_access_check(request, request_id)
    if service_request is None:
        return redirect("indy_hub:reprocessing_browse")

    if request.user.id != service_request.processor_user_id and not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Only the assigned reprocessor can submit the return contract."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    if service_request.status not in {
        ReprocessingServiceRequest.Status.PROCESSING,
        ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT,
    }:
        messages.error(request, _("Return contract cannot be submitted in the current status."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    try:
        return_contract_id = int(request.POST.get("return_contract_id") or 0)
    except (TypeError, ValueError):
        return_contract_id = 0
    if return_contract_id <= 0:
        messages.error(request, _("Enter a valid return contract ID."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    service_request.return_contract_id = return_contract_id
    service_request.return_contract_verified_at = None
    service_request.status = ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT
    service_request.save(
        update_fields=[
            "return_contract_id",
            "return_contract_verified_at",
            "status",
            "updated_at",
        ]
    )
    messages.success(request, _("Return contract submitted. Run verification once cached."))
    return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)


@indy_hub_access_required
@login_required
@require_http_methods(["POST"])
def reprocessing_request_verify_return(request, request_id: int):
    emit_view_analytics_event(
        view_name="reprocessing_services.request_verify_return",
        request=request,
    )
    service_request = _get_request_with_access_check(request, request_id)
    if service_request is None:
        return redirect("indy_hub:reprocessing_browse")

    if service_request.status != ReprocessingServiceRequest.Status.AWAITING_RETURN_CONTRACT:
        messages.error(request, _("Return verification is not available in the current status."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    matches, reason = _verify_return_contract(service_request)
    if not matches:
        logger.error(
            "Reprocessing return verification failed for request %s: %s",
            service_request.request_reference,
            reason,
        )
        messages.error(request, reason)
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    return_contract = ESIContract.objects.filter(
        contract_id=int(service_request.return_contract_id or 0)
    ).prefetch_related("items").first()
    if return_contract:
        actual_by_type = aggregate_contract_items_by_type(
            return_contract.items.filter(is_included=True)
        )
        for output in service_request.expected_outputs.all():
            output.actual_quantity = int(actual_by_type.get(int(output.type_id), 0))
            output.save(update_fields=["actual_quantity"])

    service_request.status = ReprocessingServiceRequest.Status.COMPLETED
    service_request.return_contract_verified_at = timezone.now()
    service_request.completed_at = timezone.now()
    service_request.save(
        update_fields=[
            "status",
            "return_contract_verified_at",
            "completed_at",
            "updated_at",
        ]
    )

    detail_link = reverse("indy_hub:reprocessing_request_detail", args=[service_request.id])
    notify_user(
        service_request.requester,
        _("Reprocessing request completed"),
        _(
            "Return contract for request %(reference)s has been verified and the request is complete."
        )
        % {"reference": service_request.request_reference},
        level="success",
        link=detail_link,
    )
    notify_user(
        service_request.processor_user,
        _("Reprocessing request completed"),
        _(
            "Request %(reference)s is now marked complete."
        )
        % {"reference": service_request.request_reference},
        level="success",
        link=detail_link,
    )
    messages.success(request, _("Return contract verified. Request completed."))
    return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)


@indy_hub_access_required
@login_required
@require_http_methods(["POST"])
def reprocessing_request_cancel(request, request_id: int):
    emit_view_analytics_event(view_name="reprocessing_services.request_cancel", request=request)
    service_request = _get_request_with_access_check(request, request_id)
    if service_request is None:
        return redirect("indy_hub:reprocessing_browse")

    if request.user.id != service_request.requester_id and not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Only the requester can cancel this request."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)
    if service_request.is_terminal:
        messages.error(request, _("This request is already closed."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    request_reference = service_request.request_reference
    processor_user = service_request.processor_user
    browse_link = reverse("indy_hub:reprocessing_browse")
    service_request.delete()

    if processor_user and processor_user.id != request.user.id:
        notify_user(
            processor_user,
            _("Reprocessing request cancelled"),
            _(
                "Request %(reference)s was cancelled by the requester and removed."
            )
            % {"reference": request_reference},
            level="warning",
            link=browse_link,
        )

    messages.success(request, _("Request cancelled and removed."))
    return redirect("indy_hub:index")


@indy_hub_access_required
@login_required
@require_http_methods(["POST"])
def reprocessing_request_dispute(request, request_id: int):
    emit_view_analytics_event(view_name="reprocessing_services.request_dispute", request=request)
    service_request = _get_request_with_access_check(request, request_id)
    if service_request is None:
        return redirect("indy_hub:reprocessing_browse")

    is_party = request.user.id in {service_request.requester_id, service_request.processor_user_id}
    if not is_party and not request.user.has_perm("indy_hub.can_manage_material_hub"):
        messages.error(request, _("Only request participants can dispute this request."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)
    if service_request.is_terminal:
        messages.error(request, _("This request is already closed."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    dispute_reason = str(request.POST.get("dispute_reason") or "").strip()
    if not dispute_reason:
        messages.error(request, _("Dispute reason is required."))
        return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)

    service_request.status = ReprocessingServiceRequest.Status.DISPUTED
    service_request.dispute_reason = dispute_reason
    service_request.save(update_fields=["status", "dispute_reason", "updated_at"])

    detail_link = reverse("indy_hub:reprocessing_request_detail", args=[service_request.id])
    notify_user(
        service_request.requester,
        _("Reprocessing request disputed"),
        _(
            "Request %(reference)s has been marked disputed."
        )
        % {"reference": service_request.request_reference},
        level="warning",
        link=detail_link,
    )
    notify_user(
        service_request.processor_user,
        _("Reprocessing request disputed"),
        _(
            "Request %(reference)s has been marked disputed."
        )
        % {"reference": service_request.request_reference},
        level="warning",
        link=detail_link,
    )
    _notify_material_exchange_admins(
        title="Reprocessing request disputed",
        message=(
            f"Request {service_request.request_reference} was disputed.\n"
            f"Reason: {dispute_reason}"
        ),
        level="warning",
        link=detail_link,
    )
    messages.warning(request, _("Request marked as disputed. Material Exchange admins have been notified."))
    return redirect("indy_hub:reprocessing_request_detail", request_id=service_request.id)
