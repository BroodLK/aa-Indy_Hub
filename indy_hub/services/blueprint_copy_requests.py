"""Reusable blueprint copy request eligibility and notification helpers."""

from __future__ import annotations

# Standard Library
from collections import defaultdict
from dataclasses import dataclass

# Django
from django.contrib.auth.models import User
from django.db.models import Q
from django.urls import reverse
from django.utils.translation import gettext_lazy as _

# Alliance Auth
from allianceauth.authentication.models import CharacterOwnership

# AA Example App
from indy_hub.models import (
    Blueprint,
    BlueprintCopyRequest,
    CharacterSettings,
    CorporationSharingSetting,
    NotificationWebhook,
    NotificationWebhookMessage,
)
from indy_hub.notifications import (
    build_site_url,
    notify_user,
    send_discord_webhook_with_message_id,
)
from indy_hub.utils.discord_actions import build_action_link
from indy_hub.utils.eve import get_type_name


@dataclass
class EligibleOwnerDetails:
    owner_ids: set[int]
    character_owner_ids: set[int]
    corporate_members_by_corp: dict[int, set[int]]
    user_to_corporation: dict[int, int]


def get_explicit_corp_bp_manager_ids() -> set[int]:
    """Return active users with explicit corp BP manager permission."""
    return set(
        User.objects.filter(
            Q(user_permissions__codename="can_manage_corp_bp_requests")
            | Q(groups__permissions__codename="can_manage_corp_bp_requests"),
            is_active=True,
        ).values_list("id", flat=True)
    )


def eligible_owner_details_for_request(req: BlueprintCopyRequest) -> EligibleOwnerDetails:
    """Return detailed information about users who can fulfill a request."""
    matching_blueprints = Blueprint.objects.filter(
        bp_type__in=[Blueprint.BPType.ORIGINAL, Blueprint.BPType.REACTION],
        type_id=req.type_id,
        material_efficiency=req.material_efficiency,
        time_efficiency=req.time_efficiency,
    )

    character_owned_blueprints = list(
        matching_blueprints.filter(owner_kind=Blueprint.OwnerKind.CHARACTER).values("owner_user_id", "character_id")
    )

    character_owner_ids: set[int] = set()
    if character_owned_blueprints:
        owner_user_ids = {bp["owner_user_id"] for bp in character_owned_blueprints}
        allowed_settings = CharacterSettings.objects.filter(
            user_id__in=owner_user_ids,
            allow_copy_requests=True,
        ).values("user_id", "character_id")

        allowed_map: dict[int, set[int]] = defaultdict(set)
        for setting in allowed_settings:
            allowed_map[setting["user_id"]].add(setting["character_id"])

        for blueprint_row in character_owned_blueprints:
            user_id = blueprint_row["owner_user_id"]
            if not user_id:
                continue
            char_id = blueprint_row["character_id"]
            allowed_chars = allowed_map.get(user_id)
            if not allowed_chars:
                continue
            if 0 in allowed_chars:
                character_owner_ids.add(user_id)
                continue
            if char_id is None:
                if allowed_chars:
                    character_owner_ids.add(user_id)
                continue
            if char_id in allowed_chars:
                character_owner_ids.add(user_id)

    corporation_ids = list(
        matching_blueprints.filter(owner_kind=Blueprint.OwnerKind.CORPORATION)
        .exclude(corporation_id__isnull=True)
        .values_list("corporation_id", flat=True)
        .distinct()
    )

    corporate_settings: list[CorporationSharingSetting] = []
    corporate_owner_ids: set[int] = set()
    corporate_members_by_corp: dict[int, set[int]] = defaultdict(set)
    user_to_corp: dict[int, int] = {}
    explicit_corp_manager_ids = get_explicit_corp_bp_manager_ids()

    if corporation_ids:
        corporate_settings = list(
            CorporationSharingSetting.objects.filter(
                corporation_id__in=corporation_ids,
                allow_copy_requests=True,
                share_scope__in=[
                    CharacterSettings.SCOPE_CORPORATION,
                    CharacterSettings.SCOPE_ALLIANCE,
                    CharacterSettings.SCOPE_EVERYONE,
                ],
            )
        )
        for setting in corporate_settings:
            corp_id = setting.corporation_id
            if corp_id is None:
                continue
            corporate_members_by_corp[corp_id].add(setting.user_id)
            user_to_corp[setting.user_id] = corp_id
        corporate_owner_ids = {setting.user_id for setting in corporate_settings}

    additional_corp_manager_ids: set[int] = set()
    if corporation_ids and corporate_settings and explicit_corp_manager_ids:
        settings_by_corp: dict[int, list[CorporationSharingSetting]] = defaultdict(list)
        for setting_obj in corporate_settings:
            settings_by_corp[setting_obj.corporation_id].append(setting_obj)

        corp_memberships = CharacterOwnership.objects.filter(character__corporation_id__in=corporation_ids).values(
            "user_id",
            "character__corporation_id",
            "character__character_id",
        )

        corp_user_chars: dict[int, dict[int, set[int]]] = defaultdict(lambda: defaultdict(set))
        corp_member_user_ids: set[int] = set()
        for membership in corp_memberships:
            corp_id = membership.get("character__corporation_id")
            user_id = membership.get("user_id")
            char_id = membership.get("character__character_id")
            if corp_id and user_id:
                corp_user_chars[corp_id][user_id].add(char_id)
                corp_member_user_ids.add(user_id)

        if corp_member_user_ids:
            corp_manager_ids = explicit_corp_manager_ids.intersection(corp_member_user_ids)

            for corp_id, users in corp_user_chars.items():
                corp_settings = settings_by_corp.get(corp_id)
                if not corp_settings:
                    continue
                for user_id, char_ids in users.items():
                    if user_id not in corp_manager_ids:
                        continue
                    if user_id in corporate_owner_ids:
                        continue
                    if user_id == req.requested_by_id:
                        continue
                    if any(
                        not setting_obj.restricts_characters
                        or any(setting_obj.is_character_authorized(char_id) for char_id in char_ids)
                        for setting_obj in corp_settings
                    ):
                        additional_corp_manager_ids.add(user_id)
                        corporate_members_by_corp[corp_id].add(user_id)
                        user_to_corp[user_id] = corp_id

    owner_ids: set[int] = set(character_owner_ids) | corporate_owner_ids | additional_corp_manager_ids
    owner_ids.discard(req.requested_by_id)
    character_owner_ids.discard(req.requested_by_id)
    for members in corporate_members_by_corp.values():
        members.discard(req.requested_by_id)

    user_to_corp = {uid: cid for uid, cid in user_to_corp.items() if uid in owner_ids}
    corporate_members_by_corp = {
        corp_id: {uid for uid in members if uid in owner_ids}
        for corp_id, members in corporate_members_by_corp.items()
        if members
    }

    return EligibleOwnerDetails(
        owner_ids=owner_ids,
        character_owner_ids=set(character_owner_ids),
        corporate_members_by_corp=corporate_members_by_corp,
        user_to_corporation=user_to_corp,
    )


def build_blueprint_copy_request_notification_content(req: BlueprintCopyRequest) -> tuple[str, str, str]:
    """Build default title/body text for provider notifications."""
    notification_context = {
        "username": req.requested_by.username,
        "type_name": get_type_name(req.type_id),
        "me": req.material_efficiency,
        "te": req.time_efficiency,
        "runs": req.runs_requested,
        "copies": req.copies_requested,
    }

    notification_title = _("New blueprint copy request")
    notification_body = (
        _(
            "%(username)s requested a copy of %(type_name)s (ME%(me)s, TE%(te)s) - %(runs)s runs, %(copies)s copies requested."
        )
        % notification_context
    )

    corporate_source_line = ""
    corporate_blueprint_qs = (
        Blueprint.objects.filter(
            owner_kind=Blueprint.OwnerKind.CORPORATION,
            type_id=req.type_id,
            material_efficiency=req.material_efficiency,
            time_efficiency=req.time_efficiency,
        )
        .values_list("corporation_name", flat=True)
        .distinct()
    )

    corp_labels: set[str] = set()
    for corp_name in corporate_blueprint_qs:
        label = corp_name.strip() if isinstance(corp_name, str) else ""
        if label:
            corp_labels.add(label)

    if corp_labels:
        formatted_corps = ", ".join(sorted(corp_labels, key=str.lower))
        corporate_source_line = _("Corporate source: %(corporations)s") % {"corporations": formatted_corps}

    return notification_title, notification_body, corporate_source_line


def notify_blueprint_copy_request_providers(
    request,
    req: BlueprintCopyRequest,
    *,
    notification_title: str | None = None,
    notification_body: str | None = None,
    notification_level: str = "info",
) -> None:
    """Notify eligible providers for a blueprint copy request."""
    eligible_details = eligible_owner_details_for_request(req)
    eligible_owner_ids = set(eligible_details.owner_ids)
    if not eligible_owner_ids:
        return

    default_title, default_body, corporate_source_line = build_blueprint_copy_request_notification_content(req)
    resolved_title = notification_title or default_title
    resolved_body = notification_body or default_body

    fulfill_queue_path = reverse("indy_hub:bp_copy_fulfill_requests")
    fulfill_queue_url = (
        request.build_absolute_uri(fulfill_queue_path)
        if request is not None
        else (build_site_url(fulfill_queue_path) or fulfill_queue_path)
    )
    fulfill_label = _("Review copy requests")

    if notification_body is not None:
        corporate_source_line = ""

    muted_user_ids: set[int] = set()
    direct_user_ids: set[int] = set(eligible_details.character_owner_ids)

    for corp_id, corp_user_ids in eligible_details.corporate_members_by_corp.items():
        webhooks = NotificationWebhook.get_blueprint_sharing_webhooks(corp_id)
        if not webhooks:
            continue

        provider_body = resolved_body
        if corporate_source_line:
            provider_body = f"{provider_body}\n\n{corporate_source_line}"

        sent_any = False
        for webhook in webhooks:
            sent, message_id = send_discord_webhook_with_message_id(
                webhook.webhook_url,
                resolved_title,
                provider_body,
                level=notification_level,
                link=fulfill_queue_url,
                thumbnail_url=None,
                embed_title=f"Blueprint Request: {resolved_title}",
                embed_color=0x5865F2,
                mention_everyone=bool(getattr(webhook, "ping_here", False)),
            )
            if sent:
                sent_any = True
                if message_id:
                    NotificationWebhookMessage.objects.create(
                        webhook_type=NotificationWebhook.TYPE_BLUEPRINT_SHARING,
                        webhook_url=webhook.webhook_url,
                        message_id=message_id,
                        copy_request=req,
                    )

        if sent_any:
            muted_user_ids.update(set(corp_user_ids) - direct_user_ids)

    provider_users = User.objects.filter(
        id__in=(eligible_owner_ids - muted_user_ids),
        is_active=True,
    )

    base_url = request.build_absolute_uri("/") if request is not None else None
    sent_to: set[int] = set()
    for owner in provider_users:
        if owner.id in sent_to:
            continue
        sent_to.add(owner.id)

        provider_body = resolved_body
        if corporate_source_line:
            provider_body = f"{provider_body}\n\n{corporate_source_line}"

        quick_actions = []
        link_cta = _("Click here")

        accept_link = build_action_link(
            action="accept",
            request_id=req.id,
            user_id=owner.id,
            base_url=base_url,
        )
        if accept_link:
            quick_actions.append(_("Accept: %(link)s") % {"link": f"[{link_cta}]({accept_link})"})

        conditional_link = build_action_link(
            action="conditional",
            request_id=req.id,
            user_id=owner.id,
            base_url=base_url,
        )
        if conditional_link:
            quick_actions.append(_("Send conditions: %(link)s") % {"link": f"[{link_cta}]({conditional_link})"})

        reject_link = build_action_link(
            action="reject",
            request_id=req.id,
            user_id=owner.id,
            base_url=base_url,
        )
        if reject_link:
            quick_actions.append(_("Decline: %(link)s") % {"link": f"[{link_cta}]({reject_link})"})

        if quick_actions:
            provider_body = f"{provider_body}\n\n{_('Quick actions:')}\n" + "\n".join(quick_actions)

        notify_user(
            owner,
            resolved_title,
            provider_body,
            notification_level,
            link=fulfill_queue_url,
            link_label=fulfill_label,
        )
