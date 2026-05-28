"""Django admin configuration for indy_hub models."""

# Django
from django import forms
from django.apps import apps
from django.contrib import admin, messages
from django.contrib.auth.admin import GroupAdmin
from django.contrib.auth.models import Group, User
from django.utils import timezone

from .models import (
    Blueprint,
    CapitalShipOrder,
    CharacterSettings,
    CorporationSharingSetting,
    IndustryJob,
    MaterialExchangeBuyOrder,
    MaterialExchangeBuyOrderItem,
    MaterialExchangeConfig,
    MaterialExchangeSellOrder,
    MaterialExchangeSellOrderItem,
    MaterialExchangeStock,
    MaterialExchangeTransaction,
    NotificationWebhook,
    ReprocessingServiceProfile,
    ReprocessingServiceRequest,
    ReprocessingServiceRequestItem,
    ReprocessingServiceRequestOutput,
    UserOnboardingProgress,
    WeeklyMiningPollConfig,
    WeeklyMiningPollRun,
)
from .services.mining_polls import create_main_poll_run
from .tasks.mining_polls import queue_weekly_mining_poll_post


class IndyHubGroupAdminForm(forms.ModelForm):
    class Meta:
        model = Group
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        permissions_field = self.fields.get("permissions")
        if permissions_field:
            permissions_field.label_from_instance = _indy_hub_permission_label


def _indy_hub_permission_label(permission):
    target_codenames = {
        "can_access_indy_hub",
        "can_manage_corp_bp_requests",
        "can_manage_material_hub",
        "can_manage_capital_orders",
        "can_build_capital_orders",
    }
    if permission.content_type.app_label == "indy_hub" and permission.codename in target_codenames:
        return f"indy_hub | {permission.name}"
    return str(permission)


try:
    _registered_group_admin = admin.site._registry[Group].__class__
except KeyError:
    _registered_group_admin = GroupAdmin


class IndyHubGroupAdmin(_registered_group_admin):
    form = IndyHubGroupAdminForm


class IndyHubUserAdminForm(forms.ModelForm):
    class Meta:
        model = User
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        user_permissions_field = self.fields.get("user_permissions")
        if user_permissions_field:
            user_permissions_field.label_from_instance = _indy_hub_permission_label


try:
    admin.site.unregister(Group)
except admin.sites.NotRegistered:
    pass

admin.site.register(Group, IndyHubGroupAdmin)


try:
    _registered_user_admin = admin.site._registry[User].__class__
except KeyError:
    _registered_user_admin = admin.ModelAdmin


class IndyHubUserAdmin(_registered_user_admin):
    form = IndyHubUserAdminForm


try:
    admin.site.unregister(User)
except admin.sites.NotRegistered:
    pass

admin.site.register(User, IndyHubUserAdmin)


@admin.register(Blueprint)
class BlueprintAdmin(admin.ModelAdmin):
    list_display = [
        "type_name",
        "owner_user",
        "character_id",
        "quantity",
        "material_efficiency",
        "time_efficiency",
        "runs",
        "last_updated",
    ]
    list_filter = ["owner_user", "character_id", "quantity", "last_updated"]
    search_fields = ["type_name", "type_id", "owner_user__username"]
    readonly_fields = ["item_id", "last_updated", "created_at"]

    fieldsets = (
        (
            "Basic Information",
            {
                "fields": (
                    "owner_user",
                    "character_id",
                    "item_id",
                    "type_id",
                    "type_name",
                )
            },
        ),
        ("Location", {"fields": ("location_id", "location_name", "location_flag")}),
        (
            "Blueprint Details",
            {"fields": ("quantity", "material_efficiency", "time_efficiency", "runs")},
        ),
        (
            "Timestamps",
            {"fields": ("created_at", "last_updated"), "classes": ("collapse",)},
        ),
    )


@admin.register(IndustryJob)
class IndustryJobAdmin(admin.ModelAdmin):
    list_display = [
        "job_id",
        "activity_name",
        "blueprint_type_name",
        "owner_user",
        "character_id",
        "status",
        "runs",
        "location_name",
        "start_date",
        "end_date",
    ]
    list_filter = ["status", "activity_id", "owner_user", "character_id", "start_date"]
    search_fields = [
        "blueprint_type_name",
        "product_type_name",
        "activity_name",
        "owner_user__username",
        "job_id",
    ]
    readonly_fields = ["job_id", "last_updated", "created_at", "start_date", "end_date"]
    fieldsets = (
        (
            "Job Information",
            {
                "fields": (
                    "owner_user",
                    "character_id",
                    "job_id",
                    "installer_id",
                    "status",
                )
            },
        ),
        (
            "Activity Details",
            {"fields": ("activity_id", "activity_name", "runs", "duration")},
        ),
        (
            "Blueprint Information",
            {"fields": ("blueprint_id", "blueprint_type_id", "blueprint_type_name")},
        ),
        ("Product Information", {"fields": ("product_type_id", "product_type_name")}),
        (
            "Locations",
            {
                "fields": (
                    "station_id",
                    "location_name",
                ),
                "classes": ("collapse",),
            },
        ),
        ("Financial", {"fields": ("cost", "licensed_runs"), "classes": ("collapse",)}),
        (
            "Invention/Research",
            {"fields": ("probability", "successful_runs"), "classes": ("collapse",)},
        ),
        (
            "Timestamps",
            {
                "fields": (
                    "start_date",
                    "end_date",
                    "pause_date",
                    "completed_date",
                    "created_at",
                    "last_updated",
                ),
                "classes": ("collapse",),
            },
        ),
    )


@admin.register(CharacterSettings)
class CharacterSettingsAdmin(admin.ModelAdmin):
    list_display = [
        "user",
        "character_id",
        "jobs_notify_completed",
        "allow_copy_requests",
        "copy_sharing_scope",
        "updated_at",
    ]
    list_filter = [
        "jobs_notify_completed",
        "allow_copy_requests",
        "copy_sharing_scope",
        "updated_at",
    ]
    search_fields = ["user__username", "character_id"]
    readonly_fields = ["updated_at"]
    fieldsets = (
        (
            "Character Settings",
            {
                "fields": (
                    "user",
                    "character_id",
                    "jobs_notify_completed",
                    "allow_copy_requests",
                    "copy_sharing_scope",
                    "updated_at",
                )
            },
        ),
    )


@admin.register(UserOnboardingProgress)
class UserOnboardingProgressAdmin(admin.ModelAdmin):
    list_display = ["user", "dismissed", "updated_at"]
    search_fields = ["user__username"]
    list_filter = ["dismissed"]
    readonly_fields = ["created_at", "updated_at"]
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "user",
                    "dismissed",
                    "manual_steps",
                    "created_at",
                    "updated_at",
                ),
            },
        ),
    )


@admin.register(CorporationSharingSetting)
class CorporationSharingSettingAdmin(admin.ModelAdmin):
    list_display = [
        "user",
        "corporation_id",
        "corporation_name",
        "share_scope",
        "allow_copy_requests",
        "has_manual_whitelist",
        "updated_at",
    ]
    list_filter = ["share_scope", "allow_copy_requests", "updated_at"]
    search_fields = ["user__username", "corporation_id", "corporation_name"]
    readonly_fields = ["created_at", "updated_at"]
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "user",
                    "corporation_id",
                    "corporation_name",
                    "share_scope",
                    "allow_copy_requests",
                    "authorized_characters",
                    "created_at",
                    "updated_at",
                )
            },
        ),
    )

    @admin.display(boolean=True, description="Whitelisted")
    def has_manual_whitelist(self, obj: CorporationSharingSetting) -> bool:
        return obj.restricts_characters


@admin.register(NotificationWebhook)
class NotificationWebhookAdmin(admin.ModelAdmin):
    class NotificationWebhookForm(forms.ModelForm):
        corporations = forms.MultipleChoiceField(
            required=False,
            choices=[],
            widget=forms.SelectMultiple(attrs={"size": "10"}),
            label="Corporations",
            help_text="Select one or more corporations for blueprint sharing webhooks.",
        )

        class Meta:
            model = NotificationWebhook
            fields = (
                "name",
                "webhook_type",
                "webhook_url",
                "ping_here",
                "is_active",
                "corporations",
            )

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            corp_choices = list(
                CorporationSharingSetting.objects.values_list("corporation_id", "corporation_name")
                .distinct()
                .order_by("corporation_name")
            )
            self.fields["corporations"].choices = [
                (str(corp_id), corp_name or str(corp_id)) for corp_id, corp_name in corp_choices
            ]
            if "ping_here" in self.fields:
                self.fields["ping_here"].help_text = "Send an @here mention with webhook notifications."

            instance = getattr(self, "instance", None)
            if instance and instance.pk and instance.corporation_ids:
                self.fields["corporations"].initial = [str(corp_id) for corp_id in instance.corporation_ids]

            selected_type = self.data.get("webhook_type") or (instance.webhook_type if instance else None)
            if selected_type == NotificationWebhook.TYPE_MATERIAL_EXCHANGE:
                self.fields["corporations"].disabled = True

        def clean(self):
            cleaned = super().clean()
            webhook_type = cleaned.get("webhook_type")
            corp_values = cleaned.get("corporations") or []

            if webhook_type == NotificationWebhook.TYPE_BLUEPRINT_SHARING:
                if not corp_values:
                    self.add_error(
                        "corporations",
                        "At least one corporation is required for blueprint sharing webhooks.",
                    )
            elif webhook_type == NotificationWebhook.TYPE_MATERIAL_EXCHANGE:
                cleaned["corporations"] = []

            corp_map = {
                str(corp_id): (corp_id, corp_name)
                for corp_id, corp_name in CorporationSharingSetting.objects.values_list(
                    "corporation_id", "corporation_name"
                ).distinct()
            }

            corp_ids: list[int] = []
            corp_names: list[str] = []
            for corp_id_str in corp_values:
                corp_id, corp_name = corp_map.get(
                    str(corp_id_str),
                    (int(corp_id_str), str(corp_id_str)),
                )
                corp_ids.append(int(corp_id))
                corp_names.append(corp_name or str(corp_id))

            self.instance.corporation_ids = corp_ids
            self.instance.corporation_names = corp_names

            return cleaned

        def save(self, commit=True):
            instance = super().save(commit=False)
            corp_values = self.cleaned_data.get("corporations") or []

            corp_map = {
                str(corp_id): (corp_id, corp_name)
                for corp_id, corp_name in CorporationSharingSetting.objects.values_list(
                    "corporation_id", "corporation_name"
                ).distinct()
            }

            corp_ids: list[int] = []
            corp_names: list[str] = []
            for corp_id_str in corp_values:
                corp_id, corp_name = corp_map.get(
                    str(corp_id_str),
                    (int(corp_id_str), str(corp_id_str)),
                )
                corp_ids.append(int(corp_id))
                corp_names.append(corp_name or str(corp_id))

            instance.corporation_ids = corp_ids
            instance.corporation_names = corp_names

            if instance.webhook_type == NotificationWebhook.TYPE_MATERIAL_EXCHANGE:
                instance.corporation_ids = []
                instance.corporation_names = []

            if commit:
                instance.save()
            return instance

    form = NotificationWebhookForm
    list_display = [
        "name",
        "webhook_type",
        "corporation_list",
        "ping_here",
        "is_active",
        "updated_at",
    ]
    list_filter = ["webhook_type", "is_active"]
    search_fields = ["name", "webhook_url", "corporation_names"]
    readonly_fields = ["created_at", "updated_at"]
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "name",
                    "webhook_type",
                    "webhook_url",
                    "ping_here",
                    "is_active",
                    "corporations",
                )
            },
        ),
        (
            "Audit",
            {
                "fields": (
                    "created_at",
                    "updated_at",
                )
            },
        ),
    )

    @admin.display(description="Corporations")
    def corporation_list(self, obj: NotificationWebhook) -> str:
        return ", ".join(obj.corporation_names or [])

    class Media:
        js = ("indy_hub/js/admin_notification_webhook.js",)


@admin.register(WeeklyMiningPollConfig)
class WeeklyMiningPollConfigAdmin(admin.ModelAdmin):
    class WeeklyMiningPollConfigForm(forms.ModelForm):
        known_channel_id = forms.ChoiceField(
            required=False,
            choices=[("", "---------")],
            label="Known channel",
            help_text="Choose a channel discovered by aadiscordbot, or enter a manual channel ID below.",
        )
        crontab_schedule_id = forms.ChoiceField(
            required=True,
            choices=[("", "---------")],
            label="Periodic task schedule",
            help_text="Reuse an existing django-celery-beat crontab schedule.",
        )
        options_text = forms.CharField(
            required=True,
            widget=forms.Textarea(attrs={"rows": 8}),
            label="Poll options",
            help_text="Enter one option per line. Discord native polls support 2 to 10 options.",
        )

        class Meta:
            model = WeeklyMiningPollConfig
            fields = (
                "system_name",
                "poll_name",
                "known_channel_id",
                "channel_id",
                "ping_role_id",
                "crontab_schedule_id",
                "options_text",
                "current_winner_option",
                "is_active",
            )

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.fields["channel_id"].label = "Manual channel ID"
            self.fields["channel_id"].required = False
            self.fields["channel_id"].help_text = "Used when the target channel is not listed above."
            self.fields["ping_role_id"].help_text = "Optional Discord role ID to mention when posting the poll."
            self.fields["current_winner_option"].help_text = (
                "Optional seed for the current winner. Leave blank to let the first result establish it."
            )

            known_choices = [("", "---------")]
            known_ids: set[str] = set()
            if apps.is_installed("aadiscordbot"):
                try:
                    # Third Party
                    from aadiscordbot.models import Channels

                    for channel in (
                        Channels.objects.select_related("server")
                        .filter(deleted=False)
                        .order_by("server__name", "name")
                    ):
                        value = str(channel.channel)
                        known_ids.add(value)
                        known_choices.append(
                            (
                                value,
                                f"{channel.server.name} / #{channel.name} ({channel.channel})",
                            )
                        )
                except Exception:
                    pass
            self.fields["known_channel_id"].choices = known_choices

            crontab_choices = [("", "---------")]
            if apps.is_installed("django_celery_beat"):
                try:
                    # Third Party
                    from django_celery_beat.models import CrontabSchedule, PeriodicTask

                    used_crontab_ids = list(
                        PeriodicTask.objects.exclude(crontab_id__isnull=True)
                        .values_list("crontab_id", flat=True)
                        .distinct()
                    )
                    instance = getattr(self, "instance", None)
                    if instance and instance.pk and instance.crontab_schedule_id:
                        used_crontab_ids.append(instance.crontab_schedule_id)
                    for crontab in CrontabSchedule.objects.filter(id__in=sorted(set(used_crontab_ids))).order_by(
                        "timezone",
                        "month_of_year",
                        "day_of_month",
                        "day_of_week",
                        "hour",
                        "minute",
                    ):
                        label = getattr(crontab, "human_readable", str(crontab))
                        crontab_choices.append((str(crontab.id), f"{label} [#{crontab.id}]"))
                except Exception:
                    pass
            self.fields["crontab_schedule_id"].choices = crontab_choices

            instance = getattr(self, "instance", None)
            if instance and instance.pk:
                self.fields["options_text"].initial = "\n".join(instance.options)
                if instance.channel_id and str(instance.channel_id) in known_ids:
                    self.fields["known_channel_id"].initial = str(instance.channel_id)
                if instance.crontab_schedule_id:
                    self.fields["crontab_schedule_id"].initial = str(instance.crontab_schedule_id)

        def clean(self):
            cleaned = super().clean()
            selected_known_channel = (cleaned.get("known_channel_id") or "").strip()
            manual_channel_id = cleaned.get("channel_id")
            if selected_known_channel:
                cleaned["channel_id"] = int(selected_known_channel)
            elif not manual_channel_id:
                self.add_error(
                    "channel_id",
                    "Select a known channel or provide a manual Discord channel ID.",
                )

            options = [line.strip() for line in (cleaned.get("options_text") or "").splitlines() if line.strip()]
            crontab_schedule_id = (cleaned.get("crontab_schedule_id") or "").strip()
            if not crontab_schedule_id:
                self.add_error(
                    "crontab_schedule_id",
                    "Select an existing periodic-task crontab schedule.",
                )
            else:
                cleaned["crontab_schedule_id"] = int(crontab_schedule_id)
                self.instance.crontab_schedule_id = int(crontab_schedule_id)
            cleaned["options_json"] = options
            self.instance.options_json = options
            return cleaned

        def save(self, commit=True):
            instance = super().save(commit=False)
            instance.options_json = self.cleaned_data.get("options_json") or []
            if commit:
                instance.save()
            return instance

    form = WeeklyMiningPollConfigForm
    actions = ["enable_selected_configs", "disable_selected_configs", "post_selected_now"]
    list_display = [
        "system_name",
        "poll_name",
        "channel_id",
        "current_winner_option",
        "is_active",
        "last_scheduled_post_at",
    ]
    list_filter = ["is_active"]
    search_fields = [
        "system_name",
        "poll_name",
        "channel_id",
        "current_winner_option",
    ]
    readonly_fields = ["created_at", "updated_at", "last_scheduled_post_at"]
    fieldsets = (
        (
            "Poll Target",
            {
                "fields": (
                    "system_name",
                    "poll_name",
                    "known_channel_id",
                    "channel_id",
                    "ping_role_id",
                    "is_active",
                )
            },
        ),
        (
            "Poll Content",
            {
                "fields": (
                    "options_text",
                    "current_winner_option",
                )
            },
        ),
        (
            "Schedule",
            {
                "fields": (
                    "crontab_schedule_id",
                    "last_scheduled_post_at",
                )
            },
        ),
        (
            "Audit",
            {
                "fields": ("created_at", "updated_at"),
                "classes": ("collapse",),
            },
        ),
    )

    @admin.action(description="Enable selected weekly mining polls")
    def enable_selected_configs(self, request, queryset):
        updated = queryset.update(is_active=True, updated_at=timezone.now())
        self.message_user(
            request,
            f"Enabled {updated} weekly mining poll config(s).",
            level=messages.SUCCESS,
        )

    @admin.action(description="Disable selected weekly mining polls")
    def disable_selected_configs(self, request, queryset):
        updated = queryset.update(is_active=False, updated_at=timezone.now())
        self.message_user(
            request,
            f"Disabled {updated} weekly mining poll config(s).",
            level=messages.SUCCESS,
        )

    @admin.action(description="Post selected weekly mining polls now")
    def post_selected_now(self, request, queryset):
        posted = 0
        skipped = 0
        failed = 0
        now = timezone.now()

        for config in queryset.order_by("id"):
            has_active_run = config.runs.filter(
                status__in=[
                    WeeklyMiningPollRun.Status.PENDING_POST,
                    WeeklyMiningPollRun.Status.OPEN,
                    WeeklyMiningPollRun.Status.PENDING_RESOLUTION,
                ]
            ).exists()
            if has_active_run:
                skipped += 1
                continue

            run = create_main_poll_run(config, scheduled_at=now)
            if queue_weekly_mining_poll_post(run.id):
                posted += 1
            else:
                run.status = WeeklyMiningPollRun.Status.FAILED
                run.failure_reason = "aadiscordbot task queue unavailable"
                run.save(update_fields=["status", "failure_reason", "updated_at"])
                failed += 1

        if posted:
            self.message_user(
                request,
                f"Queued {posted} weekly mining poll(s) for immediate posting.",
                level=messages.SUCCESS,
            )
        if skipped:
            self.message_user(
                request,
                f"Skipped {skipped} config(s) because they already have an active poll run.",
                level=messages.WARNING,
            )
        if failed:
            self.message_user(
                request,
                f"Failed to queue {failed} poll(s) because aadiscordbot was unavailable.",
                level=messages.ERROR,
            )


@admin.register(WeeklyMiningPollRun)
class WeeklyMiningPollRunAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "config",
        "kind",
        "tiebreak_round",
        "status",
        "winning_option",
        "total_votes",
        "posted_at",
        "finalized_at",
    ]
    list_filter = ["kind", "status", "tiebreak_round"]
    search_fields = [
        "config__system_name",
        "config__poll_name",
        "discord_message_id",
        "winning_option",
    ]
    readonly_fields = [
        "config",
        "parent_run",
        "root_run",
        "kind",
        "status",
        "tiebreak_round",
        "duration_hours",
        "discord_channel_id",
        "discord_message_id",
        "ping_role_id",
        "question_text",
        "option_labels",
        "display_option_labels",
        "previous_winner_option",
        "winning_option",
        "resolution_method",
        "total_votes",
        "resolution_attempts",
        "posted_at",
        "closes_at",
        "resolve_after",
        "finalized_at",
        "failure_reason",
        "created_at",
        "updated_at",
    ]
    fieldsets = (
        (
            "Context",
            {
                "fields": (
                    "config",
                    "parent_run",
                    "root_run",
                    "kind",
                    "status",
                    "tiebreak_round",
                    "duration_hours",
                )
            },
        ),
        (
            "Discord",
            {
                "fields": (
                    "discord_channel_id",
                    "discord_message_id",
                    "ping_role_id",
                    "question_text",
                    "option_labels",
                    "display_option_labels",
                )
            },
        ),
        (
            "Outcome",
            {
                "fields": (
                    "previous_winner_option",
                    "winning_option",
                    "resolution_method",
                    "total_votes",
                    "resolution_attempts",
                    "failure_reason",
                )
            },
        ),
        (
            "Timing",
            {
                "fields": (
                    "posted_at",
                    "closes_at",
                    "resolve_after",
                    "finalized_at",
                    "created_at",
                    "updated_at",
                )
            },
        ),
    )

    def has_add_permission(self, request):
        return False


@admin.register(MaterialExchangeConfig)
class MaterialExchangeConfigAdmin(admin.ModelAdmin):
    list_display = [
        "corporation_id",
        "structure_name",
        "hangar_division",
        "sell_markup_percent",
        "sell_markup_base",
        "buy_markup_percent",
        "buy_markup_base",
        "is_active",
        "last_stock_sync",
        "last_price_sync",
    ]
    list_filter = ["is_active", "last_stock_sync"]
    readonly_fields = ["last_stock_sync", "last_price_sync", "created_at", "updated_at"]
    fieldsets = (
        (
            "Corporation Settings",
            {
                "fields": (
                    "corporation_id",
                    "structure_id",
                    "structure_name",
                    "hangar_division",
                    "is_active",
                )
            },
        ),
        (
            "Pricing Configuration",
            {
                "fields": (
                    ("sell_markup_percent", "sell_markup_base"),
                    ("buy_markup_percent", "buy_markup_base"),
                )
            },
        ),
        (
            "Sync Status",
            {
                "fields": (
                    "last_stock_sync",
                    "last_price_sync",
                    "created_at",
                    "updated_at",
                ),
                "classes": ("collapse",),
            },
        ),
    )


@admin.register(MaterialExchangeStock)
class MaterialExchangeStockAdmin(admin.ModelAdmin):
    list_display = [
        "type_name",
        "quantity",
        "jita_buy_price",
        "jita_sell_price",
        "sell_price_to_member",
        "buy_price_from_member",
    ]
    list_filter = ["quantity"]
    search_fields = ["type_name", "type_id"]
    readonly_fields = ["sell_price_to_member", "buy_price_from_member"]


@admin.register(MaterialExchangeSellOrder)
class MaterialExchangeSellOrderAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "order_reference",
        "seller",
        "item_count",
        "total_price_display",
        "status",
        "created_at",
        "approved_by",
    ]
    list_filter = ["status", "created_at", "seller"]
    search_fields = ["seller__username", "id", "order_reference"]
    readonly_fields = ["created_at", "updated_at", "order_reference"]
    fieldsets = (
        (
            "Order Information",
            {"fields": ("order_reference", "seller", "status")},
        ),
        (
            "Status & Approval",
            {
                "fields": (
                    "approved_by",
                    "payment_verified_by",
                    "payment_journal_ref",
                )
            },
        ),
        (
            "Notes",
            {"fields": ("notes",)},
        ),
        (
            "Timestamps",
            {"fields": ("created_at", "updated_at"), "classes": ("collapse",)},
        ),
    )

    @admin.display(description="Items")
    def item_count(self, obj):
        return obj.items.count()

    @admin.display(description="Total Price")
    def total_price_display(self, obj):
        return f"{obj.total_price:,.0f} ISK"


@admin.register(MaterialExchangeBuyOrder)
class MaterialExchangeBuyOrderAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "order_reference",
        "buyer",
        "item_count",
        "total_price_display",
        "status",
        "created_at",
        "approved_by",
    ]
    list_filter = ["status", "created_at", "buyer"]
    search_fields = ["buyer__username", "id", "order_reference"]
    readonly_fields = ["created_at", "updated_at", "order_reference"]
    fieldsets = (
        (
            "Order Information",
            {"fields": ("order_reference", "buyer", "status")},
        ),
        (
            "Status & Fulfillment",
            {"fields": ("approved_by", "delivered_by", "delivery_method")},
        ),
        (
            "Notes",
            {"fields": ("notes",)},
        ),
        (
            "Timestamps",
            {"fields": ("created_at", "updated_at"), "classes": ("collapse",)},
        ),
    )

    @admin.display(description="Items")
    def item_count(self, obj):
        return obj.items.count()

    @admin.display(description="Total Price")
    def total_price_display(self, obj):
        return f"{obj.total_price:,.0f} ISK"


@admin.register(CapitalShipOrder)
class CapitalShipOrderAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "order_reference",
        "requester",
        "ship_type_name",
        "ship_class",
        "status",
        "in_production_by",
        "created_at",
    ]
    list_filter = ["status", "ship_class", "created_at"]
    search_fields = [
        "order_reference",
        "requester__username",
        "ship_type_name",
        "esi_contract_id",
    ]
    readonly_fields = ["order_reference", "created_at", "updated_at"]
    fieldsets = (
        (
            "Order",
            {
                "fields": (
                    "order_reference",
                    "config",
                    "requester",
                    "ship_type_id",
                    "ship_type_name",
                    "ship_class",
                    "reason",
                    "status",
                )
            },
        ),
        (
            "Contract",
            {
                "fields": (
                    "esi_contract_id",
                    "in_production_by",
                    "in_production_at",
                    "contract_created_at",
                    "contract_completed_at",
                )
            },
        ),
        ("Notes", {"fields": ("anomaly_reason", "notes")}),
        (
            "Timestamps",
            {
                "fields": ("created_at", "updated_at"),
                "classes": ("collapse",),
            },
        ),
    )


@admin.register(ReprocessingServiceProfile)
class ReprocessingServiceProfileAdmin(admin.ModelAdmin):
    list_display = [
        "character_name",
        "user",
        "approval_status",
        "is_available",
        "margin_percent",
        "structure_name",
        "estimated_yield_percent",
        "updated_at",
    ]
    list_filter = ["approval_status", "is_available", "updated_at"]
    search_fields = [
        "character_name",
        "user__username",
        "corporation_name",
        "structure_name",
    ]
    readonly_fields = ["created_at", "updated_at", "reviewed_at"]
    fieldsets = (
        (
            "Identity",
            {
                "fields": (
                    "user",
                    "character_id",
                    "character_name",
                    "corporation_id",
                    "corporation_name",
                    "alliance_id",
                    "alliance_name",
                )
            },
        ),
        (
            "Approval",
            {
                "fields": (
                    "approval_status",
                    "reviewed_by",
                    "reviewed_at",
                    "review_notes",
                    "is_available",
                )
            },
        ),
        (
            "Service Configuration",
            {
                "fields": (
                    "margin_percent",
                    "selected_clone_id",
                    "selected_clone_label",
                    "selected_implant_type_ids",
                    "selected_implant_names",
                    "beancounter_bonus_percent",
                    "reprocessing_skill_level",
                    "reprocessing_efficiency_level",
                    "processing_skill_level",
                    "skill_levels",
                    "structure_id",
                    "structure_name",
                    "structure_type_id",
                    "structure_type_name",
                    "structure_location_name",
                    "structure_bonus_percent",
                    "rig_profile_key",
                    "rig_profile_name",
                    "rig_bonus_percent",
                    "estimated_yield_percent",
                )
            },
        ),
        (
            "Audit",
            {
                "fields": ("created_at", "updated_at"),
            },
        ),
    )


class ReprocessingServiceRequestItemInline(admin.TabularInline):
    model = ReprocessingServiceRequestItem
    extra = 0
    readonly_fields = ["type_id", "type_name", "quantity", "created_at", "updated_at"]


class ReprocessingServiceRequestOutputInline(admin.TabularInline):
    model = ReprocessingServiceRequestOutput
    extra = 0
    readonly_fields = [
        "type_id",
        "type_name",
        "expected_quantity",
        "actual_quantity",
        "estimated_unit_price",
        "estimated_total_value",
    ]


@admin.register(ReprocessingServiceRequest)
class ReprocessingServiceRequestAdmin(admin.ModelAdmin):
    list_display = [
        "request_reference",
        "requester",
        "processor_character_name",
        "status",
        "estimated_output_value",
        "reward_isk",
        "created_at",
    ]
    list_filter = ["status", "created_at"]
    search_fields = [
        "request_reference",
        "requester__username",
        "requester_character_name",
        "processor_character_name",
    ]
    readonly_fields = ["created_at", "updated_at"]
    inlines = [ReprocessingServiceRequestItemInline, ReprocessingServiceRequestOutputInline]


@admin.register(MaterialExchangeTransaction)
class MaterialExchangeTransactionAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "transaction_type",
        "user",
        "type_name",
        "quantity",
        "total_price",
        "completed_at",
    ]
    list_filter = ["transaction_type", "completed_at", "user"]
    search_fields = ["user__username", "type_name", "id"]
    readonly_fields = ["completed_at"]
    fieldsets = (
        (
            "Transaction Details",
            {
                "fields": (
                    "transaction_type",
                    "user",
                    "type_id",
                    "type_name",
                    "quantity",
                )
            },
        ),
        (
            "Financial Information",
            {"fields": ("unit_price", "total_price")},
        ),
        (
            "Related Orders",
            {"fields": ("sell_order", "buy_order")},
        ),
        ("Timestamp", {"fields": ("completed_at",)}),
    )


@admin.register(MaterialExchangeSellOrderItem)
class MaterialExchangeSellOrderItemAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "order_id",
        "type_name",
        "quantity",
        "unit_price",
        "total_price",
        "esi_contract_validated",
    ]
    list_filter = ["esi_contract_validated", "created_at"]
    search_fields = ["type_name", "order__id"]
    readonly_fields = ["created_at", "updated_at"]
    fieldsets = (
        (
            "Item Information",
            {"fields": ("order", "type_id", "type_name", "quantity")},
        ),
        (
            "Pricing",
            {"fields": ("unit_price", "total_price")},
        ),
        (
            "ESI Validation",
            {
                "fields": (
                    "esi_contract_id",
                    "esi_contract_validated",
                    "esi_validation_checked_at",
                )
            },
        ),
        (
            "Timestamps",
            {"fields": ("created_at", "updated_at"), "classes": ("collapse",)},
        ),
    )

    @admin.display(description="Order")
    def order_id(self, obj):
        return f"Sell #{obj.order.id}"


@admin.register(MaterialExchangeBuyOrderItem)
class MaterialExchangeBuyOrderItemAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "order_id",
        "type_name",
        "quantity",
        "unit_price",
        "total_price",
        "esi_contract_validated",
    ]
    list_filter = ["esi_contract_validated", "created_at"]
    search_fields = ["type_name", "order__id"]
    readonly_fields = ["created_at", "updated_at"]
    fieldsets = (
        (
            "Item Information",
            {"fields": ("order", "type_id", "type_name", "quantity")},
        ),
        (
            "Pricing",
            {"fields": ("unit_price", "total_price", "stock_available_at_creation")},
        ),
        (
            "ESI Validation",
            {
                "fields": (
                    "esi_contract_id",
                    "esi_contract_validated",
                    "esi_validation_checked_at",
                )
            },
        ),
        (
            "Timestamps",
            {"fields": ("created_at", "updated_at"), "classes": ("collapse",)},
        ),
    )

    @admin.display(description="Order")
    def order_id(self, obj):
        return f"Buy #{obj.order.id}"
