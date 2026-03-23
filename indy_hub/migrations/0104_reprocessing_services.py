from decimal import Decimal

from django.conf import settings
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("indy_hub", "0103_materialexchangesettings_stats_preferences"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="ReprocessingServiceProfile",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("character_id", models.BigIntegerField(db_index=True, unique=True)),
                ("character_name", models.CharField(blank=True, max_length=255)),
                ("corporation_id", models.BigIntegerField(blank=True, db_index=True, null=True)),
                ("corporation_name", models.CharField(blank=True, max_length=255)),
                ("alliance_id", models.BigIntegerField(blank=True, db_index=True, null=True)),
                ("alliance_name", models.CharField(blank=True, max_length=255)),
                (
                    "selected_corporation_id",
                    models.BigIntegerField(
                        blank=True,
                        help_text="Corporation context used to discover alliance structures.",
                        null=True,
                    ),
                ),
                (
                    "approval_status",
                    models.CharField(
                        choices=[
                            ("pending", "Pending approval"),
                            ("approved", "Approved"),
                            ("rejected", "Rejected"),
                        ],
                        db_index=True,
                        default="pending",
                        max_length=16,
                    ),
                ),
                (
                    "reviewed_at",
                    models.DateTimeField(blank=True, null=True),
                ),
                ("review_notes", models.TextField(blank=True)),
                (
                    "is_available",
                    models.BooleanField(
                        default=False,
                        help_text="If enabled, this reprocessor accepts new requests.",
                    ),
                ),
                (
                    "margin_percent",
                    models.DecimalField(
                        decimal_places=2,
                        default=Decimal("5.00"),
                        help_text="Reward percentage based on Jita sell value of refined outputs.",
                        max_digits=6,
                        validators=[MinValueValidator(Decimal("0")), MaxValueValidator(Decimal("100"))],
                    ),
                ),
                ("selected_clone_id", models.BigIntegerField(blank=True, null=True)),
                ("selected_clone_label", models.CharField(blank=True, max_length=255)),
                ("selected_implant_type_ids", models.JSONField(blank=True, default=list)),
                ("selected_implant_names", models.JSONField(blank=True, default=list)),
                (
                    "beancounter_bonus_percent",
                    models.DecimalField(
                        decimal_places=3,
                        default=Decimal("0.000"),
                        help_text="Implant reprocessing bonus percentage.",
                        max_digits=6,
                    ),
                ),
                ("reprocessing_skill_level", models.PositiveSmallIntegerField(default=0)),
                ("reprocessing_efficiency_level", models.PositiveSmallIntegerField(default=0)),
                ("processing_skill_level", models.PositiveSmallIntegerField(default=0)),
                ("skill_levels", models.JSONField(blank=True, default=dict)),
                ("structure_id", models.BigIntegerField(db_index=True, default=0)),
                ("structure_name", models.CharField(blank=True, max_length=255)),
                ("structure_type_id", models.IntegerField(blank=True, null=True)),
                ("structure_type_name", models.CharField(blank=True, max_length=255)),
                ("structure_location_name", models.CharField(blank=True, max_length=255)),
                (
                    "structure_bonus_percent",
                    models.DecimalField(decimal_places=3, default=Decimal("0.000"), max_digits=6),
                ),
                ("rig_profile_key", models.CharField(blank=True, max_length=80)),
                ("rig_profile_name", models.CharField(blank=True, max_length=255)),
                (
                    "rig_bonus_percent",
                    models.DecimalField(decimal_places=3, default=Decimal("0.000"), max_digits=6),
                ),
                (
                    "estimated_yield_percent",
                    models.DecimalField(
                        decimal_places=3,
                        default=Decimal("0.000"),
                        help_text="Estimated net reprocessing yield percentage for listing sort.",
                        max_digits=6,
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "reviewed_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="reviewed_reprocessing_profiles",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "user",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="reprocessing_profiles",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Reprocessing Service Profile",
                "verbose_name_plural": "Reprocessing Service Profiles",
                "default_permissions": (),
            },
        ),
        migrations.CreateModel(
            name="ReprocessingServiceRequest",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("request_reference", models.CharField(blank=True, db_index=True, max_length=64, unique=True)),
                ("requester_character_id", models.BigIntegerField(blank=True, null=True)),
                ("requester_character_name", models.CharField(blank=True, max_length=255)),
                ("processor_character_id", models.BigIntegerField(db_index=True)),
                ("processor_character_name", models.CharField(blank=True, max_length=255)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("request_submitted", "Request submitted"),
                            ("awaiting_inbound_contract", "Awaiting inbound contract"),
                            ("inbound_contract_verified", "Inbound contract verified"),
                            ("processing", "Processing"),
                            ("awaiting_return_contract", "Awaiting return contract"),
                            ("completed", "Completed"),
                            ("disputed", "Disputed"),
                            ("cancelled", "Cancelled"),
                        ],
                        db_index=True,
                        default="request_submitted",
                        max_length=40,
                    ),
                ),
                ("structure_id", models.BigIntegerField(default=0)),
                ("structure_name", models.CharField(blank=True, max_length=255)),
                ("structure_type_name", models.CharField(blank=True, max_length=255)),
                ("structure_location_name", models.CharField(blank=True, max_length=255)),
                ("margin_percent_snapshot", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=6)),
                (
                    "estimated_yield_percent_snapshot",
                    models.DecimalField(decimal_places=3, default=Decimal("0.000"), max_digits=6),
                ),
                ("estimated_output_value", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=20)),
                ("reward_isk", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=20)),
                (
                    "tolerance_percent",
                    models.DecimalField(
                        decimal_places=2,
                        default=Decimal("1.00"),
                        max_digits=5,
                        validators=[MinValueValidator(Decimal("0")), MaxValueValidator(Decimal("5"))],
                    ),
                ),
                ("inbound_contract_id", models.BigIntegerField(blank=True, db_index=True, null=True)),
                ("inbound_contract_verified_at", models.DateTimeField(blank=True, null=True)),
                ("return_contract_id", models.BigIntegerField(blank=True, db_index=True, null=True)),
                ("return_contract_verified_at", models.DateTimeField(blank=True, null=True)),
                ("notes", models.TextField(blank=True)),
                ("dispute_reason", models.TextField(blank=True)),
                ("cancelled_at", models.DateTimeField(blank=True, null=True)),
                ("completed_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "processor_profile",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="requests",
                        to="indy_hub.reprocessingserviceprofile",
                    ),
                ),
                (
                    "processor_user",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="processor_reprocessing_requests",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "requester",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="reprocessing_requests",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Reprocessing Service Request",
                "verbose_name_plural": "Reprocessing Service Requests",
                "default_permissions": (),
                "ordering": ["-created_at"],
            },
        ),
        migrations.CreateModel(
            name="ReprocessingServiceRequestOutput",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("type_id", models.IntegerField(db_index=True)),
                ("type_name", models.CharField(blank=True, max_length=255)),
                ("expected_quantity", models.BigIntegerField(default=0)),
                ("actual_quantity", models.BigIntegerField(blank=True, null=True)),
                ("estimated_unit_price", models.DecimalField(blank=True, decimal_places=2, max_digits=20, null=True)),
                ("estimated_total_value", models.DecimalField(blank=True, decimal_places=2, max_digits=20, null=True)),
                (
                    "request",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="expected_outputs",
                        to="indy_hub.reprocessingservicerequest",
                    ),
                ),
            ],
            options={
                "verbose_name": "Reprocessing Request Output",
                "verbose_name_plural": "Reprocessing Request Outputs",
                "default_permissions": (),
                "unique_together": {("request", "type_id")},
            },
        ),
        migrations.CreateModel(
            name="ReprocessingServiceRequestItem",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("type_id", models.IntegerField(db_index=True)),
                ("type_name", models.CharField(blank=True, max_length=255)),
                ("quantity", models.BigIntegerField(validators=[MinValueValidator(1)])),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "request",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="items",
                        to="indy_hub.reprocessingservicerequest",
                    ),
                ),
            ],
            options={
                "verbose_name": "Reprocessing Request Item",
                "verbose_name_plural": "Reprocessing Request Items",
                "default_permissions": (),
                "unique_together": {("request", "type_id")},
            },
        ),
        migrations.AddIndex(
            model_name="reprocessingserviceprofile",
            index=models.Index(fields=["approval_status", "is_available"], name="indy_hub_re_approva_b1f5f7_idx"),
        ),
        migrations.AddIndex(
            model_name="reprocessingserviceprofile",
            index=models.Index(fields=["character_name"], name="indy_hub_re_charact_94e9a8_idx"),
        ),
        migrations.AddIndex(
            model_name="reprocessingserviceprofile",
            index=models.Index(fields=["estimated_yield_percent", "margin_percent"], name="indy_hub_re_estima_7c7564_idx"),
        ),
        migrations.AddIndex(
            model_name="reprocessingservicerequest",
            index=models.Index(fields=["status", "-created_at"], name="indy_hub_re_status_681ee5_idx"),
        ),
        migrations.AddIndex(
            model_name="reprocessingservicerequest",
            index=models.Index(fields=["requester", "-created_at"], name="indy_hub_re_request_f1b7d8_idx"),
        ),
        migrations.AddIndex(
            model_name="reprocessingservicerequest",
            index=models.Index(fields=["processor_user", "-created_at"], name="indy_hub_re_process_357efd_idx"),
        ),
        migrations.AddIndex(
            model_name="reprocessingservicerequestitem",
            index=models.Index(fields=["request", "type_id"], name="indy_hub_re_request_1822d6_idx"),
        ),
        migrations.AddIndex(
            model_name="reprocessingservicerequestoutput",
            index=models.Index(fields=["request", "type_id"], name="indy_hub_re_request_aeb3ec_idx"),
        ),
    ]
