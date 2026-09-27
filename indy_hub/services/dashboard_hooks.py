"""Dashboard hook registry and module status providers for Indy Hub overview."""

from __future__ import annotations

# Standard Library
from decimal import Decimal
from typing import Any

# Django
from django.contrib.auth.models import User
from django.db.models import F, Q
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger

logger = get_extension_logger(__name__)


def _format_price_isk(value: Decimal | str | int | float | None) -> str:
    if value is None:
        return ""
    try:
        dec = Decimal(str(value))
        return f"{dec:,.2f} ISK"
    except Exception:
        return ""


def get_capital_orders_dashboard_data(user: User) -> dict[str, Any] | None:
    """Collect capital orders status, active orders, and alerts for the user dashboard."""
    if not user or not user.is_authenticated:
        return None

    try:
        from indy_hub.models import (
            CapitalShipOrder,
            CapitalShipOrderChat,
            MaterialExchangeConfig,
            MaterialExchangeSettings,
        )
        from indy_hub.services.capital_ship_options import default_ship_class_label
    except ImportError:
        return None

    # Check if module is enabled
    try:
        settings = MaterialExchangeSettings.get_solo()
        if not getattr(settings, "is_enabled", True):
            return None
        config = MaterialExchangeConfig.objects.filter(is_active=True).first()
        if not config or not getattr(config, "is_active", True):
            return None
    except Exception:
        return None

    try:
        # User active orders
        terminal_statuses = {
            CapitalShipOrder.Status.COMPLETED,
            CapitalShipOrder.Status.REJECTED,
            CapitalShipOrder.Status.CANCELLED,
        }

        user_orders_qs = (
            CapitalShipOrder.objects.filter(requester=user)
            .select_related("chat", "in_production_by", "gathering_materials_by", "offer_updated_by")
            .order_by("-updated_at")
        )

        active_orders_raw = [
            order for order in user_orders_qs if order.status not in terminal_statuses
        ]

        active_orders: list[dict[str, Any]] = []
        pending_offer_confirmations: list[dict[str, Any]] = []
        alerts: list[dict[str, Any]] = []

        status_step_map = {
            CapitalShipOrder.Status.WAITING: 1,
            CapitalShipOrder.Status.GATHERING_MATERIALS: 3,
            CapitalShipOrder.Status.IN_PRODUCTION: 4,
            CapitalShipOrder.Status.CONTRACT_CREATED: 5,
            CapitalShipOrder.Status.COMPLETED: 6,
        }

        status_badge_classes = {
            CapitalShipOrder.Status.WAITING: "bg-warning-subtle text-warning border border-warning-subtle",
            CapitalShipOrder.Status.GATHERING_MATERIALS: "bg-info-subtle text-info border border-info-subtle",
            CapitalShipOrder.Status.IN_PRODUCTION: "bg-primary-subtle text-primary border border-primary-subtle",
            CapitalShipOrder.Status.CONTRACT_CREATED: "bg-success-subtle text-success border border-success-subtle",
            CapitalShipOrder.Status.COMPLETED: "bg-secondary-subtle text-secondary border border-secondary-subtle",
            CapitalShipOrder.Status.ANOMALY: "bg-danger-subtle text-danger border border-danger-subtle",
            CapitalShipOrder.Status.REJECTED: "bg-danger-subtle text-danger border border-danger-subtle",
            CapitalShipOrder.Status.CANCELLED: "bg-secondary-subtle text-muted border border-secondary-subtle",
        }

        for order in active_orders_raw:
            has_pending_offer = order.has_pending_offer_confirmation
            chat = getattr(order, "chat", None)
            has_unread_chat = False
            if chat and chat.is_open and chat.last_message_at:
                if chat.last_message_role != CapitalShipOrderChat.SenderRole.REQUESTER:
                    if not chat.requester_last_seen_at or chat.requester_last_seen_at < chat.last_message_at:
                        has_unread_chat = True

            # Determine price to display
            price_display = ""
            if order.agreed_price_isk:
                price_display = _format_price_isk(order.agreed_price_isk)
            elif order.offer_price_isk:
                price_display = _format_price_isk(order.offer_price_isk)
            elif order.guideline_price_isk:
                price_display = f"~{_format_price_isk(order.guideline_price_isk)}"

            # Determine ETA to display
            eta_display = ""
            if order.definitive_eta_min_days is not None and order.definitive_eta_max_days is not None:
                eta_display = _("Definitive ETA: %(min)d–%(max)d days") % {
                    "min": order.definitive_eta_min_days,
                    "max": order.definitive_eta_max_days,
                }
            elif order.likely_eta_min_days is not None and order.likely_eta_max_days is not None:
                eta_display = _("Agreed ETA: %(min)d–%(max)d days") % {
                    "min": order.likely_eta_min_days,
                    "max": order.likely_eta_max_days,
                }
            elif order.offer_eta_min_days is not None and order.offer_eta_max_days is not None:
                eta_display = _("Proposed ETA: %(min)d–%(max)d days") % {
                    "min": order.offer_eta_min_days,
                    "max": order.offer_eta_max_days,
                }
            elif order.guideline_eta_min_days is not None and order.guideline_eta_max_days is not None:
                eta_display = _("Estimated ETA: %(min)d–%(max)d days") % {
                    "min": order.guideline_eta_min_days,
                    "max": order.guideline_eta_max_days,
                }

            step = status_step_map.get(order.status, 1)
            if has_pending_offer:
                step = 2

            order_payload = {
                "id": order.id,
                "order_reference": order.order_reference,
                "ship_type_id": order.ship_type_id,
                "ship_type_name": order.ship_type_name,
                "ship_class": order.ship_class,
                "ship_class_label": default_ship_class_label(order.ship_class),
                "status": order.status,
                "status_display": order.get_status_display(),
                "status_badge_class": status_badge_classes.get(order.status, "bg-secondary"),
                "step": step,
                "total_steps": 5,
                "price_display": price_display,
                "eta_display": eta_display,
                "has_pending_offer": has_pending_offer,
                "has_unread_chat": has_unread_chat,
                "chat_id": chat.id if chat else None,
                "url": (
                    f"{reverse('indy_hub:capital_ship_orders')}?open_chat={chat.id}"
                    if (chat and (has_pending_offer or has_unread_chat))
                    else reverse("indy_hub:capital_ship_orders")
                ),
                "updated_at": order.updated_at,
            }
            active_orders.append(order_payload)

            if has_pending_offer:
                pending_offer_confirmations.append(order_payload)
                alerts.append(
                    {
                        "type": "capital_offer_pending",
                        "title": _("Capital Offer Pending Confirmation"),
                        "reference": order.order_reference,
                        "ship_name": order.ship_type_name,
                        "message": _("Builder proposed %(price)s, %(eta)s. Click to review and confirm.")
                        % {
                            "price": _format_price_isk(order.offer_price_isk),
                            "eta": f"{order.offer_eta_min_days}-{order.offer_eta_max_days}d",
                        },
                        "url": f"{reverse('indy_hub:capital_ship_orders')}?open_chat={chat.id}" if chat else reverse("indy_hub:capital_ship_orders"),
                        "badge": _("Action Required"),
                        "badge_class": "bg-warning text-dark",
                        "timestamp": order.offer_updated_at or order.updated_at,
                    }
                )
            elif has_unread_chat and chat:
                alerts.append(
                    {
                        "type": "capital_chat_unread",
                        "title": _("Capital Order Chat"),
                        "reference": order.order_reference,
                        "ship_name": order.ship_type_name,
                        "message": _("New message regarding %(ship)s (%(ref)s)")
                        % {"ship": order.ship_type_name, "ref": order.order_reference},
                        "url": f"{reverse('indy_hub:capital_ship_orders')}?open_chat={chat.id}",
                        "badge": _("Unread"),
                        "badge_class": "bg-info text-dark",
                        "timestamp": chat.last_message_at,
                    }
                )

        # Builder / Manager metrics
        is_builder_or_manager = (
            user.has_perm("indy_hub.can_manage_capital_orders")
            or user.has_perm("indy_hub.can_build_capital_orders")
        )
        builder_queue_summary = None
        if is_builder_or_manager:
            all_active_qs = CapitalShipOrder.objects.exclude(status__in=terminal_statuses)
            unclaimed_count = all_active_qs.filter(
                status=CapitalShipOrder.Status.WAITING,
                gathering_materials_by__isnull=True,
                in_production_by__isnull=True,
                agreement_locked_by__isnull=True,
            ).count()
            in_production_count = all_active_qs.filter(
                status=CapitalShipOrder.Status.IN_PRODUCTION
            ).count()
            gathering_count = all_active_qs.filter(
                status=CapitalShipOrder.Status.GATHERING_MATERIALS
            ).count()
            total_active_queue = all_active_qs.count()

            builder_queue_summary = {
                "total_active": total_active_queue,
                "unclaimed_count": unclaimed_count,
                "gathering_count": gathering_count,
                "in_production_count": in_production_count,
                "admin_url": reverse("indy_hub:capital_ship_orders_admin"),
            }

        return {
            "has_access": True,
            "capital_orders_url": reverse("indy_hub:capital_ship_orders"),
            "active_orders": active_orders,
            "active_count": len(active_orders),
            "pending_offer_confirmations": pending_offer_confirmations,
            "pending_offer_count": len(pending_offer_confirmations),
            "alerts": alerts,
            "builder_queue_summary": builder_queue_summary,
            "is_builder_or_manager": is_builder_or_manager,
        }

    except Exception as exc:
        logger.warning("Error collecting capital orders dashboard hook data: %s", exc, exc_info=True)
        return None
