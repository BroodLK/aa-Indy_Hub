"""URL helpers for indy_hub."""

from __future__ import annotations

# Standard Library
import re

# Django
from django.conf import settings


def site_absolute_url() -> str:
    """Return absolute site URL derived from ESI callback configuration."""
    try:
        match = re.match(r"(.+)\/sso\/callback", settings.ESI_SSO_CALLBACK_URL)
        if match:
            return match.group(1)
    except AttributeError:
        pass
    return ""
