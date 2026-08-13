# indy_hub/decorators.py
# Standard Library
from functools import wraps

# Django
from django.contrib import messages
from django.http import HttpResponseRedirect
from django.shortcuts import redirect

# Alliance Auth
from esi.decorators import single_use_token as esi_single_use_token
from esi.decorators import token_required as esi_token_required
from esi.decorators import tokens_required as esi_tokens_required


def _normalize_scopes(scopes):
    if scopes is None:
        return []
    if isinstance(scopes, str):
        return [scopes]
    return list(scopes)


def token_required(scopes=None, new=False):
    """Compatibility wrapper around django-esi's `token_required`."""

    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            # Check for 'new' in request to allow manual character change
            force_new = new or request.GET.get("new") == "True"
            esi_decorator = esi_token_required(scopes=_normalize_scopes(scopes), new=force_new)
            response = esi_decorator(view_func)(request, *args, **kwargs)
            if (
                isinstance(response, HttpResponseRedirect)
                and request.headers.get("x-requested-with") == "XMLHttpRequest"
            ):
                from django.http import JsonResponse

                return JsonResponse(
                    {
                        "error": "ESI token required. Please refresh the page or select a character.",
                        "redirect_url": response["Location"],
                    },
                    status=401,
                )
            return response

        return _wrapped

    return decorator


def tokens_required(scopes=None, new=False):
    """Compatibility wrapper around django-esi's `tokens_required`."""

    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            # Check for 'new' in request to allow manual character change
            force_new = new or request.GET.get("new") == "True"
            esi_decorator = esi_tokens_required(scopes=_normalize_scopes(scopes), new=force_new)
            response = esi_decorator(view_func)(request, *args, **kwargs)
            if (
                isinstance(response, HttpResponseRedirect)
                and request.headers.get("x-requested-with") == "XMLHttpRequest"
            ):
                from django.http import JsonResponse

                return JsonResponse(
                    {
                        "error": "ESI tokens required. Please refresh the page or select a character.",
                        "redirect_url": response["Location"],
                    },
                    status=401,
                )
            return response

        return _wrapped

    return decorator


def single_use_token(scopes=None, new=False):
    """Compatibility wrapper around django-esi's `single_use_token`."""

    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            # Check for 'new' in request to allow manual character change
            force_new = new or request.GET.get("new") == "True"
            esi_decorator = esi_single_use_token(
                scopes=_normalize_scopes(scopes), new=force_new
            )
            response = esi_decorator(view_func)(request, *args, **kwargs)
            if (
                isinstance(response, HttpResponseRedirect)
                and request.headers.get("x-requested-with") == "XMLHttpRequest"
            ):
                from django.http import JsonResponse

                return JsonResponse(
                    {
                        "error": "ESI token required. Please refresh the page or select a character.",
                        "redirect_url": response["Location"],
                    },
                    status=401,
                )
            return response

        return _wrapped

    return decorator


STRUCTURE_SCOPE = "esi-universe.read_structures.v1"


def blueprints_token_required(view_func):
    """Decorator specifically for blueprint views."""
    return token_required(
        [
            "esi-characters.read_blueprints.v1",
            STRUCTURE_SCOPE,
        ]
    )(view_func)


def industry_jobs_token_required(view_func):
    """Decorator specifically for industry jobs views."""
    return token_required(
        [
            "esi-industry.read_character_jobs.v1",
            STRUCTURE_SCOPE,
        ]
    )(view_func)


def indy_hub_access_required(view_func):
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        if not request.user.is_authenticated:
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                from django.http import JsonResponse

                return JsonResponse({"error": "Session expired. Please login again."}, status=401)
            return redirect("auth_login_user")
        if not request.user.has_perm("indy_hub.can_access_indy_hub"):
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                from django.http import JsonResponse

                return JsonResponse({"error": "Permission denied."}, status=403)
            messages.error(request, "You do not have permission to access Indy Hub.")
            return redirect("indy_hub:index")
        return view_func(request, *args, **kwargs)

    return _wrapped_view


def indy_hub_permission_required(permission_codename):
    """Ensure the logged-in user has the requested indy_hub permission."""

    def decorator(view_func):
        @wraps(view_func)
        def _wrapped_view(request, *args, **kwargs):
            if not request.user.is_authenticated:
                if request.headers.get('x-requested-with') == 'XMLHttpRequest':
                    from django.http import JsonResponse
                    return JsonResponse({"error": "Session expired. Please refresh the page."}, status=401)
                return redirect("auth_login_user")
            full_codename = f"indy_hub.{permission_codename}"
            if not request.user.has_perm(full_codename):
                if request.headers.get('x-requested-with') == 'XMLHttpRequest':
                    from django.http import JsonResponse
                    return JsonResponse({"error": "Permission denied."}, status=403)
                messages.error(request, "You do not have the required Indy Hub permission.")
                return redirect("indy_hub:index")
            return view_func(request, *args, **kwargs)

        return _wrapped_view

    return decorator
