# indy_hub/decorators.py
# Standard Library
import urllib.parse
from functools import wraps

# Django
from django.contrib import messages
from django.http import HttpResponseRedirect, JsonResponse
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


def _handle_ajax_token_redirect(request, response, view_called):
    """
    Handle ESI decorator responses for AJAX requests.
    If a token is required, return a 401 JSON with a redirect URL.
    Ensures that the redirect URL points back to the UI (Referer) rather than the API.
    """
    if not view_called and request.headers.get("x-requested-with") == "XMLHttpRequest":
        # Determine redirect URL
        if isinstance(response, HttpResponseRedirect):
            redirect_url = response["Location"]
            referer = request.headers.get("referer")
            if referer:
                try:
                    parsed = urllib.parse.urlparse(redirect_url)
                    params = urllib.parse.parse_qs(parsed.query)
                    if "next" in params:
                        params["next"] = [referer]
                        new_query = urllib.parse.urlencode(params, doseq=True)
                        redirect_url = urllib.parse.urlunparse(
                            parsed._replace(query=new_query)
                        )
                except Exception:
                    pass
        else:
            # It's likely the selection page HTML.
            # Redirect to the Referer to trigger selection in the browser at the UI level.
            redirect_url = request.headers.get("referer") or request.get_full_path()

        return JsonResponse(
            {
                "error": "ESI token required. Please select a character.",
                "redirect_url": redirect_url,
            },
            status=401,
        )
    return None


def token_required(scopes=None, new=False):
    """Compatibility wrapper around django-esi's `token_required`."""

    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            # Check for 'new' in request to allow manual character change
            force_new = new or request.GET.get("new") == "True"
            esi_decorator = esi_token_required(scopes=_normalize_scopes(scopes), new=force_new)

            view_called = [False]

            def _view_wrapper(*v_args, **v_kwargs):
                view_called[0] = True
                return view_func(*v_args, **v_kwargs)

            response = esi_decorator(_view_wrapper)(request, *args, **kwargs)

            ajax_response = _handle_ajax_token_redirect(request, response, view_called[0])
            if ajax_response:
                return ajax_response

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

            view_called = [False]

            def _view_wrapper(*v_args, **v_kwargs):
                view_called[0] = True
                return view_func(*v_args, **v_kwargs)

            response = esi_decorator(_view_wrapper)(request, *args, **kwargs)

            ajax_response = _handle_ajax_token_redirect(request, response, view_called[0])
            if ajax_response:
                return ajax_response

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

            view_called = [False]

            def _view_wrapper(*v_args, **v_kwargs):
                view_called[0] = True
                return view_func(*v_args, **v_kwargs)

            response = esi_decorator(_view_wrapper)(request, *args, **kwargs)

            ajax_response = _handle_ajax_token_redirect(request, response, view_called[0])
            if ajax_response:
                return ajax_response

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
                return JsonResponse({"error": "Session expired. Please login again."}, status=401)
            return redirect("auth_login_user")
        if not request.user.has_perm("indy_hub.can_access_indy_hub"):
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
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
                    return JsonResponse({"error": "Session expired. Please refresh the page."}, status=401)
                return redirect("auth_login_user")
            full_codename = f"indy_hub.{permission_codename}"
            if not request.user.has_perm(full_codename):
                if request.headers.get('x-requested-with') == 'XMLHttpRequest':
                    return JsonResponse({"error": "Permission denied."}, status=403)
                messages.error(request, "You do not have the required Indy Hub permission.")
                return redirect("indy_hub:index")
            return view_func(request, *args, **kwargs)

        return _wrapped_view

    return decorator
