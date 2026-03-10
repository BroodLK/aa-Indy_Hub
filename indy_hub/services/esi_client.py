"""ESI client abstraction powered by django-esi."""

from __future__ import annotations

# Standard Library
from collections.abc import Mapping

# Alliance Auth
from allianceauth.services.hooks import get_extension_logger
from esi.errors import TokenError
from esi.exceptions import (
    ESIBucketLimitException,
    ESIErrorLimitException,
    HTTPError,
    HTTPNotModified,
)
from esi.models import Token

# AA Example App
# Local
from indy_hub.app_settings import ESI_COMPATIBILITY_DATE
from indy_hub.services.providers import esi_provider

logger = get_extension_logger(__name__)

DEFAULT_COMPATIBILITY_DATE = ESI_COMPATIBILITY_DATE


class ESIClientError(Exception):
    """Base error raised when the ESI client fails."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class ESITokenError(ESIClientError):
    """Raised when a valid access token cannot be retrieved."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class ESIForbiddenError(ESIClientError):
    """Raised when ESI returns HTTP 403 for an authenticated lookup."""

    def __init__(
        self,
        message: str,
        *,
        character_id: int | None = None,
        structure_id: int | None = None,
    ) -> None:
        super().__init__(message)
        self.character_id = character_id
        self.structure_id = structure_id


class ESIRateLimitError(ESIClientError):
    """Raised when ESI signals that the error limit has been exceeded."""

    def __init__(
        self,
        message: str = "ESI rate limit exceeded",
        *,
        retry_after: float | None = None,
        remaining: int | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after = retry_after
        self.remaining = remaining


class ESIUnmodifiedError(ESIClientError):
    """Raised when ESI responds with HTTP 304 (Not Modified)."""


def _extract_headers(source) -> Mapping[str, str]:
    if source is None:
        return {}
    if isinstance(source, Mapping):
        return source
    headers = getattr(source, "headers", None)
    return headers or {}


def rate_limit_wait_seconds(source, fallback: float) -> tuple[float, int | None]:
    """Return the recommended pause in seconds from ESI headers."""

    headers = _extract_headers(source)

    wait_candidates: list[float] = []
    retry_after_header = headers.get("Retry-After")
    reset_header = headers.get("X-Esi-Error-Limit-Reset")

    for raw_value in (retry_after_header, reset_header):
        if raw_value is None:
            continue
        try:
            wait_candidates.append(float(raw_value))
        except (TypeError, ValueError):
            continue

    wait = fallback
    if wait_candidates:
        positive = [value for value in wait_candidates if value > 0]
        if positive:
            wait = max(max(positive), fallback)

    remaining_header = headers.get("X-Esi-Error-Limit-Remain")
    remaining: int | None = None
    if remaining_header is not None:
        try:
            remaining = int(remaining_header)
        except (TypeError, ValueError):
            remaining = None

    return wait, remaining


def token_rate_limit_wait_seconds(source, fallback: float) -> tuple[float, int | None]:
    """Return wait seconds from token-based rate limit headers."""

    headers = _extract_headers(source)
    retry_after_header = headers.get("Retry-After")
    reset_header = headers.get("X-Ratelimit-Reset")
    remaining_header = headers.get("X-Ratelimit-Remaining")

    wait_candidates: list[float] = []
    for raw_value in (retry_after_header, reset_header):
        if raw_value is None:
            continue
        try:
            wait_candidates.append(float(raw_value))
        except (TypeError, ValueError):
            continue

    wait = fallback
    if wait_candidates:
        positive = [value for value in wait_candidates if value > 0]
        if positive:
            wait = max(max(positive), fallback)

    remaining: int | None = None
    if remaining_header is not None:
        try:
            remaining = int(remaining_header)
        except (TypeError, ValueError):
            remaining = None

    return wait, remaining


def get_retry_after_seconds(
    exc: Exception,
    *,
    fallback: int = 60,
    minimum: int = 1,
) -> int:
    """Normalize retry delay from an ESIRateLimitError or similar exception."""

    raw_delay = getattr(exc, "retry_after", None)
    delay = 0
    if raw_delay is not None:
        try:
            delay = int(float(raw_delay))
        except (TypeError, ValueError):
            delay = 0

    if delay <= 0:
        delay = int(fallback)

    return max(delay, int(minimum))


class ESIClient:
    """Small helper around django-esi OpenAPI client with AA-friendly errors."""

    def __init__(
        self,
        base_url: str = "https://esi.evetech.net/latest",
        timeout: int = 20,
        max_attempts: int = 3,
        backoff_factor: float = 0.75,
        compatibility_date: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.backoff_factor = backoff_factor
        self.compatibility_date = (compatibility_date or "").strip() or None
        self.provider = esi_provider
        self.client = self.provider.client

    def fetch_character_blueprints(
        self, character_id: int, *, force_refresh: bool = False
    ) -> list[dict]:
        """Return the list of blueprints for a character."""
        return self._fetch_paginated(
            character_id=character_id,
            scope="esi-characters.read_blueprints.v1",
            endpoint=f"/characters/{character_id}/blueprints/",
            resource="Character",
            operation="get_characters_character_id_blueprints",
            params={"character_id": character_id},
            force_refresh=force_refresh,
        )

    def fetch_character_industry_jobs(
        self, character_id: int, *, force_refresh: bool = False
    ) -> list[dict]:
        """Return the list of industry jobs for a character."""
        return self._fetch_paginated(
            character_id=character_id,
            scope="esi-industry.read_character_jobs.v1",
            endpoint=f"/characters/{character_id}/industry/jobs/",
            resource="Industry",
            operation="get_characters_character_id_industry_jobs",
            params={"character_id": character_id},
            force_refresh=force_refresh,
        )

    def fetch_corporation_blueprints(
        self,
        corporation_id: int,
        *,
        character_id: int,
        force_refresh: bool = False,
    ) -> list[dict]:
        """Return the list of blueprints owned by a corporation."""
        return self._fetch_paginated(
            character_id=character_id,
            scope="esi-corporations.read_blueprints.v1",
            endpoint=f"/corporations/{corporation_id}/blueprints/",
            resource="Corporation",
            operation="get_corporations_corporation_id_blueprints",
            params={"corporation_id": corporation_id},
            force_refresh=force_refresh,
        )

    def fetch_corporation_industry_jobs(
        self,
        corporation_id: int,
        *,
        character_id: int,
        force_refresh: bool = False,
    ) -> list[dict]:
        """Return the list of industry jobs owned by a corporation."""
        return self._fetch_paginated(
            character_id=character_id,
            scope="esi-industry.read_corporation_jobs.v1",
            endpoint=f"/corporations/{corporation_id}/industry/jobs/",
            resource="Industry",
            operation="get_corporations_corporation_id_industry_jobs",
            params={"corporation_id": corporation_id},
            force_refresh=force_refresh,
        )

    def fetch_character_corporation_roles(
        self,
        character_id: int,
        *,
        force_refresh: bool = False,
    ) -> dict:
        """Return the corporation roles assigned to a character."""
        token_obj = self._get_token(
            character_id, "esi-characters.read_corporation_roles.v1"
        )
        operation_fn = self._resolve_operation(
            "Character", "get_characters_character_id_roles"
        )
        request_kwargs = {}
        if force_refresh:
            request_kwargs["If-None-Match"] = ""
        payload = self._call_authed(
            token_obj,
            character_id=character_id,
            endpoint=f"/characters/{character_id}/roles/",
            scope="esi-characters.read_corporation_roles.v1",
            operation=lambda token: operation_fn(
                character_id=character_id,
                token=token,
                **request_kwargs,
            ),
        )
        if isinstance(payload, list):
            if not payload:
                raise ESIClientError(
                    "ESI /characters/{character_id}/roles returned an empty payload"
                )
            payload = payload[0]
        if isinstance(payload, dict):
            return payload
        coerced = self._coerce_mapping(payload)
        if isinstance(coerced, dict):
            return coerced
        raise ESIClientError(
            "ESI /characters/{character_id}/roles returned an unexpected payload"
        )

    def fetch_character_online_status(self, character_id: int) -> dict:
        """Return the online status for a character."""
        scope = "esi-location.read_online.v1"
        token_obj = self._get_token(character_id, scope)
        operation_fn = None
        location_resource = getattr(self.client, "Location", None)
        if location_resource is not None:
            operation_fn = getattr(
                location_resource,
                "get_characters_character_id_online",
                None,
            ) or getattr(location_resource, "GetCharactersCharacterIdOnline", None)
        if not operation_fn:
            character_resource = getattr(self.client, "Character", None)
            if character_resource is not None:
                operation_fn = getattr(
                    character_resource,
                    "get_characters_character_id_online",
                    None,
                ) or getattr(character_resource, "GetCharactersCharacterIdOnline", None)
        if not operation_fn:
            raise ESIClientError(
                "ESI operation Location.get_characters_character_id_online is not available"
            )
        payload = self._call_authed(
            token_obj,
            character_id=character_id,
            endpoint=f"/characters/{character_id}/online/",
            scope=scope,
            operation=lambda token: operation_fn(
                character_id=character_id,
                token=token,
                **{"If-None-Match": ""},
            ),
        )
        if isinstance(payload, list):
            if not payload:
                raise ESIClientError(
                    "ESI /characters/{character_id}/online returned an empty payload"
                )
            payload = payload[0]
        if isinstance(payload, dict):
            return payload
        coerced = self._coerce_mapping(payload)
        if isinstance(coerced, dict):
            return coerced
        if payload is not None:
            attr_payload = {
                key: getattr(payload, key)
                for key in ("online", "last_login", "last_logout", "logins")
                if hasattr(payload, key)
            }
            if attr_payload:
                return attr_payload
        raise ESIClientError(
            "ESI /characters/{character_id}/online returned an unexpected payload"
        )

    def fetch_structure_name(
        self, structure_id: int, character_id: int | None = None
    ) -> str | None:
        """Attempt to resolve a structure name via the authenticated endpoint."""
        if not structure_id:
            return None

        if not character_id:
            return None

        token_obj = None
        try:
            token_obj = self._get_token(
                int(character_id), "esi-universe.read_structures.v1"
            )
        except ESITokenError:
            logger.debug(
                "No valid universe.read_structures token for character %s",
                character_id,
            )
            return None

        try:
            operation_fn = self._resolve_operation(
                "Universe", "get_universe_structures_structure_id"
            )
            # Use etag by default to benefit from caching
            payload = self._call_authed(
                token_obj,
                character_id=int(character_id),
                structure_id=int(structure_id),
                endpoint=f"/universe/structures/{int(structure_id)}/",
                scope="esi-universe.read_structures.v1",
                operation=lambda token: operation_fn(
                    structure_id=int(structure_id),
                    token=token,
                ),
            )
        except ESIUnmodifiedError:
            # If we get 304 and couldn't retrieve cached data, this is an error
            logger.debug(
                "Structure %s name not available (304 without cache) for character %s",
                structure_id,
                character_id,
            )
            return None
        except ESIForbiddenError:
            raise
        except ESITokenError:
            return None
        except ESIClientError:
            return None

        if isinstance(payload, list):
            if not payload:
                return None
            payload = payload[0]
        if isinstance(payload, dict):
            return payload.get("name")
        coerced = self._coerce_mapping(payload)
        if isinstance(coerced, dict):
            return coerced.get("name")
        if payload is not None:
            name = getattr(payload, "name", None)
            if name:
                return str(name)
        return None

    def _fetch_paginated(
        self,
        *,
        character_id: int,
        scope: str,
        endpoint: str,
        resource: str,
        operation: str,
        params: dict,
        force_refresh: bool = False,
    ) -> list[dict]:
        token_obj = self._get_token(character_id, scope)
        try:
            token_obj.valid_access_token()
        except Exception as exc:
            raise ESITokenError(
                f"No valid token for character {character_id} and scope {scope}"
            ) from exc

        try:
            operation_fn = self._resolve_operation(resource, operation)
        except AttributeError as exc:
            raise ESIClientError(
                f"ESI operation {resource}.{operation} is not available"
            ) from exc

        request_kwargs = {}
        if force_refresh:
            request_kwargs["If-None-Match"] = ""

        try:
            result_obj = operation_fn(
                **params, token=token_obj, **request_kwargs
            )
            # When ESI returns 304, django-esi will handle it and return cached data
            # if use_etag is True (default). We call results() to get either fresh or cached data.
            payload = result_obj.results()
        except HTTPNotModified:
            # 304 means data hasn't changed. Try to get cached data from django-esi.
            # According to django-esi docs, calling results() after 304 should return cached data.
            try:
                result_obj = operation_fn(
                    **params, token=token_obj, **request_kwargs
                )
                payload = result_obj.results(use_cache=True)
            except Exception as cache_exc:
                logger.debug(
                    "Failed to retrieve cached data for %s after 304: %s",
                    endpoint,
                    cache_exc,
                )
                # If we can't get cached data, raise the original error
                raise ESIUnmodifiedError(
                    f"ESI returned 304 for {endpoint} and no cached data available"
                ) from cache_exc
        except (ESIErrorLimitException, ESIBucketLimitException) as exc:
            retry_after = getattr(exc, "reset", None)
            raise ESIRateLimitError(
                retry_after=retry_after,
                remaining=None,
            ) from exc
        except HTTPError as exc:
            self._handle_http_error(
                exc,
                character_id=character_id,
                endpoint=endpoint,
                token_obj=token_obj,
                scope=scope,
            )
            raise
        except TokenError as exc:
            raise ESITokenError(
                f"No valid token for character {character_id} and scope {scope}"
            ) from exc
        except Exception as exc:
            raise ESIClientError(f"ESI request failed for {endpoint}: {exc}") from exc

        if not isinstance(payload, list):
            raise ESIClientError(
                f"ESI {endpoint} returned an unexpected payload type: {type(payload)}"
            )
        return [self._coerce_mapping(item) for item in payload]

    @staticmethod
    def _coerce_mapping(item):
        if isinstance(item, dict):
            return item
        for attr in ("model_dump", "dict", "to_dict"):
            converter = getattr(item, attr, None)
            if callable(converter):
                try:
                    result = converter()
                except Exception:
                    result = None
                if isinstance(result, dict):
                    return result
        return item

    def _get_token(self, character_id: int, scope: str) -> Token:
        token = (
            Token.objects.filter(character_id=int(character_id))
            .require_scopes([scope])
            .require_valid()
            .order_by("-created")
            .first()
        )
        if not token:
            raise ESITokenError(
                f"No valid token for character {character_id} and scope {scope}"
            )
        return token

    def _get_access_token(self, character_id: int, scope: str) -> str:
        token = self._get_token(character_id, scope)
        try:
            return token.valid_access_token()
        except Exception as exc:  # pragma: no cover - Alliance Auth handles details
            raise ESITokenError(
                f"No valid token for character {character_id} and scope {scope}"
            ) from exc

    def fetch_corporation_contracts(
        self,
        corporation_id: int,
        character_id: int,
        force_refresh: bool = False,
    ) -> list[dict]:
        """Fetch all contracts for a corporation using character's token."""
        return self._fetch_paginated(
            character_id=character_id,
            scope="esi-contracts.read_corporation_contracts.v1",
            endpoint=f"/corporations/{corporation_id}/contracts/",
            resource="Contracts",
            operation="get_corporations_corporation_id_contracts",
            params={"corporation_id": corporation_id},
            force_refresh=force_refresh,
        )

    def fetch_corporation_contract_items(
        self,
        corporation_id: int,
        contract_id: int,
        character_id: int,
        force_refresh: bool = False,
    ) -> list[dict]:
        """Fetch items for a specific corporation contract."""
        return self._fetch_paginated(
            character_id=character_id,
            scope="esi-contracts.read_corporation_contracts.v1",
            endpoint=f"/corporations/{corporation_id}/contracts/{contract_id}/items/",
            resource="Contracts",
            operation="get_corporations_corporation_id_contracts_contract_id_items",
            params={"corporation_id": corporation_id, "contract_id": contract_id},
            force_refresh=force_refresh,
        )

    def fetch_character_contracts(self, character_id: int) -> list[dict]:
        """Fetch all contracts for a character using their token."""
        return self._fetch_paginated(
            character_id=character_id,
            scope="esi-contracts.read_character_contracts.v1",
            endpoint=f"/characters/{character_id}/contracts/",
            resource="Contracts",
            operation="get_characters_character_id_contracts",
            params={"character_id": character_id},
        )

    def fetch_character_contract_items(
        self,
        character_id: int,
        contract_id: int,
    ) -> list[dict]:
        """Fetch items for a specific character contract."""
        return self._fetch_paginated(
            character_id=character_id,
            scope="esi-contracts.read_character_contracts.v1",
            endpoint=f"/characters/{character_id}/contracts/{contract_id}/items/",
            resource="Contracts",
            operation="get_characters_character_id_contracts_contract_id_items",
            params={"character_id": character_id, "contract_id": contract_id},
        )

    def fetch_corporation_assets(
        self,
        corporation_id: int,
        *,
        character_id: int,
    ) -> list[dict]:
        """Fetch all corporation assets for the given corp using a character token."""
        return self._fetch_paginated(
            character_id=character_id,
            scope="esi-assets.read_corporation_assets.v1",
            endpoint=f"/corporations/{corporation_id}/assets/",
            resource="Assets",
            operation="get_corporations_corporation_id_assets",
            params={"corporation_id": corporation_id},
        )

    def fetch_character_assets(
        self,
        *,
        character_id: int,
        force_refresh: bool = False,
    ) -> list[dict]:
        """Fetch all assets for a character using their token."""
        return self._fetch_paginated(
            character_id=character_id,
            scope="esi-assets.read_assets.v1",
            endpoint=f"/characters/{character_id}/assets/",
            resource="Assets",
            operation="get_characters_character_id_assets",
            params={"character_id": character_id},
            force_refresh=force_refresh,
        )

    def fetch_character_asset_names(
        self,
        *,
        character_id: int,
        item_ids: list[int],
        force_refresh: bool = False,
    ) -> dict[int, str]:
        """Fetch custom asset names for a character by item ids."""
        unique_item_ids: list[int] = []
        seen: set[int] = set()
        for item_id in item_ids or []:
            try:
                item_id_int = int(item_id)
            except (TypeError, ValueError):
                continue
            if item_id_int <= 0 or item_id_int in seen:
                continue
            seen.add(item_id_int)
            unique_item_ids.append(item_id_int)
        if not unique_item_ids:
            return {}

        token_obj = self._get_token(character_id, "esi-assets.read_assets.v1")
        try:
            operation_fn = self._resolve_operation(
                "Assets", "post_characters_character_id_assets_names"
            )
        except AttributeError:
            logger.debug(
                "ESI operation Assets.post_characters_character_id_assets_names is not available"
            )
            return {}

        request_kwargs = {}
        if force_refresh:
            request_kwargs["If-None-Match"] = ""

        name_map: dict[int, str] = {}

        for start in range(0, len(unique_item_ids), 1000):
            batch = unique_item_ids[start : start + 1000]

            def _call_with_payload(payload_kw: dict):
                return self._call_authed(
                    token_obj,
                    character_id=character_id,
                    endpoint=f"/characters/{character_id}/assets/names/",
                    scope="esi-assets.read_assets.v1",
                    operation=lambda token: operation_fn(
                        character_id=character_id,
                        token=token,
                        **payload_kw,
                        **request_kwargs,
                    ),
                )

            try:
                payload = _call_with_payload({"item_ids": batch})
            except ESIClientError:
                try:
                    payload = _call_with_payload({"body": batch})
                except ESIClientError as exc:
                    logger.debug(
                        "Failed to fetch character asset names for %s (%s items): %s",
                        character_id,
                        len(batch),
                        exc,
                    )
                    continue

            if isinstance(payload, dict):
                payload = [payload]

            for item in payload or []:
                data = self._coerce_mapping(item)
                if not isinstance(data, dict):
                    continue
                try:
                    named_item_id = int(data.get("item_id") or 0)
                except (TypeError, ValueError):
                    continue
                name = str(data.get("name") or "").strip()
                if named_item_id > 0 and name:
                    name_map[named_item_id] = name

        return name_map

    def fetch_corporation_asset_names(
        self,
        *,
        corporation_id: int,
        character_id: int,
        item_ids: list[int],
        force_refresh: bool = False,
    ) -> dict[int, str]:
        """Fetch custom corporation asset names by item ids."""
        unique_item_ids: list[int] = []
        seen: set[int] = set()
        for item_id in item_ids or []:
            try:
                item_id_int = int(item_id)
            except (TypeError, ValueError):
                continue
            if item_id_int <= 0 or item_id_int in seen:
                continue
            seen.add(item_id_int)
            unique_item_ids.append(item_id_int)
        if not unique_item_ids:
            return {}

        token_obj = self._get_token(character_id, "esi-assets.read_corporation_assets.v1")
        try:
            operation_fn = self._resolve_operation(
                "Assets", "post_corporations_corporation_id_assets_names"
            )
        except AttributeError:
            logger.debug(
                "ESI operation Assets.post_corporations_corporation_id_assets_names is not available"
            )
            return {}

        request_kwargs = {}
        if force_refresh:
            request_kwargs["If-None-Match"] = ""

        name_map: dict[int, str] = {}

        for start in range(0, len(unique_item_ids), 1000):
            batch = unique_item_ids[start : start + 1000]

            def _call_with_payload(payload_kw: dict):
                return self._call_authed(
                    token_obj,
                    character_id=character_id,
                    endpoint=f"/corporations/{corporation_id}/assets/names/",
                    scope="esi-assets.read_corporation_assets.v1",
                    operation=lambda token: operation_fn(
                        corporation_id=corporation_id,
                        token=token,
                        **payload_kw,
                        **request_kwargs,
                    ),
                )

            try:
                payload = _call_with_payload({"item_ids": batch})
            except ESIClientError:
                try:
                    payload = _call_with_payload({"body": batch})
                except ESIClientError as exc:
                    logger.debug(
                        "Failed to fetch corp asset names for %s (%s items): %s",
                        corporation_id,
                        len(batch),
                        exc,
                    )
                    continue

            if isinstance(payload, dict):
                payload = [payload]

            for item in payload or []:
                data = self._coerce_mapping(item)
                if not isinstance(data, dict):
                    continue
                try:
                    named_item_id = int(data.get("item_id") or 0)
                except (TypeError, ValueError):
                    continue
                name = str(data.get("name") or "").strip()
                if named_item_id > 0 and name:
                    name_map[named_item_id] = name

        return name_map

    def fetch_corporation_structures(
        self,
        corporation_id: int,
        *,
        character_id: int,
    ) -> list[dict]:
        """Fetch corporation structures (includes names) using corp structures scope."""
        return self._fetch_paginated(
            character_id=character_id,
            scope="esi-corporations.read_structures.v1",
            endpoint=f"/corporations/{corporation_id}/structures/",
            resource="Corporation",
            operation="get_corporations_corporation_id_structures",
            params={"corporation_id": corporation_id},
        )

    def resolve_ids_to_names(self, ids: list[int]) -> dict[int, str]:
        """Resolve a list of IDs to names via the public /universe/names/ endpoint.

        This endpoint doesn't require authentication and can resolve stations, structures,
        systems, regions, etc.

        Returns a dict mapping ID -> name for successfully resolved IDs.
        """
        if not ids:
            return {}

        # ESI accepts max 1000 IDs per request
        result: dict[int, str] = {}
        try:
            operation_fn = self._resolve_operation("Universe", "post_universe_names")
        except AttributeError:
            return result
        for i in range(0, len(ids), 1000):
            batch = ids[i : i + 1000]
            try:
                payload = operation_fn(ids=batch).results()
            except HTTPError as exc:
                self._handle_http_error(
                    exc,
                    endpoint="/universe/names/",
                )
                continue
            except Exception:
                try:
                    payload = operation_fn(body=batch).results()
                except HTTPError as exc2:
                    self._handle_http_error(
                        exc2,
                        endpoint="/universe/names/",
                    )
                    continue
                except Exception as exc2:
                    logger.warning("Resolve IDs request failed: %s", exc2)
                    continue

            try:
                for item in payload or []:
                    if "id" in item and "name" in item:
                        result[int(item["id"])] = str(item["name"])
            except (ValueError, KeyError, TypeError) as exc:
                logger.warning("Invalid payload from /universe/names/: %s", exc)

        return result

    def _call_authed(
        self,
        token_obj: Token,
        *,
        character_id: int | None = None,
        structure_id: int | None = None,
        endpoint: str | None = None,
        scope: str | None = None,
        operation=None,
        results_kwargs: dict | None = None,
    ):
        if operation is None:
            raise ESIClientError("No ESI operation provided")
        try:
            token_obj.valid_access_token()
        except Exception as exc:
            raise ESITokenError(
                f"No valid token for character {character_id} and scope {scope}"
            ) from exc
        try:
            if results_kwargs is None:
                results_kwargs = {}
            result_obj = operation(token_obj)
            # When ESI returns 304, django-esi should return cached data
            return result_obj.results(**results_kwargs)
        except HTTPNotModified:
            # 304 means data hasn't changed. Try to get cached data from django-esi.
            try:
                result_obj = operation(token_obj)
                cache_kwargs = dict(results_kwargs or {})
                cache_kwargs["use_cache"] = True
                return result_obj.results(**cache_kwargs)
            except Exception as cache_exc:
                logger.debug(
                    "Failed to retrieve cached data for %s after 304: %s",
                    endpoint or "request",
                    cache_exc,
                )
                # If we can't get cached data, raise the original error
                raise ESIUnmodifiedError(
                    f"ESI returned 304 for {endpoint or 'request'} and no cached data available"
                ) from cache_exc
        except (ESIErrorLimitException, ESIBucketLimitException) as exc:
            retry_after = getattr(exc, "reset", None)
            raise ESIRateLimitError(
                retry_after=retry_after,
                remaining=None,
            ) from exc
        except HTTPError as exc:
            self._handle_http_error(
                exc,
                character_id=character_id,
                structure_id=structure_id,
                endpoint=endpoint,
                token_obj=token_obj,
                scope=scope,
            )
            raise
        except TokenError as exc:
            raise ESITokenError(
                f"No valid token for character {character_id} and scope {scope}"
            ) from exc
        except Exception as exc:
            raise ESIClientError(f"ESI request failed for {endpoint}: {exc}") from exc

    def _resolve_operation(self, resource: str, operation: str):
        """Resolve an ESI operation name for OpenAPI clients."""
        resource_obj = getattr(self.client, resource)
        if hasattr(resource_obj, operation):
            return getattr(resource_obj, operation)

        camel = "".join(part.capitalize() for part in operation.split("_"))
        if hasattr(resource_obj, camel):
            return getattr(resource_obj, camel)

        raise AttributeError(f"{resource}.{operation}")

    def _handle_http_error(
        self,
        exc: HTTPError,
        *,
        character_id: int | None = None,
        structure_id: int | None = None,
        endpoint: str | None = None,
        token_obj: Token | None = None,
        scope: str | None = None,
    ) -> None:
        status_code = getattr(exc, "status_code", None)
        headers = _extract_headers(exc)
        if status_code == 420:
            sleep_for, remaining = rate_limit_wait_seconds(
                headers, self.backoff_factor
            )
            raise ESIRateLimitError(
                retry_after=sleep_for,
                remaining=remaining,
            ) from exc

        if status_code == 429:
            sleep_for, remaining = token_rate_limit_wait_seconds(
                headers, self.backoff_factor
            )
            raise ESIRateLimitError(
                retry_after=sleep_for,
                remaining=remaining,
            ) from exc

        if status_code == 403 and character_id is not None:
            if token_obj and scope and endpoint:
                self._handle_forbidden_token(
                    token_obj,
                    scope=scope,
                    endpoint=endpoint,
                )
            raise ESIForbiddenError(
                "ESI access forbidden",
                character_id=character_id,
                structure_id=structure_id,
            ) from exc

        if status_code in (401, 403):
            raise ESITokenError(
                f"Invalid token for {endpoint or 'ESI'} (status {status_code})",
                status_code=status_code,
            ) from exc

        raise ESIClientError(
            f"ESI returned {status_code} for {endpoint or 'request'}: {exc}",
            status_code=status_code,
        ) from exc

    def _handle_forbidden_token(
        self, token: Token, *, scope: str, endpoint: str
    ) -> None:
        character_id = getattr(token, "character_id", None)
        user_repr = None
        try:
            user_repr = token.user.username  # type: ignore[union-attr]
        except Exception:  # pragma: no cover - username optional
            user_repr = getattr(token, "user_id", None)

        logger.warning(
            "ESI returned 403 for %s (%s) through character %s (user %s). Token will be kept.",
            endpoint,
            scope,
            character_id,
            user_repr,
        )


# Module level singleton to avoid re-creating sessions
shared_client = ESIClient(compatibility_date=DEFAULT_COMPATIBILITY_DATE)
