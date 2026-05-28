# Standard Library
from unittest.mock import MagicMock

# Django
from django.test import SimpleTestCase

# Alliance Auth
from esi.exceptions import HTTPNotModified

# AA Example App
from indy_hub.services.esi_client import ESIClient


class ESIClientPaginationTest(SimpleTestCase):
    def test_fetch_paginated_propagates_force_refresh_to_results(self):
        client = ESIClient.__new__(ESIClient)
        client._get_token = MagicMock(return_value=object())
        result_obj = MagicMock()
        result_obj.results.return_value = [{"contract_id": 1}]
        operation_fn = MagicMock(return_value=result_obj)
        client._resolve_operation = MagicMock(return_value=operation_fn)
        client._coerce_mapping = MagicMock(side_effect=lambda item: item)

        payload = client._fetch_paginated(
            character_id=123,
            scope="esi-contracts.read_corporation_contracts.v1",
            endpoint="/corporations/123/contracts/",
            resource="Contracts",
            operation="get_corporations_corporation_id_contracts",
            params={"corporation_id": 123},
            force_refresh=True,
        )

        self.assertEqual(payload, [{"contract_id": 1}])
        result_obj.results.assert_called_once_with(
            force_refresh=True,
            use_cache=False,
        )

    def test_fetch_paginated_retries_without_etag_after_304_cache_miss(self):
        client = ESIClient.__new__(ESIClient)
        token_obj = MagicMock()
        client._get_token = MagicMock(return_value=token_obj)

        first_result = MagicMock()
        first_result.results.side_effect = HTTPNotModified()

        second_result = MagicMock()
        second_result.results.side_effect = HTTPNotModified()

        third_result = MagicMock()
        third_result.results.return_value = [{"contract_id": 7}]

        operation_fn = MagicMock(side_effect=[first_result, second_result, third_result])
        client._resolve_operation = MagicMock(return_value=operation_fn)
        client._coerce_mapping = MagicMock(side_effect=lambda item: item)

        payload = client._fetch_paginated(
            character_id=123,
            scope="esi-contracts.read_character_contracts.v1",
            endpoint="/characters/123/contracts/",
            resource="Contracts",
            operation="get_characters_character_id_contracts",
            params={"character_id": 123},
            force_refresh=False,
        )

        self.assertEqual(payload, [{"contract_id": 7}])
        second_result.results.assert_called_once_with(use_cache=True)
        third_result.results.assert_called_once_with(use_etag=False)


class ESIClientAuthedCallTest(SimpleTestCase):
    def test_call_authed_retries_without_etag_after_304_cache_miss(self):
        client = ESIClient.__new__(ESIClient)
        token_obj = MagicMock()

        first_result = MagicMock()
        first_result.results.side_effect = HTTPNotModified()

        second_result = MagicMock()
        second_result.results.side_effect = HTTPNotModified()

        third_result = MagicMock()
        third_result.results.return_value = {"ok": True}

        operation = MagicMock(side_effect=[first_result, second_result, third_result])

        payload = client._call_authed(
            token_obj,
            character_id=123,
            endpoint="/characters/123/contracts/",
            scope="esi-contracts.read_character_contracts.v1",
            operation=operation,
            results_kwargs={"force_refresh": False},
        )

        self.assertEqual(payload, {"ok": True})
        second_result.results.assert_called_once_with(force_refresh=False, use_cache=True)
        third_result.results.assert_called_once_with(use_etag=False)
