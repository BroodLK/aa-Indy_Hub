from unittest.mock import MagicMock

from django.test import SimpleTestCase

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
