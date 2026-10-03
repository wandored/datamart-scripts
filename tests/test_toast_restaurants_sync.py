import base64
from datetime import date
import importlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, mock_open, patch
from uuid import UUID

import psycopg2
import requests

from db_utils.toast_utils import ToastClient
from src.toast_sync import restaurants as module

dispatcher = importlib.import_module("src.toast-api-update")


def restaurant(number=1):
    return {"guid": str(UUID(int=number)), "general": {
        "name": "Example", "locationName": "Downtown", "locationCode": "DT",
        "timeZone": "America/New_York", "closeoutHour": 4,
        "managementGroupGuid": str(UUID(int=999)), "currencyCode": "USD",
        "firstBusinessDate": 20240229, "archived": False,
    }}


class RestaurantTests(unittest.TestCase):
    def test_normalization_and_nulls(self):
        payload = restaurant()
        row = module.normalize_restaurant(payload, payload["guid"])
        self.assertEqual(row["id"], UUID(int=1))
        self.assertEqual(row["management_group_id"], UUID(int=999))
        self.assertEqual(row["first_business_date"], date(2024, 2, 29))
        self.assertEqual(row["timezone"], "America/New_York")
        self.assertFalse(row["archived"])
        payload["general"] = {}
        row = module.normalize_restaurant(payload, payload["guid"])
        self.assertTrue(all(row[key] is None for key in module.RESTAURANT_COLUMNS[1:]))

    def test_rejects_bad_identity_structure_and_values(self):
        for payload in (None, [], {}, {"general": {}}, {"guid": "bad", "general": {}},
                        {"guid": str(UUID(int=2)), "general": {}}):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                module.normalize_restaurant(payload, UUID(int=1))
        for field, value in (("firstBusinessDate", 20230229), ("firstBusinessDate", 0),
                             ("archived", "false"), ("closeoutHour", True),
                             ("closeoutHour", 13), ("managementGroupGuid", "bad"),
                             ("name", [])):
            payload = restaurant()
            payload["general"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                module.normalize_restaurant(payload, payload["guid"])

    def test_partial_failure_only_writes_valid_rows(self):
        client = Mock()
        client.get_restaurant.side_effect = [restaurant(), requests.Timeout(), {}]
        locations = [{"id": i, "toast_guid": UUID(int=i)} for i in range(1, 4)]
        with patch.object(module, "upsert_restaurants", return_value={"inserted": 1, "updated": 0}) as write:
            with self.assertLogs(module.logger, level="INFO"):
                counts = module.sync_restaurants(client, locations)
        self.assertEqual(counts, {"requested": 3, "returned": 2, "rejected": 1,
                                  "api_errors": 1, "inserted": 1, "updated": 0})
        self.assertEqual(len(write.call_args.args[0]), 1)

    def test_dry_run_and_empty_write_do_not_connect(self):
        with patch("src.toast_sync.writing.DatabaseConnection") as db:
            self.assertEqual(module.upsert_restaurants([]), {"inserted": 0, "updated": 0})
            client = Mock()
            client.get_restaurant.return_value = restaurant()
            module.sync_restaurants(client, [{"id": 1, "toast_guid": UUID(int=1)}], dry_run=True)
            db.assert_not_called()


class DispatcherTests(unittest.TestCase):
    def setUp(self):
        self.locations = [{"id": 1, "toast_guid": UUID(int=1)}]
        self.get_locations = self.enterContext(patch.object(
            dispatcher, "get_configured_restaurants", return_value=self.locations,
        ))
        self.client_class = self.enterContext(patch.object(dispatcher, "ToastClient"))
        self.restaurants = Mock(return_value={"api_errors": 0, "rejected": 0})
        self.other = Mock(return_value={"api_errors": 0, "rejected": 0})
        self.enterContext(patch.dict(dispatcher.SYNC_MODULES, {
            "restaurants": SimpleNamespace(sync=self.restaurants),
            "other": SimpleNamespace(sync=self.other),
        }, clear=True))
        self.enterContext(patch.object(dispatcher.logger, "error"))

    def test_default_still_only_syncs_restaurants(self):
        self.assertEqual(dispatcher.main([]), 0)
        self.restaurants.assert_called_once_with(
            self.client_class.return_value, self.locations, dry_run=False,
        )
        self.other.assert_not_called()

    def test_selection_order_deduplication_and_shared_client(self):
        calls = Mock()
        calls.attach_mock(self.other, "other")
        calls.attach_mock(self.restaurants, "restaurants")
        self.assertEqual(dispatcher.main([
            "--sync", "other", "restaurants", "other", "--dry-run",
        ]), 0)
        self.assertEqual([call[0] for call in calls.mock_calls], ["other", "restaurants"])
        for handler in (self.other, self.restaurants):
            handler.assert_called_once_with(self.client_class.return_value, self.locations, dry_run=True)
        self.client_class.assert_called_once_with(load_locations=False)
        self.get_locations.assert_called_once()

    def test_exit_status_for_partial_failure(self):
        self.restaurants.return_value = {"api_errors": 1, "rejected": 0}
        self.assertEqual(dispatcher.main(["--sync", "restaurants", "other"]), 1)
        self.other.assert_called_once()

    def test_module_exception_does_not_prevent_next_module(self):
        self.restaurants.side_effect = RuntimeError("test failure")
        self.assertEqual(dispatcher.main(["--sync", "restaurants", "other"]), 1)
        self.other.assert_called_once()

    def test_empty_configuration_skips_authentication(self):
        self.get_locations.return_value = []
        self.assertEqual(dispatcher.main([]), 0)
        self.client_class.assert_not_called()
        self.restaurants.assert_not_called()


class ToastClientTests(unittest.TestCase):
    def setUp(self):
        self.client = object.__new__(ToastClient)
        self.client.api_access_url = "https://example.invalid"
        self.client.access_token = {"accessToken": "test-placeholder"}

    def response(self, status=200, payload=None, headers=None, links=None):
        response = Mock(status_code=status, headers=headers or {}, links=links or {})
        response.json.return_value = payload
        return response

    def test_token_cache_accepts_login_and_legacy_formats(self):
        encoded = base64.urlsafe_b64encode(json.dumps({"exp": 9999999999}).encode()).decode()
        token = f"test.{encoded}.test"
        for cached in ({"accessToken": token}, token, {"token": token}):
            with patch("db_utils.toast_utils.os.path.exists", return_value=True), \
                 patch("builtins.open", mock_open(read_data=json.dumps({"token": cached}))), \
                 patch("db_utils.toast_utils.requests.post") as post:
                self.client.access_token = self.client.generate_access_token()
                self.assertEqual(self.client.get_access_token(), token)
                post.assert_not_called()

    def test_single_restaurant_retries_and_scoping(self):
        with patch("db_utils.toast_utils.requests.get", side_effect=[
            self.response(429, headers={"Retry-After": "0"}),
            self.response(503), self.response(payload=restaurant()),
        ]) as get, patch("db_utils.toast_utils.time.sleep"):
            self.assertEqual(self.client.get_restaurant(UUID(int=1)), restaurant())
        self.assertEqual(get.call_count, 3)
        self.assertEqual(get.call_args.kwargs["headers"]["Toast-Restaurant-External-ID"], str(UUID(int=1)))
        self.assertEqual(get.call_args.kwargs["params"], {"includeArchived": "true"})
        self.assertEqual(get.call_args.kwargs["timeout"], 60)

    def test_token_pages_preserve_params_and_fail_instead_of_returning_partial_data(self):
        first = self.response(payload=[{"guid": "one"}], headers={"toast-next-page-token": "next"})
        params = {"date": "2026-01-01"}
        with patch.object(self.client, "request", side_effect=[first, self.response(payload=[{"guid": "two"}])]) as get:
            self.assertEqual(len(self.client.get_response_data("/test", "guid", params, 0)), 2)
            self.assertEqual(get.call_args.kwargs["params"], {**params, "pageToken": "next"})
            self.assertNotIn("pageToken", params)
        with patch.object(self.client, "request", side_effect=[first, requests.HTTPError()]):
            with self.assertRaises(requests.HTTPError):
                self.client.get_response_data("/test", "guid", rate_limit_wait=0)

    def test_numbered_pages_keep_orders_bulk_contract(self):
        with patch.object(self.client, "request", side_effect=[
            self.response(payload=[{"guid": "one"}], links={"next": {}}),
            self.response(payload=[]),
        ]) as get:
            result = self.client.get_paged_response_data(
                "/orders/v2/ordersBulk", "guid", {"businessDate": "20260101"}, page_size=1, rate_limit_wait=0,
            )
            self.assertEqual(result, [{"guid": "one"}])
            self.assertEqual(get.call_args.kwargs["params"], {"businessDate": "20260101", "page": 2, "pageSize": 1})

    def test_terminal_errors_and_timeouts_are_bounded(self):
        with patch("db_utils.toast_utils.requests.get", return_value=self.response(404)) as get:
            with self.assertRaises(requests.HTTPError):
                self.client.get_restaurant(UUID(int=1))
            self.assertEqual(get.call_count, 1)
        with patch("db_utils.toast_utils.requests.get", side_effect=requests.Timeout()) as get, \
             patch("db_utils.toast_utils.time.sleep"):
            with self.assertRaises(requests.RequestException):
                self.client.get_restaurant(UUID(int=1))
            self.assertEqual(get.call_count, 3)


@unittest.skipUnless(os.environ.get("TOAST_TEST_PG_SOCKET"), "Requires isolated temporary PostgreSQL")
class RestaurantPostgresTests(unittest.TestCase):
    def test_repeat_upsert_pagination_counts_nulls_and_rollback(self):
        socket = os.environ["TOAST_TEST_PG_SOCKET"]
        self.assertTrue(socket.startswith("/tmp/toast-restaurants-pg-"))
        connect = psycopg2.connect

        def test_connection(*args, **kwargs):
            return connect(host=socket, port=55440, dbname="postgres", user="toast_test")

        connection = test_connection()
        self.addCleanup(connection.close)
        with connection.cursor() as cur:
            cur.execute(Path("db_utils/schema/toast_restaurants.sql").read_text())
            cur.execute(Path("db_utils/schema/toast_restaurants.sql").read_text())
        connection.commit()
        rows = [module.normalize_restaurant(restaurant(i), UUID(int=i)) for i in range(1, 103)]
        with patch("db_utils.dbconnect.psycopg2.connect", side_effect=test_connection):
            self.assertEqual(module.upsert_restaurants(rows), {"inserted": 102, "updated": 0})
            rows[0].update(name="Renamed", archived=True, first_business_date=None)
            self.assertEqual(module.upsert_restaurants(rows), {"inserted": 0, "updated": 102})
            # A failure on page two must roll back changes already made by page one.
            rows[0]["name"] = "Must roll back"
            rows[-1]["closeout_hour"] = "not-an-integer"
            with self.assertRaises(psycopg2.Error):
                module.upsert_restaurants(rows)
        with connection.cursor() as cur:
            cur.execute("SELECT name, archived, first_business_date FROM toast.restaurants WHERE id = %s", (UUID(int=1),))
            self.assertEqual(cur.fetchone(), ("Renamed", True, None))
            cur.execute("SELECT count(*) FROM toast.restaurants")
            self.assertEqual(cur.fetchone()[0], 102)
            cur.execute("SELECT count(*) FROM pg_constraint WHERE conrelid = 'toast.restaurants'::regclass AND contype = 'f'")
            self.assertEqual(cur.fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
