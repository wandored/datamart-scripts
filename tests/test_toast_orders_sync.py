"""Orders tests use synthetic API data; optional PostgreSQL stays under /tmp."""

from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal
import importlib
import io
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
from uuid import UUID

import psycopg2
import requests

from db_utils.toast_utils import ToastClient
from src.toast_sync import orders

dispatcher = importlib.import_module("src.toast-api-update")
BUSINESS_DATE = date(2026, 9, 30)
RESTAURANT_ID = UUID(int=9000)


def order(number=1):
    return {
        "guid": str(UUID(int=number)), "businessDate": 20260930,
        "externalId": "example:1", "displayNumber": "001", "source": "In Store",
        "approvalStatus": "APPROVED", "numberOfGuests": 2, "calculatedGuestCount": 1.25,
        "openedDate": "2026-10-01T01:30:00.000-0400",
        "modifiedDate": "2026-10-01T02:00:00.000-0400",
        "deletedDate": "1970-01-01T00:00:00.000+0000", "voidBusinessDate": 0,
        "deleted": False, "voided": False, "createdInTestMode": False,
        "server": {"guid": str(UUID(int=8000))}, "diningOption": None,
        "createdDevice": {"id": "non-uuid-device-id"}, "pricingFeatures": ["TAXESV2"],
        "checks": [{"guid": str(UUID(int=7000 + number)), "selections": [], "payments": []}],
    }


def table_counts(inserted):
    return {table: {"inserted": inserted if table == "orders" else 0, "updated": 0}
            for table in orders.TABLE_COLUMNS}


class OrderNormalizationTests(unittest.TestCase):
    def test_business_date_uuid_offsets_nulls_and_no_children(self):
        row = orders.normalize_order(order(), RESTAURANT_ID, BUSINESS_DATE)
        self.assertEqual(set(row), set(orders.ORDER_COLUMNS))
        self.assertEqual(row["id"], UUID(int=1))
        self.assertEqual(row["restaurant_id"], RESTAURANT_ID)
        self.assertEqual(row["server_id"], UUID(int=8000))
        self.assertEqual(row["business_date"], BUSINESS_DATE)
        self.assertNotEqual(row["business_date"], row["opened_date"].date())
        self.assertEqual(row["opened_date"].astimezone(timezone.utc), datetime(2026, 10, 1, 5, 30, tzinfo=timezone.utc))
        self.assertEqual(row["calculated_guest_count"], Decimal("1.25"))
        self.assertEqual(row["display_number"], "001")
        self.assertEqual(row["created_device_id"], "non-uuid-device-id")
        self.assertEqual(row["deleted_date"].year, 1970)
        self.assertIsNone(row["dining_option_id"])
        self.assertIsNone(row["void_business_date"])
        self.assertNotIn("checks", row)

    def test_sparse_header_and_void_date(self):
        row = orders.normalize_order({"guid": str(UUID(int=1)), "businessDate": 20260930}, RESTAURANT_ID, BUSINESS_DATE)
        self.assertTrue(all(row[column] is None for column in orders.ORDER_COLUMNS[3:] if column != "last_synced_at"))
        payload = order()
        payload.update(voided=True, deleted=True, voidBusinessDate=20261001)
        row = orders.normalize_order(payload, RESTAURANT_ID, BUSINESS_DATE)
        self.assertTrue(row["voided"])
        self.assertTrue(row["deleted"])
        self.assertEqual(row["void_business_date"], date(2026, 10, 1))

    def test_malformed_header_fields_are_rejected(self):
        for field, value in (
            ("guid", None), ("guid", "bad"), ("businessDate", 20260929),
            ("businessDate", 20260230), ("businessDate", None),
            ("openedDate", "2026-10-01T01:30:00"), ("openedDate", "bad"),
            ("voided", "false"), ("numberOfGuests", True), ("server", []),
            ("server", {"guid": "bad"}), ("source", []), ("createdDevice", {"id": 1}),
            ("calculatedGuestCount", float("nan")), ("voidBusinessDate", False),
            ("pricingFeatures", "TAXESV2"),
        ):
            payload = order()
            payload[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                orders.normalize_order(payload, RESTAURANT_ID, BUSINESS_DATE)


class OrdersSyncTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.locations = [{"id": 1, "toast_guid": RESTAURANT_ID}]
        self.write = self.enterContext(patch.object(orders, "upsert_order_tables", return_value=table_counts(1)))
        self.enterContext(patch.object(orders.logger, "info"))
        self.enterContext(patch.object(orders.logger, "error"))

    def sync(self, **kwargs):
        return orders.sync_orders(self.client, self.locations, business_date_start=BUSINESS_DATE,
                                  business_date_end=kwargs.pop("end", BUSINESS_DATE), **kwargs)

    def test_all_dates_and_locations_and_partial_errors(self):
        next_day = deepcopy(order(2))
        next_day["businessDate"] = 20261001
        self.locations.append({"id": 2, "toast_guid": UUID(int=9001)})
        self.client.get_paged_response_data.side_effect = [[order()], requests.HTTPError(), [], [next_day]]
        self.write.side_effect = [table_counts(1), table_counts(0), table_counts(1)]
        counts = self.sync(end=date(2026, 10, 1))
        self.assertEqual(counts["requested"], 4)
        self.assertEqual(counts["returned"], 2)
        self.assertEqual(counts["api_errors"], 1)
        self.assertEqual(counts["inserted"], 2)
        calls = self.client.get_paged_response_data.call_args_list
        self.assertEqual([call.kwargs["params"] for call in calls], [
            {"businessDate": "20260930"}, {"businessDate": "20261001"},
            {"businessDate": "20260930"}, {"businessDate": "20261001"},
        ])
        self.assertEqual(self.write.call_args.args[0]["orders"][0]["restaurant_id"], UUID(int=9001))

    def test_deduplicates_identical_rows_and_skips_conflicts(self):
        changed = order(2)
        changed["displayNumber"] = "changed"
        self.client.get_paged_response_data.return_value = [order(), order(), order(2), changed, {}]
        counts = self.sync()
        self.assertEqual(counts["duplicates"], 2)
        self.assertEqual(counts["rejected"], 2)
        self.assertEqual([row["id"] for row in self.write.call_args.args[0]["orders"]], [UUID(int=1)])

    def test_dry_run_validates_without_writing(self):
        self.client.get_paged_response_data.return_value = [order(), {}]
        counts = self.sync(dry_run=True)
        self.assertEqual(counts["rejected"], 1)
        self.assertEqual(counts["inserted"], 0)
        self.write.assert_not_called()

    def test_database_error_not_counted_as_committed_and_next_day_runs(self):
        self.client.get_paged_response_data.side_effect = [[order()], []]
        self.write.side_effect = [psycopg2.OperationalError(), table_counts(0)]
        counts = self.sync(end=date(2026, 10, 1))
        self.assertEqual(counts["database_errors"], 1)
        self.assertEqual(counts["inserted"], 0)
        self.assertEqual(self.write.call_count, 2)

    def test_full_page_without_link_continues_and_later_failure_writes_nothing(self):
        client = object.__new__(ToastClient)
        first_page = Mock(links={})
        first_page.json.return_value = [order(i) for i in range(1, 101)]
        empty_page = Mock(links={})
        empty_page.json.return_value = []
        with patch.object(client, "request", side_effect=[first_page, empty_page]) as request:
            rows = client.get_paged_response_data("/orders/v2/ordersBulk", RESTAURANT_ID, rate_limit_wait=0)
        self.assertEqual(len(rows), 100)
        self.assertEqual(request.call_args.kwargs["params"]["page"], 2)
        self.client = client
        with patch.object(client, "request", side_effect=[first_page, requests.HTTPError()]), \
             patch("db_utils.toast_utils.time.sleep"):
            counts = self.sync()
        self.assertEqual(counts["api_errors"], 1)
        self.write.assert_not_called()

    def test_empty_response_does_not_open_database(self):
        with patch("src.toast_sync.writing.DatabaseConnection") as db:
            from src.toast_sync.writing import upsert_rows
            self.assertEqual(upsert_rows([], "orders", orders.ORDER_COLUMNS), {"inserted": 0, "updated": 0})
            db.assert_not_called()


class OrdersDispatcherTests(unittest.TestCase):
    def setUp(self):
        self.locations = self.enterContext(patch.object(dispatcher, "get_configured_restaurants", return_value=[{"id": 1}]))
        self.client = self.enterContext(patch.object(dispatcher, "ToastClient"))
        self.orders_sync = self.enterContext(patch.object(orders, "sync", return_value={"api_errors": 0, "rejected": 0}))
        self.restaurant_sync = self.enterContext(patch.object(dispatcher.restaurants, "sync", return_value={"api_errors": 0, "rejected": 0}))
        self.enterContext(patch.object(dispatcher.logger, "info"))

    def test_combined_selection_passes_dates_only_to_orders(self):
        self.assertEqual(dispatcher.main(["--sync", "restaurants", "orders", "--business-date", "2026-09-30", "--dry-run"]), 0)
        self.assertEqual(self.orders_sync.call_args.kwargs, {
            "dry_run": True, "business_date_start": BUSINESS_DATE, "business_date_end": BUSINESS_DATE,
        })
        self.assertEqual(self.restaurant_sync.call_args.kwargs, {"dry_run": True})

    def test_inclusive_range_and_single_boundary(self):
        for args, end in ((["--business-date-start", "2026-09-30", "--business-date-end", "2026-10-01"], date(2026, 10, 1)),
                          (["--business-date-end", "2026-09-30"], BUSINESS_DATE)):
            with self.subTest(args=args):
                self.assertEqual(dispatcher.main(["--sync", "orders", *args]), 0)
                self.assertEqual(self.orders_sync.call_args.kwargs["business_date_start"], BUSINESS_DATE)
                self.assertEqual(self.orders_sync.call_args.kwargs["business_date_end"], end)

    def test_invalid_dates_fail_before_external_calls(self):
        for args in (
            ["--sync", "orders"], ["--business-date", "2026-09-30"],
            ["--sync", "orders", "--business-date", "2026-02-30"],
            ["--sync", "orders", "--business-date-start", "2026-10-01", "--business-date-end", "2026-09-30"],
            ["--sync", "orders", "--business-date", "2026-09-30", "--business-date-start", "2026-09-30"],
        ):
            with self.subTest(args=args), patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit) as exc:
                dispatcher.main(args)
            self.assertEqual(exc.exception.code, 2)
        self.locations.assert_not_called()
        self.client.assert_not_called()

    def test_database_errors_produce_failing_exit_status(self):
        self.orders_sync.return_value = {"api_errors": 0, "rejected": 0, "database_errors": 1}
        self.assertEqual(dispatcher.main(["--sync", "orders", "--business-date", "2026-09-30"]), 1)


@unittest.skipUnless(os.environ.get("TOAST_TEST_PG_SOCKET"), "Requires isolated temporary PostgreSQL")
class OrdersPostgresTests(unittest.TestCase):
    def test_upserts_preserve_other_rows_and_rollback_all_pages(self):
        socket = os.environ["TOAST_TEST_PG_SOCKET"]
        self.assertTrue(socket.startswith("/tmp/toast-restaurants-pg-"))
        connect = psycopg2.connect

        def test_connection(*args, **kwargs):
            return connect(host=socket, port=55440, dbname="postgres", user="toast_test")

        connection = test_connection()
        self.addCleanup(connection.close)
        with connection.cursor() as cur:
            cur.execute(Path("db_utils/schema/toast_orders.sql").read_text())
            cur.execute(Path("db_utils/schema/toast_orders.sql").read_text())
        connection.commit()
        rows = [orders.normalize_order(order(i), RESTAURANT_ID, BUSINESS_DATE) for i in range(1, 103)]
        other = orders.normalize_order(order(103), UUID(int=9001), BUSINESS_DATE)
        with patch("db_utils.dbconnect.psycopg2.connect", side_effect=test_connection):
            self.assertEqual(orders.upsert_orders(rows + [other]), {"inserted": 103, "updated": 0})
            self.assertEqual(orders.upsert_orders(rows), {"inserted": 0, "updated": 102})
            changed = deepcopy(rows[0])
            changed.update(deleted=True, voided=True, display_number="changed", paid_date=None)
            new = orders.normalize_order(order(104), RESTAURANT_ID, BUSINESS_DATE)
            self.assertEqual(orders.upsert_orders([changed, new]), {"inserted": 1, "updated": 1})
            rows[0]["display_number"] = "must roll back"
            rows[-1]["number_of_guests"] = "invalid-integer"
            with self.assertRaises(psycopg2.Error):
                orders.upsert_orders(rows)
        with connection.cursor() as cur:
            cur.execute("SELECT display_number, deleted, voided, paid_date, calculated_guest_count FROM toast.orders WHERE id = %s", (UUID(int=1),))
            self.assertEqual(cur.fetchone(), ("changed", True, True, None, Decimal("1.25")))
            cur.execute("SELECT restaurant_id FROM toast.orders WHERE id = %s", (UUID(int=103),))
            self.assertEqual(cur.fetchone()[0], UUID(int=9001))
            cur.execute("SELECT count(*) FROM toast.orders WHERE id = ANY(%s)", ([UUID(int=i) for i in range(1, 105)],))
            self.assertEqual(cur.fetchone()[0], 104)
            cur.execute("SELECT count(*) FROM pg_constraint WHERE conrelid = 'toast.orders'::regclass AND contype = 'f'")
            self.assertEqual(cur.fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
