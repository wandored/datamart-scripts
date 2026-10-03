"""Synthetic Labor fixtures; live systems are never used by default."""

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
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

from src.toast_sync import labor

dispatcher = importlib.import_module("src.toast-api-update")
DAY = date(2026, 10, 1)
RESTAURANT = UUID(int=40000)
STAMP = datetime(2026, 10, 2, tzinfo=timezone.utc)
METADATA = {"timezone": "America/New_York", "closeout_hour": 4}


def guid(number):
    return str(UUID(int=number))


def employee():
    return {"guid": guid(40001), "externalId": "payroll:00042", "externalEmployeeId": "00042",
            "firstName": "Alex", "chosenName": "Al", "lastName": "Example", "email": "example@example.invalid",
            "phoneNumber": "0000000000", "phoneNumberCountryCode": "1", "v2EmployeeGuid": guid(40002),
            "createdDate": "2026-01-01T12:00:00Z", "modifiedDate": "2026-10-01T12:00:00+0000",
            "deleted": False, "passcode": "do-not-persist",
            "jobReferences": [{"guid": guid(40003), "externalId": "jobs:01"}, {"guid": guid(40004)}],
            "wageOverrides": [{"jobReference": {"guid": guid(40003)}, "wage": Decimal("17.123456789")} ]}


def time_entry():
    return {"guid": guid(40005), "employeeReference": {"guid": guid(40001)},
            "jobReference": {"guid": guid(40003)}, "shiftReference": {"guid": guid(40006)},
            "businessDate": "20261001", "inDate": "2026-10-02T01:00:00-0400",
            "outDate": "2026-10-02T03:00:00-0400", "regularHours": Decimal("1.875"),
            "overtimeHours": Decimal("0.125"), "hourlyWage": Decimal("17.123456789"),
            "autoClockedOut": False, "nonCashTips": Decimal("30.015"), "cashSales": 0,
            "deleted": False, "modifiedDate": "2026-10-02T07:00:00Z"}


def shift():
    return {"guid": guid(40006), "employeeReference": {"guid": guid(40001)},
            "jobReference": {"guid": guid(40003)}, "inDate": "2026-10-02T01:00:00-04:00",
            "outDate": "2026-10-02T05:00:00-04:00", "deleted": False,
            "scheduleConfig": {"guid": guid(40007), "minBeforeClockIn": 5,
                               "minAfterClockIn": Decimal("7.5"), "minBeforeClockOut": 3, "minAfterClockOut": 10}}


class LaborNormalizationTests(unittest.TestCase):
    def test_employee_and_relations_preserve_values_exclude_passcode(self):
        tables = labor.normalize_batch("employees", [employee()], RESTAURANT, STAMP)
        row = tables["employees"][0]
        self.assertEqual(row["external_employee_id"], "00042")
        self.assertEqual(row["chosen_name"], "Al")
        self.assertEqual(row["v2_employee_guid"], UUID(int=40002))
        self.assertNotIn("passcode", row)
        self.assertEqual(len(tables["employee_jobs"]), 2)
        self.assertEqual(tables["employee_wage_overrides"][0]["wage"], Decimal("17.123456789"))
        for rows in tables.values():
            for item in rows:
                self.assertEqual(item["restaurant_id"], RESTAURANT)
                self.assertEqual(item["last_synced_at"], STAMP)

    def test_job_salary_and_source_name(self):
        row = labor.normalize_job({"guid": guid(40003), "title": "ABC - Kitchen", "code": "001",
                                   "defaultWage": Decimal("52000.125"), "wageFrequency": "SALARY",
                                   "tipped": False, "excludeFromReporting": True}, RESTAURANT)
        self.assertEqual(row["title"], "ABC - Kitchen")
        self.assertEqual(row["code"], "001")
        self.assertEqual(row["wage_frequency"], "SALARY")
        self.assertEqual(row["default_wage"], Decimal("52000.125"))
        self.assertTrue(row["exclude_from_reporting"])

    def test_clocked_entry_uses_source_business_date_hours_and_wage(self):
        row = labor.normalize_time_entry(time_entry(), RESTAURANT, STAMP)
        self.assertEqual(row["business_date"], DAY)
        self.assertNotEqual(row["business_date"], row["in_date"].date())
        self.assertEqual(row["regular_hours"], Decimal("1.875"))
        self.assertEqual(row["overtime_hours"], Decimal("0.125"))
        self.assertEqual(row["hourly_wage"], Decimal("17.123456789"))
        self.assertIsNone(row["declared_cash_tips"])
        self.assertEqual(row["cash_sales"], Decimal(0))
        self.assertNotIn("regular_pay", row)

    def test_open_entry_and_old_documented_shift_reference(self):
        payload = time_entry()
        payload.update(outDate=None, hourlyWage=None, shiftReference=guid(40006))
        row = labor.normalize_time_entry(payload, RESTAURANT)
        self.assertIsNone(row["out_date"])
        self.assertIsNone(row["hourly_wage"])
        self.assertEqual(row["shift_id"], UUID(int=40006))

    def test_edited_entry_retains_identity(self):
        payload = time_entry()
        first = labor.normalize_time_entry(payload, RESTAURANT, STAMP)
        payload.update(regularHours=Decimal("1.75"), modifiedDate="2026-10-03T12:00:00Z")
        later = labor.normalize_time_entry(payload, RESTAURANT, STAMP + timedelta(days=1))
        self.assertEqual(first["id"], later["id"])
        self.assertNotEqual(first["regular_hours"], later["regular_hours"])
        self.assertGreater(later["modified_date"], first["modified_date"])

    def test_breaks_are_excluded_without_blocking_time_entries(self):
        expected = labor.normalize_batch("time_entries", [time_entry()], RESTAURANT, STAMP)
        for breaks in (None, [], [{"paid": True, "inDate": "2026-10-02T06:00:00Z"}]):
            with self.subTest(breaks=breaks):
                payload = {**time_entry(), "breaks": breaks}
                self.assertEqual(labor.normalize_batch("time_entries", [payload], RESTAURANT, STAMP), expected)

    def test_deleted_state_on_all_four_resources(self):
        for resource in labor.ENDPOINTS:
            with self.subTest(resource=resource):
                rows = labor.normalize_batch(resource, [{"guid": guid(40010), "deleted": True,
                                                        "deletedDate": "2026-10-02T12:00:00Z"}], RESTAURANT)[resource]
                self.assertTrue(rows[0]["deleted"])
                self.assertEqual(rows[0]["deleted_date"].tzinfo, timezone.utc)

    def test_shift_source_timestamps_and_schedule_config(self):
        row = labor.normalize_shift(shift(), RESTAURANT)
        self.assertEqual(row["schedule_config_id"], UUID(int=40007))
        self.assertEqual(row["min_after_clock_in"], Decimal("7.5"))
        self.assertNotIn("business_date", row)
        self.assertNotIn("status", row)
        self.assertNotIn("scheduled_hours", row)

    def test_optional_nulls_and_column_contract(self):
        for resource in labor.ENDPOINTS:
            with self.subTest(resource=resource):
                tables = labor.normalize_batch(resource, [{"guid": guid(40010)}], RESTAURANT, STAMP)
                row = tables[resource][0]
                self.assertEqual(set(row), set(labor.TABLE_COLUMNS[resource]))
                self.assertTrue(all(value is None for key, value in row.items()
                                    if key not in ("id", "restaurant_id", "last_synced_at")))

    def test_naive_timestamps_invalid_dates_numbers_and_refs_reject_safely(self):
        for field, value in (("inDate", "2026-10-01T04:00:00"), ("businessDate", 20261001),
                             ("businessDate", "20260230"), ("regularHours", True),
                             ("hourlyWage", float("nan")), ("employeeReference", "bad")):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, guid(40005)):
                labor.normalize_batch("time_entries", [{**time_entry(), field: value}], RESTAURANT)
        with self.assertRaisesRegex(ValueError, "jobReferences"):
            labor.normalize_batch("employees", [{**employee(), "jobReferences": [{"guid": "bad"}]}], RESTAURANT)

    def test_duplicate_keys_deduplicate_or_reject_conflict(self):
        tables = labor.normalize_batch("employees", [employee(), employee()], RESTAURANT)
        self.assertEqual(len(tables["employees"]), 1)
        with self.assertRaisesRegex(ValueError, "Conflicting employees"):
            labor.normalize_batch("employees", [employee(), {**employee(), "chosenName": "Changed"}], RESTAURANT)
        payload = employee()
        payload["wageOverrides"].append({"jobReference": {"guid": guid(40003)}, "wage": 10})
        with self.assertRaisesRegex(ValueError, "Conflicting employee_wage_overrides"):
            labor.normalize_batch("employees", [payload], RESTAURANT)


class LaborWindowTests(unittest.TestCase):
    def test_dst_and_restaurant_timezone(self):
        start, end = labor.business_window(date(2026, 3, 7), METADATA)
        self.assertEqual(end - start, timedelta(hours=23))
        self.assertEqual(start.hour, 9)
        self.assertEqual(end.hour, 8)
        start, end = labor.business_window(date(2026, 10, 31), METADATA)
        self.assertEqual(end - start, timedelta(hours=25))
        other = labor.business_window(DAY, {"timezone": "America/Chicago", "closeout_hour": 4})
        self.assertEqual(other[0].hour, 9)

    def test_missing_invalid_or_ambiguous_metadata_fails(self):
        for metadata, day in (({}, DAY), ({**METADATA, "timezone": "bad/zone"}, DAY),
                              ({**METADATA, "closeout_hour": None}, DAY),
                              ({**METADATA, "closeout_hour": 2}, date(2026, 3, 8)),
                              ({**METADATA, "closeout_hour": 1}, date(2026, 11, 1))):
            with self.subTest(metadata=metadata), self.assertRaises(ValueError):
                labor.business_window(day, metadata)

    def test_time_windows_include_archived_without_business_date_parameter(self):
        windows = list(labor.request_windows("time_entries", METADATA, business_date_start=DAY, business_date_end=DAY+timedelta(days=1)))
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0][0]["endDate"], windows[1][0]["startDate"])
        self.assertNotIn("businessDate", windows[0][0])
        self.assertEqual(windows[0][0]["includeArchived"], "true")
        self.assertNotIn("includeMissedBreaks", windows[0][0])

    def test_modified_chunks_are_contiguous_and_under_month(self):
        windows = list(labor.request_windows("time_entries", modified_start=STAMP, modified_end=STAMP+timedelta(days=70)))
        self.assertEqual(len(windows), 3)
        for index, (params, _, _) in enumerate(windows):
            self.assertNotIn("startDate", params)
            self.assertNotIn("includeMissedBreaks", params)
            start = datetime.fromisoformat(params["modifiedStartDate"])
            end = datetime.fromisoformat(params["modifiedEndDate"])
            self.assertLessEqual(end-start, labor.MAX_WINDOW)
            if index:
                self.assertEqual(params["modifiedStartDate"], windows[index-1][0]["modifiedEndDate"])
        self.assertEqual(end, STAMP+timedelta(days=70))

    def test_shift_lookahead_and_local_half_open_filter(self):
        params, window, _ = next(labor.request_windows("shifts", METADATA, business_date_start=DAY, business_date_end=DAY))
        self.assertEqual(datetime.fromisoformat(params["endDate"])-window[0], labor.MAX_WINDOW)
        items = [shift(), {**shift(), "inDate": window[0].isoformat()},
                 {**shift(), "inDate": window[1].isoformat()}]
        selected = labor.select_shifts(items, window)
        self.assertEqual(len(selected), 2)
        self.assertGreater(datetime.fromisoformat(selected[0]["outDate"]), window[1])
        with self.assertRaisesRegex(ValueError, "shift="):
            labor.select_shifts([{**shift(), "inDate": None}], window)


class LaborSyncTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.locations = [{"toast_guid": RESTAURANT}, {"toast_guid": UUID(int=40999)}]
        self.writer = self.enterContext(patch.object(labor, "upsert_labor_tables", side_effect=lambda tables: {
            table: {"inserted": len(rows), "updated": 0} for table, rows in tables.items()}))
        self.metadata = self.enterContext(patch.object(labor, "load_restaurant_metadata", return_value={str(RESTAURANT): METADATA, guid(40999): METADATA}))
        self.enterContext(patch.object(labor.logger, "info"))
        self.error = self.enterContext(patch.object(labor.logger, "error"))

    def test_fetch_uses_existing_client_decimal_decoder_and_no_pagination(self):
        response = requests.Response()
        response.status_code = 200
        response._content = b'[{"hourlyWage":17.1234567890123456789}]'
        self.client.request.return_value = response
        result = labor.fetch_time_entries(self.client, str(RESTAURANT), {"startDate": "start"})
        self.assertEqual(result[0]["hourlyWage"], Decimal("17.1234567890123456789"))
        self.client.request.assert_called_once_with("/labor/v1/timeEntries", str(RESTAURANT), params={"startDate": "start"})
        response._content = b'{"unexpected": []}'
        with self.assertRaisesRegex(ValueError, "array"):
            labor.fetch_employees(self.client, str(RESTAURANT))

    def test_refs_need_no_metadata_and_are_written_atomically(self):
        self.client.request.return_value.json.return_value = [employee()]
        counts = labor.sync_labor(self.client, self.locations[:1], resources=("employees",))
        self.metadata.assert_not_called()
        self.assertEqual(counts["tables"]["employees"]["inserted"], 1)
        self.assertEqual(counts["tables"]["employee_jobs"]["inserted"], 2)
        self.assertEqual(set(self.writer.call_args.args[0]), {"employees", "employee_jobs", "employee_wage_overrides"})

    def test_api_failure_logs_http_and_continues_next_restaurant(self):
        response = requests.Response()
        response.status_code = 403
        self.client.request.side_effect = [requests.HTTPError(response=response), Mock(json=Mock(return_value=[]))]
        counts = labor.sync_labor(self.client, self.locations, resources=("jobs",))
        self.assertEqual(counts["api_errors"], 1)
        self.assertEqual(counts["restaurants_processed"], 1)
        self.assertEqual(self.client.request.call_count, 2)
        self.assertIn("/labor/v1/jobs", self.error.call_args.args[1])
        self.assertIn(str(RESTAURANT), self.error.call_args.args[1])
        self.assertEqual(self.error.call_args.args[-1], 403)

    def test_malformed_nested_record_rejects_endpoint_batch(self):
        self.client.request.return_value.json.return_value = [employee(), {**employee(), "wageOverrides": "bad"}]
        counts = labor.sync_labor(self.client, self.locations[:1], resources=("employees",))
        self.assertEqual(counts["rejected"], 2)
        self.writer.assert_not_called()
        self.assertNotIn("do-not-persist", str(self.error.call_args))
        self.assertIn(guid(40001), str(self.error.call_args))

    def test_database_failure_continues_and_does_not_count_inserts(self):
        self.client.request.return_value.json.return_value = [time_entry()]
        self.writer.side_effect = [psycopg2.OperationalError(), {"time_entries": {"inserted": 1, "updated": 0}}]
        counts = labor.sync_labor(self.client, self.locations, resources=("time_entries",), business_date_start=DAY, business_date_end=DAY)
        self.assertEqual(counts["database_errors"], 1)
        self.assertEqual(counts["tables"]["time_entries"]["inserted"], 1)
        self.assertIn("business_date=2026-10-01", self.error.call_args.args[1])

    def test_dry_run_validates_and_never_writes(self):
        self.client.request.return_value.json.return_value = [time_entry()]
        counts = labor.sync_labor(self.client, self.locations[:1], dry_run=True, resources=("time_entries",), business_date_start=DAY, business_date_end=DAY)
        self.assertEqual(counts["tables"]["time_entries"]["selected"], 1)
        self.assertEqual(counts["tables"]["time_entries"]["inserted"], 0)
        self.writer.assert_not_called()

    def test_missing_metadata_fails_facts_but_keeps_reference_sync(self):
        self.metadata.return_value = {}
        self.client.request.return_value.json.return_value = []
        counts = labor.sync_labor(self.client, self.locations[:1], resources=("jobs", "time_entries"), business_date_start=DAY, business_date_end=DAY)
        self.assertEqual(counts["rejected"], 1)
        self.client.request.assert_called_once_with("/labor/v1/jobs", str(RESTAURANT), params=None)

    def test_modified_retrieval_needs_no_restaurant_metadata(self):
        self.client.request.return_value.json.return_value = []
        counts = labor.sync_labor(self.client, self.locations[:1], resources=("time_entries",), modified_start=STAMP, modified_end=STAMP+timedelta(days=1))
        self.metadata.assert_not_called()
        self.assertEqual(counts["api_errors"], 0)


class LaborDispatcherTests(unittest.TestCase):
    def setUp(self):
        self.locations = self.enterContext(patch.object(dispatcher, "get_configured_restaurants", return_value=[{"toast_guid": RESTAURANT}]))
        self.client = self.enterContext(patch.object(dispatcher, "ToastClient"))
        self.sync = self.enterContext(patch.object(labor, "sync", return_value={"api_errors": 0, "rejected": 0}))
        self.orders = self.enterContext(patch.object(dispatcher.orders, "sync", return_value={"api_errors": 0, "rejected": 0}))
        self.enterContext(patch.object(dispatcher.logger, "info"))

    def test_default_labor_and_shared_dates_with_orders(self):
        self.assertEqual(dispatcher.main(["--sync", "orders", "labor", "--business-date", "2026-10-01", "--dry-run"]), 0)
        self.assertEqual(self.sync.call_args.kwargs["business_date_start"], DAY)
        self.assertEqual(self.sync.call_args.kwargs["resources"], tuple(labor.ENDPOINTS))
        self.assertNotIn("resources", self.orders.call_args.kwargs)

    def test_reference_only_and_modified_only_options(self):
        self.assertEqual(dispatcher.main(["--sync", "labor", "--labor-resources", "employees", "jobs"]), 0)
        self.assertEqual(self.sync.call_args.kwargs["resources"], ("employees", "jobs"))
        self.assertEqual(dispatcher.main(["--sync", "labor", "--labor-resources", "time-entries", "--labor-modified-start", "2026-10-01T00:00:00Z", "--labor-modified-end", "2026-10-02T00:00:00Z"]), 0)
        self.assertEqual(self.sync.call_args.kwargs["modified_end"], STAMP)

    def test_invalid_options_fail_before_external_calls(self):
        for args in (["--sync", "labor"], ["--labor-resources", "jobs"],
                     ["--sync", "labor", "--labor-resources", "time-entries", "--labor-modified-start", "2026-10-01T00:00:00Z"],
                     ["--sync", "labor", "--labor-resources", "time-entries", "--labor-modified-start", "2026-10-01T00:00:00", "--labor-modified-end", "2026-10-02T00:00:00Z"]):
            with self.subTest(args=args), patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit) as exc:
                dispatcher.main(args)
            self.assertEqual(exc.exception.code, 2)
        self.locations.assert_not_called()
        self.client.assert_not_called()


@unittest.skipUnless(os.environ.get("TOAST_TEST_PG_SOCKET"), "Requires isolated temporary PostgreSQL")
class LaborPostgresTests(unittest.TestCase):
    def test_schema_repeat_upsert_composite_keys_precision_and_atomic_rollback(self):
        socket = os.environ["TOAST_TEST_PG_SOCKET"]
        self.assertTrue(socket.startswith("/tmp/toast-restaurants-pg-"))
        connect = psycopg2.connect

        def test_connection(*args, **kwargs):
            return connect(host=socket, port=55440, dbname="postgres", user="toast_test")

        connection = test_connection()
        self.addCleanup(connection.close)
        with connection.cursor() as cur:
            cur.execute(Path("db_utils/schema/toast_labor.sql").read_text())
            cur.execute(Path("db_utils/schema/toast_labor.sql").read_text())
        connection.commit()
        tables = labor.normalize_batch("employees", [employee()], RESTAURANT, STAMP)
        for resource, fixture in (("jobs", {"guid": guid(40003), "defaultWage": Decimal("17.123456789")}), ("time_entries", time_entry()), ("shifts", shift())):
            tables.update(labor.normalize_batch(resource, [fixture], RESTAURANT, STAMP))
        with patch("db_utils.dbconnect.psycopg2.connect", side_effect=test_connection):
            first = labor.upsert_labor_tables(tables)
            self.assertEqual({t: r["inserted"] for t, r in first.items()}, {t: len(r) for t, r in tables.items()})
            for rows in tables.values():
                for row in rows:
                    row["last_synced_at"] = STAMP+timedelta(days=1)
            tables["time_entries"][0].update(deleted=True, out_date=None, regular_hours=Decimal("1.8123456789"))
            second = labor.upsert_labor_tables(tables)
            self.assertEqual({t: r["updated"] for t, r in second.items()}, {t: len(r) for t, r in tables.items()})
            bad = deepcopy(tables)
            bad["employees"][0]["chosen_name"] = "must roll back"
            bad["employee_wage_overrides"][0]["wage"] = "invalid-numeric"
            with self.assertRaises(psycopg2.Error):
                labor.upsert_labor_tables(bad)
            # Disappearing assignments are retained; their older timestamp exposes staleness.
            refresh = labor.normalize_batch("employees", [{**employee(), "jobReferences": [], "wageOverrides": []}], RESTAURANT, STAMP+timedelta(days=2))
            labor.upsert_labor_tables(refresh)
        with connection.cursor() as cur:
            cur.execute("SELECT chosen_name, last_synced_at FROM toast.employees WHERE id=%s", (UUID(int=40001),))
            self.assertEqual(cur.fetchone(), ("Al", STAMP+timedelta(days=2)))
            cur.execute("SELECT wage, last_synced_at FROM toast.employee_wage_overrides WHERE employee_id=%s", (UUID(int=40001),))
            self.assertEqual(cur.fetchone(), (Decimal("17.123456789"), STAMP+timedelta(days=1)))
            cur.execute("SELECT regular_hours, deleted, out_date FROM toast.time_entries WHERE id=%s", (UUID(int=40005),))
            self.assertEqual(cur.fetchone(), (Decimal("1.8123456789"), True, None))
            cur.execute("SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='toast' AND c.contype='f'")
            self.assertEqual(cur.fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
