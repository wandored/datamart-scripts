import runpy
import unittest
from datetime import datetime
from unittest.mock import Mock, patch
from uuid import UUID

import test_r365_invoice_sync as invoice_tests
from db_utils.r365_importers import get_employees, get_jobs
from db_utils.r365_utils import R365Client

module = invoice_tests.module


def employee(pos_id=2):
    return {
        "employeeId": str(UUID(int=1)),
        "posEmployeeId": str(UUID(int=pos_id)) if pos_id else None,
        "payRate": 18.5, "paySchedule": "Hourly", "payrollId": "0012",
        "hireDate": "2025-01-01T00:00:00-05:00", "terminationDate": None,
        "primaryLocation": {"id": str(UUID(int=3))},
        "primaryJob": {"id": str(UUID(int=4))}, "inactive": False,
        "otherLocations": [{"id": str(UUID(int=5))}, {"id": str(UUID(int=6))}],
        "otherJobs": [],
    }


def job():
    return {
        "id": str(UUID(int=4)), "name": "Server", "code": "01",
        "department": "Front", "payRate": 0.0,
        "excludeFromSchedule": False, "excludeFromPOSImport": True,
        "location": {"id": str(UUID(int=3))},
        "generalLedgerAccount": {"id": str(UUID(int=7))},
        "responsibilities": [{"responsibility": "Serve guests"}],
    }


def user():
    return {
        "id": str(UUID(int=8)), "defaultLocation": {"id": str(UUID(int=3))},
        "inactive": False, "allLocationsAccess": True,
        "canGrantAccessBeyondPersonalLevel": False,
        "locations": [{"id": str(UUID(int=3))}, {"id": str(UUID(int=5))}],
        "userRoles": [{"id": str(UUID(int=9))}], "reportRoles": [],
    }


class LaborUsersSyncTests(unittest.TestCase):
    setUp = invoice_tests.InvoiceSyncTests.setUp

    def test_employees_paginate_and_keep_multiple_pos_mappings(self):
        self.client.request.side_effect = [
            {"data": [employee()], "nextLink": "/public/v1/labor/employees?continuationToken=abc"},
            {"data": [employee(10), employee()]},
        ]
        module.sync_employees(self.client, [UUID(int=3), UUID(int=5)], "2000-01-01", "2026-09-28")
        self.assertEqual(self.client.request.call_args_list[0].args, ("GET", "/v1/labor/employees"))
        self.assertEqual(self.client.request.call_args_list[0].kwargs["params"], {
            "locations": f"{UUID(int=3)},{UUID(int=5)}", "modifiedOnStart": "2000-01-01",
            "modifiedOnEnd": "2026-09-28", "pageSize": 250,
        })
        self.assertEqual(self.client.request.call_args_list[1].args,
                         ("GET", "/public/v1/labor/employees?continuationToken=abc"))
        employees, mappings = self.execute.call_args_list
        self.assertIn('INSERT INTO "r365"."employees"', employees.args[1])
        self.assertIn('ON CONFLICT ("id")', employees.args[1])
        self.assertEqual(employees.args[2], [(
            UUID(int=1), 18.5, "Hourly", "2025-01-01T00:00:00-05:00", None,
            "0012", UUID(int=3), UUID(int=4), False, [UUID(int=5), UUID(int=6)], [],
        )])
        self.assertIn("ON CONFLICT (pos_employee_id)", mappings.args[1])
        self.assertIn("employee_id = EXCLUDED.employee_id", mappings.args[1])
        self.assertEqual(mappings.args[2], [(UUID(int=2), UUID(int=1)), (UUID(int=10), UUID(int=1))])
        self.connect.assert_called_once()
        self.connection.commit.assert_called_once()

    def test_employee_without_pos_mapping_is_retained(self):
        row = employee(None)
        row.update(primaryLocation=None, primaryJob=None, otherLocations=None, otherJobs=None)
        self.client.request.return_value = {"data": [row]}
        module.sync_employees(self.client, [UUID(int=3)])
        self.execute.assert_called_once()
        self.assertEqual(self.execute.call_args.args[2][0][6:], (None, None, False, None, None))
        self.assertIn("Skipped 1 mappings", self.output.getvalue())

    def test_employee_map_failure_rolls_back_both_tables(self):
        self.client.request.return_value = {"data": [employee()]}
        self.execute.side_effect = [None, RuntimeError("mapping write failed")]
        with self.assertRaisesRegex(RuntimeError, "mapping write failed"):
            module.sync_employees(self.client, [UUID(int=3)])
        self.connection.rollback.assert_called_once()
        self.connection.commit.assert_not_called()
        self.assertNotIn("Upserted", self.output.getvalue())

    def test_conflicting_duplicates_fail_before_writing(self):
        changed_employee = dict(employee(), payRate=20)
        changed_mapping = dict(employee(), employeeId=str(UUID(int=20)))
        for changed in (changed_employee, changed_mapping):
            with self.subTest(changed=changed), self.assertRaisesRegex(ValueError, "Conflicting"):
                self.client.request.return_value = {"data": [employee(), changed]}
                module.sync_employees(self.client, [UUID(int=3)])
        self.connect.assert_not_called()

    def test_jobs_columns_dates_and_pagination(self):
        self.client.request.side_effect = [
            {"data": [job()], "nextLink": "/next"}, {"data": []},
        ]
        module.sync_jobs(self.client, "2026-09-01", "2026-09-28")
        first = self.client.request.call_args_list[0]
        self.assertEqual(first.args, ("GET", "/v1/labor/jobs"))
        self.assertEqual(first.kwargs["params"], {
            "modifiedOnStart": "2026-09-01", "modifiedOnEnd": "2026-09-28", "pageSize": 250,
        })
        self.assertEqual(self.client.request.call_args_list[1].args, ("GET", "/next"))
        query, rows = self.execute.call_args.args[1:]
        self.assertIn('INSERT INTO "r365"."jobs"', query)
        self.assertNotIn("responsibilit", query)
        self.assertEqual(rows, [(UUID(int=4), "Server", "01", "Front", 0.0, False, True,
                                 UUID(int=3), UUID(int=7))])

    def test_users_arrays_next_page_and_report_access_default(self):
        other = dict(user(), id=str(UUID(int=10)), defaultLocation=None,
                     locations=None, userRoles=[], reportRoles=[{"id": str(UUID(int=11))}])
        self.client.request.side_effect = [
            {"entities": [user()], "nextPage": "https://example.test/public/v1/user-management/users?continuationToken=abc"},
            {"entities": [other]},
        ]
        module.sync_users(self.client)
        first = self.client.request.call_args_list[0]
        self.assertEqual(first.args, ("GET", "/v1/user-management/users"))
        self.assertEqual(first.kwargs["params"], {"pageSize": 250})
        self.assertEqual(self.client.request.call_count, 2)
        self.assertNotIn("next_link_key", first.kwargs["params"])
        query, rows = self.execute.call_args.args[1:]
        self.assertIn('INSERT INTO "r365"."users"', query)
        self.assertNotIn("all_reports_access", query)
        self.assertEqual(rows, [
            (UUID(int=8), UUID(int=3), False, True, False, [UUID(int=3), UUID(int=5)], [UUID(int=9)], []),
            (UUID(int=10), None, False, True, False, None, [], [UUID(int=11)]),
        ])

    def test_empty_results_do_not_connect(self):
        self.client.request.return_value = {"data": [], "entities": []}
        module.sync_employees(self.client, [UUID(int=3)])
        module.sync_jobs(self.client)
        module.sync_users(self.client)
        self.connect.assert_not_called()
        self.assertEqual(len(module.r365_employees([]).columns), 11)
        self.assertEqual(len(module.r365_employee_map([]).columns), 2)
        self.assertEqual(len(module.r365_jobs(self.client).columns), 9)
        self.assertEqual(len(module.r365_users(self.client).columns), 8)

    def test_invalid_data_fails_before_writing(self):
        for field, value in (("employeeId", None), ("posEmployeeId", "bad"),
                             ("inactive", "false"), ("otherJobs", [{"id": None}]),
                             ("otherLocations", [{"id": "bad"}])):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.client.request.return_value = {"data": [dict(employee(), **{field: value})]}
                module.sync_employees(self.client, [UUID(int=3)])
        for field in ("id", "excludeFromSchedule", "excludeFromPOSImport"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.client.request.return_value = {"data": [dict(job(), **{field: None})]}
                module.sync_jobs(self.client)
        for field in ("id", "inactive", "allLocationsAccess", "canGrantAccessBeyondPersonalLevel"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.client.request.return_value = {"entities": [dict(user(), **{field: None})]}
                module.sync_users(self.client)
        self.connect.assert_not_called()

    def test_second_page_failure_prevents_writes(self):
        for sync, payload, args in (
            (module.sync_employees, {"data": [employee()], "nextLink": "/next"}, [[UUID(int=3)]]),
            (module.sync_jobs, {"data": [job()], "nextLink": "/next"}, []),
            (module.sync_users, {"entities": [user()], "nextPage": "/next"}, []),
        ):
            self.client.request.side_effect = [payload, RuntimeError("fetch failed")]
            with self.subTest(sync=sync.__name__), self.assertRaisesRegex(RuntimeError, "fetch failed"):
                sync(self.client, *args)
        self.connect.assert_not_called()

    def test_date_defaults_and_required_scope(self):
        with patch.object(module, "datetime") as clock:
            clock.now.return_value = datetime(2026, 9, 28, 15)
            self.assertEqual(module.labor_date_range(), ("2026-09-28", "2026-09-28"))
        self.assertEqual(module.labor_date_range("2026-09-01"), ("2026-09-01", "2026-09-01"))
        self.assertEqual(module.labor_date_range(modified_on_end="2026-09-02"), ("2026-09-02", "2026-09-02"))
        with self.assertRaises(ValueError):
            module.sync_jobs(self.client, "2026-09-28", "2026-09-01")
        with self.assertRaises(ValueError):
            get_jobs(self.client)
        with self.assertRaises(ValueError):
            get_employees(self.client, [], "2026-09-28")
        module.sync_employees(self.client, [])
        self.client.request.assert_not_called()
        self.connect.assert_not_called()

    def test_cli_runs_only_selected_resources_with_historical_range(self):
        self.client.request.side_effect = [
            {"data": [job()]}, {"items": [{"id": str(UUID(int=3))}]},
            {"data": [employee()]}, {"entities": [user()]},
        ]
        with patch("sys.argv", [
            "r365-api-update", "--sync", "employees", "jobs", "users",
            "--modified-on-start", "2000-01-01", "--modified-on-end", "2026-09-28",
        ]), patch("db_utils.r365_utils.R365Client", return_value=self.client):
            with self.assertRaises(SystemExit) as result:
                runpy.run_path(module.__file__, run_name="__main__")
        self.assertEqual(result.exception.code, 0)
        self.assertEqual([call.args[1] for call in self.client.request.call_args_list], [
            "/v1/labor/jobs", "/v1/core/locations", "/v1/labor/employees", "/v1/user-management/users",
        ])
        self.assertEqual(self.client.request.call_args_list[2].kwargs["params"]["modifiedOnEnd"], "2026-09-28")
        self.assertEqual(self.execute.call_count, 4)


class ClientPaginationTests(unittest.TestCase):
    def test_continuation_urls_do_not_duplicate_public_prefix(self):
        client = object.__new__(R365Client)
        client.base_url = "https://example.test/public"
        client.session = Mock()
        for endpoint, expected in (
            ("/v1/labor/jobs", "https://example.test/public/v1/labor/jobs"),
            ("/public/v1/labor/jobs?continuationToken=abc", "https://example.test/public/v1/labor/jobs?continuationToken=abc"),
            ("https://example.test/public/v1/labor/jobs?continuationToken=abc", "https://example.test/public/v1/labor/jobs?continuationToken=abc"),
        ):
            with self.subTest(endpoint=endpoint):
                client.request("GET", endpoint)
                self.assertEqual(client.session.request.call_args.kwargs["url"], expected)


if __name__ == "__main__":
    unittest.main()
