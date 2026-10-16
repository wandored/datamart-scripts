"""Offline CLI dispatch and shared fiscal-week regression tests."""
import importlib
import io
import sqlite3
import unittest
from contextlib import ExitStack, redirect_stderr
from datetime import date, timedelta
from unittest.mock import MagicMock, Mock, patch

from db_utils import fiscal_calendar


toast = importlib.import_module("src.toast-api-update")
r365 = importlib.import_module("src.r365-api-update")
BOUNDS = (date(2026, 9, 30), date(2026, 10, 6))
RANGES = [BOUNDS, (date(2026, 11, 4), date(2026, 11, 10))]


class ToastModesTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.object(toast, "get_configured_restaurants", return_value=[{"toast_guid": "test"}]))
        self.client = self.enterContext(patch.object(toast, "ToastClient"))
        self.syncs = {name: self.enterContext(patch.object(module, "sync", return_value={"api_errors": 0, "rejected": 0}))
                      for name, module in toast.SYNC_MODULES.items()}
        self.daily = self.enterContext(patch.object(fiscal_calendar, "daily_date_range", return_value=BOUNDS))
        self.weekly = self.enterContext(patch.object(fiscal_calendar, "weekly_fiscal_date_range", return_value=BOUNDS))
        self.period = self.enterContext(patch.object(fiscal_calendar, "fiscal_date_ranges", return_value=RANGES))

    def test_all_modes_default_to_all_endpoints(self):
        for args, count in [(["--daily"], 1), (["--weekly"], 1), (["--period", "10", "12", "--year", "2026"], 2)]:
            with self.subTest(args=args):
                for sync in self.syncs.values():
                    sync.reset_mock()
                self.assertEqual(toast.main([*args, "--dry-run"]), 0)
                self.syncs["restaurants"].assert_called_once()
                self.syncs["configuration"].assert_called_once()
                self.assertEqual(self.syncs["orders"].call_count, count)
                self.assertEqual(self.syncs["labor"].call_count, count)
                first = self.syncs["labor"].call_args_list[0].kwargs
                self.assertEqual(first["resources"], tuple(toast.labor.ENDPOINTS))
                self.assertTrue(first["dry_run"])
                self.assertEqual(first["business_date_start"], BOUNDS[0])
                if count == 2:
                    last = self.syncs["labor"].call_args.kwargs
                    self.assertEqual(last["resources"], ("time_entries", "shifts"))
                    self.assertEqual(last["business_date_start"], RANGES[1][0])
        self.daily.assert_called_once_with(None)
        self.weekly.assert_called_once()
        self.period.assert_called_once_with(2026, [10, 12], None)

    def test_selected_endpoint_and_labor_selection(self):
        self.assertEqual(toast.main(["--weekly", "--tables", "labor", "labor", "--labor-resources", "jobs"]), 0)
        self.syncs["labor"].assert_called_once()
        self.assertEqual(self.syncs["labor"].call_args.kwargs["resources"], ("jobs",))
        self.syncs["orders"].assert_not_called()
        self.syncs["restaurants"].assert_not_called()

    def test_configuration_cutoff_preserved_and_runs_once_across_periods(self):
        self.assertEqual(toast.main([
            "--period", "10", "12", "--year", "2026", "--sync", "configuration",
            "--config-resources", "menu_items", "--config-last-modified", "2026-10-01T00:00:00Z",
            "--dry-run",
        ]), 0)
        self.syncs["configuration"].assert_called_once()
        options = self.syncs["configuration"].call_args.kwargs
        self.assertEqual(options["resources"], ("menu_items",))
        self.assertEqual(options["last_modified"].isoformat(), "2026-10-01T00:00:00+00:00")
        self.assertTrue(options["dry_run"])
        self.syncs["orders"].assert_not_called()
        self.syncs["labor"].assert_not_called()

    def test_all_alias_and_single_period_week(self):
        toast.main(["--period", "10", "--year", "2026", "--week", "2", "--sync", "all"])
        self.period.assert_called_once_with(2026, [10], 2)
        for sync in self.syncs.values():
            self.assertTrue(sync.called)

    def test_errors_continue_to_later_ranges_and_endpoints(self):
        self.syncs["orders"].side_effect = [RuntimeError(), {"api_errors": 0, "rejected": 0}]
        self.assertEqual(toast.main(["--period", "10", "12", "--year", "2026"]), 1)
        self.assertEqual(self.syncs["orders"].call_count, 2)
        self.assertEqual(self.syncs["labor"].call_count, 2)

    def test_invalid_combinations_fail_before_external_calls(self):
        cases = [["--daily", "--weekly"], ["--period", "10"], ["--year", "2026"],
                 ["--weekly", "--business-date", "2026-10-01"],
                 ["--daily", "--labor-modified-start", "2026-10-01T00:00:00Z"],
                 ["--period", "1", "2", "--year", "2026", "--week", "1"],
                 ["--daily", "--sync", "orders", "--labor-resources", "jobs"]]
        for args in cases:
            with self.subTest(args=args), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                toast.main(args)
        self.client.assert_not_called()
        self.daily.assert_not_called()
        self.weekly.assert_not_called()
        self.period.assert_not_called()


class R365ModesTests(unittest.TestCase):
    def test_daily_defaults_to_all_and_accepts_selected_endpoints(self):
        for selection in [[], ["--sync", "all"], ["--tables", "users", "daily_sales", "users"]]:
            with self.subTest(selection=selection), ExitStack() as stack:
                stack.enter_context(patch.object(fiscal_calendar, "daily_date_range", return_value=(BOUNDS[0], BOUNDS[0])))
                client = stack.enter_context(patch.object(r365, "R365Client"))
                stack.enter_context(patch.object(r365, "get_locations", return_value=[]))
                sync = stack.enter_context(patch.object(r365, "sync_modified_tables"))
                r365.main(["--daily", *selection])
                selected = "users" in selection
                sync.assert_called_once_with(client.return_value, BOUNDS[0],
                    ["users", "daily_sales"] if selected else [*r365.REFERENCE_TABLES, *r365.DAILY_TABLES],
                    None if selected else [])

    def test_reference_selection_does_not_fetch_unselected_endpoints(self):
        with patch.object(r365, "sync_users") as users, patch.object(r365, "r365_locations") as locations, \
                patch.object(r365, "write_to_db") as write, patch.object(r365, "sync_jobs") as jobs:
            r365.sync_weekly_tables("client", ["users"])
        users.assert_called_once_with("client")
        locations.assert_not_called()
        write.assert_not_called()
        jobs.assert_not_called()


class DailyDateTests(unittest.TestCase):
    def test_defaults_and_explicit_dates_reach_both_dispatchers(self):
        for module in (toast, r365):
            for today, args, expected in (
                (date(2026, 10, 10), ["--daily"], date(2026, 10, 9)),
                (date(2026, 1, 1), ["--daily"], date(2025, 12, 31)),
                (date(2026, 10, 10), ["--daily", "2026-09-15"], date(2026, 9, 15)),
            ):
                with self.subTest(module=module.__name__, args=args, today=today), ExitStack() as stack:
                    clock = stack.enter_context(patch.object(fiscal_calendar, "date"))
                    clock.today.return_value = today
                    week = stack.enter_context(patch.object(fiscal_calendar, "fiscal_week_to_date"))
                    periods = stack.enter_context(patch.object(fiscal_calendar, "fiscal_date_ranges"))
                    if module is toast:
                        stack.enter_context(patch.object(toast, "ToastClient"))
                        stack.enter_context(patch.object(toast, "get_configured_restaurants", return_value=[{}]))
                        sync = stack.enter_context(patch.object(toast.orders, "sync", return_value={"api_errors": 0, "rejected": 0}))
                        toast.main([*args, "--sync", "orders"])
                        self.assertEqual(sync.call_args.kwargs["business_date_start"], expected)
                        self.assertEqual(sync.call_args.kwargs["business_date_end"], expected)
                    else:
                        stack.enter_context(patch.object(r365, "R365Client"))
                        sync = stack.enter_context(patch.object(r365, "sync_modified_tables"))
                        r365.main([*args, "--tables", "daily_sales"])
                        self.assertEqual(sync.call_args.args[1], expected)
                    week.assert_not_called()
                    periods.assert_not_called()

    def test_invalid_daily_dates_fail_before_initialization(self):
        for module, client_name in ((toast, "ToastClient"), (r365, "R365Client")):
            for day in ("2026-02-30", "not-a-date"):
                with self.subTest(module=module.__name__, day=day), patch.object(module, client_name) as client, \
                        redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    module.main(["--daily", day])
                self.assertEqual(error.exception.code, 2)
                client.assert_not_called()


class FiscalWeeklyTests(unittest.TestCase):
    def test_completed_week_across_period_and_year_boundaries(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.row_factory = sqlite3.Row
        connection.execute("ATTACH DATABASE ':memory:' AS core")
        connection.execute("CREATE TABLE core.calendar (date TEXT, year INT, period INT, week INT)")
        for first, year, period, week in [(date(2025, 12, 17), 2025, 13, 4),
                                          (date(2025, 12, 24), 2026, 1, 1),
                                          (date(2026, 9, 2), 2026, 9, 5),
                                          (date(2026, 9, 9), 2026, 10, 1)]:
            connection.executemany("INSERT INTO core.calendar VALUES (?, ?, ?, ?)", [
                ((first + timedelta(days=i)).isoformat(), year, period, week) for i in range(7)])
        cursor = connection.cursor()
        db = Mock()
        db.execute.side_effect = lambda query, params: cursor.execute(query.replace("%s", "?"), tuple(str(p) for p in params))
        db.fetchall.side_effect = cursor.fetchall
        factory = MagicMock()
        factory.return_value.__enter__.return_value = db
        for today, start in [(date(2025, 12, 24), date(2025, 12, 17)),
                             (date(2025, 12, 27), date(2025, 12, 17)),
                             (date(2026, 9, 9), date(2026, 9, 2)),
                             (date(2026, 9, 13), date(2026, 9, 2))]:
            with self.subTest(today=today):
                self.assertEqual(fiscal_calendar.weekly_fiscal_date_range(today=today, connection_factory=factory),
                                 (start, start + timedelta(days=6)))
        with self.assertRaises(ValueError):
            fiscal_calendar.weekly_fiscal_date_range(today=date(2026, 10, 1), connection_factory=factory)


if __name__ == "__main__":
    unittest.main()
