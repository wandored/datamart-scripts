import io
import sqlite3
import unittest
from contextlib import ExitStack, redirect_stderr
from datetime import date
from unittest.mock import Mock, patch

from test_r365_invoice_sync import module
from db_utils.r365_importers import get_invoices, get_transactions, get_vendor_invoices
from db_utils.r365_utils import R365Client


class SyncCliTests(unittest.TestCase):
    def calendar(self, rows):
        db = Mock()
        db.fetchall.return_value = rows
        connection = self.enterContext(patch.object(module, "DatabaseConnection"))
        connection.return_value.__enter__.return_value = db
        return db

    def test_fiscal_periods_preserve_gaps_and_calendar_year_boundaries(self):
        db = self.calendar([
            {"date": date(2025, 12, 31), "period": 1},
            {"date": date(2026, 1, 1), "period": 1},
            {"date": date(2026, 3, 1), "period": 3},
        ])
        self.assertEqual(module.fiscal_date_ranges(2026, [1, 3, 1]), [
            (date(2025, 12, 31), date(2026, 1, 1)),
            (date(2026, 3, 1), date(2026, 3, 1)),
        ])
        self.assertIn("core.calendar", db.execute.call_args.args[0])
        self.assertEqual(db.execute.call_args.args[1], (2026, [1, 3]))

    def test_week_uses_all_three_fiscal_values(self):
        db = self.calendar([{"date": date(2026, 9, 1), "period": 9}])
        module.fiscal_date_ranges(2026, [9], 5)
        self.assertIn("AND week = %s", db.execute.call_args.args[0])
        self.assertEqual(db.execute.call_args.args[1], (2026, [9], 5))

    def test_missing_selection_fails_before_api(self):
        self.calendar([{"date": date(2026, 9, 1), "period": 9}])
        with patch.object(module, "R365Client") as client, redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                module.main(["bulk", "--year", "2026", "--period", "9", "10",
                             "--tables", "daily_sales"])
        self.assertEqual(error.exception.code, 2)
        client.assert_not_called()

    def test_daily_week_on_day_one_and_midweek(self):
        for today, first, last in (
            (date(2026, 1, 5), date(2025, 12, 29), date(2026, 1, 4)),
            (date(2026, 1, 8), date(2026, 1, 5), date(2026, 1, 7)),
        ):
            with self.subTest(today=today), patch.object(module, "date") as dates:
                dates.today.return_value = today
                rows = [{"date": date.fromordinal(day)} for day in
                        range(first.toordinal(), last.toordinal() + 1)]
                db = self.calendar(rows)
                self.assertEqual(module.daily_fiscal_date_range(), (first, last))
                self.assertEqual(db.execute.call_args.args[1], (last, last))
                self.assertIn("SELECT year, period, week", db.execute.call_args.args[0])

    def test_daily_sql_keeps_repeated_week_numbers_in_their_fiscal_period(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.row_factory = sqlite3.Row
        connection.execute("ATTACH DATABASE ':memory:' AS core")
        connection.execute("CREATE TABLE core.calendar (date TEXT, year INT, period INT, week INT)")
        # Actual 2026 calendar boundaries, including the fifth week of period 9.
        for year, period, week, first in (
            (2025, 13, 4, date(2025, 12, 17)),
            (2026, 1, 1, date(2025, 12, 24)),
            (2026, 9, 4, date(2026, 8, 26)),
            (2026, 9, 5, date(2026, 9, 2)),
            (2026, 10, 1, date(2026, 9, 9)),
            (2026, 10, 4, date(2026, 9, 30)),
            (2026, 11, 1, date(2026, 10, 7)),
        ):
            connection.executemany("INSERT INTO core.calendar VALUES (?, ?, ?, ?)", [
                (date.fromordinal(first.toordinal() + day).isoformat(), year, period, week)
                for day in range(7)
            ])
        cursor = connection.cursor()
        db = Mock()
        db.execute.side_effect = lambda query, params: cursor.execute(
            query.replace("%s", "?"), tuple(str(value) for value in params))
        db.fetchall.side_effect = cursor.fetchall
        with patch.object(module, "DatabaseConnection") as factory, \
                patch.object(module, "date") as clock:
            factory.return_value.__enter__.return_value = db
            for today, first, last in (
                (date(2026, 10, 7), date(2026, 9, 30), date(2026, 10, 6)),
                (date(2026, 10, 9), date(2026, 10, 7), date(2026, 10, 8)),
                (date(2026, 9, 9), date(2026, 9, 2), date(2026, 9, 8)),
                (date(2026, 9, 11), date(2026, 9, 9), date(2026, 9, 10)),
                (date(2025, 12, 25), date(2025, 12, 24), date(2025, 12, 24)),
            ):
                with self.subTest(today=today):
                    clock.today.return_value = today
                    self.assertEqual(module.daily_fiscal_date_range(), (first, last))

    def test_daily_missing_calendar_and_gap_fail(self):
        for rows in ([], [{"date": date(2026, 1, 5)}, {"date": date(2026, 1, 7)}]):
            with self.subTest(rows=rows), patch.object(module, "date") as dates:
                dates.today.return_value = date(2026, 1, 8)
                self.calendar(rows)
                with self.assertRaises(ValueError):
                    module.daily_fiscal_date_range()

    def test_invalid_cli_fails_before_database_or_api(self):
        cases = [[], ["daily", "--year", "2026"], ["weekly", "--week", "1"],
                 ["bulk", "--year", "2026", "--period", "9"],
                 ["bulk", "--year", "2026", "--period", "0", "--tables", "daily_sales"],
                 ["bulk", "--year", "2026", "--period", "9", "--tables", "jobs"],
                 ["bulk", "--year", "2026", "--period", "9", "--tables", "invoices"],
                 ["bulk", "--year", "2026", "--period", "9", "10", "--week", "5",
                  "--tables", "daily_sales"]]
        with patch.object(module, "R365Client") as client, \
                patch.object(module, "DatabaseConnection") as db, redirect_stderr(io.StringIO()):
            for args in cases:
                with self.subTest(args=args), self.assertRaises(SystemExit) as error:
                    module.main(args)
                self.assertEqual(error.exception.code, 2)
        client.assert_not_called()
        db.assert_not_called()

    def test_bulk_dispatches_only_requested_tables_for_each_range(self):
        ranges = [(date(2026, 7, 1), date(2026, 7, 7)),
                  (date(2026, 9, 1), date(2026, 9, 7))]
        with patch.object(module, "fiscal_date_ranges", return_value=ranges), \
                patch.object(module, "R365Client") as client, \
                patch.object(module, "sync_daily_tables") as sync, \
                patch.object(module, "sync_weekly_tables") as weekly:
            module.main(["bulk", "--year", "2026", "--period", "7", "9",
                         "--tables", "daily_sales", "vendor_invoices"])
        self.assertEqual(sync.call_count, 2)
        for call, (start, end) in zip(sync.call_args_list, ranges):
            self.assertEqual(call.args, (client.return_value, start, end, ["daily_sales", "vendor_invoices"]))
        weekly.assert_not_called()

    def test_daily_dispatches_all_tables_using_fiscal_range(self):
        bounds = (date(2026, 1, 5), date(2026, 1, 7))
        with patch.object(module, "daily_fiscal_date_range", return_value=bounds), \
                patch.object(module, "R365Client") as client, \
                patch.object(module, "sync_daily_tables") as sync:
            module.main(["daily"])
        sync.assert_called_once_with(client.return_value, *bounds, module.DAILY_TABLES)

    def test_selected_tables_do_not_fetch_or_write_other_resources(self):
        names = ["sync_daily_sales", "r365_inventory_counts", "r365_transactions",
                 "sync_invoices", "sync_vendor_invoices", "get_locations", "write_to_db"]
        with ExitStack() as stack:
            mocks = {name: stack.enter_context(patch.object(module, name)) for name in names}
            module.sync_daily_tables("client", "2026-01-01", "2026-01-07",
                                     ["daily_sales", "vendor_invoices", "daily_sales"])
            mocks["sync_daily_sales"].assert_called_once_with(
                "client", business_date_start="2026-01-01", business_date_end="2026-01-07")
            mocks["sync_vendor_invoices"].assert_called_once_with("client", "2026-01-01", "2026-01-07")
            for name in names[1:4] + names[5:]:
                mocks[name].assert_not_called()

    def test_weekly_full_reference_refresh_excludes_daily(self):
        names = ["r365_locations", "r365_units_of_measure", "r365_item_categories",
                 "r365_gl_accounts", "r365_purchase_items", "r365_vendors",
                 "sync_vendor_items", "sync_jobs", "sync_employees", "sync_users",
                 "write_to_db", "sync_daily_tables", "DatabaseConnection", "R365Client"]
        with ExitStack() as stack:
            mocks = {name: stack.enter_context(patch.object(module, name)) for name in names}
            mocks["r365_locations"].return_value = module.pd.DataFrame({"id": ["location"]})
            module.main(["weekly"])
            client = mocks["R365Client"].return_value
            mocks["sync_jobs"].assert_called_once_with(client, "0001-01-01", date.today().isoformat())
            mocks["sync_employees"].assert_called_once_with(
                client, ["location"], "0001-01-01", date.today().isoformat())
            mocks["sync_vendor_items"].assert_called_once_with(client, full_download=True)
            mocks["sync_users"].assert_called_once_with(client)
            self.assertEqual([call.args[1] for call in mocks["write_to_db"].call_args_list],
                             ["locations", "units_of_measure", "item_categories",
                              "gl_accounts", "purchase_items", "vendors"])
            mocks["sync_daily_tables"].assert_not_called()
            mocks["DatabaseConnection"].assert_not_called()

    def test_record_date_filters_reach_api_without_modification_constraints(self):
        client = object.__new__(R365Client)
        client.request = Mock(return_value={"transactions": [], "invoices": [], "items": []})
        for fetch, kwargs, keys in (
            (get_transactions, {"location_id": "location", "business_date_start": "2026-01-01",
                                "business_date_end": "2026-01-07"},
             ("dateOfBusinessStart", "dateOfBusinessEnd")),
            (get_invoices, {"document_date_start": "2026-01-01", "document_date_end": "2026-01-07"},
             ("DateStart", "DateEnd")),
            (get_vendor_invoices, {"date_start": "2026-01-01", "date_end": "2026-01-07"},
             ("dateStart", "dateEnd")),
        ):
            with self.subTest(fetch=fetch.__name__):
                fetch(client, **kwargs)
                params = client.request.call_args.kwargs["params"]
                self.assertEqual(params[keys[0]], "2026-01-01")
                self.assertEqual(params[keys[1]], "2026-01-07")
                self.assertIsNone(params.get("modifiedOnStart"))
                self.assertIsNone(params.get("modifiedOnEnd"))

    def test_dated_group_passes_calendar_dates_and_handles_empty_responses(self):
        client = object.__new__(R365Client)
        client.request = Mock(side_effect=[
            {"items": []}, {"items": [{"id": "location"}]},
            {"transactions": []}, {"items": []},
        ])
        start, end = date(2026, 1, 1), date(2026, 1, 7)
        with patch.object(module, "DatabaseConnection") as db:
            module.sync_daily_tables(client, start, end, [
                "inventory_counts", "transactions", "vendor_invoices",
            ])
        db.assert_not_called()
        calls = client.request.call_args_list
        for index, start_key, end_key in (
            (0, "dateOfBusinessStart", "dateOfBusinessEnd"),
            (2, "dateOfBusinessStart", "dateOfBusinessEnd"),
            (3, "dateStart", "dateEnd"),
        ):
            params = calls[index].kwargs["params"]
            self.assertEqual(str(params[start_key]), str(start))
            self.assertEqual(str(params[end_key]), str(end))
            self.assertIsNone(params.get("modifiedOnStart"))
        self.assertEqual(len(calls), 4)
        self.assertNotIn("/v1/accounting/accounts-payable/invoices",
                         [call.args[1] for call in calls])


if __name__ == "__main__":
    unittest.main()
