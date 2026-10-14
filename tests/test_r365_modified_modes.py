"""Offline regression tests for modification windows and fiscal bulk imports."""
import importlib
import io
import unittest
from contextlib import ExitStack, redirect_stderr
from datetime import date, timedelta
from unittest.mock import Mock, patch

from db_utils import fiscal_calendar, r365_importers

module = importlib.import_module("src.r365-api-update")


class ModifiedModesTests(unittest.TestCase):
    def test_week_uses_calendar_week_containing_yesterday_and_requests_each_day(self):
        for today, first, last in [
            (date(2026, 10, 7), date(2026, 9, 30), date(2026, 10, 6)),
            (date(2026, 10, 10), date(2026, 10, 7), date(2026, 10, 9)),
            (date(2026, 1, 1), date(2025, 12, 31), date(2025, 12, 31)),
        ]:
            with self.subTest(today=today), ExitStack() as stack:
                clock = stack.enter_context(patch.object(module, "date"))
                clock.today.return_value = today
                calendar = stack.enter_context(patch.object(fiscal_calendar, "fiscal_week_to_date", return_value=(first, last)))
                client = stack.enter_context(patch.object(module, "R365Client"))
                sync = stack.enter_context(patch.object(module, "sync_modified_tables"))
                module.main(["--week", "--tables", "users"])
                calendar.assert_called_once_with(today=today, connection_factory=module.DatabaseConnection)
                expected = [first + timedelta(days=i) for i in range((last - first).days + 1)]
                self.assertEqual([call.args[1] for call in sync.call_args_list], expected)
                self.assertTrue(all(call.args[0] is client.return_value for call in sync.call_args_list))

    def test_all_supported_endpoints_receive_full_day_modification_filters(self):
        client = Mock()
        client.get_resource.return_value = []
        client.request.return_value = {"transactions": [], "nextLink": None}
        with patch.object(module, "write_to_db", return_value=0), patch.object(module, "DatabaseConnection") as db, \
                patch.object(module, "sync_daily_sales") as sales:
            module.sync_modified_tables(client, date(2026, 10, 6),
                                        [*module.REFERENCE_TABLES, *module.DAILY_TABLES], ["location"])
        resources = {call.args[1]: call.kwargs for call in client.get_resource.call_args_list
                     if call.args[1] != "locations" or call.kwargs.get("modifiedOnStart")}
        self.assertEqual(set(resources), {"locations", "units-of-measure", "item-categories", "gl-accounts",
                                         "items", "vendors", "vendor-items", "jobs", "employees", "users",
                                         "inventory-counts", "invoices", "labor-punches"})
        for name, params in resources.items():
            with self.subTest(endpoint=name):
                normalized = {key.lower(): value for key, value in params.items()}
                if name in ("jobs", "employees"):
                    self.assertEqual(normalized["modifiedonstart"], "2026-10-06")
                    self.assertEqual(normalized["modifiedonend"], "2026-10-06")
                else:
                    self.assertTrue(normalized["modifiedonstart"].startswith("2026-10-06T00:00:00"))
                    self.assertTrue(normalized["modifiedonend"].startswith("2026-10-06T23:59:59.999999"))
                self.assertFalse(params.get("dateOfBusinessStart"))
                self.assertFalse(params.get("dateStart"))
        params = client.request.call_args.kwargs["params"]
        self.assertTrue(params["modifiedOnStart"].startswith("2026-10-06T00:00:00"))
        self.assertTrue(params["modifiedOnEnd"].startswith("2026-10-06T23:59:59.999999"))
        self.assertNotIn("dateOfBusinessStart", params)
        sales.assert_called_once_with(client, business_date_start=date(2026, 10, 6), business_date_end=date(2026, 10, 6))

    def test_bulk_defaults_and_sales_aliases_never_dispatch_reference_sync(self):
        for selection, expected in [([], list(module.BULK_TABLES)),
                                    (["--tables", "sales_details", "sales_tickets"], ["daily_sales"]),
                                    (["--tables", "inventory_counts"], ["inventory_counts"])]:
            with self.subTest(selection=selection), patch.object(module, "R365Client") as client, \
                    patch.object(module, "sync_daily_tables") as bulk, \
                    patch.object(module, "sync_modified_tables") as delta, \
                    patch.object(module, "get_locations") as locations:
                module.main(["--bulk", "--business-date", "2026-10-06", *selection])
                bulk.assert_called_once_with(client.return_value, date(2026, 10, 6), date(2026, 10, 6), expected)
                delta.assert_not_called()
                locations.assert_not_called()

    def test_bulk_fiscal_selectors_and_whole_year(self):
        for args, expected in [(["-y", "2026"], (2026, None, None)),
                               (["-y", "2026", "-p", "10", "-w", "2"], (2026, [10], 2))]:
            with self.subTest(args=args), patch.object(module, "fiscal_date_ranges", return_value=[(date(2026, 10, 1), date(2026, 10, 2))]) as calendar, \
                    patch.object(module, "R365Client"), patch.object(module, "sync_daily_tables") as sync:
                module.main(["--bulk", *args])
                calendar.assert_called_once_with(*expected)
                self.assertEqual(sync.call_count, 2)

    def test_invalid_selections_fail_before_external_access(self):
        cases = [["--bulk"], ["--bulk", "-y", "2026", "--tables", "users"],
                 ["--bulk", "-y", "2026", "-w", "2"],
                 ["--bulk", "--business-date", "2026-10-06", "-y", "2026"],
                 ["--daily", "--business-date", "2026-10-06"], ["--week", "-y", "2026"]]
        for args in cases:
            with self.subTest(args=args), patch.object(module, "R365Client") as client, \
                    patch.object(module, "DatabaseConnection") as db, redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                try:
                    module.main(args)
                finally:
                    client.assert_not_called()
                    db.assert_not_called()

    def test_year_calendar_query_and_empty_year(self):
        with patch.object(module, "DatabaseConnection") as factory:
            db = factory.return_value.__enter__.return_value
            db.fetchall.return_value = [{"date": date(2025, 12, 31), "period": 1}]
            self.assertEqual(module.fiscal_date_ranges(2026, None), [(date(2025, 12, 31), date(2025, 12, 31))])
            self.assertEqual(db.execute.call_args.args[1], (2026,))
            self.assertNotIn("ANY", db.execute.call_args.args[0])
            db.fetchall.return_value = []
            with self.assertRaises(ValueError):
                module.fiscal_date_ranges(2026, None)


if __name__ == "__main__":
    unittest.main()
