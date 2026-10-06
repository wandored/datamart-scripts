"""Exercise resource selection without connecting to R365 or PostgreSQL."""

import importlib
import unittest
from unittest.mock import Mock, patch

from src.r365_sync import locations, transactions


module = importlib.import_module("src.r365-api-update")


class DispatcherTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.create_client = self.enterContext(patch.object(module, "R365Client", return_value=self.client))
        self.syncs = {
            name: self.enterContext(patch.object(resource, "sync"))
            for name, resource in module.SYNC_MODULES.items()
        }

    def called_resources(self):
        return [name for name, sync in self.syncs.items() if sync.called]

    def test_default_runs_every_daily_resource_and_no_reference_tables(self):
        self.assertEqual(module.main([]), 0)
        self.assertEqual(self.called_resources(), list(module.DAILY_RESOURCES))
        for name in module.DAILY_RESOURCES:
            self.syncs[name].assert_called_once()

    def test_reference_group_requires_dates_before_initialization(self):
        for args in (["--sync", "reference"], ["--sync", "all", "--modified-on-start", "2026-10-01"]):
            with self.subTest(args=args), patch("sys.stderr"), self.assertRaises(SystemExit) as result:
                module.main(args)
            self.assertEqual(result.exception.code, 2)
        self.create_client.assert_not_called()
        self.assertEqual(self.called_resources(), [])

    def test_reference_group_uses_explicit_job_window(self):
        self.assertEqual(module.main([
            "--sync", "reference", "--modified-on-start", "2026-09-01",
            "--modified-on-end", "2026-10-06",
        ]), 0)
        self.assertEqual(set(self.called_resources()), set(module.REFERENCE_RESOURCES))
        self.syncs["jobs"].assert_called_once_with(
            self.client, modified_on_start="2026-09-01", modified_on_end="2026-10-06",
        )

    def test_all_deduplicates_overlapping_groups_and_explicit_resources(self):
        self.assertEqual(module.main([
            "--sync", "all", "daily", "jobs", "reference", "--modified-on-start", "2026-10-01",
            "--modified-on-end", "2026-10-06",
        ]), 0)
        self.assertEqual(self.called_resources(), list(module.SYNC_MODULES))
        for sync in self.syncs.values():
            sync.assert_called_once()

    def test_explicit_tables_do_not_run_other_groups(self):
        self.assertEqual(module.main(["--sync", "vendors", "purchase_items", "vendors"]), 0)
        self.assertEqual(self.called_resources(), ["vendors", "purchase_items"])
        self.syncs["vendors"].assert_called_once_with(self.client)

    def test_full_vendor_items_legacy_option(self):
        self.assertEqual(module.main(["--full-vendor-items"]), 0)
        self.assertEqual(self.called_resources(), ["vendor_items"])
        self.syncs["vendor_items"].assert_called_once_with(self.client, full_download=True)

    def test_sales_window_is_forwarded_only_to_sales(self):
        self.assertEqual(module.main([
            "--sync", "daily-sales", "users", "--business-date-start", "2026-10-01",
        ]), 0)
        from datetime import date
        self.syncs["daily-sales"].assert_called_once_with(
            self.client, business_date_start=date(2026, 10, 1), business_date_end=date(2026, 10, 1),
        )
        self.syncs["users"].assert_called_once_with(self.client)

    def test_invalid_filters_fail_before_initialization(self):
        for args in (
            ["--sync", "users", "--business-date-start", "2026-10-01"],
            ["--full-vendor-items", "--business-date-start", "2026-10-01"],
            ["--sync", "daily-sales", "--business-date-start", "2026-10-02", "--business-date-end", "2026-10-01"],
            ["--sync", "employees", "--modified-on-start", "2026-10-02", "--modified-on-end", "2026-10-01"],
        ):
            with self.subTest(args=args), patch("sys.stderr"), self.assertRaises(SystemExit) as result:
                module.main(args)
            self.assertEqual(result.exception.code, 2)
        self.create_client.assert_not_called()

    def test_failed_resource_returns_failure_and_continues_independent_resources(self):
        self.syncs["vendors"].side_effect = RuntimeError("private response must not be logged")
        with self.assertLogs(module.logger, level="ERROR") as logs:
            self.assertEqual(module.main(["--sync", "vendors", "users"]), 1)
        self.syncs["users"].assert_called_once_with(self.client)
        self.assertIn("vendors", logs.output[0])
        self.assertNotIn("private response", str(logs.output))

    def test_initialization_failure_runs_no_resources(self):
        self.create_client.side_effect = RuntimeError("private URL")
        with self.assertLogs(module.logger, level="ERROR") as logs:
            self.assertEqual(module.main([]), 1)
        self.assertEqual(self.called_resources(), [])
        self.assertNotIn("private URL", str(logs.output))


class TableEntryPointTests(unittest.TestCase):
    def test_empty_simple_tables_do_not_open_database_connections(self):
        for name in (
            "locations", "units_of_measure", "item_categories", "gl_accounts",
            "purchase_items", "vendors", "inventory_counts", "transactions",
        ):
            with self.subTest(name=name), patch("db_utils.dbconnect.psycopg2.connect") as connect:
                client = Mock()
                client.get_resource.return_value = []
                module.SYNC_MODULES[name].sync(client)
                connect.assert_not_called()

    def test_transaction_entry_point_fetches_location_scope(self):
        client = Mock()
        with patch.object(locations, "get_locations", return_value=[{"id": "location-1"}]), \
                patch.object(transactions, "r365_transactions") as fetch, \
                patch.object(transactions, "write_to_db") as write:
            transactions.sync(client)
        fetch.assert_called_once_with(client, ["location-1"])
        write.assert_called_once_with(fetch.return_value, "transactions", "r365")


if __name__ == "__main__":
    unittest.main()
