import os
import time
import unittest
from datetime import datetime
from unittest.mock import patch
from uuid import UUID

import test_r365_invoice_sync as invoice_tests
from src.r365_sync import vendor_items


def item():
    return {
        "id": str(UUID(int=1)), "name": "Vendor item", "vendor": {"id": str(UUID(int=2))},
        "number": "VI-1", "brandItemNumber": "BR-1", "isPrimary": False,
        "item": {"id": str(UUID(int=3))}, "purchaseUnitOfMeasure": {"id": str(UUID(int=4))},
        "splitUnitOfMeasure": {"id": str(UUID(int=5))}, "vendorPackSizeDescription": "6 pack",
        "price": 0.0, "splitPrice": 1.25, "createdOn": "2020-01-01T12:00:00-05:00",
        "modifiedOn": "2026-09-27T12:00:00-04:00",
    }


class VendorItemsSyncTests(unittest.TestCase):
    setUp = invoice_tests.InvoiceSyncTests.setUp

    def test_full_download_omits_dates_and_daily_run_restores_them(self):
        self.client.request.side_effect = [
            {"items": [item()], "nextLink": "/next"},
            {"items": [{"id": str(UUID(int=6)), "isPrimary": True}]},
            {"items": []},
        ]
        vendor_items.sync_vendor_items(self.client, full_download=True)
        self.assertEqual(self.client.request.call_args_list[0].kwargs["params"], {"pageSize": 250})
        self.assertEqual(self.client.request.call_args_list[1].args, ("GET", "/next"))
        self.assertEqual(len(self.execute.call_args.args[2]), 2)
        vendor_items.sync_vendor_items(self.client)
        params = self.client.request.call_args.kwargs["params"]
        self.assertIn("modifiedOnStart", params)
        self.assertIn("modifiedOnEnd", params)

    def test_pagination_mapping_and_upsert(self):
        self.client.request.side_effect = [
            {"items": [item()], "nextLink": "/next"},
            {"items": [{"id": str(UUID(int=6)), "isPrimary": True}]},
        ]
        vendor_items.sync_vendor_items(self.client)
        self.assertEqual(self.client.request.call_count, 2)
        self.assertEqual(self.client.request.call_args_list[0].args, ("GET", "/v1/inventory/vendor-items"))
        self.assertEqual(self.client.request.call_args_list[1].args, ("GET", "/next"))
        self.assertEqual(self.client.request.call_args_list[0].kwargs["params"]["pageSize"], 250)
        self.execute.assert_called_once()
        query, rows = self.execute.call_args.args[1:]
        self.assertIn('INSERT INTO "r365"."vendor_items"', query)
        self.assertIn('ON CONFLICT ("id")', query)
        self.assertIn('"is_primary" = EXCLUDED."is_primary"', query)
        self.assertEqual(rows[0], (
            UUID(int=1), "Vendor item", UUID(int=2), "VI-1", "BR-1", False,
            UUID(int=3), UUID(int=4), UUID(int=5), "6 pack", 0.0, 1.25,
            "2020-01-01T12:00:00-05:00", "2026-09-27T12:00:00-04:00",
        ))
        self.assertEqual(rows[1], (UUID(int=6), None, None, None, None, True) + (None,) * 8)
        self.connect.assert_called_once()
        self.connection.commit.assert_called_once()

    def test_empty_results_skip_connection(self):
        for payload in ({"items": []}, {"items": None}, {}):
            self.client.request.return_value = payload
            vendor_items.sync_vendor_items(self.client)
            self.assertEqual(len(vendor_items.r365_vendor_items(self.client).columns), 14)
        self.connect.assert_not_called()

    def test_required_id_and_boolean(self):
        for row in ({"isPrimary": True}, {"id": None, "isPrimary": False},
                    {"id": "bad", "isPrimary": True},
                    *({"id": str(UUID(int=1)), "isPrimary": value} for value in (None, "false", 0))):
            self.client.request.return_value = {"items": [row]}
            with self.subTest(row=row), self.assertRaises(ValueError):
                vendor_items.sync_vendor_items(self.client)
        self.connect.assert_not_called()

    def test_local_day_window_handles_dst(self):
        try:
            with patch.dict(os.environ, {"TZ": "America/New_York"}):
                time.tzset()
                self.client.request.return_value = {"items": []}
                with patch.object(vendor_items, "datetime") as clock:
                    clock.now.return_value = datetime(2026, 11, 1, 15)
                    vendor_items.sync_vendor_items(self.client)
                    clock.now.assert_called_once_with()
                self.assertEqual(self.client.request.call_args.kwargs["params"], {
                    "modifiedOnStart": "2026-11-01T00:00:00-04:00",
                    "modifiedOnEnd": "2026-11-01T23:59:59.999999-05:00", "pageSize": 250,
                })
        finally:
            time.tzset()

    def test_write_failure_rolls_back(self):
        self.client.request.return_value = {"items": [item()]}
        self.execute.side_effect = RuntimeError("write failed")
        with self.assertRaisesRegex(RuntimeError, "write failed"):
            vendor_items.sync_vendor_items(self.client)
        self.connection.rollback.assert_called_once()
        self.connection.commit.assert_not_called()
        self.assertNotIn("Upserted", self.output.getvalue())

    def test_api_failure_does_not_write(self):
        self.client.request.side_effect = RuntimeError("fetch failed")
        with self.assertRaisesRegex(RuntimeError, "fetch failed"):
            vendor_items.sync_vendor_items(self.client)
        self.connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
