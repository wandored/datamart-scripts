import os
import time
import unittest
from datetime import datetime
from unittest.mock import patch
from uuid import UUID

import pandas as pd
import test_r365_invoice_sync as invoice_tests

module = invoice_tests.module


def invoice(number=1):
    return {
        "id": str(UUID(int=number)), "transactionType": "Invoice", "name": "Vendor bill",
        "number": "INV-1", "amount": 12.5, "isPaid": False, "creditExpected": 0,
        "comment": "Header", "status": "Approved", "date": "2020-01-01",
        "documentDate": "2019-12-31", "dueDate": "2020-01-31",
        "location": {"id": str(UUID(int=30))}, "vendor": {"id": str(UUID(int=31))},
        "purchaseOrder": {"id": str(UUID(int=32))},
        "details": [{
            "id": str(UUID(int=number + 10)), "eachAmount": 2.5,
            "generalLedgerAccount": {"id": str(UUID(int=33))},
            "item": {"id": str(UUID(int=34))}, "quantity": 5, "quantityOrdered": 6,
            "total": 12.5, "unitOfMeasure": {"id": str(UUID(int=35))},
            "vendorItem": {"id": str(UUID(int=36))},
        }],
    }


class VendorInvoiceSyncTests(unittest.TestCase):
    setUp = invoice_tests.InvoiceSyncTests.setUp

    def test_paginated_fetch_maps_both_tables_and_commits_once(self):
        self.client.request.side_effect = [
            {"items": [invoice()], "nextLink": "/next"}, {"items": [invoice(2)]},
        ]
        module.sync_vendor_invoices(self.client)
        self.assertEqual(self.client.request.call_count, 2)
        first = self.client.request.call_args_list[0]
        self.assertEqual(first.args, ("GET", "/v1/inventory/invoices"))
        self.assertEqual(first.kwargs["params"]["pageSize"], 250)
        self.assertNotIn("dateStart", first.kwargs["params"])
        self.assertNotIn("IncludeDetails", first.kwargs["params"])
        self.assertEqual(self.client.request.call_args_list[1].args, ("GET", "/next"))
        self.connect.assert_called_once()
        self.connection.commit.assert_called_once()
        self.assertEqual(self.execute.call_count, 2)
        headers, details = self.execute.call_args_list
        self.assertIn('INSERT INTO "r365"."vendor_invoices"', headers.args[1])
        self.assertIn('INSERT INTO "r365"."vendor_invoice_details"', details.args[1])
        self.assertIn('ON CONFLICT ("id")', details.args[1])
        self.assertEqual(headers.args[2][0], (
            UUID(int=1), "Invoice", "Vendor bill", "INV-1", 12.5, False, 0, "Header",
            "Approved", "2020-01-01", "2019-12-31", "2020-01-31",
            UUID(int=30), UUID(int=31), UUID(int=32),
        ))
        self.assertEqual(details.args[2][0], (
            UUID(int=11), UUID(int=1), 2.5, UUID(int=33), UUID(int=34), 5, 6,
            12.5, UUID(int=35), UUID(int=36),
        ))
        self.assertEqual(details.args[2][1][1], UUID(int=2))

    def test_today_window_including_dst(self):
        try:
            with patch.dict(os.environ, {"TZ": "America/New_York"}):
                time.tzset()
                for day, start_offset, end_offset in (
                    (datetime(2026, 9, 27, 15), "-04:00", "-04:00"),
                    (datetime(2026, 11, 1, 15), "-04:00", "-05:00"),
                ):
                    with patch.object(module, "datetime") as clock:
                        clock.now.return_value = day
                        self.client.request.return_value = {"items": []}
                        module.get_today_vendor_invoices(self.client)
                        clock.now.assert_called_once_with()
                    params = self.client.request.call_args.kwargs["params"]
                    self.assertEqual(params["modifiedOnStart"], f"{day.date()}T00:00:00{start_offset}")
                    self.assertEqual(params["modifiedOnEnd"], f"{day.date()}T23:59:59.999999{end_offset}")
        finally:
            time.tzset()

    def test_empty_payloads_skip_database(self):
        for payload in ({"items": []}, {"items": None}, {}):
            self.client.request.return_value = payload
            module.sync_vendor_invoices(self.client)
        self.connect.assert_not_called()
        self.assertEqual(len(module.r365_vendor_invoices(invoices=[]).columns), 15)
        self.assertEqual(len(module.r365_vendor_invoice_details([]).columns), 10)

    def test_zero_values_and_null_references(self):
        row = invoice()
        row.update(amount=0, location=None, vendor={}, purchaseOrder=None)
        row["details"] = [{"id": str(UUID(int=11)), "eachAmount": 0, "quantity": 0, "total": 0}]
        self.client.request.return_value = {"items": [row]}
        module.sync_vendor_invoices(self.client)
        headers, details = self.execute.call_args_list
        self.assertEqual(headers.args[2][0][4], 0)
        self.assertEqual(headers.args[2][0][-3:], (None, None, None))
        self.assertEqual(details.args[2][0], (UUID(int=11), UUID(int=1), 0, None, None, 0, None, 0, None, None))

    def test_required_fields_fail_before_database(self):
        for scope, fields in (
            ("header", ("id", "amount", "date", "documentDate")),
            ("detail", ("id", "eachAmount", "quantity", "total")),
        ):
            for field in fields:
                for value in (None, "", pd.NA, float("nan")):
                    row = invoice()
                    target = row if scope == "header" else row["details"][0]
                    target[field] = value
                    self.client.request.return_value = {"items": [row]}
                    with self.subTest(scope=scope, field=field, value=value):
                        with self.assertRaisesRegex(ValueError, field):
                            module.sync_vendor_invoices(self.client)
        self.connect.assert_not_called()

    def test_null_details_write_headers_only(self):
        row = invoice()
        row["details"] = None
        self.client.request.return_value = {"items": [row]}
        module.sync_vendor_invoices(self.client)
        self.execute.assert_called_once()
        self.connection.commit.assert_called_once()

    def test_detail_failure_rolls_back_both(self):
        self.client.request.return_value = {"items": [invoice()]}
        self.execute.side_effect = [None, RuntimeError("detail failed")]
        with self.assertRaisesRegex(RuntimeError, "detail failed"):
            module.sync_vendor_invoices(self.client)
        self.connection.rollback.assert_called_once()
        self.connection.commit.assert_not_called()
        self.assertNotIn("Upserted", self.output.getvalue())


if __name__ == "__main__":
    unittest.main()
