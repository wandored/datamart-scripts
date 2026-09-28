import importlib
import io
import unittest
from contextlib import redirect_stdout
from decimal import Decimal
from unittest.mock import Mock, patch
from uuid import UUID

from db_utils.r365_utils import R365Client
from psycopg2 import sql

module = importlib.import_module("src.r365-api-update")


def render_sql(node, context=None):
    if isinstance(node, sql.Composed):
        return "".join(render_sql(part) for part in node.seq)
    if isinstance(node, sql.Identifier):
        return ".".join('"' + part.replace('"', '""') + '"' for part in node.strings)
    return node.string


def invoice(number=1):
    return {
        "id": str(UUID(int=number)),
        "documentDate": "2020-01-01",
        "details": [{
            "id": str(UUID(int=number + 10)),
            "glAccount": {"id": str(UUID(int=30))},
            "location": {"id": str(UUID(int=31))},
            "inventoryItem": {"id": str(UUID(int=32))},
            "uom": {"id": str(UUID(int=33))},
            "comment": "Line comment",
            "total": 12.34,
            "quantity": 1.123456,
            "eachAmount": 2.123456,
            "vendorItemNumber": "ITEM-1",
        }],
    }


class InvoiceSyncTests(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        self.enterContext(redirect_stdout(self.output))
        self.connection = Mock(autocommit=False)
        self.connect = self.enterContext(patch(
            "db_utils.dbconnect.psycopg2.connect", return_value=self.connection
        ))
        self.execute = self.enterContext(patch("db_utils.dbconnect.execute_values"))
        self.enterContext(patch.object(sql.Composed, "as_string", render_sql))
        self.client = object.__new__(R365Client)
        self.client.request = Mock()

    def test_one_paginated_fetch_two_upserts_one_commit(self):
        self.client.request.side_effect = [
            {"invoices": [invoice()], "nextLink": "/next"},
            {"invoices": [invoice(2)]},
        ]
        module.sync_invoices(self.client)
        self.assertEqual(self.client.request.call_count, 2)
        first = self.client.request.call_args_list[0]
        self.assertEqual(first.args, ("GET", "/v1/accounting/accounts-payable/invoices"))
        params = first.kwargs["params"]
        self.assertEqual(params["IncludeDetails"], "true")
        self.assertEqual(params["PageSize"], 250)
        self.assertIn("modifiedOnStart", params)
        self.assertIn("modifiedOnEnd", params)
        self.assertNotIn("DateStart", params)
        self.assertEqual(self.client.request.call_args_list[1].args, ("GET", "/next"))
        self.connect.assert_called_once()
        self.connection.commit.assert_called_once()
        self.connection.rollback.assert_not_called()
        self.assertEqual(self.execute.call_count, 2)
        headers, details = self.execute.call_args_list
        self.assertIn('INSERT INTO "r365"."invoices"', headers.args[1])
        self.assertIn('INSERT INTO "r365"."invoice_details"', details.args[1])
        self.assertIn('ON CONFLICT ("id")', details.args[1])
        self.assertIn('"invoice_id" = EXCLUDED."invoice_id"', details.args[1])
        self.assertEqual(headers.args[2][0][4], "2020-01-01")
        self.assertEqual(details.args[2][0], (
            UUID(int=11), UUID(int=1), UUID(int=30), UUID(int=31), "Line comment",
            Decimal("12.34"), UUID(int=32), UUID(int=33), Decimal("1.123456"),
            Decimal("2.123456"), "ITEM-1",
        ))
        self.assertEqual(details.args[2][1][1], UUID(int=2))

    def test_detail_failure_rolls_back_and_reports_no_success(self):
        self.client.request.return_value = {"invoices": [invoice()]}
        self.execute.side_effect = [None, RuntimeError("detail write failed")]
        with self.assertRaisesRegex(RuntimeError, "detail write failed"):
            module.sync_invoices(self.client)
        self.connection.rollback.assert_called_once()
        self.connection.commit.assert_not_called()
        self.assertNotIn("Upserted", self.output.getvalue())

    def test_commit_failure_reports_no_success(self):
        self.client.request.return_value = {"invoices": [invoice()]}
        self.connection.commit.side_effect = RuntimeError("commit failed")
        with self.assertRaisesRegex(RuntimeError, "commit failed"):
            module.sync_invoices(self.client)
        self.assertNotIn("Upserted", self.output.getvalue())

    def test_empty_invoices_skip_database(self):
        self.client.request.return_value = {"invoices": []}
        module.sync_invoices(self.client)
        self.connect.assert_not_called()
        self.assertEqual(len(module.r365_invoice_details([]).columns), 11)

    def test_null_details_and_references(self):
        for details in (None, []):
            self.assertTrue(module.r365_invoice_details([
                {"id": str(UUID(int=1)), "details": details}
            ]).empty)
        row = {"id": str(UUID(int=1)), "details": [
            {"id": str(UUID(int=11)), "glAccount": None, "inventoryItem": {}, "uom": None}
        ]}
        df = module.r365_invoice_details([row])
        self.assertEqual(df.iloc[0].tolist(), [UUID(int=11), UUID(int=1)] + [None] * 9)

    def test_missing_ids_fail_before_database(self):
        for row in ({"details": []}, {"id": None},
                    {"id": str(UUID(int=1)), "details": [{}]}):
            with self.subTest(row=row):
                self.client.request.return_value = {"invoices": [row]}
                with self.assertRaises(ValueError):
                    module.sync_invoices(self.client)
        self.connect.assert_not_called()

    def test_existing_header_only_and_standalone_writer(self):
        self.client.request.return_value = {"invoices": [invoice()]}
        df = module.r365_invoices(self.client)
        self.assertEqual(self.client.request.call_count, 1)
        self.assertEqual(self.client.request.call_args.kwargs["params"]["IncludeDetails"], "false")
        count = module.write_to_db(df, "invoices", "r365")
        self.assertEqual(count, 1)
        self.connection.commit.assert_called_once()
        self.assertIn("Upserted 1 rows", self.output.getvalue())

    def test_supplied_empty_payload_does_not_refetch(self):
        df = module.r365_invoices(self.client, invoices=[])
        self.assertTrue(df.empty)
        self.client.request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
