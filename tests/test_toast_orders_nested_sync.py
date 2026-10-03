"""Focused fixtures for the seven normalized Orders API source tables."""

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
from uuid import UUID

import psycopg2
import requests

from db_utils.toast_utils import ToastClient
from src.toast_sync import orders
from test_toast_orders_sync import order, BUSINESS_DATE, RESTAURANT_ID

SYNCED_AT = datetime(2026, 10, 2, tzinfo=timezone.utc)


def uid(number):
    return str(UUID(int=number))


def discount(number):
    return {"guid": uid(number), "discount": {"guid": uid(60000)}, "name": "Member",
            "discountType": "PERCENT", "discountAmount": Decimal("1.234567890123456789"),
            "nonTaxDiscountAmount": Decimal("1.20"), "discountPercent": 10,
            "processingState": "APPLIED", "appliedPromoCode": "SAVE",
            "appliedDiscountReason": {"name": "Test", "discountReason": {"guid": uid(60001)}}}


def tax(number):
    return {"guid": uid(number), "taxRate": {"guid": uid(60002)}, "name": "Sales tax",
            "rate": Decimal("0.0625"), "taxAmount": Decimal("0.125"), "type": "PERCENT",
            "facilitatorCollectAndRemitTax": False}


def selection(number):
    return {"guid": uid(number), "item": {"guid": uid(60003)}, "displayName": "CONCEPT Filet 8oz",
            "quantity": Decimal("1.5"), "seatNumber": -1, "preDiscountPrice": Decimal("30.50"),
            "price": Decimal("28.50"), "receiptLinePrice": Decimal("20.00"), "tax": Decimal("1.78125"),
            "guestCountWeight": Decimal("0.5"), "premodifierPlu": "001", "taxInclusion": "NOT_INCLUDED",
            "modifiers": [], "appliedDiscounts": [], "appliedTaxes": []}


def complete_order():
    payload = order(20000)
    payload["voided"] = True
    check = payload["checks"][0]
    check.update(amount=Decimal("33.25"), taxAmount=Decimal("2.125"), totalAmount=Decimal("35.375"),
                 displayNumber="001", taxExempt=False, voided=True, openedBy={"guid": uid(60004)})
    item, modifier, nested = selection(21000), selection(21001), selection(21002)
    item["voided"] = True
    item["appliedDiscounts"] = [discount(23000)]
    item["appliedTaxes"] = [tax(24000)]
    modifier.update(displayName="Add Lobster", price=Decimal("15.25"), optionGroupPricingMode="ADJUSTS_PRICE")
    nested.update(displayName="No Butter", price=Decimal("0"), item=None)
    modifier["modifiers"] = [nested]
    item["modifiers"] = [modifier]
    check["selections"] = [item]
    check["appliedDiscounts"] = [discount(23001)]
    check["payments"] = [
        {"guid": uid(22000), "type": "CASH", "amount": Decimal("10.00"), "tipAmount": None,
         "paidBusinessDate": 20260930, "paidDate": "2026-10-01T01:40:00-0400"},
        {"guid": uid(22001), "type": "CREDIT", "amount": Decimal("25.375"), "tipAmount": Decimal("5"),
         "first6Digits": "never-store", "last4Digits": "never-store", "cardPaymentId": "never-store",
         "refundStatus": "PARTIAL", "refund": {"refundAmount": Decimal("1.25"), "tipRefundAmount": None,
         "refundBusinessDate": 20261001, "refundDate": "2026-10-01T10:00:00Z"}},
    ]
    check["appliedServiceCharges"] = [{"guid": uid(25000), "serviceCharge": {"guid": uid(60005)},
        "name": "Gratuity", "chargeType": "PERCENT", "chargeAmount": Decimal("3.25"),
        "gratuity": True, "taxable": True, "serviceChargeCalculation": "POST_DISCOUNT",
        "serviceChargeCategory": "SERVICE_CHARGE", "appliedTaxes": [tax(24001)]}]
    payload["checks"].append({"guid": uid(27001), "selections": [selection(21003)], "payments": []})
    return payload


class NestedNormalizationTests(unittest.TestCase):
    def flatten(self, payload=None):
        return orders.flatten_order(payload or complete_order(), RESTAURANT_ID, BUSINESS_DATE, SYNCED_AT)

    def test_all_tables_multiple_checks_split_payments_and_voids(self):
        tables = self.flatten()
        expected = {"orders": 1, "checks": 2, "selections": 4, "payments": 2,
                    "applied_discounts": 2, "service_charges": 1, "applied_taxes": 2}
        self.assertEqual({table: len(rows) for table, rows in tables.items()}, expected)
        for table, rows in tables.items():
            for row in rows:
                self.assertEqual(set(row), set(orders.TABLE_COLUMNS[table]))
                self.assertEqual(row["restaurant_id"], RESTAURANT_ID)
                self.assertEqual(row["business_date"], BUSINESS_DATE)
                self.assertEqual(row["last_synced_at"], SYNCED_AT)
        self.assertTrue(tables["orders"][0]["voided"])
        self.assertTrue(tables["checks"][0]["voided"])
        self.assertTrue(tables["selections"][0]["voided"])
        self.assertEqual(tables["checks"][0]["opened_by_id"], UUID(int=60004))
        self.assertEqual(tables["payments"][1]["refund_amount"], Decimal("1.25"))
        self.assertIsNone(tables["payments"][1]["tip_refund_amount"])
        self.assertEqual(tables["payments"][1]["refund_business_date"], date(2026, 10, 1))
        self.assertNotIn("never-store", str(tables))

    def test_parent_chain_prices_and_names_preserved(self):
        rows = self.flatten()["selections"]
        self.assertEqual([r["parent_selection_id"] for r in rows], [None, UUID(int=21000), UUID(int=21001), None])
        self.assertEqual(rows[0]["display_name"], "CONCEPT Filet 8oz")
        self.assertEqual(rows[1]["display_name"], "Add Lobster")
        self.assertEqual(rows[2]["display_name"], "No Butter")
        self.assertEqual(rows[0]["quantity"], Decimal("1.5"))
        self.assertEqual(rows[0]["receipt_line_price"], Decimal("20.00"))
        self.assertEqual(rows[1]["price"], Decimal("15.25"))
        self.assertIsNone(rows[0]["open_price_amount"])
        self.assertIsNone(rows[2]["item_id"])

    def test_discount_tax_and_charge_context_and_precision(self):
        tables = self.flatten()
        discounts = {r["id"]: r for r in tables["applied_discounts"]}
        self.assertEqual(discounts[UUID(int=23000)]["selection_id"], UUID(int=21000))
        self.assertIsNone(discounts[UUID(int=23001)]["selection_id"])
        self.assertEqual(discounts[UUID(int=23000)]["discount_amount"], Decimal("1.234567890123456789"))
        taxes = {r["id"]: r for r in tables["applied_taxes"]}
        self.assertEqual(taxes[UUID(int=24000)]["selection_id"], UUID(int=21000))
        self.assertEqual(taxes[UUID(int=24000)]["tax_inclusion"], "NOT_INCLUDED")
        self.assertEqual(taxes[UUID(int=24001)]["service_charge_id"], UUID(int=25000))
        self.assertIsNone(taxes[UUID(int=24001)]["tax_inclusion"])
        self.assertEqual(taxes[UUID(int=24000)]["tax_rate_id"], taxes[UUID(int=24001)]["tax_rate_id"])
        self.assertEqual(tables["service_charges"][0]["service_charge_id"], UUID(int=60005))
        self.assertTrue(tables["service_charges"][0]["gratuity"])

    def test_one_modifier_and_deeper_than_python_recursion_limit(self):
        payload = order(20001)
        root = selection(30000)
        child = selection(30001)
        root["modifiers"] = [child]
        payload["checks"][0]["selections"] = [root]
        self.assertEqual(len(self.flatten(payload)["selections"]), 2)
        for number in range(30002, 31100):
            next_child = selection(number)
            child["modifiers"] = [next_child]
            child = next_child
        rows = self.flatten(payload)["selections"]
        self.assertEqual(len(rows), 1100)
        self.assertEqual(rows[-1]["parent_selection_id"], UUID(int=31098))

    def test_nullable_objects_arrays_and_zero_are_distinct(self):
        payload = order(20002)
        check = payload["checks"][0]
        check.update(selections=[{"guid": uid(21000), "quantity": None, "price": 0, "modifiers": None,
                                 "appliedTaxes": None, "appliedDiscounts": None}], payments=None,
                     appliedServiceCharges=None, appliedDiscounts=None)
        tables = self.flatten(payload)
        self.assertIsNone(tables["selections"][0]["quantity"])
        self.assertEqual(tables["selections"][0]["price"], Decimal(0))
        self.assertIsNone(tables["checks"][0]["tax_amount"])
        self.assertEqual(tables["payments"], [])

    def test_malformed_children_context_and_no_partial_order(self):
        payload = complete_order()
        payload["checks"][0]["selections"][0]["quantity"] = "not numeric"
        with self.assertRaisesRegex(ValueError, "order=.*check=.*quantity"):
            self.flatten(payload)
        with self.assertLogs(orders.logger, "ERROR") as logged:
            tables, rejected, _ = orders.normalize_orders([payload], RESTAURANT_ID, BUSINESS_DATE, SYNCED_AT)
        self.assertEqual(rejected, 1)
        self.assertTrue(all(not rows for rows in tables.values()))
        self.assertIn(uid(20000), str(logged.output))
        self.assertIn(uid(27000), str(logged.output))

    def test_missing_application_guid_is_not_invented(self):
        payload = complete_order()
        del payload["checks"][0]["appliedServiceCharges"][0]["appliedTaxes"][0]["guid"]
        with self.assertRaisesRegex(ValueError, "applied_taxes.*guid"):
            self.flatten(payload)

    def test_duplicate_graph_checks_children_and_repeated_tax_context(self):
        payload = complete_order()
        changed = deepcopy(payload)
        changed["checks"][0]["payments"][0]["amount"] = 12
        with self.assertLogs(orders.logger, "ERROR"):
            tables, rejected, duplicates = orders.normalize_orders([payload, changed], RESTAURANT_ID, BUSINESS_DATE, SYNCED_AT)
        self.assertEqual((rejected, duplicates), (1, 1))
        self.assertFalse(tables["orders"])
        payload["checks"][0]["appliedServiceCharges"][0]["appliedTaxes"][0]["guid"] = uid(24000)
        with self.assertRaisesRegex(ValueError, "Conflicting applied_taxes"):
            self.flatten(payload)

    def test_payment_parent_mismatch_and_selection_cycle_rejected(self):
        payload = complete_order()
        payload["checks"][0]["payments"][0]["checkGuid"] = uid(99)
        with self.assertRaisesRegex(ValueError, "differs from parent"):
            self.flatten(payload)
        payload = complete_order()
        selection_row = payload["checks"][0]["selections"][0]
        selection_row["modifiers"] = [selection_row]
        with self.assertRaisesRegex(ValueError, "Repeated/cyclic"):
            self.flatten(payload)

    def test_fetch_decodes_decimal_from_json_without_binary_rounding(self):
        response = requests.Response()
        response.status_code = 200
        response._content = b'[{"guid":"00000000-0000-0000-0000-000000000001","quantity":0.1234567890123456789}]'
        client = object.__new__(ToastClient)
        with patch.object(client, "request", return_value=response) as request:
            payload = orders.fetch_orders(client, RESTAURANT_ID, BUSINESS_DATE)
        self.assertEqual(payload[0]["quantity"], Decimal("0.1234567890123456789"))
        self.assertEqual(request.call_args.kwargs["params"]["pageSize"], 100)


@unittest.skipUnless(os.environ.get("TOAST_TEST_PG_SOCKET"), "Requires isolated temporary PostgreSQL")
class NestedPostgresTests(unittest.TestCase):
    def test_additive_upgrade_preserves_header(self):
        socket = os.environ["TOAST_TEST_PG_SOCKET"]
        self.assertTrue(socket.startswith("/tmp/toast-restaurants-pg-"))
        connection = psycopg2.connect(host=socket, port=55440, dbname="postgres", user="toast_test")
        self.addCleanup(connection.close)
        ddl = Path("db_utils/schema/toast_orders.sql").read_text()
        legacy = ddl.split("-- Additive upgrade")[0].replace("    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),\n", "")
        with connection.cursor() as cur:
            cur.execute(legacy)
            cur.execute("INSERT INTO toast.orders (id, restaurant_id, business_date, calculated_guest_count, external_id) VALUES (%s,%s,%s,%s,%s)",
                        (UUID(int=80000), RESTAURANT_ID, BUSINESS_DATE, Decimal("1.25"), "legacy-kept"))
        connection.commit()
        with connection.cursor() as cur:
            cur.execute(ddl)
            cur.execute("SELECT calculated_guest_count, external_id, last_synced_at IS NOT NULL FROM toast.orders WHERE id=%s", (UUID(int=80000),))
            self.assertEqual(cur.fetchone(), (Decimal("1.25"), "legacy-kept", True))
        connection.commit()

    def test_repeat_updates_timestamp_stale_children_and_atomic_rollback(self):
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
        payload = complete_order()
        first = orders.flatten_order(payload, RESTAURANT_ID, BUSINESS_DATE, SYNCED_AT)
        later = SYNCED_AT + timedelta(seconds=1)
        with patch("db_utils.dbconnect.psycopg2.connect", side_effect=test_connection):
            inserted = orders.upsert_order_tables(first)
            self.assertEqual({t: c["inserted"] for t, c in inserted.items()}, {t: len(rows) for t, rows in first.items()})
            payload["checks"][0]["payments"][1]["refund"]["refundAmount"] = Decimal("2.50")
            # A missing modifier is retained, with its old observation timestamp.
            payload["checks"][0]["selections"][0]["modifiers"] = []
            second = orders.flatten_order(payload, RESTAURANT_ID, BUSINESS_DATE, later)
            updated = orders.upsert_order_tables(second)
            self.assertEqual({t: c["updated"] for t, c in updated.items()}, {t: len(rows) for t, rows in second.items()})
            bad = deepcopy(second)
            bad["orders"][0]["source"] = "must roll back"
            bad["checks"][0]["amount"] = Decimal("999")
            bad["applied_taxes"][-1]["rate"] = "not-numeric"
            with self.assertRaises(psycopg2.Error):
                orders.upsert_order_tables(bad)
        with connection.cursor() as cur:
            cur.execute("SELECT source, last_synced_at FROM toast.orders WHERE id=%s", (UUID(int=20000),))
            self.assertEqual(cur.fetchone(), ("In Store", later))
            cur.execute("SELECT amount FROM toast.checks WHERE id=%s", (UUID(int=27000),))
            self.assertEqual(cur.fetchone()[0], Decimal("33.25"))
            cur.execute("SELECT refund_amount FROM toast.payments WHERE id=%s", (UUID(int=22001),))
            self.assertEqual(cur.fetchone()[0], Decimal("2.50"))
            cur.execute("SELECT last_synced_at FROM toast.selections WHERE id=%s", (UUID(int=21001),))
            self.assertEqual(cur.fetchone()[0], SYNCED_AT)
            cur.execute("SELECT discount_amount FROM toast.applied_discounts WHERE id=%s", (UUID(int=23000),))
            self.assertEqual(cur.fetchone()[0], Decimal("1.234567890123456789"))
            cur.execute("SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='toast' AND c.contype='f'")
            self.assertEqual(cur.fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
