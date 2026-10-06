"""Optional integration test against a fresh, isolated PostgreSQL instance in /tmp.

Set R365_TEST_PG_SOCKET to its Unix socket directory (port 55439, user sales_test).
The test creates the sales tables and deliberately leaves them in the test database.
"""

import copy
import os
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import psycopg2

from src.r365_sync import daily_sales
from test_r365_daily_sales_sync import page, ticket, uid


@unittest.skipUnless(os.environ.get("R365_TEST_PG_SOCKET"), "Requires temporary PostgreSQL instance")
class DailySalesPostgresTests(unittest.TestCase):
    def test_repeat_import_inactivation_reactivation_and_rollback(self):
        socket = os.environ["R365_TEST_PG_SOCKET"]
        if not socket.startswith("/tmp/r365-sales-pg-"):
            self.fail("Integration test requires an isolated /tmp/r365-sales-pg-* instance")
        connect = psycopg2.connect

        def test_connection(*args, **kwargs):
            return connect(host=socket, port=55439, dbname="postgres", user="sales_test")

        connection = test_connection()
        self.addCleanup(connection.close)
        with connection.cursor() as cur:
            cur.execute(Path("db_utils/schema/r365_daily_sales.sql").read_text())
        connection.commit()

        client = Mock()

        def sync(response):
            client.request.return_value = response
            with patch("db_utils.dbconnect.psycopg2.connect", side_effect=test_connection):
                daily_sales.sync_daily_sales(client, [uid(50)], "2026-09-29", "2026-09-29")

        def counts(table):
            with connection.cursor() as cur:
                cur.execute(f"SELECT count(*), count(*) FILTER (WHERE is_current) FROM r365.{table}")
                result = cur.fetchone()
            connection.commit()
            return result

        first = ticket()
        first["taxDetail"].append({"rateId": "tax-2", "rate": 0.01, "appliedTaxPortionAmount": 0.10})
        sync(page([first, ticket(3)]))
        sync(page([first, ticket(3)]))
        self.assertEqual(counts("sales_tickets"), (2, 2))
        self.assertEqual(counts("sales_details"), (1, 1))
        self.assertEqual(counts("sales_payments"), (2, 2))
        self.assertEqual(counts("sales_ticket_taxes"), (3, 3))

        with connection.cursor() as cur:
            cur.execute("SELECT sale_amount, quantity FROM r365.sales_details WHERE is_current")
            amounts = cur.fetchone()
            self.assertEqual(float(amounts[0]), 20.5)
            self.assertEqual(float(amounts[1]), 2.25)
            cur.execute("SELECT count(*) FROM r365.sales_account")
            self.assertEqual(cur.fetchone()[0], 1)
        connection.commit()

        # A partial correction overwrites totals instead of adding to them.
        sync(page([first]))
        self.assertEqual(counts("sales_details"), (1, 1))
        with connection.cursor() as cur:
            cur.execute("SELECT sale_amount FROM r365.sales_details WHERE is_current")
            self.assertEqual(float(cur.fetchone()[0]), 10.25)
        connection.commit()

        # One removed ticket; one surviving ticket loses items, payments, and a tax slot.
        changed = copy.deepcopy(first)
        changed["salesDetails"] = []
        changed["salesPayments"] = []
        changed["taxDetail"] = [changed["taxDetail"][1]]
        sync(page([changed]))
        self.assertEqual(counts("sales_tickets"), (2, 1))
        self.assertEqual(counts("sales_details"), (1, 0))
        self.assertEqual(counts("sales_payments"), (2, 0))
        self.assertEqual(counts("sales_ticket_taxes"), (3, 1))
        with connection.cursor() as cur:
            cur.execute("SELECT rate_id FROM r365.sales_ticket_taxes WHERE is_current")
            self.assertEqual(cur.fetchone()[0], "tax-2")
        connection.commit()

        # Reactivate the original IDs, without creating duplicate tax slots.
        sync(page([first, ticket(3)]))
        self.assertEqual(counts("sales_tickets"), (2, 2))
        self.assertEqual(counts("sales_ticket_taxes"), (3, 3))

        # A real SQL failure after inactivation must roll back headers and all children.
        with connection.cursor() as cur:
            cur.execute("ALTER TABLE r365.sales_ticket_taxes ADD CONSTRAINT test_rate CHECK (rate < 1)")
        connection.commit()
        invalid = copy.deepcopy(changed)
        invalid["taxDetail"][0]["rate"] = 2
        bad_page = page([invalid])
        bad_page["data"]["netSales"] = 999
        with self.assertRaises(psycopg2.errors.CheckViolation):
            sync(bad_page)
        self.assertEqual(counts("sales_tickets"), (2, 2))
        self.assertEqual(counts("sales_details"), (1, 1))
        self.assertEqual(counts("sales_payments"), (2, 2))
        self.assertEqual(counts("sales_ticket_taxes"), (3, 3))
        with connection.cursor() as cur:
            cur.execute("SELECT net_sales FROM r365.daily_sales")
            self.assertEqual(float(cur.fetchone()[0]), 10.25)
            cur.execute("""SELECT column_name FROM information_schema.columns
                           WHERE table_schema = 'r365'""")
            columns = {row[0] for row in cur.fetchall()}
            self.assertTrue(columns.isdisjoint({"server_name", "server_payroll_id", "comment",
                                               "location_name", "location_number"}))
        connection.commit()

        sync(page([]))
        for table in ("sales_tickets", "sales_details", "sales_payments", "sales_ticket_taxes"):
            self.assertEqual(counts(table)[1], 0)

    def test_migration_preserves_history_totals_and_rejects_dependencies(self):
        socket = os.environ["R365_TEST_PG_SOCKET"]
        if not socket.startswith("/tmp/r365-sales-pg-"):
            self.fail("Integration test requires an isolated /tmp/r365-sales-pg-* instance")
        admin = psycopg2.connect(host=socket, port=55439, dbname="postgres", user="sales_test")
        admin.autocommit = True
        self.addCleanup(admin.close)
        with admin.cursor() as cur:
            cur.execute("CREATE DATABASE r365_sales_migration_test")
        connection = psycopg2.connect(
            host=socket, port=55439, dbname="r365_sales_migration_test", user="sales_test",
        )
        self.addCleanup(connection.close)
        migration = Path("db_utils/schema/r365_sales_details_daily_migration.sql").read_text()
        with connection.cursor() as cur:
            cur.execute(Path("tests/fixtures/r365_daily_sales_ticket_schema.sql").read_text())
            for header, day in ((1, "2026-09-29"), (10, "2024-01-02")):
                cur.execute("""INSERT INTO r365.daily_sales
                    (id, location_id, business_date, last_synced_at) VALUES (%s, %s, %s, now())""",
                            (uid(header), uid(50), day))
            for ticket_id, header, active in ((2, 1, True), (3, 1, True), (4, 10, True), (5, 1, False)):
                cur.execute("""INSERT INTO r365.sales_tickets
                    (id, daily_sales_id, void, is_current, last_synced_at)
                    VALUES (%s, %s, false, %s, now())""", (uid(ticket_id), uid(header), active))
            # Same item across tickets merges; void, null references and old dates remain separate.
            for line, parent, item, account, void, active, amount in (
                (102, 2, 123, uid(80), False, True, 10),
                (103, 3, 123, uid(80), False, True, 20),
                (104, 2, 123, uid(80), True, True, 5),
                (105, 2, None, None, False, True, None),
                (106, 2, 123, uid(80), False, False, 900),
                (107, 5, 123, uid(80), False, True, 800),
                (108, 4, 123, uid(80), False, True, 40),
            ):
                cur.execute("""INSERT INTO r365.sales_details
                    (id, sales_ticket_id, pos_item_id, pos_item_name, sales_account_id,
                     sales_account_name, sales_account_number, sales_account_gl_type,
                     void, is_current, sale_amount, quantity, last_synced_at)
                    VALUES (%s, %s, %s, 'Item', %s, 'Food', '0400', 'Sales', %s, %s, %s, 1, now())""",
                            (uid(line), uid(parent), item, account, void, active, amount))
            cur.execute("CREATE VIEW public.dependency_check AS SELECT id FROM r365.sales_details")
        connection.commit()
        # An existing consumer must stop the migration, not be silently removed.
        with connection.cursor() as cur:
            with self.assertRaises(psycopg2.errors.DependentObjectsStillExist):
                cur.execute(migration)
        connection.rollback()
        with connection.cursor() as cur:
            cur.execute("SELECT count(*) FROM r365.sales_details")
            self.assertEqual(cur.fetchone()[0], 7)
            cur.execute("SELECT to_regclass('r365.sales_account')")
            self.assertIsNone(cur.fetchone()[0])
            cur.execute("DROP VIEW public.dependency_check")
        connection.commit()
        with connection.cursor() as cur:
            cur.execute(migration)
        connection.commit()
        with connection.cursor() as cur:
            cur.execute("SELECT count(*), sum(sale_amount), sum(quantity) FROM r365.sales_details")
            self.assertEqual(cur.fetchone(), (4, 75, 5))
            cur.execute("SELECT min(business_date)::text FROM r365.sales_details")
            self.assertEqual(cur.fetchone()[0], "2024-01-02")
            cur.execute("SELECT name, number, gl_type FROM r365.sales_account WHERE id = %s", (uid(80),))
            self.assertEqual(cur.fetchone(), ("Food", "0400", "Sales"))
            cur.execute("""SELECT sale_amount, quantity FROM r365.sales_details
                           WHERE business_date = '2026-09-29' AND NOT void AND pos_item_id = 123""")
            self.assertEqual(cur.fetchone(), (30, 2))
        connection.commit()
