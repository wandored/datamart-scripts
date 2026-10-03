import copy
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch
from uuid import UUID

from requests import HTTPError, Response

import test_r365_invoice_sync as invoice_tests

module = invoice_tests.module


def uid(number):
    return str(UUID(int=number))


def ticket(number=2):
    return {
        "id": uid(number), "void": False, "netSales": 10.25,
        "saleDateTime": "2026-09-29T12:00:00-04:00",
        "comment": "DO NOT STORE COMMENT",
        "server": {"id": uid(90), "name": "DO NOT STORE NAME", "payrollId": "PRIVATE"},
        "salesDetails": [{
            "id": uid(number + 100), "void": False, "saleAmount": 10.25, "quantity": 1.125,
            "posItem": {"id": 9007199254740993, "name": "Casual - Item"},
            "salesAccount": {"id": uid(80), "number": "0400", "glType": "Sales"},
            "menuItemCategory1": "Food",
        }],
        "salesPayments": [{"id": uid(number + 200), "amount": 11.0,
                           "paymentDate": "2026-09-29T12:00:00-04:00"}],
        "taxDetail": [{"rateId": "tax-1", "rate": 0.075, "appliedTaxPortionAmount": 0.75}],
    }


def page(tickets=None, next_link=None):
    return {"data": {
        "id": uid(1), "location": {"id": uid(50), "name": "OMIT LOCATION", "number": "0050"},
        "businessDate": "2026-09-29", "netSales": 10.25,
        "salesTickets": [ticket()] if tickets is None else tickets,
    }, "nextLink": next_link}


def not_found():
    response = Response()
    response.status_code = 404
    return HTTPError(response=response)


class DailySalesSyncTests(unittest.TestCase):
    setUp = invoice_tests.InvoiceSyncTests.setUp

    def sync(self):
        module.sync_daily_sales(self.client, [uid(50)], "2026-09-29", "2026-09-29")

    def test_default_sync_uses_active_restaurant_ids_and_database_timezone(self):
        self.connection.cursor.return_value.fetchall.return_value = [
            {"id": UUID(int=50), "timezone": "America/Chicago"},
        ]
        response = page()
        response["data"]["salesTickets"][0]["saleDateTime"] = "2026-09-29T12:00:00"
        self.client.request.return_value = response
        with patch.object(module, "get_locations") as api_locations:
            module.sync_daily_sales(
                self.client, business_date_start="2026-09-29", business_date_end="2026-09-29",
            )
        api_locations.assert_not_called()
        query = self.connection.cursor.return_value.execute.call_args_list[0].args[0]
        self.assertIn("FROM core.restaurants", query)
        self.assertIn("r365_guid AS id", query)
        self.assertIn("active IS TRUE", query)
        self.assertNotIn("is_active", query)
        self.assertIn("r365_guid IS NOT NULL", query)
        self.assertEqual(self.client.request.call_args.kwargs["params"]["location"], uid(50))
        self.assertEqual(self.execute.call_args_list[1].args[2][0][4],
                         datetime(2026, 9, 29, 17, tzinfo=timezone.utc))

    def test_no_active_restaurants_skips_api_and_writes(self):
        self.connection.cursor.return_value.fetchall.return_value = []
        module.sync_daily_sales(self.client, business_date_start="2026-09-29")
        self.client.request.assert_not_called()
        self.execute.assert_not_called()
        self.assertIn("No active restaurants", self.output.getvalue())

    def test_pagination_atomic_upserts_composite_key_and_pii_exclusion(self):
        self.client.request.side_effect = [page(next_link="/next"), page([ticket(3)])]
        self.sync()
        self.assertEqual(self.client.request.call_count, 2)
        self.assertEqual(self.client.request.call_args_list[1].args, ("GET", "/next"))
        self.assertEqual(self.client.request.call_args_list[0].kwargs["params"], {
            "businessDate": "2026-09-29", "location": uid(50), "pageSize": 250,
        })
        self.assertEqual(self.execute.call_count, 6)
        self.connection.commit.assert_called_once()
        self.connection.rollback.assert_not_called()
        header, tickets, accounts, details, payments, taxes = self.execute.call_args_list
        self.assertEqual(len(header.args[2]), 1)
        self.assertEqual(len(tickets.args[2]), 2)
        self.assertIn('ON CONFLICT ("sales_ticket_id", "tax_index")', taxes.args[1])
        self.assertEqual(details.args[2][0][2], 9007199254740993)
        self.assertEqual(len(details.args[2]), 1)
        self.assertEqual(details.args[2][0][9:11], (Decimal("20.50"), Decimal("2.250")))
        self.assertEqual(accounts.args[2][0][:4], (UUID(int=80), None, "0400", "Sales"))
        self.assertIn('ON CONFLICT ("business_date", "location_id", "pos_item_id"', details.args[1])
        written = str(self.execute.call_args_list)
        for forbidden in ("DO NOT STORE", "PRIVATE", "OMIT LOCATION", "server_name",
                          "server_payroll_id", "location_name", "location_number", '"comment"'):
            self.assertNotIn(forbidden, written)
        self.assertEqual(self.connection.cursor.return_value.execute.call_count, 4)

    def test_first_page_404_skips_but_continuation_404_fails_without_writes(self):
        self.client.request.side_effect = not_found()
        self.sync()
        self.connect.assert_not_called()
        self.client.request.side_effect = [page(next_link="/next"), not_found()]
        with self.assertRaises(HTTPError):
            self.sync()
        self.connect.assert_not_called()

    def test_missing_or_inconsistent_snapshot_never_changes_database(self):
        cases = [[{}], [{"data": None, "nextLink": None}],
                 [page(next_link="/next"), page(next_link="/next")]]
        missing_array = page()
        del missing_array["data"]["salesTickets"][0]["salesPayments"]
        cases.append([missing_array])
        missing_pagination = page()
        del missing_pagination["nextLink"]
        cases.append([missing_pagination])
        for field, value in (("id", uid(8)), ("netSales", 999), ("businessDate", "2026-09-28")):
            changed = page([ticket(3)])
            changed["data"][field] = value
            cases.append([page(next_link="/next"), changed])
        changed = page()
        changed["data"]["location"]["id"] = uid(51)
        cases.append([changed])
        changed = page()
        changed["data"]["salesTickets"][0]["salesDetails"][0]["id"] = None
        cases.append([changed])
        for responses in cases:
            with self.subTest(responses=len(responses)):
                self.client.request.side_effect = responses
                with self.assertRaises(ValueError):
                    self.sync()
                self.connect.assert_not_called()

    def test_duplicate_ticket_and_conflicting_child_ids(self):
        self.client.request.side_effect = [page(next_link="/next"), page()]
        self.sync()
        self.assertEqual(len(self.execute.call_args_list[-1].args[2]), 1)
        self.connect.reset_mock()
        changed = ticket(3)
        changed["salesDetails"][0]["id"] = ticket()["salesDetails"][0]["id"]
        self.client.request.side_effect = [page([ticket(), changed])]
        with self.assertRaises(ValueError):
            self.sync()
        self.connect.assert_not_called()

    def test_valid_empty_snapshot_reconciles_existing_children(self):
        self.client.request.return_value = page([])
        self.sync()
        self.assertEqual(self.execute.call_count, 1)
        self.assertEqual(self.connection.cursor.return_value.execute.call_count, 4)
        self.connection.commit.assert_called_once()

    def test_null_arrays_and_nullable_identifiers_keep_explicit_columns(self):
        record = ticket()
        record.update(salesDetails=None, salesPayments=None, taxDetail=None, server=None)
        raw = module.r365_sales_detail_rows([record])
        self.assertEqual(len(module.r365_sales_details(raw, "2026-09-29", uid(50)).columns), 11)
        self.assertTrue(module.r365_sales_accounts(raw).empty)
        self.assertTrue(module.r365_sales_payments([record]).empty)
        self.assertTrue(module.r365_sales_ticket_taxes([record]).empty)
        self.assertIsNone(module.r365_sales_tickets([record], UUID(int=1)).iloc[0].server_id)

    def test_tax_failure_rolls_back_inactivation_and_reports_no_success(self):
        self.client.request.return_value = page()
        self.execute.side_effect = [None, None, None, None, None, RuntimeError("tax write failed")]
        with self.assertRaisesRegex(RuntimeError, "tax write failed"):
            self.sync()
        self.connection.rollback.assert_called_once()
        self.connection.commit.assert_not_called()
        self.assertNotIn("fetched/upserted", self.output.getvalue())

    def test_rollup_preserves_dimensions_and_null_groups(self):
        base = ticket()
        tickets = [base, ticket(3)]
        for index, (field, value) in enumerate((
            ("void", True), ("menuItemCategory1", "Other"),
            ("menuItemCategory2", "Subcategory"), ("menuItemCategory3", "Subsubcategory"),
            ("salesAccount", {"id": uid(81)}),
            ("posItem", {"id": 9007199254740993, "name": "Renamed"}),
        ), start=4):
            changed = ticket(index)
            changed["salesDetails"][0][field] = value
            tickets.append(changed)
        raw = module.r365_sales_detail_rows(tickets)
        grouped = module.r365_sales_details(raw, "2026-09-29", uid(50))
        self.assertEqual(len(grouped), 7)
        self.assertEqual(grouped.iloc[0].sale_amount, Decimal("20.50"))
        self.assertEqual(grouped.iloc[0].quantity, Decimal("2.250"))
        self.assertEqual(grouped.iloc[0].location_id, UUID(int=50))
        self.assertEqual(grouped.iloc[0].business_date, date(2026, 9, 29))
        self.assertEqual(set(grouped["void"]), {True, False})
        self.assertEqual(len(module.r365_sales_accounts(raw)), 2)

    def test_rollup_null_measures_and_duplicate_source_lines(self):
        first = ticket()
        first["salesDetails"][0].update(posItem=None, salesAccount=None, saleAmount=None, quantity=None)
        second = copy.deepcopy(first)
        second["id"] = uid(3)
        second["salesDetails"][0]["id"] = uid(103)
        second["salesDetails"][0]["quantity"] = 2
        raw = module.r365_sales_detail_rows([first, first, second])
        grouped = module.r365_sales_details(raw, "2026-09-29", uid(50))
        self.assertEqual(len(grouped), 1)
        self.assertIsNone(grouped.iloc[0].sale_amount)
        self.assertIsNone(grouped.iloc[0].pos_item_id)
        self.assertEqual(grouped.iloc[0].quantity, Decimal("2"))
        self.assertTrue(module.r365_sales_accounts(raw).empty)

    def test_conflicting_account_attributes_fail_before_writes(self):
        second = ticket(3)
        second["salesDetails"][0]["salesAccount"]["number"] = "different"
        self.client.request.return_value = page([ticket(), second])
        with self.assertRaises(ValueError):
            self.sync()
        self.connect.assert_not_called()

    def test_default_dates_and_single_boundary(self):
        with patch.object(module, "date") as clock:
            clock.today.return_value = date(2026, 9, 30)
            self.assertEqual(module.daily_sales_date_range(), (date(2026, 9, 23), date(2026, 9, 29)))
        self.assertEqual(module.daily_sales_date_range(None, "2026-09-29"),
                         (date(2026, 9, 29), date(2026, 9, 29)))
        with self.assertRaises(ValueError):
            module.daily_sales_date_range("2026-09-30", "2026-09-29")

    def test_local_timestamp_conversion_and_dst_ambiguity(self):
        self.assertEqual(module.sales_timestamp("2026-09-29T12:00:00", "America/New_York"),
                         datetime(2026, 9, 29, 16, tzinfo=timezone.utc))
        for stamp, zone in (("2026-09-29T12:00:00", None),
                            ("2026-11-01T01:30:00", "America/New_York"),
                            ("2026-03-08T02:30:00", "America/New_York")):
            with self.assertRaises(ValueError):
                module.sales_timestamp(stamp, zone)

    def test_windows_timezones_use_seasonal_offsets(self):
        for zone, winter_hour, summer_hour in (
            ("Eastern Standard Time", 17, 16),
            ("Central Standard Time", 18, 17),
            ("Mountain Standard Time", 19, 18),
            ("US Mountain Standard Time", 19, 19),
            ("Pacific Standard Time", 20, 19),
            ("Alaskan Standard Time", 21, 20),
            ("Hawaiian Standard Time", 22, 22),
            ("UTC", 12, 12),
        ):
            with self.subTest(zone=zone):
                for month, hour in ((1, winter_hour), (7, summer_hour)):
                    self.assertEqual(
                        module.sales_timestamp(f"2026-{month:02d}-15T12:00:00", zone),
                        datetime(2026, month, 15, hour, tzinfo=timezone.utc),
                    )

    def test_windows_zone_dst_ambiguity_and_explicit_offset(self):
        for stamp in ("2026-11-01T01:30:00", "2026-03-08T02:30:00"):
            with self.assertRaises(ValueError):
                module.sales_timestamp(stamp, "Central Standard Time")
        self.assertEqual(
            module.sales_timestamp("2026-11-01T01:30:00-05:00", "Central Standard Time"),
            datetime(2026, 11, 1, 6, 30, tzinfo=timezone.utc),
        )

    def test_sync_converts_offset_free_ticket_and_payment_timestamps(self):
        response = page()
        row = response["data"]["salesTickets"][0]
        row["saleDateTime"] = "2026-09-29T12:00:00"
        row["salesPayments"][0]["paymentDate"] = "2026-09-29T12:00:00"
        self.client.request.return_value = response
        module.sync_daily_sales(
            self.client, [uid(50)], "2026-09-29", "2026-09-29",
            {uid(50): "Central Standard Time"},
        )
        expected = datetime(2026, 9, 29, 17, tzinfo=timezone.utc)
        self.assertEqual(self.execute.call_args_list[1].args[2][0][4], expected)
        self.assertEqual(self.execute.call_args_list[4].args[2][0][5], expected)
        self.connection.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
