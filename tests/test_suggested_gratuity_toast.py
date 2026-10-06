import copy
import unittest
from datetime import date
from decimal import Decimal

from src.suggested_gratuity_toast import compact_checks, match_row, receipt_date


DAY = date(2026, 10, 3)


def order():
    return {
        "guid": "order-1", "displayNumber": "194", "businessDate": 20261003,
        "server": {"guid": "server-1"},
        "checks": [{
            "guid": "check-1", "displayNumber": "0293",
            "customer": {"email": "omit@example.test"},
            "appliedServiceCharges": [{
                "guid": "charge-1", "name": "Suggested Gratuity (May be changed by guest)",
                "chargeAmount": Decimal("1.05"),
            }],
            "appliedDiscounts": [{"approver": {"guid": "discount-approver"}}],
            "payments": [{"voidInfo": {"voidApprover": {"guid": "void-approver"}}}],
        }],
    }


def row():
    return {"receipt_number": "0293 - 7 - 10/03/2026", "check_number": "0293", "amount": "1.05", "manager_id": ""}


class ToastGratuityTests(unittest.TestCase):
    def test_matches_service_charge_and_keeps_approval_meanings_separate(self):
        candidates = compact_checks([order()], DAY)
        result = match_row(row(), candidates, "restaurant-1", DAY)
        self.assertEqual(result["toast_match_status"], "matched")
        self.assertEqual(result["toast_order_number"], "194")
        self.assertEqual(result["toast_check_number"], "0293")
        self.assertEqual(result["toast_employee_id"], "server-1")
        self.assertEqual(result["toast_discount_approver_ids"], "discount-approver")
        self.assertEqual(result["toast_payment_void_approver_ids"], "void-approver")
        self.assertEqual(result["manager_id"], "")
        self.assertEqual(result["toast_gratuity_approval_status"], "not_exposed_by_orders_api")
        self.assertNotIn("omit@example.test", str(candidates))

    def test_duplicate_check_numbers_need_unique_gratuity_amount_match(self):
        first = order()
        other = copy.deepcopy(first)
        other.update(guid="order-2", displayNumber="195")
        other["checks"][0]["guid"] = "check-2"
        candidates = compact_checks([first, other], DAY)
        result = match_row(row(), candidates, "restaurant-1", DAY)
        self.assertEqual(result["toast_match_status"], "ambiguous_check")
        self.assertEqual(result["toast_order_id"], "")
        other["checks"][0]["appliedServiceCharges"][0]["chargeAmount"] = Decimal("2.00")
        result = match_row(row(), compact_checks([first, other], DAY), "restaurant-1", DAY)
        self.assertEqual(result["toast_order_id"], "order-1")

    def test_missing_or_changed_gratuity_is_reported(self):
        source = order()
        source["checks"][0]["appliedServiceCharges"] = None
        result = match_row(row(), compact_checks([source], DAY), "restaurant-1", DAY)
        self.assertEqual(result["toast_match_status"], "gratuity_not_found")
        result = match_row(dict(row(), amount="100"), compact_checks([order()], DAY), "restaurant-1", DAY)
        self.assertEqual(result["toast_match_status"], "gratuity_amount_mismatch")
        result = match_row(row(), [], "restaurant-1", DAY)
        self.assertEqual(result["toast_match_status"], "check_not_found")

    def test_nested_selections_and_nullable_arrays(self):
        source = order()
        check = source["checks"][0]
        check.update(appliedServiceCharges=None, appliedDiscounts=None, payments=None)
        check["selections"] = [{"modifiers": [{
            "guid": "selection-1", "displayName": "Suggested Gratuity", "price": Decimal("1.05"),
            "appliedDiscounts": [{"approver": {"guid": "item-approver"}}],
        }]}]
        result = match_row(row(), compact_checks([source], DAY), "restaurant-1", DAY)
        self.assertEqual(result["toast_gratuity_type"], "selection")
        self.assertEqual(result["toast_discount_approver_ids"], "item-approver")

    def test_receipt_date_and_response_business_date(self):
        self.assertEqual(receipt_date(row()), DAY)
        with self.assertRaises(ValueError):
            receipt_date(dict(row(), receipt_number="bad receipt"))
        with self.assertRaisesRegex(ValueError, "business date"):
            compact_checks([order()], date(2026, 10, 2))
        self.assertEqual(len(compact_checks([order(), order()], DAY)), 1)


if __name__ == "__main__":
    unittest.main()
