import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock
from uuid import UUID

import requests

from db_utils.r365_utils import R365ODataClient
from src.suggested_gratuity import build_report, fetch_by_ids, read_input


def uid(number):
    return str(UUID(int=number))


class SuggestedGratuityTests(unittest.TestCase):
    def test_join_preserves_line_items_and_does_not_guess_employee_id(self):
        source = {"salesdetailid": uid(1), "location": uid(2), "menuitem": "Suggested Gratuity"}
        detail = {"salesID": uid(3), "location": uid(2), "menuitem": "Concept - Suggested Gratuity"}
        ticket = {"location": uid(2), "receiptNumber": "001 - 7", "checkNumber": "001", "server": "A Server"}
        rows = [source, dict(source, salesdetailid=uid(4)), dict(source, salesdetailid=uid(5))]
        report = build_report(rows, {uid(1): detail, uid(4): detail}, {uid(3): ticket}, {uid(2): "Store"}, {}, "suggested gratuity")
        self.assertEqual(len(report), 3)
        self.assertEqual(report[0]["check_number"], "001")
        self.assertEqual(report[0]["receipt_number"], "001 - 7")
        self.assertIsNone(report[0]["employee_id"])
        self.assertIsNone(report[0]["manager_id"])
        self.assertEqual(report[1]["sales_id"], report[0]["sales_id"])
        self.assertEqual(report[2]["match_status"], "detail_not_found")
        self.assertIsNone(report[2]["check_number"])
        enriched = build_report([source], {uid(1): detail}, {uid(3): ticket}, {uid(2): "Store"}, {uid(3): uid(9)}, "Suggested Gratuity")
        self.assertEqual(enriched[0]["employee_id"], uid(9))

    def test_location_mismatch_does_not_attach_another_stores_check(self):
        source = {"salesdetailid": uid(1), "location": uid(2)}
        detail = {"salesID": uid(3), "location": uid(2), "menuitem": "Suggested Gratuity"}
        ticket = {"location": uid(9), "checkNumber": "wrong"}
        report = build_report([source], {uid(1): detail}, {uid(3): ticket}, {}, {}, "Suggested Gratuity")
        self.assertEqual(report[0]["match_status"], "location_mismatch")
        self.assertIsNone(report[0]["check_number"])

    def test_empty_csv_and_substring_filter(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.csv"
            header = "salesdetailid,menuitem,date,location\n"
            path.write_text(header)
            self.assertEqual(read_input(path, "Suggested Gratuity")[1:], ([], 0))
            path.write_text(header + f"{uid(1)},Prefix SUGGESTED GRATUITY suffix,2026-10-03,{uid(2)}\n{uid(3)},Other,2026-10-03,{uid(2)}\n")
            _, rows, skipped = read_input(path, "Suggested Gratuity")
            self.assertEqual(len(rows), 1)
            self.assertEqual(skipped, 1)

    def test_lookup_windows_stay_under_31_days_across_months(self):
        client = Mock()
        client.get_all.return_value = []
        rows = [{"salesdetailid": uid(n), "date": day} for n, day in enumerate(
            ["2025-12-31", "2026-01-01", "2026-01-31", "2026-02-01", "2026-10-03"], 1)]
        rows.extend({"salesdetailid": uid(n), "date": "2026-10-03"} for n in range(6, 30))
        fetch_by_ids(client, "SalesDetail", "salesdetailID", rows, "salesdetailid")
        for call in client.get_all.call_args_list:
            query = call.kwargs["params"]["$filter"]
            self.assertLessEqual(query.count("salesdetailID eq "), 10)
            start = date.fromisoformat(query.split("date ge ")[1][:10])
            end = date.fromisoformat(query.split("date lt ")[1][:10])
            self.assertLessEqual((end - start).days, 31)

    def test_pagination_and_failed_continuation(self):
        client = R365ODataClient()
        client.session.close()
        client.base_url = "https://example.test/views/"
        client.session = Mock()
        first = Mock(url=client.base_url + "SalesDetail")
        first.json.return_value = {"value": [{"id": 1}], "@odata.nextLink": "SalesDetail?page=2"}
        second = Mock(url=client.base_url + "SalesDetail?page=2")
        second.json.return_value = {"value": [{"id": 2}]}
        client.session.get.side_effect = [first, second]
        self.assertEqual(list(client.get_all("SalesDetail", {"$filter": "example"})), [{"id": 1}, {"id": 2}])
        self.assertIsNone(client.session.get.call_args_list[1].kwargs["params"])
        second.raise_for_status.side_effect = requests.HTTPError("failed")
        client.session.get.side_effect = [first, second]
        with self.assertRaises(requests.HTTPError):
            list(client.get_all("SalesDetail"))
        first.json.return_value["@odata.nextLink"] = "https://another.test/page"
        client.session.get.side_effect = [first]
        with self.assertRaisesRegex(ValueError, "origin"):
            list(client.get_all("SalesDetail"))


if __name__ == "__main__":
    unittest.main()
