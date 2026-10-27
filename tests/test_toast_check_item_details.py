import io
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

import pandas as pd
from src import toast_check_item_details as module


def order():
    return {"displayNumber": "12", "checks": [{
        "displayNumber": "13", "selections": [{
            "guid": "selection-id", "displayName": "Lunch", "quantity": 2, "price": 15,
        }],
    }]}


class CheckItemDetailsTests(unittest.TestCase):
    def test_filters_orders_and_handles_null_checks(self):
        client = Mock()
        client.get_response_data.return_value = [
            {"displayNumber": "other"}, order(),
            {"displayNumber": "12", "checks": None},
        ]
        frame = module.get_order_guid(client, "restaurant-id", "20261026", 12)
        self.assertEqual(frame["item_name"].tolist(), ["Lunch"])
        self.assertEqual(frame["check_number"].tolist(), ["13"])
        client.get_response_data.assert_called_once_with(
            "/orders/v2/ordersBulk", "restaurant-id", params={"businessDate": "20261026"},
        )

    def test_no_match_preserves_csv_headers(self):
        client = Mock()
        client.get_response_data.return_value = [order()]
        with redirect_stdout(io.StringIO()) as output:
            frame = module.get_order_guid(client, "restaurant-id", "20261026", 99)
        self.assertTrue(frame.empty)
        self.assertEqual(list(frame.columns), module.SELECTION_COLUMNS)
        self.assertIn("No order 99 found", output.getvalue())

    def test_single_order_uses_unflattened_response(self):
        client = Mock()
        client.request.return_value.json.return_value = order()
        frame = module.get_ticket_selections(client, "restaurant-id", "order-id")
        self.assertEqual(frame["item_guid"].tolist(), ["selection-id"])
        self.assertEqual(client.get_response_data.call_count, 0)

    def test_help_and_invalid_arguments_do_not_connect(self):
        for args, code in ((["--help"], 0), ([], 2), ([
            "--business_date", "20260230", "--location_name", "Test", "--order_number", "12",
        ], 2)):
            with self.subTest(args=args), patch("sys.argv", ["report", *args]), \
                    patch.object(module, "ToastClient") as client, \
                    patch.object(module, "DatabaseConnection") as db, \
                    redirect_stdout(io.StringIO()), patch("sys.stderr", io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    module.main()
                self.assertEqual(error.exception.code, code)
                self.assertEqual(client.call_count, 0)
                self.assertEqual(db.call_count, 0)

    def test_unknown_location_stops_before_authentication(self):
        with patch.object(module, "get_arguments", return_value=("20261026", "Unknown", 12)), \
                patch.object(module, "DatabaseConnection") as db, \
                patch.object(module, "ToastClient") as client:
            db.return_value.__enter__.return_value.cur.fetchone.return_value = None
            with self.assertRaisesRegex(SystemExit, "Location not found"):
                module.main()
            self.assertEqual(client.call_count, 0)

    def test_export_creates_directory_and_separates_locations(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(module, "get_arguments") as args, \
                patch.object(module, "DatabaseConnection") as db, \
                patch.object(module, "ToastClient") as client, \
                patch.object(module, "Path", return_value=Path(directory) / "output"), \
                redirect_stdout(io.StringIO()):
            db.return_value.__enter__.return_value.cur.fetchone.return_value = {"toast_guid": "restaurant-id"}
            client.return_value.get_response_data.return_value = [order()]
            for location in ("Test / One", "Test Two"):
                args.return_value = ("20261026", location, 12)
                module.main()
            files = list((Path(directory) / "output").glob("*.csv"))
            self.assertEqual(len(files), 2)
            self.assertEqual(pd.read_csv(files[0])["item_qty"].tolist(), [2])
            self.assertEqual(client.call_args.kwargs, {"load_locations": False})


if __name__ == "__main__":
    unittest.main()
