"""Configuration pagination, validation, dispatch, and isolated PostgreSQL tests."""

import importlib
import io
import os
import unittest
from contextlib import redirect_stderr
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import UUID

import psycopg2
import requests

from src.toast_sync import configuration as config

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 14, 12, tzinfo=UTC)
RID = UUID(int=700001)
LOCATIONS = [{"id": 1, "toast_guid": RID}]
dispatcher = importlib.import_module("src.toast-api-update")


def record(number=1, **kwargs):
    return {"guid": str(UUID(int=number)), "entityType": "MenuItem", "name": "Example", **kwargs}


def response(payload, token=None):
    return Mock(json=Mock(return_value=payload), headers={"Toast-Next-Page-Token": token} if token else {})


class ConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.object(config.time, "sleep"))

    def test_pagination_keeps_filter_and_reads_after_empty_page(self):
        client = Mock()
        client.request.side_effect = [response([], "next"), response([record()])]
        self.assertEqual(config.fetch_records(client, "menu_items", RID, NOW), [record()])
        self.assertEqual(client.request.call_args_list[0].kwargs["params"], {"lastModified": "2026-10-14T12:00:00.000+0000"})
        self.assertEqual(client.request.call_args_list[1].kwargs["params"], {"lastModified": "2026-10-14T12:00:00.000+0000", "pageToken": "next"})
        self.assertEqual(client.request.call_args.args[0], "/config/v2/menuItems")

    def test_full_load_omits_filter_and_tip_ignores_it(self):
        client = Mock()
        client.request.return_value = response([])
        self.assertEqual(config.fetch_records(client, "menu_items", RID), [])
        client.request.assert_called_once_with("/config/v2/menuItems", RID, params={})
        client.reset_mock()
        payload = {"enabled": False, "percentage": 0}
        client.request.return_value = response(payload)
        self.assertEqual(config.fetch_records(client, "tip_withholding", RID, NOW), [payload])
        client.request.assert_called_once_with("/config/v2/tipWithholding", RID)
        row = config.normalize_record("tip_withholding", payload, RID, NOW)
        self.assertIsNone(row["id"])
        self.assertFalse(row["enabled"])
        self.assertEqual(row["percentage"], Decimal(0))
        client.request.return_value = response({})
        with patch.object(config, "upsert_rows", wraps=config.upsert_rows), \
                patch("src.toast_sync.writing.DatabaseConnection") as db:
            counts = config.sync(client, LOCATIONS, resources=["tip_withholding"], last_modified=NOW)
        self.assertEqual(counts["returned"], 0)
        self.assertEqual(counts["api_errors"], 0)
        db.assert_not_called()

    def test_timestamp_offset_and_precision_match_toast_format(self):
        client = Mock()
        client.request.return_value = response([])
        cutoff = datetime.fromisoformat("2026-10-14T08:00:00.123456-04:00")
        config.fetch_records(client, "menu_items", RID, cutoff)
        self.assertEqual(client.request.call_args.kwargs["params"],
                         {"lastModified": "2026-10-14T12:00:00.123+0000"})

    def test_failed_or_repeated_pages_never_write_partial_results(self):
        for tail in (requests.Timeout(), response([record(2)], "next"), response({"unexpected": []})):
            with self.subTest(tail=type(tail).__name__), patch.object(config, "upsert_rows") as write:
                client = Mock()
                client.request.side_effect = [response([record()], "next"), tail]
                counts = config.sync(client, LOCATIONS, resources=["menu_items"])
                self.assertEqual(counts["api_errors"], 1)
                write.assert_not_called()

    def test_all_24_endpoints_dispatch_and_use_restaurant_scoped_keys(self):
        client = Mock()
        client.request.side_effect = lambda endpoint, *args, **kwargs: response(
            {"enabled": True} if endpoint.endswith("tipWithholding") else [record()])
        with patch.object(config, "upsert_rows", return_value={"inserted": 1, "updated": 0}) as write:
            counts = config.sync(client, LOCATIONS, last_modified=NOW)
        self.assertEqual(len(config.ENDPOINTS), 24)
        self.assertEqual(counts["inserted"], 24)
        self.assertEqual(counts["rejected"], 0)
        self.assertEqual({c.args[0] for c in client.request.call_args_list},
                         {"/config/v2/" + path for path in config.ENDPOINTS.values()})
        for call in write.call_args_list:
            resource = call.args[1]
            self.assertEqual(call.args[0][0]["restaurant_id"], RID)
            self.assertEqual(set(call.args[0][0]), set(config.TABLE_COLUMNS[resource]))
            self.assertEqual(call.kwargs["conflict_columns"], ("restaurant_id",) if resource == "tip_withholding" else ("restaurant_id", "id"))

    def test_normalization_preserves_identifiers_nulls_nested_data_and_zeroes(self):
        payload = record(sku="000123", plu="0001", calories=0, inheritOptionGroups=False,
                         optionGroups=[{"guid": str(UUID(int=9)), "externalId": "source:9"}], images=[])
        row = config.normalize_record("menu_items", payload, RID, NOW)
        self.assertEqual(row["sku"], "000123")
        self.assertEqual(row["plu"], "0001")
        self.assertEqual(row["calories"], 0)
        self.assertFalse(row["inherit_option_groups"])
        self.assertEqual(row["option_groups"].adapted, payload["optionGroups"])
        self.assertEqual(row["images"].adapted, [])
        self.assertIsNone(row["external_id"])
        group = config.normalize_record("menu_groups", record(menu={"guid": str(UUID(int=5))}), RID, NOW)
        self.assertEqual(group["menu_id"], UUID(int=5))
        self.assertEqual(group["menu"].adapted, {"guid": str(UUID(int=5))})

    def test_invalid_records_and_duplicates_reject_entire_batch(self):
        bad = [None, {}, record(guid="bad"), record(calories=True), record(optionGroups=["bad"]),
               record(name=[]), record(inheritOptionGroups="false"), record(images=[{"x": float("nan")}]), record()]
        for payload in bad:
            with self.subTest(payload=payload), patch.object(config, "upsert_rows") as write:
                client = Mock()
                client.request.return_value = response([record(), payload])
                counts = config.sync(client, LOCATIONS, resources=["menu_items"])
                self.assertEqual(counts["rejected"], 1)
                self.assertEqual(counts["skipped"], 1)
                write.assert_not_called()

    def test_dry_run_empty_response_and_database_failure(self):
        client = Mock()
        client.request.return_value = response([record()])
        with patch.object(config, "upsert_rows") as write:
            counts = config.sync(client, LOCATIONS, dry_run=True, resources=["menu_items"])
            self.assertEqual(counts["returned"], 1)
            write.assert_not_called()
        with patch("src.toast_sync.writing.DatabaseConnection") as db:
            client.request.return_value = response([])
            config.sync(client, LOCATIONS, resources=["menu_items"])
            db.assert_not_called()
        client.request.return_value = response([record()])
        with patch.object(config, "upsert_rows", side_effect=[psycopg2.OperationalError(), {"inserted": 1, "updated": 0}]):
            counts = config.sync(client, LOCATIONS, resources=["menu_items", "menus"])
            self.assertEqual(counts["database_errors"], 1)
            self.assertEqual(counts["inserted"], 1)

    def test_cli_selection_timestamp_validation_and_failure_exit(self):
        with patch.object(dispatcher, "get_configured_restaurants", return_value=LOCATIONS), \
                patch.object(dispatcher, "ToastClient"), \
                patch.object(config, "sync", return_value={"api_errors": 0, "rejected": 0}) as sync:
            self.assertEqual(dispatcher.main(["--sync", "configuration", "--config-resources", "menu_items", "menu_items",
                                             "--config-last-modified", "2026-10-14T12:00:00Z", "--dry-run"]), 0)
            self.assertEqual(sync.call_args.kwargs["resources"], ("menu_items",))
            self.assertEqual(sync.call_args.kwargs["last_modified"], NOW)
            sync.return_value = {"api_errors": 0, "rejected": 0, "database_errors": 1}
            self.assertEqual(dispatcher.main(["--sync", "configuration"]), 1)
        for args in (["--config-last-modified", "2026-10-14T12:00:00Z"],
                     ["--sync", "configuration", "--config-last-modified", "2026-10-14"],
                     ["--sync", "configuration", "--config-last-modified", ""],
                     ["--config-resources", "menus"]):
            with self.subTest(args=args), patch.object(dispatcher, "ToastClient") as client, \
                    redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                dispatcher.main(args)
            client.assert_not_called()


@unittest.skipUnless(os.environ.get("TOAST_CONFIG_TEST_PG_SOCKET"), "Requires isolated temporary PostgreSQL")
class ConfigurationPostgresTests(unittest.TestCase):
    def test_schema_upserts_restaurant_isolation_json_nulls_rollback_and_migration(self):
        socket = os.environ["TOAST_CONFIG_TEST_PG_SOCKET"]
        self.assertTrue(socket.startswith("/tmp/toast-config-pg-"))
        connect = psycopg2.connect
        def test_connection(*args, **kwargs):
            return connect(host=socket, port=55441, dbname="postgres", user="toast_test")
        connection = test_connection()
        self.addCleanup(connection.close)
        schema = (ROOT / "db_utils/schema/toast_configuration.sql").read_text()
        migration = (ROOT / "db_utils/schema/toast_configuration_migration.sql").read_text()
        with connection.cursor() as cur:
            # Minimal legacy schema recreates the actual relationship being retired.
            cur.execute('CREATE SCHEMA toast')
            cur.execute('CREATE TABLE toast.service_charges (id uuid PRIMARY KEY, restaurant_id uuid, order_id uuid, service_charge_id uuid)')
            cur.execute('CREATE TABLE toast.applied_taxes (id uuid PRIMARY KEY, restaurant_id uuid, service_charge_id uuid)')
            cur.execute('INSERT INTO toast.service_charges VALUES (%s, %s, %s, %s)', (UUID(int=10), RID, UUID(int=11), UUID(int=12)))
            cur.execute('INSERT INTO toast.applied_taxes VALUES (%s, %s, %s)', (UUID(int=13), RID, UUID(int=10)))
        connection.commit()
        # Setup refuses to silently reuse the old, incompatible table.
        with self.assertRaises(psycopg2.Error), connection.cursor() as cur:
            cur.execute(schema)
        connection.rollback()
        with connection.cursor() as cur:
            cur.execute('CREATE VIEW toast.legacy_charge_dependency AS SELECT id FROM toast.service_charges')
        connection.commit()
        with self.assertRaises(psycopg2.Error), connection.cursor() as cur:
            cur.execute(migration)
        connection.rollback()
        with connection.cursor() as cur:
            cur.execute('SELECT count(*) FROM toast.service_charges')
            self.assertEqual(cur.fetchone()[0], 1)
            cur.execute("SELECT count(*) FROM information_schema.columns WHERE table_schema='toast' AND table_name='applied_taxes' AND column_name='configured_service_charge_id'")
            self.assertEqual(cur.fetchone()[0], 0)
            cur.execute('DROP VIEW toast.legacy_charge_dependency')
        with connection.cursor() as cur:
            cur.execute(migration)
            cur.execute(schema)
            cur.execute(migration)
            cur.execute(schema)
            cur.execute('SELECT configured_service_charge_id, service_charge_id FROM toast.applied_taxes')
            self.assertEqual(cur.fetchone(), (UUID(int=12), UUID(int=10)))
        connection.commit()
        with patch("db_utils.dbconnect.psycopg2.connect", side_effect=test_connection):
            # Verify every table matches the declared writer, including singleton keys.
            client = Mock()
            client.request.side_effect = lambda endpoint, *args, **kwargs: response(
                {"enabled": False, "percentage": 0} if endpoint.endswith("tipWithholding") else [record()])
            counts = config.sync(client, LOCATIONS)
            self.assertEqual(counts["inserted"], 24)
            self.assertEqual(counts["database_errors"], 0)
            self.assertEqual(config.sync(client, LOCATIONS)["updated"], 24)
            rows = [config.normalize_record("menu_items", record(i, optionGroups=[], plu="0001"), RID, NOW) for i in range(2, 104)]
            self.assertEqual(config.upsert_rows(rows, "menu_items", config.TABLE_COLUMNS["menu_items"], conflict_columns=("restaurant_id", "id")), {"inserted": 102, "updated": 0})
            rows[0]["name"] = "Changed"
            rows[0]["plu"] = None
            config.upsert_rows(rows, "menu_items", config.TABLE_COLUMNS["menu_items"], conflict_columns=("restaurant_id", "id"))
            other = dict(rows[0], restaurant_id=UUID(int=700002), name="Other location")
            config.upsert_rows([other], "menu_items", config.TABLE_COLUMNS["menu_items"], conflict_columns=("restaurant_id", "id"))
            rows[0]["name"] = "Must roll back"
            rows[-1]["calories"] = "bad integer"
            with self.assertRaises(psycopg2.Error):
                config.upsert_rows(rows, "menu_items", config.TABLE_COLUMNS["menu_items"], conflict_columns=("restaurant_id", "id"))
        with connection.cursor() as cur:
            cur.execute('SELECT name, plu, option_groups FROM toast.menu_items WHERE restaurant_id=%s AND id=%s', (RID, UUID(int=2)))
            self.assertEqual(cur.fetchone(), ("Changed", None, []))
            cur.execute('SELECT count(*) FROM toast.menu_items WHERE id=%s', (UUID(int=2),))
            self.assertEqual(cur.fetchone()[0], 2)
            cur.execute("SELECT count(*) FROM information_schema.columns WHERE table_schema='toast' AND table_name='service_charges' AND column_name='order_id'")
            self.assertEqual(cur.fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
