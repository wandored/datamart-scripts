import csv
import importlib
import io
import tempfile
import unittest
from contextlib import redirect_stderr
from datetime import date
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

from db_utils.r365_csv_importers import (
    POS_ITEM_COLUMNS, MENU_ITEM_COLUMNS, get_pos_items_csv, get_menu_items_csv,
)

module = importlib.import_module('src.r365-api-update')


class CsvImportTests(unittest.TestCase):
    def export(self, columns, rows):
        directory = self.enterContext(tempfile.TemporaryDirectory())
        path = Path(directory) / 'export.csv'
        with path.open('w', encoding='utf-8-sig', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def pos(self, **changes):
        row = {'Id': str(UUID(int=1)), 'PosItemId': '9007199254740993',
               'POS Item Name': 'NA', 'PLU': '00123', 'is Menu Item': 'Yes'}
        row.update(changes)
        return row

    def test_pos_identifiers_text_and_nulls_preserved(self):
        frame = get_pos_items_csv(self.export(POS_ITEM_COLUMNS, [self.pos()]))
        row = frame.iloc[0]
        self.assertEqual(row['id'], UUID(int=1))
        self.assertEqual(row['pos_item_id'], 9007199254740993)
        self.assertIsInstance(row['pos_item_id'], int)
        self.assertEqual(row['pos_item_name'], 'NA')
        self.assertEqual(row['plu'], '00123')
        self.assertIs(row['is_menu_item'], True)
        self.assertIsNone(row['sku'])
        self.assertEqual(list(frame.columns), list(POS_ITEM_COLUMNS.values()))

    def test_menu_nullable_recipe_id_and_duplicate_names_are_preserved(self):
        rows = [
            {'Id': str(UUID(int=1)), 'Name': 'Same', 'Is Recipe Assigned': 'No'},
            {'Id': str(UUID(int=2)), 'Name': 'Same', 'Is Recipe Assigned': 'Yes',
             'Recipe Id': str(UUID(int=3)), 'Created on': '10/18/2026 12:00 PM'},
        ]
        frame = get_menu_items_csv(self.export(MENU_ITEM_COLUMNS, rows))
        self.assertIsNone(frame.iloc[0]['recipe_id'])
        self.assertIs(frame.iloc[0]['is_recipe_assigned'], False)
        self.assertEqual(frame.iloc[1]['recipe_id'], UUID(int=3))
        self.assertEqual(frame.iloc[1]['created_on'], '10/18/2026 12:00 PM')
        self.assertEqual(len(frame), 2)

    def test_invalid_keys_types_and_duplicate_ids_fail(self):
        for changes in [{'Id': ''}, {'Id': 'bad'}, {'PosItemId': ''},
                        {'PosItemId': '1.5'}, {'PosItemId': str(2**63)},
                        {'PosItemId': '-1'}, {'is Menu Item': 'maybe'}]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                get_pos_items_csv(self.export(POS_ITEM_COLUMNS, [self.pos(**changes)]))
        for other in [self.pos(PosItemId='1'), self.pos(Id=str(UUID(int=2)))]:
            with self.subTest(other=other), self.assertRaisesRegex(ValueError, 'duplicate'):
                get_pos_items_csv(self.export(POS_ITEM_COLUMNS, [self.pos(), other]))
        with self.assertRaisesRegex(ValueError, 'recipe_id'):
            get_menu_items_csv(self.export(MENU_ITEM_COLUMNS, [
                {'Id': str(UUID(int=1)), 'Recipe Id': 'invalid'},
            ]))

    def test_missing_malformed_changed_headers_and_empty_exports(self):
        with self.assertRaises(FileNotFoundError):
            get_pos_items_csv(Path('/nonexistent/PosItems.csv'))
        with self.assertRaisesRegex(ValueError, 'headers'):
            get_pos_items_csv(self.export(['Id'], []))
        path = self.export(POS_ITEM_COLUMNS, [])
        with path.open('a') as stream:
            stream.write('too,few,columns\n')
        with self.assertRaisesRegex(ValueError, 'malformed'):
            get_pos_items_csv(path)
        for columns, fetch in [(POS_ITEM_COLUMNS, get_pos_items_csv),
                               (MENU_ITEM_COLUMNS, get_menu_items_csv)]:
            frame = fetch(self.export(columns, []))
            self.assertTrue(frame.empty)
            self.assertEqual(list(frame.columns), list(columns.values()))

    def test_selected_csv_only_needs_no_api_or_calendar(self):
        frame = get_pos_items_csv(self.export(POS_ITEM_COLUMNS, [self.pos()]))
        with patch.object(module, 'get_pos_items_csv', return_value=frame), \
                patch.object(module, 'get_menu_items_csv') as menu, \
                patch.object(module, 'sync_csv_tables') as sync, \
                patch.object(module, 'R365Client') as client, \
                patch.object(module.fiscal_calendar, 'fiscal_week_to_date') as calendar:
            module.main(['--week', '--tables', 'pos_items', 'pos_items'])
            self.assertIs(sync.call_args.args[0]['pos_items'], frame)
            sync.assert_called_once()
            menu.assert_not_called()
            client.assert_not_called()
            calendar.assert_not_called()

    def test_weekly_defaults_import_csv_once_before_api_days(self):
        events = []
        with patch.object(module, 'load_csv_tables', return_value={'pos_items': object()}) as load, \
                patch.object(module, 'sync_csv_tables', side_effect=lambda frames: events.append('csv')) as sync, \
                patch.object(module, 'R365Client'), patch.object(module, 'get_locations', return_value=[]), \
                patch.object(module, 'sync_modified_tables', side_effect=lambda *args: events.append('api')) as api, \
                patch.object(module.fiscal_calendar, 'fiscal_week_to_date',
                             return_value=(date(2026, 10, 14), date(2026, 10, 16))):
            module.main(['--week'])
            self.assertTrue(set(module.CSV_TABLES).issubset(load.call_args.args[0]))
            sync.assert_called_once()
            self.assertEqual(events, ['csv', 'api', 'api', 'api'])
            self.assertFalse(set(module.CSV_TABLES).intersection(api.call_args.args[2]))

    def test_daily_default_does_not_require_csv_files_and_bulk_rejects_them(self):
        with patch.object(module, 'load_csv_tables') as load, \
                patch.object(module, 'R365Client'), patch.object(module, 'get_locations', return_value=[]), \
                patch.object(module, 'sync_modified_tables'):
            module.main(['--daily'])
            load.assert_not_called()
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                module.main(['--bulk', '--business-date', '2026-10-18', '--tables', 'pos_items'])
            load.assert_not_called()

    def test_missing_second_export_prevents_writes(self):
        with patch.object(module, 'get_pos_items_csv'), \
                patch.object(module, 'get_menu_items_csv', side_effect=FileNotFoundError), \
                patch.object(module, 'sync_csv_tables') as sync, \
                patch.object(module, 'R365Client') as client:
            with self.assertRaises(FileNotFoundError):
                module.main(['--week', '--tables', 'pos_items', 'menu_items'])
            sync.assert_not_called()
            client.assert_not_called()

    def test_both_tables_share_transaction_and_propagate_write_failure(self):
        frames = {'pos_items': [1], 'menu_items': [2]}
        with patch.object(module, 'DatabaseConnection') as connection, \
                patch.object(module, 'write_to_db', return_value=1) as write:
            module.sync_csv_tables(frames)
            self.assertEqual([call.args[1:] for call in write.call_args_list],
                             [('pos_items', 'r365'), ('menu_items', 'r365')])
            self.assertTrue(all(call.kwargs['db'] is connection.return_value.__enter__.return_value
                                for call in write.call_args_list))
            write.side_effect = [1, RuntimeError('failed')]
            with self.assertRaises(RuntimeError):
                module.sync_csv_tables(frames)
            self.assertIs(connection.return_value.__exit__.call_args.args[0], RuntimeError)
