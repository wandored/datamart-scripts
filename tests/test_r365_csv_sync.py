import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

import pandas as pd

from db_utils.r365_csv_importers import (
    INGREDIENT_COLUMNS, INGREDIENT_KEYS, RECIPE_COLUMNS,
    get_ingredients_csv, get_recipe_items_csv,
)
from test_r365_invoice_sync import module


class RecipeCsvTests(unittest.TestCase):
    def export(self, columns, rows):
        directory = self.enterContext(tempfile.TemporaryDirectory())
        path = Path(directory) / 'export.csv'
        pd.DataFrame(rows, columns=columns).fillna('').to_csv(
            path, index=False, encoding='utf-8-sig'
        )
        return path

    def test_recipe_fields_preserve_ids_numbers_nulls_and_precision(self):
        path = self.export(RECIPE_COLUMNS, [{
            'ID': str(UUID(int=1)), 'Name': 'NA', 'Number': '00123',
            'Avg Cost': '0.1234567890123456789', 'Active': 'False',
        }])
        row = get_recipe_items_csv(path).iloc[0]
        self.assertEqual(row['id'], UUID(int=1))
        self.assertEqual(row['name'], 'NA')
        self.assertEqual(row['number'], '00123')
        self.assertEqual(row['avg_cost'], Decimal('0.1234567890123456789'))
        self.assertEqual(row['active'], False)
        self.assertIsNone(row['description'])

    def test_ingredient_units_distinguish_lines(self):
        rows = [{'Recipe': 'Recipe', 'Item': 'Item', 'UofM': unit,
                 'Qty': '1.25', 'Yield percent': '80'} for unit in ['Each', 'Oz']]
        frame = get_ingredients_csv(self.export(INGREDIENT_COLUMNS, rows))
        self.assertEqual(len(frame), 2)
        self.assertEqual(frame.iloc[0]['yield_percent'], Decimal('80'))
        with self.assertRaisesRegex(ValueError, 'duplicate conflict keys'):
            get_ingredients_csv(self.export(INGREDIENT_COLUMNS, rows + rows[:1]))

    def test_invalid_keys_numbers_and_schema_fail(self):
        for changes in [{'Recipe': ''}, {'Qty': 'bad'}, {'Qty': 'NaN'}]:
            row = {'Recipe': 'Recipe', 'Item': 'Item', 'UofM': 'Each', 'Qty': '1'}
            row.update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                get_ingredients_csv(self.export(INGREDIENT_COLUMNS, [row]))
        with self.assertRaisesRegex(ValueError, 'columns changed'):
            get_ingredients_csv(self.export(['Recipe'], []))
        with self.assertRaises(FileNotFoundError):
            get_ingredients_csv(Path('/nonexistent/ingredients.csv'))

    def test_empty_exports_have_stable_columns(self):
        for columns, fetch in [(RECIPE_COLUMNS, get_recipe_items_csv),
                               (INGREDIENT_COLUMNS, get_ingredients_csv)]:
            frame = fetch(self.export(columns, []))
            self.assertTrue(frame.empty)
            self.assertEqual(list(frame.columns), list(columns.values()))

    def test_missing_second_file_prevents_weekly_writes(self):
        with patch.object(module, 'get_recipe_items_csv'), \
                patch.object(module, 'get_ingredients_csv', side_effect=FileNotFoundError), \
                patch.object(module, 'DatabaseConnection') as db, \
                patch.object(module, 'r365_locations') as locations:
            with self.assertRaises(FileNotFoundError):
                module.sync_weekly_tables(None)
            db.assert_not_called()
            locations.assert_not_called()

    def test_recipe_writes_share_transaction_and_use_correct_keys(self):
        with patch.object(module, 'DatabaseConnection') as connection, \
                patch.object(module, 'write_to_db', return_value=1) as write:
            db = connection.return_value.__enter__.return_value
            recipes, ingredients = object(), object()
            module.sync_recipe_tables(recipes, ingredients)
            self.assertEqual(write.call_args_list[0].args, (recipes, 'recipe_items', 'r365'))
            self.assertEqual(write.call_args_list[1].args, (ingredients, 'ingredients', 'r365'))
            self.assertEqual(write.call_args_list[1].kwargs,
                             {'db': db, 'conflict_columns': INGREDIENT_KEYS})
            self.assertIs(write.call_args_list[0].kwargs['db'], db)

    def test_write_failure_exits_transaction_with_error(self):
        with patch.object(module, 'DatabaseConnection') as connection, \
                patch.object(module, 'write_to_db', side_effect=[1, RuntimeError('failed')]):
            with self.assertRaises(RuntimeError):
                module.sync_recipe_tables(object(), object())
            self.assertIs(connection.return_value.__exit__.call_args.args[0], RuntimeError)
