import importlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import UUID

module = importlib.import_module('src.menu-item-map')


class MenuMappingTests(unittest.TestCase):
    def toast(self, restaurant=1, concept='Casual', name='Burger'):
        return dict(restaurant_id=restaurant, toast_restaurant_id=UUID(int=restaurant),
                    toast_menu_item_id=UUID(int=99), toast_name=name,
                    concept=concept, r365_guid=UUID(int=restaurant + 100))

    def sources(self):
        return ([dict(id=UUID(int=10), name='Casual - Burger'),
                 dict(id=UUID(int=20), name='Steakhouse - Burger')],
                [dict(pos_item_id=1000, menu_item='Casual - Burger'),
                 dict(pos_item_id=2000, menu_item='Steakhouse - Burger')])

    def test_restaurants_share_concept_targets_but_keep_toast_scope(self):
        menus, pos = self.sources()
        rows, report = module.build_mapping([
            self.toast(1), self.toast(2), self.toast(3, 'Steakhouse'),
        ], menus, pos, [])
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][2:], rows[1][2:])
        self.assertEqual(rows[2][2:], (UUID(int=20), 2000))
        self.assertTrue(all(row['status'] == 'matched' for row in report))

    def test_case_whitespace_normalized_but_punctuation_preserved(self):
        menus, pos = self.sources()
        rows, _ = module.build_mapping([self.toast(concept=' CASUAL ', name=' burger ')], menus, pos, [])
        self.assertEqual(len(rows), 1)
        rows, report = module.build_mapping([self.toast(name='Burger!')], menus, pos, [])
        self.assertFalse(rows)
        self.assertEqual(report[0]['status'], 'unmatched_menu')

    def test_ambiguous_candidates_never_picked(self):
        menus, pos = self.sources()
        for more_menus, more_pos, status in [
            (menus + [dict(id=UUID(int=30), name='casual - burger')], pos, 'ambiguous_menu'),
            (menus, pos + [dict(pos_item_id=3000, menu_item='Casual - Burger')], 'ambiguous_pos'),
            (menus, [], 'unmatched_pos'),
        ]:
            rows, report = module.build_mapping([self.toast()], more_menus, more_pos, [])
            self.assertFalse(rows)
            self.assertEqual(report[0]['status'], status)

    def test_missing_links_concept_name_and_duplicate_restaurant_assignments(self):
        menus, pos = self.sources()
        for changes, status in [({'restaurant_id': None}, 'missing_restaurant_link'),
                                ({'r365_guid': None}, 'missing_restaurant_link'),
                                ({'concept': None}, 'missing_concept'),
                                ({'toast_name': None}, 'missing_toast_name')]:
            row = self.toast()
            row.update(changes)
            rows, report = module.build_mapping([row], menus, pos, [])
            self.assertFalse(rows)
            self.assertEqual(report[0]['status'], status)
        rows, report = module.build_mapping([self.toast(), self.toast()], menus, pos, [])
        self.assertFalse(rows)
        self.assertTrue(all(row['status'] == 'ambiguous_restaurant' for row in report))

    def test_existing_matches_are_preserved_and_conflicts_reported(self):
        menus, pos = self.sources()
        existing = [dict(restaurant_id=1, toast_menu_item_id=UUID(int=99),
                         r365_menu_item_id=UUID(int=10), r365_pos_item_id=1000)]
        for toast, status in [(self.toast(), 'existing_unchanged'),
                              (self.toast(concept='Steakhouse'), 'existing_conflict'),
                              (self.toast(name='Renamed'), 'existing_unmatched_menu')]:
            rows, report = module.build_mapping([toast], menus, pos, existing)
            self.assertFalse(rows)
            self.assertEqual(report[0]['status'], status)
            self.assertEqual(report[0]['existing_pos_id'], 1000)

    def test_empty_sources_and_write_batches_count_actual_inserts(self):
        self.assertEqual(module.build_mapping([], [], [], []), ([], []))
        db = Mock()
        db.fetchall.side_effect = [[{}] * 499, [{}]]
        rows = [(i, UUID(int=99), UUID(int=10), 1000) for i in range(501)]
        self.assertEqual(module.write_mappings(db, rows), 500)
        self.assertEqual(db.executemany.call_count, 2)
        self.assertIn('DO NOTHING', db.executemany.call_args.args[0])
        db.reset_mock()
        self.assertEqual(module.write_mappings(db, []), 0)
        db.executemany.assert_not_called()

    def test_preview_is_read_only_and_write_requires_schema(self):
        menus, pos = self.sources()
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(module, 'DatabaseConnection') as connection, \
                patch.object(module, 'read_sources', return_value=([self.toast()], menus, pos, [], False)), \
                patch.object(module, 'write_mappings') as write:
            path = Path(directory) / 'review.csv'
            module.main(['--report', str(path)])
            db = connection.return_value.__enter__.return_value
            self.assertIn('READ ONLY', db.execute.call_args.args[0])
            write.assert_not_called()
            self.assertIn('matched', path.read_text())
            with self.assertRaisesRegex(ValueError, 'Apply'):
                module.main(['--write', '--report', str(path)])
            write.assert_not_called()

    def test_write_failure_reaches_transaction_exit(self):
        menus, pos = self.sources()
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(module, 'DatabaseConnection') as connection, \
                patch.object(module, 'read_sources', return_value=([self.toast()], menus, pos, [], True)), \
                patch.object(module, 'write_mappings', side_effect=RuntimeError('write failed')):
            with self.assertRaises(RuntimeError):
                module.main(['--write', '--report', str(Path(directory) / 'review.csv')])
            self.assertIs(connection.return_value.__exit__.call_args.args[0], RuntimeError)
