"""Match Toast menu items to concept-specific R365 menu and POS records.

Default: read-only preview. Apply core_menu_item_mapping.sql before --write.
"""

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path

from db_utils.dbconnect import DatabaseConnection


REPORT_COLUMNS = (
    'restaurant_id', 'toast_restaurant_id', 'toast_menu_item_id', 'toast_name',
    'concept', 'status', 'r365_menu_item_id', 'r365_pos_item_id',
    'candidate_menu_ids', 'candidate_pos_ids',
    'existing_menu_id', 'existing_pos_id',
)


def normalize_name(value):
    """Normalize only case and whitespace; preserve punctuation and concept."""
    return ' '.join((value or '').split()).casefold()


def build_mapping(toast_items, menu_items, pos_items, existing):
    menus = defaultdict(list)
    positions = defaultdict(list)
    for row in menu_items:
        if normalize_name(row['name']):
            menus[normalize_name(row['name'])].append(row)
    for row in pos_items:
        if normalize_name(row['menu_item']):
            positions[normalize_name(row['menu_item'])].append(row)
    saved = {(row['restaurant_id'], row['toast_menu_item_id']): row for row in existing}
    # Duplicate company assignments must not result in guessed restaurant mappings.
    toast_counts = Counter((row['toast_restaurant_id'], row['toast_menu_item_id'])
                           for row in toast_items)
    proposed, report = [], []
    for row in toast_items:
        result = {column: '' for column in REPORT_COLUMNS}
        result.update({key: row.get(key) for key in (
            'restaurant_id', 'toast_restaurant_id', 'toast_menu_item_id',
            'toast_name', 'concept',
        )})
        key = (row['restaurant_id'], row['toast_menu_item_id'])
        previous = saved.get(key)
        if previous:
            result['existing_menu_id'] = previous['r365_menu_item_id']
            result['existing_pos_id'] = previous['r365_pos_item_id']
        candidate = None
        if toast_counts[(row['toast_restaurant_id'], row['toast_menu_item_id'])] != 1:
            status = 'ambiguous_restaurant'
        elif row['restaurant_id'] is None or row['r365_guid'] is None:
            status = 'missing_restaurant_link'
        elif not normalize_name(row['concept']):
            status = 'missing_concept'
        elif not normalize_name(row['toast_name']):
            status = 'missing_toast_name'
        else:
            name = normalize_name(f"{row['concept'].strip()} - {row['toast_name'].strip()}")
            menu_candidates = menus[name]
            result['candidate_menu_ids'] = '|'.join(str(item['id']) for item in menu_candidates)
            if len(menu_candidates) != 1:
                status = 'unmatched_menu' if not menu_candidates else 'ambiguous_menu'
            else:
                menu = menu_candidates[0]
                result['r365_menu_item_id'] = menu['id']
                pos_candidates = positions[normalize_name(menu['name'])]
                result['candidate_pos_ids'] = '|'.join(str(item['pos_item_id']) for item in pos_candidates)
                if len(pos_candidates) != 1:
                    status = 'unmatched_pos' if not pos_candidates else 'ambiguous_pos'
                else:
                    result['r365_pos_item_id'] = pos_candidates[0]['pos_item_id']
                    candidate = (menu['id'], pos_candidates[0]['pos_item_id'])
                    status = 'matched'
        if previous:
            old = (previous['r365_menu_item_id'], previous['r365_pos_item_id'])
            status = ('existing_unchanged' if candidate == old else
                      'existing_conflict' if candidate is not None else 'existing_' + status)
        elif candidate is not None:
            proposed.append((row['restaurant_id'], row['toast_menu_item_id'], *candidate))
        result['status'] = status
        report.append(result)
    return proposed, report


def read_sources(db):
    db.execute('''
        SELECT restaurant.id AS restaurant_id, item.restaurant_id AS toast_restaurant_id,
               item.id AS toast_menu_item_id, item.name AS toast_name,
               restaurant.concept, restaurant.r365_guid
        FROM toast.menu_items AS item
        LEFT JOIN core.restaurants AS restaurant ON restaurant.toast_guid = item.restaurant_id
        ORDER BY item.restaurant_id, item.id, restaurant.id
    ''')
    toast_items = db.fetchall()
    db.execute('SELECT id, name FROM r365.menu_items ORDER BY id')
    menus = db.fetchall()
    db.execute('SELECT pos_item_id, menu_item FROM r365.pos_items ORDER BY pos_item_id')
    positions = db.fetchall()
    db.execute("SELECT to_regclass('core.menu_item_mapping') AS mapping_table")
    exists = db.fetchone()['mapping_table'] is not None
    existing = []
    if exists:
        db.execute('''
            SELECT restaurant_id, toast_menu_item_id, r365_menu_item_id, r365_pos_item_id
            FROM core.menu_item_mapping
        ''')
        existing = db.fetchall()
    return toast_items, menus, positions, existing, exists


def write_mappings(db, rows):
    """Insert new matches only; never overwrite automatic or manual decisions."""
    inserted = 0
    # Batches give accurate insert counts, including conflicts from concurrent runs.
    for start in range(0, len(rows), 500):
        db.executemany('''
            INSERT INTO core.menu_item_mapping
                (restaurant_id, toast_menu_item_id, r365_menu_item_id, r365_pos_item_id,
                 match_method)
            VALUES %s
            ON CONFLICT (restaurant_id, toast_menu_item_id) DO NOTHING
            RETURNING restaurant_id
        ''', [(*row, 'concept_name') for row in rows[start:start + 500]], page_size=500)
        inserted += len(db.fetchall())
    return inserted


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true', help='Insert new unique matches; preserve existing mappings.')
    parser.add_argument('--report', type=Path, default=Path('output/menu_item_mapping_review.csv'))
    args = parser.parse_args(argv)
    with DatabaseConnection() as db:
        # Keep source reads and existing mappings consistent throughout this run.
        db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ' +
                   ('' if args.write else ', READ ONLY'))
        toast_items, menus, positions, existing, exists = read_sources(db)
        if args.write and not exists:
            raise ValueError('Apply db_utils/schema/core_menu_item_mapping.sql before --write')
        proposed, report = build_mapping(toast_items, menus, positions, existing)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        with args.report.open('w', encoding='utf-8', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=REPORT_COLUMNS)
            writer.writeheader()
            writer.writerows(report)
        inserted = write_mappings(db, proposed) if args.write else 0
    print(f"Toast rows={len(toast_items):,}; new matches={len(proposed):,}; inserted={inserted:,}.")
    print(', '.join(f'{status}={count:,}' for status, count in sorted(Counter(row['status'] for row in report).items())))
    print(f"{'Write complete' if args.write else 'Read-only preview'}; report: {args.report}")


if __name__ == '__main__':
    main()
