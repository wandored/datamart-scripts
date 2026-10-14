"""Opt-in isolated PostgreSQL checks for dated labor table constraints."""
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

import psycopg2
from test_r365_dated_labor_sync import module, DAY, LOC, punch, payroll, analytics


@unittest.skipUnless(os.environ.get('R365_TEST_PG_SOCKET'), 'Requires isolated temporary PostgreSQL')
class DatedLaborPostgresTests(unittest.TestCase):
    def test_reruns_null_keys_disappearance_and_atomic_forecast_rollback(self):
        socket = os.environ['R365_TEST_PG_SOCKET']
        self.assertTrue(socket.startswith('/tmp/r365-sales-pg-'))
        connect = psycopg2.connect

        def connection_factory(*args, **kwargs):
            return connect(host=socket, port=55439, user='sales_test', dbname='postgres')

        connection = connection_factory()
        self.addCleanup(connection.close)
        with connection.cursor() as cur:
            cur.execute(Path('db_utils/schema/r365_labor.sql').read_text())
            # Recreate the original constraint to verify upgrades of existing tables.
            cur.execute('ALTER TABLE r365.payroll_summaries DROP CONSTRAINT payroll_summaries_grain_key')
            cur.execute('ALTER TABLE r365.payroll_summaries ADD UNIQUE NULLS NOT DISTINCT '
                        '(business_date, location_id, legal_entity_id, employee_id, job_code_id, earning_type)')
            cur.execute("INSERT INTO r365.payroll_summaries "
                        "(business_date, location_id, employee_id, earning_type_name, cost, last_synced_at) "
                        "VALUES (%s, %s, %s, 'Regular Pay', 10, now())", (DAY, LOC, payroll()['employee']['id']))
        connection.commit()
        with connection.cursor() as cur:
            migration = Path('db_utils/schema/r365_payroll_earning_type_migration.sql').read_text()
            cur.execute(migration)
            cur.execute(migration)  # A retry after success must also preserve rows.
            cur.execute('SELECT earning_type_name, cost FROM r365.payroll_summaries')
            self.assertEqual(cur.fetchall(), [('Regular Pay', 10)])
        connection.commit()
        client = Mock()

        def sync(table, payload):
            client.request.return_value = payload
            client.get_resource.return_value = payload
            with patch('db_utils.dbconnect.psycopg2.connect', side_effect=connection_factory), \
                    patch.object(module, 'get_locations', return_value=[{'id': LOC, 'timeZone': 'America/New_York'}]):
                module.sync_dated_labor(client, DAY, DAY, [table])

        def query(sql):
            with connection.cursor() as cur:
                cur.execute(sql)
                result = cur.fetchall()
            connection.commit()
            return result

        for _ in range(2):
            sync('labor_punches', [punch()])
            sync('payroll_summaries', [payroll()])
            sync('sales_analytics', analytics())
        for table in ('labor_punches', 'payroll_summaries', 'sales_analytics', 'sales_analytics_jobs'):
            expected = 2 if table == 'payroll_summaries' else 1
            self.assertEqual(query(f'SELECT count(*) FROM r365.{table}'), [(expected,)])
        changed = payroll()
        changed['cost'] = 99
        sync('payroll_summaries', [changed])
        self.assertEqual(query('SELECT cost FROM r365.payroll_summaries WHERE is_current'), [(99,)])
        sync('payroll_summaries', [])
        self.assertEqual(query('SELECT is_current FROM r365.payroll_summaries'), [(False,), (False,)])
        sync('payroll_summaries', [payroll()])
        self.assertEqual(query('SELECT count(*) FROM r365.payroll_summaries WHERE is_current'), [(1,)])
        regular = payroll()
        regular['earningTypeName'] = 'Regular Pay'
        tips = payroll()
        tips.update(earningTypeName='Charge Tips', cost=42, rate=0, totalSeconds=0)
        for _ in range(2):
            sync('payroll_summaries', [regular, tips])
        self.assertEqual(query('SELECT earning_type_name, cost FROM r365.payroll_summaries '
                               'WHERE is_current ORDER BY earning_type_name'),
                         [('Charge Tips', 42), ('Regular Pay', 15.625)])
        with connection.cursor() as cur:
            cur.execute('ALTER TABLE r365.sales_analytics_jobs ADD CONSTRAINT test_forecast CHECK (labor_forecast < 100)')
        connection.commit()
        bad = analytics()
        bad['DailyData'][0]['Intervals'][0]['ForecastedNetSales'] = 999
        bad['DailyData'][0]['Intervals'][0]['JobDetails'][0]['LaborForecast'] = 999
        with self.assertRaises(psycopg2.errors.CheckViolation):
            sync('sales_analytics', bad)
        self.assertEqual(float(query('SELECT forecasted_net_sales FROM r365.sales_analytics WHERE is_current')[0][0]), 100.25)
        self.assertEqual(query('SELECT count(*) FROM r365.sales_analytics_jobs WHERE is_current'), [(1,)])
        sync('sales_analytics', {'locationId': LOC, 'DailyData': []})
        self.assertEqual(query('SELECT is_current FROM r365.sales_analytics'), [(False,)])
        self.assertEqual(query('SELECT is_current FROM r365.sales_analytics_jobs'), [(False,)])
