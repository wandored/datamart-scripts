"""Synthetic contract, date dispatch, normalization and atomic-write tests."""
from datetime import date
from decimal import Decimal
from unittest import TestCase
from unittest.mock import Mock, patch
from uuid import UUID

from db_utils.r365_importers import get_labor_punches, get_payroll_summaries, get_sales_analytics
from db_utils.r365_utils import R365Client
import test_r365_invoice_sync as invoice_tests

module = invoice_tests.module

DAY = date(2026, 10, 12)
LOC = str(UUID(int=1))
EMP = str(UUID(int=2))
JOB = str(UUID(int=3))


def punch():
    return {"laborId": str(UUID(int=4)), "dateWorked": str(DAY),
            "startTime": "2026-10-12T09:00:00-04:00", "endTime": None,
            "hours": 1.25, "payRate": 12.5, "total": 15.625,
            "employee": {"id": EMP}, "job": {"id": JOB}, "location": {"id": LOC}}


def payroll():
    return {"businessDate": str(DAY), "location": {"id": LOC}, "employee": {"id": EMP},
            "jobCode": None, "legalEntity": None, "earningType": None,
            "totalSeconds": 4500, "rate": 12.5, "cost": 15.625}


def analytics():
    return {"locationId": LOC, "DailyData": [{"date": str(DAY), "Intervals": [
        {"startDateTime": "2026-10-12T09:00:00", "ForecastedNetSales": 100.25,
         "ActualNetSales": 99, "JobDetails": [{"jobId": JOB, "LaborForecast": 5.5}]}]}]}


def camel_fields(value):
    if isinstance(value, dict):
        return {key[:1].lower() + key[1:]: camel_fields(item) for key, item in value.items()}
    if isinstance(value, list):
        return [camel_fields(item) for item in value]
    return value


class DatedLaborTests(TestCase):
    def test_null_earning_codes_keep_regular_pay_and_tips_separate(self):
        regular = payroll()
        regular['earningTypeName'] = 'Regular Pay'
        tips = payroll()
        tips.update(earningTypeName='Charge Tips', rate=0, totalSeconds=0, cost=42)
        frame = module.r365_payroll_summaries([regular, tips], LOC, DAY, DAY)
        self.assertEqual(len(frame), 2)
        self.assertEqual(frame['earning_type'].tolist(), [None, None])
        self.assertEqual(frame['earning_type_name'].tolist(), ['Regular Pay', 'Charge Tips'])
        self.assertEqual(frame['cost'].tolist(), [Decimal('15.625'), Decimal('42')])
        # A genuine conflict within one category still fails rather than losing data.
        conflicting = dict(regular, cost=999)
        with self.assertRaises(ValueError):
            module.r365_payroll_summaries([regular, conflicting], LOC, DAY, DAY)

    def test_live_camelcase_preserves_all_interval_and_job_measures(self):
        documented = analytics()
        interval = documented['DailyData'][0]['Intervals'][0]
        for field in module.ANALYTICS_MEASURES.values():
            interval[field] = 12.5
        for field in module.ANALYTICS_JOB_MEASURES.values():
            interval['JobDetails'][0][field] = 7.25
        expected = module.r365_sales_analytics(documented, LOC, DAY, DAY, 'America/New_York')
        actual = module.r365_sales_analytics(camel_fields(documented), LOC, DAY, DAY, 'America/New_York')
        for left, right in zip(expected, actual):
            module.pd.testing.assert_frame_equal(left, right)
        for column in module.ANALYTICS_MEASURES:
            self.assertEqual(actual[0].iloc[0][column], Decimal('12.5'))
        for column in module.ANALYTICS_JOB_MEASURES:
            self.assertEqual(actual[1].iloc[0][column], Decimal('7.25'))

    def test_live_empty_intervals_and_conflicting_aliases(self):
        empty = {'locationId': LOC, 'dailyData': [{'date': str(DAY), 'intervals': []}]}
        frames = module.r365_sales_analytics(empty, LOC, DAY, DAY, None)
        self.assertTrue(all(frame.empty for frame in frames))
        bad = camel_fields(analytics())
        bad['DailyData'] = []
        with self.assertRaisesRegex(ValueError, 'Conflicting sales analytics'):
            module.r365_sales_analytics(bad, LOC, DAY, DAY, 'America/New_York')
        with self.assertRaises(ValueError):
            module.r365_sales_analytics({'locationId': LOC}, LOC, DAY, DAY, None)

    def test_punch_pagination_and_filters(self):
        client = object.__new__(R365Client)
        client.request = Mock(side_effect=[{"data": [punch()], "nextLink": "/next"}, {"data": []}])
        self.assertEqual(get_labor_punches(client, business_date_start=DAY, business_date_end=DAY), [punch()])
        params = client.request.call_args_list[0].kwargs['params']
        self.assertEqual(params['dateOfBusinessStart'], str(DAY))
        self.assertIsNone(params['modifiedOnStart'])
        self.assertEqual(client.request.call_args_list[1].args, ('GET', '/next'))

    def test_raw_array_and_forecast_object_use_documented_filters(self):
        client = Mock()
        client.request.return_value = [payroll()]
        self.assertEqual(get_payroll_summaries(client, LOC, DAY, DAY), [payroll()])
        self.assertEqual(client.request.call_args.kwargs['params']['businessDateStart'], str(DAY))
        client.request.return_value = analytics()
        self.assertEqual(get_sales_analytics(client, LOC, DAY, DAY), analytics())
        self.assertEqual(client.request.call_args.kwargs['params'],
                         {'locationId': LOC, 'startDate': str(DAY), 'endDate': str(DAY)})
        for value in ({'data': []}, None, [payroll()] * 250):
            client.request.return_value = value
            with self.assertRaises(ValueError):
                get_payroll_summaries(client, LOC, DAY, DAY)

    def test_normalization_nulls_and_duplicate_conflicts(self):
        frame = module.r365_labor_punches([punch(), punch()], {})
        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0].labor_id, UUID(int=4))
        self.assertEqual(frame.iloc[0].total, Decimal('15.625'))
        self.assertIsNone(frame.iloc[0].end_time)
        changed = punch()
        changed['hours'] = 2
        with self.assertRaises(ValueError):
            module.r365_labor_punches([punch(), changed], {})
        frame = module.r365_payroll_summaries([payroll()], LOC, DAY, DAY)
        self.assertIsNone(frame.iloc[0].job_code_id)
        self.assertEqual(frame.iloc[0].cost, Decimal('15.625'))
        changed = payroll()
        changed['location'] = {'id': JOB}
        with self.assertRaises(ValueError):
            module.r365_payroll_summaries([changed], LOC, DAY, DAY)
        self.assertEqual(tuple(module.r365_labor_punches([], {}).columns), module.LABOR_PUNCH_COLUMNS)

    def test_analytics_nested_jobs_and_timezone(self):
        intervals, jobs = module.r365_sales_analytics(analytics(), LOC, DAY, DAY, 'America/New_York')
        self.assertEqual(intervals.iloc[0].start_date_time.hour, 13)
        self.assertEqual(intervals.iloc[0].forecasted_net_sales, Decimal('100.25'))
        self.assertEqual(jobs.iloc[0].job_id, UUID(int=3))
        self.assertEqual(jobs.iloc[0].labor_forecast, Decimal('5.5'))
        bad = analytics()
        del bad['DailyData'][0]['Intervals'][0]['JobDetails']
        with self.assertRaises(ValueError):
            module.r365_sales_analytics(bad, LOC, DAY, DAY, 'America/New_York')
        with self.assertRaises(ValueError):
            module.r365_sales_analytics(analytics(), LOC, date(2026, 10, 13), date(2026, 10, 13), 'America/New_York')

    def test_daily_week_and_bulk_dispatch_new_tables(self):
        with patch.object(module, 'sync_dated_labor') as sync:
            module.sync_modified_tables('client', DAY, list(module.DATED_LABOR_TABLES))
            self.assertEqual(sync.call_args.kwargs['modified_dates'], module.modified_day_bounds(DAY))
            sync.reset_mock()
            module.sync_daily_tables('client', DAY, DAY, list(module.DATED_LABOR_TABLES))
            sync.assert_called_once_with('client', DAY, DAY, list(module.DATED_LABOR_TABLES))
        for mode in (['--daily'], ['--week'], ['--bulk', '--business-date', str(DAY)]):
            with patch.object(module, 'R365Client'), patch.object(module, 'sync_dated_labor') as sync, \
                    patch.object(module.fiscal_calendar, 'daily_date_range', return_value=(DAY, DAY)), \
                    patch.object(module.fiscal_calendar, 'fiscal_week_to_date', return_value=(DAY, DAY)):
                module.main([*mode, '--tables', *module.DATED_LABOR_TABLES])
                self.assertEqual(sync.call_count, 1)


class DatedLaborWriteTests(TestCase):
    setUp = invoice_tests.InvoiceSyncTests.setUp

    def test_employee_reference_writes_only_id(self):
        for table, row in (("labor_punches", punch()), ("payroll_summaries", payroll())):
            with self.subTest(table=table):
                self.execute.reset_mock()
                row["employee"].update(name="EXCLUDED_EMPLOYEE_NAME", payrollId="EXCLUDED_PAYROLL_ALIAS")
                payload = {"data": [row], "nextLink": None} if table == "labor_punches" else [row]
                self.sync([table], payload)
                statement, records = self.execute.call_args.args[1:3]
                self.assertIn('"employee_id"', statement)
                self.assertNotIn('"employee_name"', statement)
                self.assertIn(UUID(int=2), records[0])
                self.assertNotIn("EXCLUDED_EMPLOYEE_NAME", str(records))
                self.assertNotIn("EXCLUDED_PAYROLL_ALIAS", str(records))

    def sync(self, tables, response):
        self.client.request.return_value = response
        with patch.object(module, 'get_locations', return_value=[{'id': LOC, 'timeZone': 'America/New_York'}]):
            module.sync_dated_labor(self.client, DAY, DAY, tables)

    def test_payroll_null_keys_upsert_and_empty_snapshot(self):
        self.sync(['payroll_summaries'], [payroll()])
        self.assertIn('ON CONFLICT ("business_date", "location_id", "legal_entity_id", "employee_id", "job_code_id", "earning_type", "earning_type_name")', self.execute.call_args.args[1])
        self.connection.commit.assert_called_once()
        self.execute.reset_mock()
        self.sync(['payroll_summaries'], [])
        self.execute.assert_not_called()
        query = self.connection.cursor.return_value.execute.call_args
        self.assertIn('is_current = false', str(query.args[0]))
        self.assertEqual(query.args[1], (UUID(int=1), DAY, DAY))

    def test_forecast_child_failure_rolls_back_and_no_success_log(self):
        self.execute.side_effect = [None, RuntimeError('child write failed')]
        with self.assertRaises(RuntimeError):
            self.sync(['sales_analytics'], analytics())
        self.connection.rollback.assert_called_once()
        self.connection.commit.assert_not_called()
        self.assertNotIn('intervals=', self.output.getvalue())

    def test_live_camelcase_reaches_database_with_numeric_values(self):
        self.sync(['sales_analytics'], camel_fields(analytics()))
        interval, jobs = self.execute.call_args_list
        self.assertIn(Decimal('100.25'), interval.args[2][0])
        self.assertIn(Decimal('5.5'), jobs.args[2][0])
        self.connection.commit.assert_called_once()

    def test_bad_snapshot_and_page_failure_do_not_open_database(self):
        bad = analytics()
        del bad['DailyData']
        with self.assertRaises(ValueError):
            self.sync(['sales_analytics'], bad)
        self.connect.assert_not_called()
        self.client.request.side_effect = [{'data': [punch()], 'nextLink': '/next'}, RuntimeError('page failed')]
        with self.assertRaises(RuntimeError):
            self.sync(['labor_punches'], None)
        self.connect.assert_not_called()
