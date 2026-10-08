import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, call, patch

import requests

from db_utils.r365_utils import R365Client
from test_r365_invoice_sync import module


def response(payload):
    result = Mock(content=b'json')
    result.json.return_value = payload
    return result


class ClientTimeoutTests(unittest.TestCase):
    def setUp(self):
        self.client = object.__new__(R365Client)
        self.client.base_url = 'https://example.test/public'
        self.client.session = Mock()
        self.sleep = self.enterContext(patch('db_utils.r365_utils.time.sleep'))
        self.output = io.StringIO()
        self.enterContext(redirect_stdout(self.output))

    def test_timeout_retries_identical_request_then_returns_data(self):
        self.client.session.request.side_effect = [
            requests.ReadTimeout(), requests.ConnectTimeout(), response({'items': [1]}),
        ]
        result = self.client.request('GET', '/v1/inventory/invoices', params={'dateStart': '2026-09-30'})
        self.assertEqual(result, {'items': [1]})
        calls = self.client.session.request.call_args_list
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(calls[1], calls[2])
        self.assertEqual(self.sleep.call_args_list, [call(2), call(4)])
        self.assertIn('attempt 3/3', self.output.getvalue())

    def test_continuation_timeout_does_not_restart_or_duplicate_pages(self):
        self.client.session.request.side_effect = [
            response({'items': [1], 'nextLink': '/public/next?continuationToken=private'}),
            requests.ReadTimeout(), response({'items': [2]}),
        ]
        self.assertEqual(self.client.get_resource('inventory', 'invoices'), [1, 2])
        calls = self.client.session.request.call_args_list
        self.assertEqual(calls[1], calls[2])
        self.assertNotEqual(calls[0], calls[1])
        self.assertNotIn('private', self.output.getvalue())
        self.sleep.assert_called_once_with(2)

    def test_exhausted_continuation_timeout_prevents_partial_invoice_writes(self):
        error = requests.ReadTimeout('timeout')
        self.client.session.request.side_effect = [
            response({'items': [{'id': 'not processed'}], 'nextLink': '/public/next'}),
            error, error, error,
        ]
        with patch.object(module, 'DatabaseConnection') as db, \
                self.assertRaises(requests.ReadTimeout) as caught:
            module.sync_vendor_invoices(self.client, '2026-09-30', '2026-10-06')
        self.assertIs(caught.exception, error)
        self.assertEqual(self.client.session.request.call_count, 4)
        self.assertEqual(self.sleep.call_count, 2)
        db.assert_not_called()

    def test_writes_are_not_retried(self):
        for method in ('POST', 'PATCH', 'PUT', 'DELETE'):
            with self.subTest(method=method):
                self.client.session.request.reset_mock()
                self.client.session.request.side_effect = requests.ReadTimeout()
                with self.assertRaises(requests.ReadTimeout):
                    self.client.request(method, '/v1/resource', json={'value': 1})
                self.client.session.request.assert_called_once()
        self.sleep.assert_not_called()

    def test_http_errors_are_not_treated_as_timeouts(self):
        result = response({})
        result.raise_for_status.side_effect = requests.HTTPError()
        self.client.session.request.return_value = result
        with self.assertRaises(requests.HTTPError):
            self.client.request('GET', '/v1/resource')
        self.client.session.request.assert_called_once()
        self.sleep.assert_not_called()


if __name__ == '__main__':
    unittest.main()
