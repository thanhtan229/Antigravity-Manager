"""Exercise the live harness entirely on a localhost mock; no Google traffic."""

import base64
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class HarnessMockTests(unittest.TestCase):
    def run_harness(self, *, fail=False, delay=0, malformed=False, redirect=False, arguments=None, stdin=""):
        requests = []

        class QuietServer(ThreadingHTTPServer):
            def handle_error(self, _request, _client_address):
                pass

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                requests.append((self.path, json.loads(self.rfile.read(int(self.headers['Content-Length'])))))
                if delay:
                    time.sleep(delay)
                if redirect:
                    self.send_response(302)
                    self.send_header('Location', f'http://127.0.0.1:{self.server.server_port}/redirect-target')
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
                if fail:
                    self.send_response(503)
                    response = json.dumps({'error': {'message': 'mock failure'}}).encode()
                elif malformed:
                    self.send_response(200)
                    response = b'{broken mock response'
                else:
                    self.send_response(200)
                    # Header-only fixture exercises byte preservation and the dimension reader.
                    raw = b'\x89PNG\r\n\x1a\n' + b'\0' * 8 + struct.pack('>II', 32, 32)
                    response = json.dumps({'data': [{'b64_json': base64.b64encode(raw).decode()}]}).encode()
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(response)))
                self.end_headers()
                self.wfile.write(response)

        with QuietServer(('127.0.0.1', 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            with tempfile.TemporaryDirectory(prefix='native-image-harness-mock-') as destination:
                cmd = [sys.executable, str(Path(__file__).with_name('native_image_resolution.py')),
                       '--base-url', f'http://127.0.0.1:{server.server_port}/v1',
                       '--output-dir', destination]
                cmd += arguments if arguments is not None else ['--confirm-live', '--confirmation', 'GENERATE']
                process = subprocess.run(cmd, input=stdin, capture_output=True, text=True,
                                         env={**os.environ, 'AGM_API_KEY': 'mock-local-only'}, timeout=5)
                report_path = Path(destination) / 'native_resolution_results.json'
                report = json.loads(report_path.read_text()) if report_path.exists() else None
                events_path = Path(destination) / 'native_resolution_events.jsonl'
                events = [json.loads(line) for line in events_path.read_text().splitlines()] if events_path.exists() else []
                pending = [p.name for p in Path(destination).glob('*.pending')]
            server.shutdown()
            thread.join(timeout=5)
        return process, requests, report, events, pending

    def test_two_square_calls_with_identical_prompt_and_no_retry(self):
        result, calls, report, events, _pending = self.run_harness()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertEqual([body['image_size'] for _, body in calls], ['2K', '4K'])
        self.assertEqual([path for path, _ in calls], ['/v1/images/generations'] * 2)
        self.assertEqual(calls[0][1]['prompt'], calls[1][1]['prompt'])
        self.assertTrue(all(body['n'] == 1 and body['size'] == '1024x1024' for _, body in calls))
        self.assertEqual(report['client_request_count'], 2)
        self.assertEqual(report['native_output_verdict'], 'INCONCLUSIVE')
        self.assertNotIn('mock-local-only', json.dumps(report))
        self.assertIn('upstream progress is not observable', result.stdout)
        self.assertNotIn('mock-local-only', json.dumps(events))
        self.assertEqual([e['event'] for e in events if e['event'] in {'SEND_STARTED', 'RESPONSE_RECEIVED', 'SAVED'}],
                         ['SEND_STARTED', 'RESPONSE_RECEIVED', 'SAVED'] * 2)

    def test_error_stops_after_one_call(self):
        result, calls, report, _events, _pending = self.run_harness(fail=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(report['client_request_count'], 1)

    def test_piped_confirmation_is_not_read_and_returns_without_hanging(self):
        result, calls, report, events, _pending = self.run_harness(arguments=['--confirm-live'], stdin='GENERATE\n')
        self.assertEqual(result.returncode, 2)
        self.assertEqual(calls, [])
        self.assertIsNone(report)
        self.assertIn('No interactive terminal', result.stdout)
        self.assertEqual([e['event'] for e in events], ['AWAITING_CONFIRMATION', 'CONFIRMATION_REQUIRED'])

    def test_timeout_is_reported_once_and_stops(self):
        result, calls, report, events, _pending = self.run_harness(
            delay=0.4,
            arguments=['--confirm-live', '--confirmation', 'GENERATE', '--max-requests', '1', '--timeout-seconds', '0.1'],
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(report['client_request_count'], 1)
        self.assertIn('INTERRUPTED', [e['event'] for e in events])

    def test_redirect_is_not_followed(self):
        result, calls, report, events, _pending = self.run_harness(
            redirect=True,
            arguments=['--confirm-live', '--confirmation', 'GENERATE', '--max-requests', '1'],
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(report['client_calls'][0]['http_status'], 302)
        self.assertEqual([e['event'] for e in events if e['event'] == 'RESPONSE_RECEIVED'], ['RESPONSE_RECEIVED'])

    def test_raw_response_is_preserved_before_parse_failure(self):
        result, calls, report, events, _pending = self.run_harness(
            malformed=True,
            arguments=['--confirm-live', '--confirmation', 'GENERATE', '--max-requests', '1'],
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(calls), 1)
        self.assertTrue(any(name.startswith('.square-2k.response.pending') for name in _pending))
        self.assertEqual(report['client_calls'][0]['http_status'], 200)
        self.assertIn('RESPONSE_RECEIVED', [e['event'] for e in events])

    def test_rejects_more_than_two_planned_calls_before_network(self):
        result, calls, _report, _events, _pending = self.run_harness(arguments=['--confirm-live', '--confirmation', 'GENERATE', '--max-requests', '3'])
        self.assertEqual(result.returncode, 2)
        self.assertEqual(calls, [])


if __name__ == '__main__':
    unittest.main()
