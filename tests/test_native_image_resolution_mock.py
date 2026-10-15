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
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class HarnessMockTests(unittest.TestCase):
    def run_harness(self, *, fail=False, arguments=None):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                requests.append((self.path, json.loads(self.rfile.read(int(self.headers['Content-Length'])))))
                if fail:
                    self.send_response(503)
                    response = json.dumps({'error': {'message': 'mock failure'}}).encode()
                else:
                    self.send_response(200)
                    # Header-only fixture exercises byte preservation and the dimension reader.
                    raw = b'\x89PNG\r\n\x1a\n' + b'\0' * 8 + struct.pack('>II', 32, 32)
                    response = json.dumps({'data': [{'b64_json': base64.b64encode(raw).decode()}]}).encode()
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(response)))
                self.end_headers()
                self.wfile.write(response)

        with ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            with tempfile.TemporaryDirectory(prefix='native-image-harness-mock-') as destination:
                cmd = [sys.executable, str(Path(__file__).with_name('native_image_resolution.py')),
                       '--base-url', f'http://127.0.0.1:{server.server_port}/v1',
                       '--output-dir', destination]
                cmd += arguments if arguments is not None else ['--confirm-live']
                process = subprocess.run(cmd, input='GENERATE\n', capture_output=True, text=True,
                                         env={**os.environ, 'AGM_API_KEY': 'mock-local-only'}, timeout=30)
                report_path = Path(destination) / 'native_resolution_results.json'
                report = json.loads(report_path.read_text()) if report_path.exists() else None
            server.shutdown()
            thread.join(timeout=5)
        return process, requests, report

    def test_two_square_calls_with_identical_prompt_and_no_retry(self):
        result, calls, report = self.run_harness()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertEqual([body['image_size'] for _, body in calls], ['2K', '4K'])
        self.assertEqual([path for path, _ in calls], ['/v1/images/generations'] * 2)
        self.assertEqual(calls[0][1]['prompt'], calls[1][1]['prompt'])
        self.assertTrue(all(body['n'] == 1 and body['size'] == '1024x1024' for _, body in calls))
        self.assertEqual(report['client_request_count'], 2)
        self.assertEqual(report['native_output_verdict'], 'INCONCLUSIVE')
        self.assertNotIn('mock-local-only', json.dumps(report))

    def test_error_stops_after_one_call(self):
        result, calls, report = self.run_harness(fail=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(report['client_request_count'], 1)

    def test_missing_confirmation_makes_no_calls(self):
        result, calls, _ = self.run_harness(arguments=[])
        self.assertEqual(result.returncode, 2)
        self.assertEqual(calls, [])

    def test_rejects_more_than_two_planned_calls_before_network(self):
        result, calls, _ = self.run_harness(arguments=['--confirm-live', '--max-requests', '3'])
        self.assertEqual(result.returncode, 2)
        self.assertEqual(calls, [])


if __name__ == '__main__':
    unittest.main()
