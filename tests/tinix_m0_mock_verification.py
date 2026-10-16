#!/usr/bin/env python3
"""
Tinix M0 Hardened Proxy — End-to-End Subprocess & Mock Upstream Verification Suite.
All tests use a loopback mock upstream (127.0.0.1). Zero real Google requests are made.
"""

import argparse
import base64
import http.client
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ONE_BY_ONE_PNG_B64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg=="
)


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class MockUpstreamState:
    def __init__(self):
        self.lock = threading.Lock()
        self.mode = "200"
        self.delay_sec = 0.0
        self.calls = []

    def reset(self, mode="200", delay_sec=0.0):
        with self.lock:
            self.mode = mode
            self.delay_sec = delay_sec
            self.calls = []

    def record_call(self, entry):
        with self.lock:
            self.calls.append(entry)

    def get_calls(self):
        with self.lock:
            return list(self.calls)


def make_handler(mock_state: MockUpstreamState):
    class MockUpstreamHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def do_POST(self):
            content_len = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(content_len) if content_len > 0 else b""
            try:
                parsed_body = json.loads(raw_body.decode("utf-8"))
            except Exception:
                parsed_body = None

            with mock_state.lock:
                mode = mock_state.mode
                delay_sec = mock_state.delay_sec

            mock_state.record_call(
                {
                    "path": self.path,
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                    "body": parsed_body,
                    "timestamp": time.time(),
                }
            )

            if delay_sec > 0:
                time.sleep(delay_sec)

            if mode == "reset":
                # Abruptly close socket without sending HTTP status line
                self.close_connection = True
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                self.connection.close()
                return

            if mode == "200":
                payload = json.dumps(
                    {
                        "response": {
                            "candidates": [
                                {
                                    "content": {
                                        "parts": [
                                            {
                                                "inlineData": {
                                                    "mimeType": "image/png",
                                                    "data": ONE_BY_ONE_PNG_B64,
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                ).encode("utf-8")
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(payload)
                    self.wfile.flush()
                except OSError:
                    pass
                self.close_connection = True
                return

            status_code = int(mode)
            err_payload = json.dumps(
                {
                    "error": {
                        "code": status_code,
                        "message": f"Mock upstream HTTP {status_code}",
                        "status": "MOCK_FAILURE",
                    }
                }
            ).encode("utf-8")
            try:
                self.send_response(status_code)
                if status_code == 429:
                    self.send_header("Retry-After", "1")
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(err_payload)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(err_payload)
                self.wfile.flush()
            except OSError:
                pass
            self.close_connection = True

    return MockUpstreamHandler


def write_isolated_profile(
    profile_dir: Path,
    port: int,
    models_on_accounts=None,
    custom_mapping=None,
    request_timeout=4,
    include_accounts=True,
):
    profile_dir.mkdir(parents=True, exist_ok=True)
    accounts_dir = profile_dir / "accounts"
    accounts_dir.mkdir(parents=True, exist_ok=True)

    identity = {
        "profileId": "tinix-m0-isolated-test-profile",
        "application": "tinix-media-creator",
        "purpose": "tinix-m0-hardened-proxy",
        "isolated": True,
        "isResearchProfile": False,
    }
    (profile_dir / "tinix-profile-identity.json").write_text(
        json.dumps(identity, indent=2), encoding="utf-8"
    )

    gui_config = {
        "language": "en",
        "theme": "dark",
        "auto_refresh": False,
        "refresh_interval": 15,
        "auto_sync": False,
        "sync_interval": 30,
        "proxy": {
            "enabled": True,
            "port": port,
            "auto_start": True,
            "api_key": "sk-tinix-m0-test-key",
            "auth_mode": "off",
            "allow_lan_access": False,
            "request_timeout": request_timeout,
            "enable_logging": False,
            "custom_mapping": custom_mapping or {},
            "upstream_proxy": {"enabled": False, "url": ""},
        },
        "scheduled_warmup": {"enabled": False, "monitored_models": []},
        "quota_protection": {"enabled": False, "threshold_percentage": 10, "monitored_models": []},
    }
    (profile_dir / "gui_config.json").write_text(
        json.dumps(gui_config, indent=2), encoding="utf-8"
    )

    if not include_accounts:
        (profile_dir / "accounts.json").write_text(
            json.dumps({"accounts": [], "current_account_id": None}, indent=2),
            encoding="utf-8",
        )
        return

    if models_on_accounts is None:
        models_on_accounts = ["gemini-3.1-flash-image"]

    now_ts = int(time.time())
    expiry_ts = now_ts + 86400
    account_entries = []
    for idx, (acc_id, email) in enumerate(
        [
            ("acc-tinix-mock-1", "mock1@tinix.local"),
            ("acc-tinix-mock-2", "mock2@tinix.local"),
        ]
    ):
        account_entries.append(
            {
                "id": acc_id,
                "email": email,
                "name": f"Mock {idx + 1}",
                "account_type": "antigravity",
                "disabled": False,
                "proxy_disabled": False,
                "created_at": now_ts,
                "last_used": now_ts,
            }
        )
        acc_data = {
            "id": acc_id,
            "email": email,
            "name": f"Mock {idx + 1}",
            "token": {
                "access_token": f"ya29.mock_token_{idx + 1}",
                "refresh_token": f"1//mock_refresh_{idx + 1}",
                "expires_in": 86400,
                "expiry_timestamp": expiry_ts,
                "token_type": "Bearer",
                "email": email,
                "project_id": f"mock-project-{idx + 1}",
                "is_gcp_tos": True,
            },
            "quota": {
                "models": [
                    {
                        "name": m,
                        "percentage": 100,
                        "reset_time": "2026-12-31T00:00:00Z",
                    }
                    for m in models_on_accounts
                ],
                "last_updated": now_ts,
                "subscription_tier": "FREE",
                "is_forbidden": False,
            },
            "disabled": False,
            "proxy_disabled": False,
            "protected_models": [],
            "live_limited_models": {},
            "validation_blocked": False,
            "created_at": now_ts,
            "last_used": now_ts,
        }
        (accounts_dir / f"{acc_id}.json").write_text(
            json.dumps(acc_data, indent=2), encoding="utf-8"
        )

    (profile_dir / "accounts.json").write_text(
        json.dumps(
            {
                "accounts": account_entries,
                "current_account_id": "acc-tinix-mock-1",
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def http_request(port: int, method: str, path: str, body=None, timeout=10.0):
    url = f"http://127.0.0.1:{port}{path}"
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = raw
            return resp.status, parsed
    except urllib.error.HTTPError as err:
        raw = err.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = raw
        return err.code, parsed


def read_log_file(path: Path) -> str:
    if path and path.exists():
        try:
            return path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return ""
    return ""


def spawn_proxy_process(binary_path: Path, profile_dir: Path, port: int, env: dict):
    stdout_path = profile_dir / "proxy-stdout.log"
    stderr_path = profile_dir / "proxy-stderr.log"
    stdout_f = open(stdout_path, "w", encoding="utf-8")
    stderr_f = open(stderr_path, "w", encoding="utf-8")
    proc = subprocess.Popen(
        [
            str(binary_path),
            "--headless",
            "--data-dir",
            str(profile_dir),
            "--port",
            str(port),
        ],
        env=env,
        stdout=stdout_f,
        stderr=stderr_f,
        text=True,
    )
    return proc, stdout_f, stderr_f, stdout_path, stderr_path


def wait_for_health(
    port: int,
    proc: subprocess.Popen,
    stdout_path: Path,
    stderr_path: Path,
    timeout_sec: float = 15.0,
):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f"Proxy exited early with code {proc.returncode}:\n"
                f"STDOUT:\n{read_log_file(stdout_path)}\n"
                f"STDERR:\n{read_log_file(stderr_path)}"
            )
        try:
            status, payload = http_request(port, "GET", "/health", timeout=1.5)
            if status == 200 and isinstance(payload, dict) and payload.get("status") == "ok":
                return payload
        except Exception:
            time.sleep(0.2)
    raise TimeoutError(
        f"Timed out waiting for proxy /health on 127.0.0.1:{port}\n"
        f"STDOUT:\n{read_log_file(stdout_path)}\n"
        f"STDERR:\n{read_log_file(stderr_path)}"
    )


def run_startup_guard_tests(binary_path: Path, temp_root: Path):
    results = []

    # 1. Missing --data-dir / ABV_DATA_DIR
    env_no_dir = {k: v for k, v in os.environ.items() if k != "ABV_DATA_DIR"}
    proc = subprocess.run(
        [str(binary_path), "--headless", "--port", str(find_free_port())],
        env=env_no_dir,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert proc.returncode != 0, "Expected non-zero exit when --data-dir is missing"
    assert "TINIX_M0_PROFILE_REQUIRED" in (proc.stderr + proc.stdout)
    results.append({"case": "startup_missing_data_dir_rejected", "passed": True})

    # 2. Forbidden .antigravity_tools path
    forbidden_dir = temp_root / ".antigravity_tools"
    proc = subprocess.run(
        [
            str(binary_path),
            "--headless",
            "--data-dir",
            str(forbidden_dir),
            "--port",
            str(find_free_port()),
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert proc.returncode != 0, "Expected non-zero exit on .antigravity_tools path"
    assert "TINIX_M0_PROFILE_ISOLATION_VIOLATION" in (proc.stderr + proc.stdout)
    results.append({"case": "startup_default_profile_rejected", "passed": True})

    # 3. Forbidden research profile marker (isResearchProfile: true)
    bad_identity_dir = temp_root / "bad-identity-profile"
    bad_identity_dir.mkdir(parents=True, exist_ok=True)
    (bad_identity_dir / "tinix-profile-identity.json").write_text(
        json.dumps(
            {
                "profileId": "research-profile",
                "isolated": True,
                "isResearchProfile": True,
            }
        ),
        encoding="utf-8",
    )
    proc = subprocess.run(
        [
            str(binary_path),
            "--headless",
            "--data-dir",
            str(bad_identity_dir),
            "--port",
            str(find_free_port()),
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert proc.returncode != 0, "Expected non-zero exit when isResearchProfile=True"
    assert "TINIX_M0_PROFILE_IDENTITY_REJECTED" in (proc.stderr + proc.stdout)
    results.append({"case": "startup_research_identity_rejected", "passed": True})

    # 4. Forbidden port 8045
    port_8045_dir = temp_root / "port-8045-profile"
    write_isolated_profile(port_8045_dir, port=8045)
    proc = subprocess.run(
        [
            str(binary_path),
            "--headless",
            "--data-dir",
            str(port_8045_dir),
            "--port",
            "8045",
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert proc.returncode != 0, "Expected non-zero exit when attempting to bind port 8045"
    results.append({"case": "startup_port_8045_rejected", "passed": True})

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, help="Path to antigravity-tools executable")
    parser.add_argument("--report", required=True, help="Path to write JSON verification report")
    args = parser.parse_args()

    binary_path = Path(args.binary).resolve()
    if not binary_path.exists():
        raise FileNotFoundError(f"Binary not found: {binary_path}")

    temp_root = Path(tempfile.mkdtemp(prefix="tinix_m0_verify_"))
    mock_state = MockUpstreamState()
    mock_port = find_free_port()
    mock_server = ThreadingHTTPServer(("127.0.0.1", mock_port), make_handler(mock_state))
    mock_thread = threading.Thread(target=mock_server.serve_forever, daemon=True)
    mock_thread.start()
    mock_upstream_url = f"http://127.0.0.1:{mock_port}/v1internal"

    report = {
        "binary": str(binary_path),
        "mockUpstreamUrl": mock_upstream_url,
        "startupGuards": [],
        "scenarios": [],
        "allPassed": False,
    }

    try:
        report["startupGuards"] = run_startup_guard_tests(binary_path, temp_root)

        proxy_port = find_free_port()
        while proxy_port == 8045:
            proxy_port = find_free_port()
        profile_dir = temp_root / "isolated-tinix-profile"
        write_isolated_profile(profile_dir, port=proxy_port, request_timeout=10)

        env = os.environ.copy()
        env["TINIX_M0_MOCK_UPSTREAM_URL"] = mock_upstream_url
        env["TINIX_M0_MOCK_UPSTREAM_TIMEOUT_MS"] = "1500"
        env["ABV_DATA_DIR"] = str(profile_dir)

        proc, stdout_f, stderr_f, stdout_path, stderr_path = spawn_proxy_process(
            binary_path, profile_dir, proxy_port, env
        )

        try:
            health = wait_for_health(proxy_port, proc, stdout_path, stderr_path)
            assert health.get("buildFeature") == "tinix-m0-hardened", f"Unexpected health: {health}"
            m0_policy = health.get("m0Policy", {})
            assert m0_policy.get("maxUpstreamAttemptsPerRequest") == 1
            assert m0_policy.get("maxRetries") == 0
            assert m0_policy.get("accountRotation") is False
            report["scenarios"].append(
                {"scenario": "health_and_m0_policy", "status": 200, "upstreamAttempts": 0, "passed": True}
            )

            status_models, models_payload = http_request(proxy_port, "GET", "/v1/models")
            assert status_models == 200
            model_ids = [m.get("id") for m in models_payload.get("data", [])]
            assert "gemini-3.1-flash-image" in model_ids
            report["scenarios"].append(
                {"scenario": "list_models_includes_exact_model", "status": 200, "upstreamAttempts": 0, "passed": True}
            )

            # Scenario: HTTP 200 (2K, prompt unmodified even when quality=hd & style=vivid)
            mock_state.reset(mode="200")
            raw_prompt = "Minimalist geometric studio render without prompt tampering"
            status_2k, resp_2k = http_request(
                proxy_port,
                "POST",
                "/v1/images/generations",
                {
                    "model": "gemini-3.1-flash-image",
                    "prompt": raw_prompt,
                    "n": 1,
                    "size": "2048x2048",
                    "image_size": "2K",
                    "aspect_ratio": "1:1",
                    "quality": "hd",
                    "style": "vivid",
                    "response_format": "b64_json",
                },
            )
            calls_2k = mock_state.get_calls()
            assert status_2k == 200, f"Expected 200, got {status_2k}: {resp_2k}"
            assert len(calls_2k) == 1, f"Expected 1 upstream call, got {len(calls_2k)}"
            sent_body_2k = calls_2k[0]["body"]
            assert sent_body_2k["model"] == "gemini-3.1-flash-image"
            assert (
                sent_body_2k["request"]["contents"][0]["parts"][0]["text"] == raw_prompt
            ), "Prompt was modified!"
            assert sent_body_2k["request"]["generationConfig"]["candidateCount"] == 1
            assert sent_body_2k["request"]["generationConfig"]["imageConfig"] == {
                "aspectRatio": "1:1",
                "imageSize": "2K",
            }
            assert resp_2k["data"][0]["b64_json"] == ONE_BY_ONE_PNG_B64
            report["scenarios"].append(
                {"scenario": "http_200_2k_unmodified_prompt", "status": status_2k, "upstreamAttempts": len(calls_2k), "passed": True}
            )

            # Scenario: HTTP 200 (4K, 1:1, n=1)
            mock_state.reset(mode="200")
            status_4k, resp_4k = http_request(
                proxy_port,
                "POST",
                "/v1/images/generations",
                {
                    "model": "gemini-3.1-flash-image",
                    "prompt": raw_prompt,
                    "n": 1,
                    "size": "4096x4096",
                    "image_size": "4K",
                    "aspect_ratio": "1:1",
                    "response_format": "b64_json",
                },
            )
            calls_4k = mock_state.get_calls()
            assert status_4k == 200
            assert len(calls_4k) == 1
            assert calls_4k[0]["body"]["request"]["generationConfig"]["imageConfig"] == {
                "aspectRatio": "1:1",
                "imageSize": "4K",
            }
            report["scenarios"].append(
                {"scenario": "http_200_4k", "status": status_4k, "upstreamAttempts": len(calls_4k), "passed": True}
            )

            # Scenarios: HTTP 401, 403, 429, 500, 503 -> each must send exactly 1 upstream request
            for code in ["401", "403", "429", "500", "503"]:
                mock_state.reset(mode=code)
                st, _ = http_request(
                    proxy_port,
                    "POST",
                    "/v1/images/generations",
                    {
                        "model": "gemini-3.1-flash-image",
                        "prompt": f"Test upstream {code}",
                        "n": 1,
                        "size": "2048x2048",
                        "image_size": "2K",
                        "response_format": "b64_json",
                    },
                )
                calls = mock_state.get_calls()
                assert st == int(code), f"Expected HTTP {code}, got {st}"
                assert len(calls) == 1, f"Expected 1 upstream attempt on {code}, got {len(calls)}"
                report["scenarios"].append(
                    {"scenario": f"http_{code}_single_attempt", "status": st, "upstreamAttempts": len(calls), "passed": True}
                )

            # Scenario: Upstream Timeout (TINIX_M0_MOCK_UPSTREAM_TIMEOUT_MS=1500ms, mock sleeps 2.5s)
            mock_state.reset(mode="200", delay_sec=2.5)
            st_timeout, _ = http_request(
                proxy_port,
                "POST",
                "/v1/images/generations",
                {
                    "model": "gemini-3.1-flash-image",
                    "prompt": "Test upstream timeout",
                    "n": 1,
                    "size": "2048x2048",
                    "image_size": "2K",
                    "response_format": "b64_json",
                },
                timeout=10.0,
            )
            calls_timeout = mock_state.get_calls()
            assert st_timeout == 502, f"Expected 502 on upstream timeout, got {st_timeout}"
            assert len(calls_timeout) == 1, f"Expected 1 upstream attempt on timeout, got {len(calls_timeout)}"
            report["scenarios"].append(
                {"scenario": "upstream_timeout_single_attempt", "status": st_timeout, "upstreamAttempts": len(calls_timeout), "passed": True}
            )

            # Scenario: Connection Reset
            mock_state.reset(mode="reset")
            st_reset, _ = http_request(
                proxy_port,
                "POST",
                "/v1/images/generations",
                {
                    "model": "gemini-3.1-flash-image",
                    "prompt": "Test upstream connection reset",
                    "n": 1,
                    "size": "2048x2048",
                    "image_size": "2K",
                    "response_format": "b64_json",
                },
            )
            calls_reset = mock_state.get_calls()
            assert st_reset == 502, f"Expected 502 on connection reset, got {st_reset}"
            assert len(calls_reset) == 1, f"Expected 1 upstream attempt on connection reset, got {len(calls_reset)}"
            report["scenarios"].append(
                {"scenario": "connection_reset_single_attempt", "status": st_reset, "upstreamAttempts": len(calls_reset), "passed": True}
            )

            # Scenario: Interrupted Client Request (client closes socket while upstream is in-flight)
            mock_state.reset(mode="200", delay_sec=1.0)
            conn = http.client.HTTPConnection("127.0.0.1", proxy_port, timeout=5)
            body_interrupted = json.dumps(
                {
                    "model": "gemini-3.1-flash-image",
                    "prompt": "Test interrupted client request",
                    "n": 1,
                    "size": "2048x2048",
                    "image_size": "2K",
                    "response_format": "b64_json",
                }
            ).encode("utf-8")
            conn.request(
                "POST",
                "/v1/images/generations",
                body=body_interrupted,
                headers={"Content-Type": "application/json"},
            )
            wait_in_flight_deadline = time.time() + 2.5
            while time.time() < wait_in_flight_deadline and len(mock_state.get_calls()) == 0:
                time.sleep(0.05)
            conn.close()
            time.sleep(1.2)
            calls_interrupted = mock_state.get_calls()
            assert len(calls_interrupted) == 1, f"Expected 1 upstream attempt on interrupted request, got {len(calls_interrupted)}"
            report["scenarios"].append(
                {"scenario": "interrupted_client_request_single_attempt", "status": "client_aborted", "upstreamAttempts": len(calls_interrupted), "passed": True}
            )

            # Scenarios: Pre-send Rejections (0 upstream attempts)
            pre_send_cases = [
                (
                    "missing_model_rejected_before_send",
                    {"prompt": "Missing model", "n": 1, "size": "2048x2048", "image_size": "2K"},
                ),
                (
                    "suffixed_model_rejected_before_send",
                    {
                        "model": "gemini-3.1-flash-image-4k",
                        "prompt": "Suffixed model",
                        "n": 1,
                        "size": "4096x4096",
                        "image_size": "4K",
                    },
                ),
                (
                    "other_model_rejected_before_send",
                    {
                        "model": "gemini-3-pro-image",
                        "prompt": "Other model",
                        "n": 1,
                        "size": "2048x2048",
                        "image_size": "2K",
                    },
                ),
                (
                    "invalid_n_rejected_before_send",
                    {
                        "model": "gemini-3.1-flash-image",
                        "prompt": "Two images",
                        "n": 2,
                        "size": "2048x2048",
                        "image_size": "2K",
                    },
                ),
                (
                    "resolution_1k_rejected_before_send",
                    {
                        "model": "gemini-3.1-flash-image",
                        "prompt": "1K downgrade forbidden",
                        "n": 1,
                        "size": "1024x1024",
                        "image_size": "1K",
                    },
                ),
                (
                    "non_square_ratio_rejected_before_send",
                    {
                        "model": "gemini-3.1-flash-image",
                        "prompt": "16:9 forbidden",
                        "n": 1,
                        "size": "2048x2048",
                        "image_size": "2K",
                        "aspect_ratio": "16:9",
                    },
                ),
                (
                    "conflicting_size_and_image_size_rejected_before_send",
                    {
                        "model": "gemini-3.1-flash-image",
                        "prompt": "Conflict forbidden",
                        "n": 1,
                        "size": "4096x4096",
                        "image_size": "2K",
                    },
                ),
            ]
            for name, req_body in pre_send_cases:
                mock_state.reset(mode="200")
                st_rej, _ = http_request(proxy_port, "POST", "/v1/images/generations", req_body)
                calls_rej = mock_state.get_calls()
                assert st_rej == 400, f"Case {name}: expected 400, got {st_rej}"
                assert len(calls_rej) == 0, f"Case {name}: expected 0 upstream calls, got {len(calls_rej)}"
                report["scenarios"].append(
                    {"scenario": name, "status": st_rej, "upstreamAttempts": len(calls_rej), "passed": True}
                )

            # Verify SQLite counter in isolated profile recorded exact attempts and no research 3/5 DB exists
            assert not (profile_dir / "native_image_attempt_budget.sqlite3").exists()
            m0_db = profile_dir / "tinix_m0_image_attempts.sqlite3"
            assert m0_db.exists()
            with sqlite3.connect(m0_db) as conn_db:
                used_count = conn_db.execute(
                    "SELECT used FROM tinix_m0_attempt_counter WHERE singleton = 1"
                ).fetchone()[0]
                req_rows = conn_db.execute(
                    "SELECT COUNT(*) FROM tinix_m0_request_attempts"
                ).fetchone()[0]
            # 2 (200 2K + 4K) + 5 (401,403,429,500,503) + 1 (timeout) + 1 (reset) + 1 (interrupted) = 10 attempts
            assert used_count == 10, f"Expected 10 total upstream attempts in DB, got {used_count}"
            assert req_rows == 10, f"Expected 10 unique correlation_id rows, got {req_rows}"

        except Exception:
            stdout_f.flush()
            stderr_f.flush()
            print("=== PROC1 STDOUT ===", file=sys.stderr)
            print(read_log_file(stdout_path), file=sys.stderr)
            print("=== PROC1 STDERR ===", file=sys.stderr)
            print(read_log_file(stderr_path), file=sys.stderr)
            raise
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
            stdout_f.close()
            stderr_f.close()

        # Second subprocess check: Account missing exact model ("gemini-3.1-flash-image")
        proxy_port_2 = find_free_port()
        while proxy_port_2 == 8045:
            proxy_port_2 = find_free_port()
        profile_dir_2 = temp_root / "isolated-no-exact-model"
        write_isolated_profile(
            profile_dir_2,
            port=proxy_port_2,
            models_on_accounts=["gemini-3-pro-image"],
        )
        env2 = os.environ.copy()
        env2["TINIX_M0_MOCK_UPSTREAM_URL"] = mock_upstream_url
        env2["TINIX_M0_MOCK_UPSTREAM_TIMEOUT_MS"] = "1500"
        env2["ABV_DATA_DIR"] = str(profile_dir_2)
        proc2, stdout2_f, stderr2_f, stdout2_path, stderr2_path = spawn_proxy_process(
            binary_path, profile_dir_2, proxy_port_2, env2
        )
        try:
            wait_for_health(proxy_port_2, proc2, stdout2_path, stderr2_path)
            st_models2, models2 = http_request(proxy_port_2, "GET", "/v1/models")
            assert st_models2 == 200
            ids2 = [m.get("id") for m in models2.get("data", [])]
            assert "gemini-3.1-flash-image" not in ids2, "Expected /v1/models to omit unavailable exact model"

            mock_state.reset(mode="200")
            st_unavail, msg_unavail = http_request(
                proxy_port_2,
                "POST",
                "/v1/images/generations",
                {
                    "model": "gemini-3.1-flash-image",
                    "prompt": "Account lacks exact model",
                    "n": 1,
                    "size": "2048x2048",
                    "image_size": "2K",
                    "response_format": "b64_json",
                },
            )
            calls_unavail = mock_state.get_calls()
            assert st_unavail == 400, f"Expected 400 when account lacks exact model, got {st_unavail}: {msg_unavail}"
            assert "MODEL_UNAVAILABLE_ON_ACCOUNT" in str(msg_unavail)
            assert len(calls_unavail) == 0
            report["scenarios"].append(
                {
                    "scenario": "account_missing_exact_model_rejected_before_send",
                    "status": st_unavail,
                    "upstreamAttempts": 0,
                    "passed": True,
                }
            )
        except Exception:
            stdout2_f.flush()
            stderr2_f.flush()
            print("=== PROC2 STDOUT ===", file=sys.stderr)
            print(read_log_file(stdout2_path), file=sys.stderr)
            print("=== PROC2 STDERR ===", file=sys.stderr)
            print(read_log_file(stderr2_path), file=sys.stderr)
            raise
        finally:
            proc2.terminate()
            try:
                proc2.wait(timeout=5)
            except Exception:
                proc2.kill()
            stdout2_f.close()
            stderr2_f.close()

        # Third subprocess check: custom_mapping attempting to rewrite gemini-3.1-flash-image is blocked
        proxy_port_3 = find_free_port()
        while proxy_port_3 == 8045:
            proxy_port_3 = find_free_port()
        profile_dir_3 = temp_root / "isolated-custom-mapping-blocked"
        write_isolated_profile(
            profile_dir_3,
            port=proxy_port_3,
            models_on_accounts=["gemini-3.1-flash-image"],
            custom_mapping={"gemini-3.1-flash-image": "gemini-3-pro-image"},
        )
        env3 = os.environ.copy()
        env3["TINIX_M0_MOCK_UPSTREAM_URL"] = mock_upstream_url
        env3["TINIX_M0_MOCK_UPSTREAM_TIMEOUT_MS"] = "1500"
        env3["ABV_DATA_DIR"] = str(profile_dir_3)
        proc3, stdout3_f, stderr3_f, stdout3_path, stderr3_path = spawn_proxy_process(
            binary_path, profile_dir_3, proxy_port_3, env3
        )
        try:
            wait_for_health(proxy_port_3, proc3, stdout3_path, stderr3_path)
            mock_state.reset(mode="200")
            st_rewrite, msg_rewrite = http_request(
                proxy_port_3,
                "POST",
                "/v1/images/generations",
                {
                    "model": "gemini-3.1-flash-image",
                    "prompt": "Custom mapping rewrite blocked",
                    "n": 1,
                    "size": "2048x2048",
                    "image_size": "2K",
                    "response_format": "b64_json",
                },
            )
            calls_rewrite = mock_state.get_calls()
            assert st_rewrite == 400
            assert "MODEL_REWRITE_FORBIDDEN" in str(msg_rewrite)
            assert len(calls_rewrite) == 0
            report["scenarios"].append(
                {
                    "scenario": "custom_mapping_rewrite_rejected_before_send",
                    "status": st_rewrite,
                    "upstreamAttempts": 0,
                    "passed": True,
                }
            )
        except Exception:
            stdout3_f.flush()
            stderr3_f.flush()
            print("=== PROC3 STDOUT ===", file=sys.stderr)
            print(read_log_file(stdout3_path), file=sys.stderr)
            print("=== PROC3 STDERR ===", file=sys.stderr)
            print(read_log_file(stderr3_path), file=sys.stderr)
            raise
        finally:
            proc3.terminate()
            try:
                proc3.wait(timeout=5)
            except Exception:
                proc3.kill()
            stdout3_f.close()
            stderr3_f.close()

        report["allPassed"] = True
    finally:
        mock_server.shutdown()
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps(report, indent=2), encoding="utf-8")
        shutil.rmtree(temp_root, ignore_errors=True)

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
