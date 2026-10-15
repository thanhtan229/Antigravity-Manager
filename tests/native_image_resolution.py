#!/usr/bin/env python3
"""Bounded, opt-in image-size comparison against a local Antigravity-Manager proxy."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import struct
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MAX_CALLS = 5
MODEL = "gemini-3-pro-image"
PROMPT = "A clean vector illustration of one red circle centered on a plain white background."
MATRIX = [
    ("square-2k", "1024x1024", "2K"),
    ("square-4k", "1024x1024", "4K"),
]


def image_dimensions(data: bytes) -> tuple[str, int, int]:
    if data.startswith(b"\x89PNG\r\n\x1a\n") and len(data) >= 24:
        width, height = struct.unpack(">II", data[16:24])
        return "PNG", width, height

    if data[:2] == b"\xff\xd8":
        pos = 2
        sof = {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}
        while pos + 4 <= len(data):
            if data[pos] != 0xFF:
                pos += 1
                continue
            while pos < len(data) and data[pos] == 0xFF:
                pos += 1
            if pos >= len(data):
                break
            marker = data[pos]
            pos += 1
            if marker in (0xD8, 0xD9, 0x01) or 0xD0 <= marker <= 0xD7:
                continue
            if pos + 2 > len(data):
                break
            length = int.from_bytes(data[pos : pos + 2], "big")
            if length < 2 or pos + length > len(data):
                break
            if marker in sof and length >= 7:
                height, width = struct.unpack(">HH", data[pos + 3 : pos + 7])
                return "JPEG", width, height
            pos += length
        raise ValueError("Could not find JPEG frame dimensions")

    if data[:4] == b"RIFF" and data[8:12] == b"WEBP" and len(data) >= 30:
        kind = data[12:16]
        if kind == b"VP8X":
            width = 1 + int.from_bytes(data[24:27], "little")
            height = 1 + int.from_bytes(data[27:30], "little")
            return "WebP", width, height
        if kind == b"VP8L" and len(data) >= 25 and data[20] == 0x2F:
            b1, b2, b3, b4 = data[21:25]
            width = 1 + (((b2 & 0x3F) << 8) | b1)
            height = 1 + (((b4 & 0x0F) << 10) | (b3 << 2) | ((b2 & 0xC0) >> 6))
            return "WebP", width, height
        if kind == b"VP8 " and len(data) >= 30 and data[23:26] == b"\x9d\x01\x2a":
            width = int.from_bytes(data[26:28], "little") & 0x3FFF
            height = int.from_bytes(data[28:30], "little") & 0x3FFF
            return "WebP", width, height

    raise ValueError("Unsupported or malformed image format; no output image was altered")


def metadata_summary(data: bytes) -> dict[str, Any]:
    summary: dict[str, Any] = {"metadata_reader": "header-only"}
    try:
        from PIL import Image  # type: ignore[import-not-found]

        from io import BytesIO

        with Image.open(BytesIO(data)) as im:
            summary = {
                "metadata_reader": "Pillow",
                "mode": im.mode,
                "metadata_keys": sorted(str(key) for key in im.info.keys()),
                "exif_tag_ids": sorted(str(key) for key in im.getexif().keys()),
                "icc_profile_present": bool(im.info.get("icc_profile")),
            }
    except ImportError:
        pass
    except Exception as exc:  # metadata failure must not discard a returned sample
        summary["metadata_error"] = str(exc)
    return summary


def safe_json_error(body: bytes) -> str:
    text = body.decode("utf-8", errors="replace")
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            err = obj.get("error", obj)
            if isinstance(err, dict):
                return str(err.get("message", err.get("status", "HTTP error")))[:1200]
    except json.JSONDecodeError:
        pass
    return text[:1200]


def ask_for_live_approval() -> bool:
    print("This will send two real image-generation calls through the configured local proxy.")
    print("Google quota may be consumed. No retry is made by this script or by the isolated test proxy.")
    return input('Type "GENERATE" to continue: ').strip() == "GENERATE"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm-live", action="store_true", help="also require interactive typing of GENERATE")
    parser.add_argument("--base-url", default="http://127.0.0.1:8045/v1", help="must use loopback HTTP")
    parser.add_argument("--model", default=MODEL)
    parser.add_argument("--prompt", default=PROMPT)
    parser.add_argument("--api-key-env", default="AGM_API_KEY", help="environment variable name; value is never recorded")
    parser.add_argument("--max-requests", type=int, default=2, help="client-call ceiling (maximum 2 in this plan)")
    parser.add_argument("--output-dir", default="samples")
    args = parser.parse_args()

    if not args.confirm_live:
        parser.error("live generation is disabled unless --confirm-live is provided")
    if args.max_requests < 1 or args.max_requests > len(MATRIX):
        parser.error(f"--max-requests must be between 1 and {len(MATRIX)}")
    if not args.base_url.startswith("http://127.0.0.1:"):
        parser.error("Only an http://127.0.0.1:<port> proxy URL is allowed")
    api_key = os.environ.get(args.api_key_env)
    if not api_key:
        parser.error(f"Set {args.api_key_env} to the local proxy API key; the value is not printed or written")
    if not ask_for_live_approval():
        print("Cancelled; no request sent.")
        return 2

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_model_alias": args.model,
        "endpoint": args.base_url.rstrip("/") + "/images/generations",
        "prompt": args.prompt,
        "client_call_limit": args.max_requests,
        "client_calls": [],
        "actual_upstream_model_id": None,
        "actual_upstream_endpoint": None,
        "upstream_payload_redacted": "Not captured by this client; capture only through safe, redacted local proxy diagnostics.",
        "proxy_retry_note": "Feature-gated proxy uses a durable global upstream attempt budget of five; this plan makes at most two client calls.",
        "quota_delta": "Not observable by this script.",
        "native_output_verdict": "INCONCLUSIVE",
    }
    successes = 0
    calls = 0
    for name, size, image_size in MATRIX[: args.max_requests]:
        payload = {
            "model": args.model,
            "prompt": args.prompt,
            "size": size,
            "image_size": image_size,
            "n": 1,
            "response_format": "b64_json",
        }
        entry: dict[str, Any] = {
            "label": name,
            "requested_image_size": image_size,
            "requested_ratio": "1:1",
            "client_payload_redacted": payload,
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        start = time.monotonic()
        calls += 1
        req = urllib.request.Request(
            args.base_url.rstrip("/") + "/images/generations",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=600) as response:
                response_body = response.read()
                entry["http_status"] = response.status
            obj = json.loads(response_body)
            images = obj.get("data") if isinstance(obj, dict) else None
            if not isinstance(images, list) or not images:
                raise ValueError("Response did not contain data[0]")
            b64 = images[0].get("b64_json") if isinstance(images[0], dict) else None
            if not isinstance(b64, str):
                raise ValueError("Response did not contain data[0].b64_json")
            raw = base64.b64decode(b64, validate=True)
            fmt, width, height = image_dimensions(raw)
            sample_path = output_dir / f"{name}.{fmt.lower()}"
            sample_path.write_bytes(raw)
            entry.update({
                "ok": True,
                "format": fmt,
                "width": width,
                "height": height,
                "bytes": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "metadata": metadata_summary(raw),
                "sample": str(sample_path),
            })
            successes += 1
        except urllib.error.HTTPError as exc:
            entry.update({"ok": False, "http_status": exc.code, "error": safe_json_error(exc.read())})
        except Exception as exc:
            entry.update({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
        entry["latency_seconds"] = round(time.monotonic() - start, 3)
        entry["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["client_calls"].append(entry)
        print(json.dumps(entry, ensure_ascii=False, indent=2))
        if not entry.get("ok"):
            break

    report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    report["client_request_count"] = calls
    report["successful_client_requests"] = successes
    report["result"] = "COMPLETED" if successes == calls and calls == len(MATRIX[: args.max_requests]) else "FAILED_OR_INCOMPLETE"
    report_path = output_dir / "native_resolution_results.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Saved redacted run metadata to {report_path}")
    if calls > 2:
        print("Internal error: test plan call cap exceeded", file=sys.stderr)
        return 3
    return 0 if report["result"] == "COMPLETED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
