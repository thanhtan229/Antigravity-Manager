# Native 2K/4K Test Report

## Current status — 2026-10-16 (UTC+7)

**READY_FOR_LIVE_TEST — awaiting separate approval for two real image requests.**

**Experiment verdict: INCONCLUSIVE / NOT RUN.** Readiness means the isolated proxy and safety controls are ready; it does not mean native 2K/4K has been demonstrated. Real image-generation requests sent: **0**. No generated samples exist.

Readiness checked at `2026-10-16T01:53:28.9298159Z`. The API key rotated during this task authenticates successfully; its value is not included here. Google OAuth was not repeated, and Google/Antigravity credentials were not read or exported by the agent.

## Source, fork and build provenance

| Item | Evidence |
|---|---|
| Audited base commit | `2f7641a68e90a2a45e5507903d127bac2d698d53` |
| Built source commit | `b32c2774ed73f287a33071432822560291811b94` |
| Writable fork | https://github.com/thanhtan229/Antigravity-Manager |
| Branch | `codex/native-4k-safe-test` |
| CI run | [38781819143](https://github.com/thanhtan229/Antigravity-Manager/actions/runs/38781819143) — SUCCESS |
| Toolchain/target | GitHub `windows-2022`, VS2022/MSVC, stable Rust, x64 release |
| Build feature | `native-image-live-test` |
| Artifact retention | 7 days on GitHub; downloaded copy is retained locally |
| Local artifact directory | `.runtime/native-image-safe-b32c2774/` |

`origin` remains `https://github.com/lbjlaq/Antigravity-Manager.git`; separate remote `fork` points to the user's fork. Fork `main` remains at the audited base SHA. All pushes were fast-forward pushes to the requested branch. No additional fork, PR, release, force push, or upstream push was created during this continuation.

Only application source, mock/test source and workflow files were committed. Runtime profile, API keys, OAuth data, cookies, databases and samples were excluded. Added lines and changed paths were reviewed for credential patterns and private/runtime paths. The artifact contains only the executable, WebView2 loader, source commit and checksum manifest; CI had no Google authentication material.

| File | SHA-256 verified after download |
|---|---|
| `antigravity-tools.exe` | `55664fcdd2f0031ae23d266c8a0c3415a9b2096e924736b7d51371bae2c85df0` |
| `WebView2Loader.dll` | `8427b1fc58ec707813e5c0a51eb5d69397bb333250a7b891be4d3b123f1e0f1c` |

The source-commit file matches the run's `headSha`. Checksums match the CI manifest. This is a custom unsigned test build with recorded provenance, not a reproducible-build proof.

## Build and mock verification — PASS

- **13 targeted Rust tests passed** in the release feature build: five persistent-budget/evidence tests, seven upstream-client mock tests, and one image-count/account-attempt-policy test.
- **4 Python harness tests passed** both locally and in CI: exactly two square 2K/4K calls with identical prompt and `n=1`; stop after the first HTTP error; no calls without confirmation; reject a plan exceeding two calls before network access.
- All image-generation traffic in tests used localhost mock upstreams and synthetic image fixtures. These fixtures do not test Google's image quality or resolution.
- CI unit-test version lookup is offline under `cfg(test)`. Windows Firewall profiles are enabled in the ephemeral runner and the Rust test executable is blocked from public-unicast egress. No real Google image request was made by CI. The normal application binary is never launched in CI.
- Local formatter/diff checks, Python syntax and PowerShell launcher syntax passed. This was a targeted safety suite; the full application test suite was not executed.

| Safety test | Observed/asserted result |
|---|---|
| 32 concurrent SQLite reservations | Exactly 5 accepted; 27 rejected |
| Budget reopen | Five remain consumed; a later reservation is rejected |
| Six separate child-process invocations | First five reserve successfully; sixth is rejected using the same DB |
| Corrupt SQLite state | Fails closed |
| Redacted attempt evidence | Keeps resolution fields, omits secret/project/prompt fields; refuses overwrite |
| HTTP 500 from first mock endpoint | One hit and one slot consumed; fallback receives zero hits |
| Network failure at first endpoint | One slot consumed; fallback receives zero hits |
| HTTP 307 redirect | Original endpoint receives one hit; redirect target receives zero hits |
| Repeated calls/new client instances | Five upstream hits; sixth call rejected before send |
| 24 concurrent mock calls | Exactly 5 upstream hits and 19 budget rejections |
| HTTP/2 retry configuration | Configured retry count is zero |
| Image request count | `n=1` accepted; `n=0`, `2`, `5` rejected; handler account-attempt limit is one |
| Non-image endpoint fallback | Existing fallback behavior remains available |

The upstream mocks use HTTP/1 on loopback. The HTTP/2 check verifies the library retry configuration; it does not simulate HTTP/2 GOAWAY frames. Source inspection confirms `rquest` retries are set to zero and redirects disabled in the test build. Conservative reservation can overcount a connection failure or crash; it cannot release a spent slot to cause an extra send.

CI environment issues resolved: NASM was missing initially; official NASM 3.02 ZIP is downloaded with pinned SHA-256 `161d0bfaff53c2f9e9f3e69fd0672323ebabafd1268976a5cec11be92a19aee7`. Tauri's lib test executable lacked a Common Controls v6 manifest and exited before tests; CI embeds a minimal manifest into the test executable only using Windows SDK `mt.exe`, following [the upstream Tauri issue](https://github.com/tauri-apps/tauri/issues/13419). An invalid firewall address range was corrected and validated before compilation. Application build script and application manifest were not altered to bypass compiler errors. The local VS2019 environment was not upgraded or repaired.

Local CI records: `.build-tools/native-image-ci-38781819143.log` and `.build-tools/native-image-ci-38781819143.json`.

## Hard upstream image-attempt boundary

The `native-image-live-test` feature implements the controls in `proxy/handlers/openai.rs`, `proxy/upstream/client.rs` and `proxy/image_attempt_budget.rs`:

1. `/v1/images/generations` accepts exactly one image per request; the upstream payload uses `candidateCount: 1`.
2. The image handler makes one account attempt. The upstream client selects only the first configured Google endpoint for `generateContent` with `requestType=image_gen`; HTTP errors and socket failures return without endpoint fallback or header-downgrade retry.
3. Forwarding proxies, environment proxies, automatic redirects and rquest HTTP/2 retries are disabled in the test client. Client construction fails closed rather than falling back to an unrestricted bare client.
4. Immediately before each `send()`, SQLite commits one reservation under `.runtime/profile/native_image_attempt_budget.sqlite3`. `BEGIN IMMEDIATE`, a five-slot constraint, a busy timeout and `synchronous=FULL` coordinate concurrent callers and persist across normal process restarts. The path requires explicit `ABV_DATA_DIR` selection.
5. Every attempted send consumes a slot even if it later fails. No slot is refunded. Storage/evidence-write errors block sending. A crash between reservation and send may consume a slot without a Google request. No reset API was added.

**Maximum: five reserved upstream image sends total for this profile, across repeated clients, concurrency and normal restarts.** This assumes the counter file is retained and not deliberately deleted, restored from an earlier snapshot, or manually edited. The tests validate process restart behavior; they do not claim protection against manual state rollback or disk/hardware failure.

Current live budget: **0/5 consumed**. No ledger or attempt evidence file exists yet; the first real reservation initializes the counter and consumes slot 1. Mock tests used disposable storage and did not touch the live profile's budget. Never delete the live counter/evidence files between tests or restarts.

## Running local instance and model evidence — PASS

| Check | Result |
|---|---|
| Mode | Separate custom binary, `--headless`, hidden background process |
| PID | `43396` |
| Process start UTC | `2026-10-16T01:52:56.4917280Z` |
| Binary | `.runtime/native-image-safe-b32c2774/antigravity-tools.exe` |
| Isolated data directory | `.runtime/profile`, explicitly passed as `ABV_DATA_DIR` |
| Listener | `127.0.0.1:8045`, owned by PID `43396` |
| Health | `GET /health`: `ok`, version `4.9.6` |
| Authenticated proxy status | Running; one active account |
| Account-derived model catalogue | `only_raw_quota_models=true`; 17 IDs |
| Image model currently listed | **`gemini-3.1-flash-image`** |
| Preferred `gemini-3-pro-image` | **Not listed** in the current account-derived catalogue |
| Warmup / debug logging | Both disabled |
| Public exposure | Loopback binding enforced by this build; no tunnel/deployment enabled |

The earlier stock process was already stopped and port 8045 was free. The verified test binary was started after the final code/build/launcher changes. Its process path, source checksum, listener owner, health and authenticated APIs were verified. No active task was interrupted. The existing Antigravity Desktop profile and other projects were not modified.

`gemini-3.1-flash-image` is the current candidate because it appears in the authenticated account-derived catalogue. This is stronger evidence than the stock list of built-in aliases, but does not prove current remaining quota or guarantee a successful generation. No failing probe was sent to test `gemini-3-pro-image`. The exact model emitted after account-specific resolution will be recorded by the attempt evidence on the first approved call.

The previous local API key was rotated; the replacement remains in the isolated local config and was used in memory for authenticated checks. It was not printed, added to a commit or included in an artifact. Headless authentication is `all_except_health`. No OAuth credential file, token, cookie or account database was inspected/exported by the agent.

## Planned live run — not approved or executed

Use the reusable harness `tests/native_image_resolution.py` with an explicit `--model gemini-3.1-flash-image` argument. Its historical default is the preferred Pro alias, which is currently unavailable and must be overridden. The script additionally requires `--confirm-live` and interactive `GENERATE` confirmation; no live command has been run.

Prompt for both calls: `A clean vector illustration of one red circle centered on a plain white background.`

| Call | Current selected request model | Ratio input | image_size | Images |
|---|---|---|---|---|
| 1 | `gemini-3.1-flash-image` | `1024x1024` (1:1) | `2K` | 1 |
| 2 | Same | Same | `4K` | 1 |

Calls are sequential, use the identical prompt and stop on the first failure. No automatic client retry, 1K baseline, 16:9 extension, fallback model probe or extra error test is included in this approval scope.

Client endpoint: `http://127.0.0.1:8045/v1/images/generations`. The test build's selected upstream is `https://daily-cloudcode-pa.googleapis.com/v1internal:generateContent`; no fallback is permitted for these image requests. This endpoint is source-confirmed; no real image attempt has yet exercised it.

Immediately before sending, the proxy writes `native_image_attempt_budget.attempt-N.json` beside the counter, using an allowlist of endpoint, timestamp, attempt number, resolved model, requestType, candidateCount, imageSize and aspectRatio. All headers and other body fields, including project/account/session identifiers, prompt and inline data, are omitted. This is pre-send evidence, not proof that Google accepted or honored the requested size. The harness saves received bytes unchanged plus dimensions, format, available metadata, hash, timing and errors under `samples/`.

## Quota/cost and native-resolution conclusions

No billing was enabled, no credits were purchased, and no public Gemini Developer API key/paid per-image endpoint was configured. The test uses the Cloud Code internal subscription route. The user's reported AI Ultra subscription does not establish, from this proxy alone, the quota or charge treatment of this internal endpoint. Remaining image quota and any quota delta have not been independently measured. An errored attempt may still consume quota if Google processed it before the error was observed. Separate approval is required before the two real calls; additional calls require further approval.

| Classification | Current result |
|---|---|
| Native requested | **PARTIALLY OBSERVED**: the reserved 2K request evidence records `imageSize=2K`; no 4K request was sent |
| Proxy output processing | Prior source audit found no output resize/upscale stage; the test patch adds no such processing. Real output has not been inspected |
| Native output verified | **NOT RUN / INCONCLUSIVE** |
| Google internal processing | **INCONCLUSIVE**: proxy source, matching dimensions and a 2K/4K request cannot rule out internal Google upscaling |
| Safety/build/readiness | **PASS — ready for a separately approved live attempt; first 2K outcome unresolved** |
| Experimental native 2K/4K claim | **INCONCLUSIVE**, awaiting approved live samples |

No native claim will be based on dimensions alone. The only remaining action needed before generation is explicit user approval for this two-request plan and its currently available model. If runtime state/model availability changes, readiness must be rechecked without sending an image probe.

## Earlier release provenance (retained; not the running test binary)

Official v4.9.6 tag source: `b601f5e7375343343922ccab5e50653bc3450c3b`. Original MSI SHA-256: `5ac5fe7648f5aec58707a1ac3ace5aef6bd8243f0e608f6822db0f5fb058ed12`; extracted official EXE SHA-256: `ab0eae65d2de71a19071e3c0d9e0d098380e9531bc23f202c62563d1822b5f72`. That stock binary was suitable for local setup but lacked the strict aggregate attempt cap, so it is not used for live testing. The prior report is archived locally at `.build-tools/NATIVE_4K_TEST_REPORT.before-ready.md`; the Phase 1 research document remains `NATIVE_4K_RESEARCH.md`.

## Live test update — 2026-10-16

The user approved exactly two live requests (2K then 4K) for `gemini-3.1-flash-image`, square 1:1, one image each, with no retry or fallback. The run did **not** complete. Stop condition was applied after the runner failed to preserve the first response; no further image-generation call was made.

### Request 1 — 2K square

- Result: **INCONCLUSIVE**. The isolated proxy durably reserved attempt 1 immediately before its upstream transport send. The runner was interrupted before it captured the HTTP response, returned image, or timing. The evidence proves the proxy prepared/reserved the request; it does not prove Google accepted or completed it.
- Safe upstream evidence: endpoint `https://daily-cloudcode-pa.googleapis.com/v1internal:generateContent`; method `generateContent`; resolved model `gemini-3.1-flash-image`; `requestType=image_gen`; `candidateCount=1`; `imageSize=2K`; `aspectRatio=1:1`. Prompt, headers, account/project/session identifiers, and credentials are omitted.
- Pixel dimensions, image format/metadata, image hash, response status, and processing time: unavailable; no sample was saved.
- Attempt budget: **1/5 reserved**. Do not reset or delete the durable budget. Whether the upstream service received or charged/consumed quota for the interrupted attempt is unknown.
- Native classification: **Native requested at the proxy: YES (request was constructed and attempt reserved). Native output verified at proxy: NO (no response captured). Google internal processing: INCONCLUSIVE.**

### Request 2 — 4K square

- Result: **NOT SENT**. Stopped after the first request result became unavailable. No 4K request was issued.

### Run outcome and security

- Overall live test: **INCONCLUSIVE / INCOMPLETE**, not a pass. The requested two-point comparison was not obtained.
- The attempt ledger and redacted evidence file are retained in the isolated profile. No OAuth credentials were read or exported, and the local API key was not printed. No additional image-generation calls were made.
- No inference about Google quota or billing is possible from the interrupted call. The consumed proxy reservation is conservative; Google-side quota consumption is unobservable here.
- A new live attempt requires a separately approved run plan because the first request outcome is unknown and the hard counter has already advanced to 1/5.

## Interruption investigation and harness hardening — 2026-10-16

No image-generation endpoint was queried during this investigation. The current Windows binary, isolated profile, original evidence JSON, and durable attempt database were left untouched.

### Evidence and timeline

- The existing evidence file is unchanged (SHA-256 `964822072f106e127662ee8f0769f2eb4fde7a7e153af47075ec09398b256bf3`). It records reservation `1`, timestamp `2026-10-16T01:59:42.301701300Z`, endpoint `https://daily-cloudcode-pa.googleapis.com/v1internal:generateContent`, model `gemini-3.1-flash-image`, `requestType=image_gen`, `candidateCount=1`, `imageSize=2K`, `aspectRatio=1:1`, and stage `reserved_before_send`.
- The persistent database still reads `used=1, limit_count=5`; no reset or write was performed. No later attempt evidence exists.
- Source ordering in `src-tauri/src/proxy/upstream/client.rs` calls `record_attempt_evidence_at(...)` and then executes `req_builder.send().await`. This places the last source-backed point immediately before the upstream send future. The running release has no separate durable `SEND_STARTED` marker, so the evidence does not prove that a socket write completed or that Google received the request.
- The original harness printed its confirmation prompt, then had no progress message while `urllib.request.urlopen(..., timeout=600)` waited. It printed the result only after completion. `input()` flushes its prompt; the earlier visible prompt was therefore not evidence of stdout being stuck. The reserved proxy evidence proves the confirmation gate was passed and the request reached the local image-generation route. The blank interval is consistent with waiting for a proxy response, but cannot distinguish proxy work, network wait, or an unobserved error.
- The previous Codex `write_stdin` call reported that stdin was closed, so later keyboard input could not be sent through that process session. The run was then interrupted. That explains why the client result was not collected; it does not identify the upstream outcome.
- There is no harness result JSON, response body, `.pending` response, image sample, or running harness process. `samples/` contains only its pre-existing README. The proxy request-log database has zero request rows and was last modified before this run, so it supplies no status or duration evidence. No HTTP status, timeout exception, Google acceptance, or quota delta can be recovered from local files.

### Causal assessment

- Confirmation accepted and local proxy image route reached: **HIGH confidence**, based on the only durable reservation record and the harness control flow.
- Proxy attempt reservation immediately before the source's send call: **HIGH confidence**.
- Upstream socket send began or Google received the request: **UNKNOWN**; no post-send instrumentation or network response record exists.
- Google HTTP status, completion, timeout, quota use, or returned image: **UNKNOWN**. No native output conclusion is possible.
- The most defensible explanation for the apparent hang is **waiting without progress output for the local proxy response**, followed by interruption. It was not blocked at the confirmation prompt. It is not possible to attribute the wait specifically to Google's service from the retained evidence.

### Harness changes and mock-only verification

`tests/native_image_resolution.py` now writes an fsynced, credential-free event journal and reports `AWAITING_CONFIRMATION`, `SEND_STARTED`, `RESPONSE_RECEIVED`, `SAVED`, and `INTERRUPTED`/`PROCESSING_FAILED` with timestamps. `SEND_STARTED` means the client is about to call the local proxy; it does not claim Google send status. The proxy's existing safe attempt file remains the independent `RESERVED` evidence.

For non-interactive runs, piped stdin is never treated as approval: without the explicit `--confirmation GENERATE` option the harness exits before a request instead of waiting on a terminal or command-approval UI. Interactive runs still ask for `GENERATE`. Output is flushed at each state. Requests have a configurable socket inactivity timeout (default 600 seconds), disable environment proxies and HTTP redirects, and do not retry. The timeout is per socket inactivity interval, not a guaranteed total wall-clock deadline.

On a successful HTTP response, the raw response envelope is fsynced to a `.pending` file before parsing. A crash during parsing leaves it available for recovery. Decoded image bytes are fsynced to a temporary file and renamed only after completion; the original bytes are not resized. The final report is also written atomically. Event records use an allowlist and exclude prompt, headers, API keys, and account data.

Seven localhost-only mock tests pass: two-call success, stop after HTTP error, piped confirmation rejected without hanging or making a request, configured timeout with one call, redirect blocked, raw response retained before parse failure, and over-limit plan rejected. Python compilation and `git diff --check` pass. No credentials are configured in these mock runs and they cannot contact Google.

### Current safety and retry readiness

- Runtime binary remains the same isolated release at PID 43396, bound only to `127.0.0.1:8045`; the listener and process are present. No application/runtime code changed, so restarting the local proxy was not applicable.
- The original attempt counter remains **1/5** and the original redacted evidence is intact. The new harness changes do not reset, reduce, or bypass it.
- Structurally, the isolated proxy's hard cap and one-endpoint/no-retry behavior remain intact. The improved harness is mock-verified. The unresolved first attempt may have consumed Google quota; this cannot be ruled out.
- **No retry or new live request was made.** A future live request would be a new, separately approved action and would reserve another attempt. Do not repeat 2K as a diagnostic probe; the prior request outcome cannot be reconstructed from available evidence.
- Readiness: **READY_FOR_A_SEPARATELY_APPROVED_TEST WITH THE UPDATED HARNESS**, subject to rechecking the unchanged loopback listener and model before any later approval. The live experiment remains **INCONCLUSIVE**.
