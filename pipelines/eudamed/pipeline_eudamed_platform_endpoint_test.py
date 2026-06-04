#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EUDAHUB Intelligence - EUDAMED Platform Endpoint Test

Tiny diagnostic pipeline that tests whether selected EUDAMED Platform API
endpoints are reachable from the GitHub runner.

It intentionally performs only one request per endpoint:
- page=0
- size=1
- no pagination
- no parallelism
- no retries

Outputs:
- platform_endpoint_test.json
- PLATFORM_ENDPOINT_TEST.md
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


PIPELINE_NAME = "eudamed_platform_endpoint_test"
PIPELINE_VERSION = "1.0.0"
DEFAULT_TIMEOUT_SECONDS = 60

ENDPOINTS = [
    {
        "name": "udi",
        "description": "UDI/Devices list endpoint",
        "url": "https://ec.europa.eu/tools/eudamed/api/devices/udiDiData?page=0&size=1&languageIso2Code=en",
    },
    {
        "name": "eos",
        "description": "Actors/Economic operators endpoint",
        "url": "https://ec.europa.eu/tools/eudamed/api/eos?page=0&size=1&languageIso2Code=en",
    },
    {
        "name": "ses",
        "description": "SES/NB endpoint",
        "url": "https://ec.europa.eu/tools/eudamed/api/ses?page=0&size=1&languageIso2Code=en",
    },
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def utc_iso() -> str:
    return utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def compact_timestamp() -> str:
    return utc_now().strftime("%Y%m%d_%H%M%S")


def log(message: str) -> None:
    print(f"[{utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}] {message}", flush=True)


def lower_contains(text: str | None, needle: str) -> bool:
    return needle.lower() in (text or "").lower()


def classify_result(status_code: int | None, body_text: str | None, error: str | None) -> str:
    text = body_text or ""
    if error:
        return "ERROR"
    if status_code == 200:
        return "OK"
    if lower_contains(text, "web filter") or lower_contains(text, "access denied"):
        return "BLOCKED_WEB_FILTER"
    if status_code == 429:
        return "RATE_LIMITED_429"
    if status_code and 500 <= status_code <= 599:
        return "SERVER_ERROR"
    if status_code and 400 <= status_code <= 499:
        return "CLIENT_ERROR"
    return "UNKNOWN"


def test_endpoint(endpoint: dict[str, str], timeout: int) -> dict[str, Any]:
    url = endpoint["url"]
    started = time.perf_counter()
    tested_at = utc_iso()

    result: dict[str, Any] = {
        "endpoint": endpoint["name"],
        "description": endpoint["description"],
        "url": url,
        "tested_at_utc": tested_at,
        "status_code": None,
        "success": False,
        "classification": None,
        "elapsed_ms": None,
        "content_type": None,
        "content_length_header": None,
        "response_text_length": None,
        "contains_web_filter": False,
        "contains_access_denied": False,
        "retry_after": None,
        "x_ms_correlation_id": None,
        "total_elements": None,
        "total_pages": None,
        "page_size": None,
        "page_number": None,
        "number_of_elements": None,
        "first": None,
        "last": None,
        "empty": None,
        "json_parse_ok": False,
        "error": None,
        "response_excerpt": None,
    }

    try:
        response = requests.get(
            url,
            timeout=timeout,
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Cache-Control": "no-cache",
                "User-Agent": "EUDAHUB-Intelligence endpoint-test/1.0",
            },
        )
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        text = response.text or ""

        result.update(
            {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "elapsed_ms": elapsed_ms,
                "content_type": response.headers.get("content-type"),
                "content_length_header": response.headers.get("content-length"),
                "response_text_length": len(text),
                "contains_web_filter": lower_contains(text, "web filter"),
                "contains_access_denied": lower_contains(text, "access denied"),
                "retry_after": response.headers.get("retry-after"),
                "x_ms_correlation_id": response.headers.get("x-ms-correlation-id"),
                "response_excerpt": text[:1000],
            }
        )

        try:
            data = response.json()
            result["json_parse_ok"] = True
            result["total_elements"] = data.get("totalElements")
            result["total_pages"] = data.get("totalPages")
            result["page_size"] = data.get("size")
            result["page_number"] = data.get("number")
            result["number_of_elements"] = data.get("numberOfElements")
            result["first"] = data.get("first")
            result["last"] = data.get("last")
            result["empty"] = data.get("empty")
        except Exception as exc:
            result["json_parse_ok"] = False
            result["error"] = f"JSON parse failed: {exc}" if response.status_code == 200 else None

        result["classification"] = classify_result(response.status_code, text, None)
        return result

    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        result.update(
            {
                "elapsed_ms": elapsed_ms,
                "error": repr(exc),
                "classification": classify_result(None, None, repr(exc)),
            }
        )
        return result


def infer_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    ok = [r for r in results if r.get("classification") == "OK"]
    blocked = [r for r in results if r.get("classification") == "BLOCKED_WEB_FILTER"]
    rate_limited = [r for r in results if r.get("classification") == "RATE_LIMITED_429"]
    server_error = [r for r in results if r.get("classification") == "SERVER_ERROR"]
    errors = [r for r in results if r.get("classification") == "ERROR"]

    if len(blocked) == len(results):
        conclusion = "ENTIRE_PLATFORM_API_BLOCKED_FROM_RUNNER"
    elif blocked and ok:
        conclusion = "PARTIAL_ENDPOINT_BLOCKING_FROM_RUNNER"
    elif len(ok) == len(results):
        conclusion = "ALL_ENDPOINTS_REACHABLE_FROM_RUNNER"
    elif rate_limited:
        conclusion = "RATE_LIMITING_OBSERVED"
    elif server_error:
        conclusion = "SERVER_ERRORS_OBSERVED"
    elif errors:
        conclusion = "REQUEST_ERRORS_OBSERVED"
    else:
        conclusion = "MIXED_OR_UNKNOWN_RESULT"

    return {
        "tested_endpoints": len(results),
        "ok_count": len(ok),
        "blocked_web_filter_count": len(blocked),
        "rate_limited_429_count": len(rate_limited),
        "server_error_count": len(server_error),
        "request_error_count": len(errors),
        "conclusion": conclusion,
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    summary = payload["summary"]
    results = payload["results"]

    lines: list[str] = [
        "# EUDAMED Platform Endpoint Test",
        "",
        "This diagnostic release tests whether selected EUDAMED Platform API endpoints are reachable from the GitHub runner.",
        "",
        "The test intentionally performs only one request per endpoint: `page=0`, `size=1`, no pagination, no parallelism, and no retries.",
        "",
        "## Run",
        "",
        f"- **Pipeline:** `{payload['pipeline_name']}`",
        f"- **Pipeline version:** `{payload['pipeline_version']}`",
        f"- **Tested at UTC:** `{payload['tested_at_utc']}`",
        f"- **Conclusion:** `{summary['conclusion']}`",
        "",
        "## Summary",
        "",
        "| Metric | Count |",
        "|---|---:|",
        f"| Endpoints tested | {summary['tested_endpoints']} |",
        f"| OK | {summary['ok_count']} |",
        f"| Web Filter / Access Denied | {summary['blocked_web_filter_count']} |",
        f"| 429 rate limited | {summary['rate_limited_429_count']} |",
        f"| Server errors | {summary['server_error_count']} |",
        f"| Request errors | {summary['request_error_count']} |",
        "",
        "## Endpoint results",
        "",
        "| Endpoint | Status | Classification | Total elements | Total pages | Correlation ID |",
        "|---|---:|---|---:|---:|---|",
    ]

    for r in results:
        lines.append(
            "| `{endpoint}` | {status} | `{classification}` | {total_elements} | {total_pages} | `{corr}` |".format(
                endpoint=r.get("endpoint"),
                status=r.get("status_code") if r.get("status_code") is not None else "",
                classification=r.get("classification"),
                total_elements=r.get("total_elements") if r.get("total_elements") is not None else "",
                total_pages=r.get("total_pages") if r.get("total_pages") is not None else "",
                corr=r.get("x_ms_correlation_id") or "",
            )
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- If all three endpoints are blocked with `Web Filter` / `Access Denied`, the GitHub runner or its IP range is likely blocked by the Platform WAF.",
            "- If only one endpoint is blocked, the block is likely endpoint-specific.",
            "- If all three endpoints return `200 OK`, the Platform API is reachable from the runner.",
            "",
            "## Tested URLs",
            "",
        ]
    )

    for r in results:
        lines.append(f"- `{r.get('endpoint')}`: `{r.get('url')}`")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test EUDAMED Platform endpoint reachability.")
    parser.add_argument("--out-dir", default="dist/eudamed_platform_endpoint_test")
    parser.add_argument("--timestamp", default=None, help="Optional timestamp YYYYMMDD_HHMMSS.")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = args.timestamp or compact_timestamp()
    tested_at = utc_iso()

    log("=== EUDAMED Platform Endpoint Test started ===")
    log(f"timestamp={ts} out_dir={out_dir}")

    results = []
    for endpoint in ENDPOINTS:
        log(f"Testing {endpoint['name']}: {endpoint['url']}")
        result = test_endpoint(endpoint, args.timeout)
        log(
            f"{endpoint['name']} status={result.get('status_code')} "
            f"classification={result.get('classification')} "
            f"totalElements={result.get('total_elements')}"
        )
        results.append(result)

    payload = {
        "pipeline_name": PIPELINE_NAME,
        "pipeline_version": PIPELINE_VERSION,
        "tested_at_utc": tested_at,
        "timestamp": ts,
        "endpoints": ENDPOINTS,
        "summary": infer_summary(results),
        "results": results,
        "github": {
            "repository": os.environ.get("GITHUB_REPOSITORY"),
            "run_id": os.environ.get("GITHUB_RUN_ID"),
            "run_number": os.environ.get("GITHUB_RUN_NUMBER"),
            "sha": os.environ.get("GITHUB_SHA"),
        },
    }

    json_latest = out_dir / "platform_endpoint_test.json"
    md_latest = out_dir / "PLATFORM_ENDPOINT_TEST.md"
    json_dated = out_dir / f"platform_endpoint_test_{ts}.json"
    md_dated = out_dir / f"PLATFORM_ENDPOINT_TEST_{ts}.md"

    json_text = json.dumps(payload, ensure_ascii=False, indent=2)
    json_latest.write_text(json_text, encoding="utf-8")
    json_dated.write_text(json_text, encoding="utf-8")

    write_markdown(md_latest, payload)
    write_markdown(md_dated, payload)

    log(f"OK wrote {json_latest}")
    log(f"OK wrote {md_latest}")
    log("=== EUDAMED Platform Endpoint Test complete ===")
    log(json.dumps(payload["summary"], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
