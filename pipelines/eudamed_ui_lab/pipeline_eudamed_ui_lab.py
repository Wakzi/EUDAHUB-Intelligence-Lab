#!/usr/bin/env python3
"""
EUDAHUB EUDAMED UI API raw acquisition v4.5

Purpose
-------
Full device-list audit for the EUDAMED UI API:
1) Discover totalElements/totalPages from page=1.
2) Fetch all requested list pages with bounded parallelism.
3) Validate requested pages vs successful pages and received rows vs expected totalElements.
4) Optionally fetch a controlled number of device-detail rows for market-country enrichment.
5) Export primarily to DuckDB.
6) Export CSV files into one ZIP only, so GitHub Release assets stay manageable.
7) Keep stats JSON unzipped for quick inspection.

Semantics
---------
max_pages:
  0  = all pages discovered from API
  >0 = first N pages

max_device_detail:
  0  = skip details
  -1 = all fetched devices
  >0 = sample N devices after local stable sort by uuid

max_basic_udi:
  0  = skip Basic UDI attempts
  -1 = all candidates
  >0 = sample N candidates

max_actor_detail:
  0  = skip actor detail attempts
  -1 = all actor candidates
  >0 = sample N actor candidates

Notes
-----
The API appears to cap pageSize at 300. Keep page_size=300 unless testing.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import hashlib
import json
import math
import os
import random
import shutil
import threading
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import duckdb
import pandas as pd
import requests

BASE_URL = "https://ec.europa.eu/tools/eudamed/api"
DEFAULT_USER_AGENT = "EUDAHUB-Intelligence-EUDAMED-UI-API-Raw/0.4.4"
PIPELINE_VERSION = "v4.5"
STATE_FILENAME = "ui_api_state_v4_5.json"


# -----------------------------
# Helpers
# -----------------------------


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {message}", flush=True)


def human_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def human_bytes(n: int | float | None) -> str:
    if n is None:
        return "n/a"
    n = float(n)
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024
        i += 1
    return f"{n:.2f} {units[i]}"


def path_size(path: Path) -> int | None:
    try:
        return path.stat().st_size if path.exists() else None
    except Exception:
        return None


def dir_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for f in path.rglob("*"):
        if f.is_file():
            try:
                total += f.stat().st_size
            except Exception:
                pass
    return total


def json_hash(obj: Any) -> str:
    raw = json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def stable_text(obj: Any) -> str:
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)


def flatten_json(obj: Any, prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            out.update(flatten_json(v, key))
    elif isinstance(obj, list):
        out[prefix] = json.dumps(obj, ensure_ascii=False, default=str)
        out[f"{prefix}.__count"] = len(obj)
    else:
        out[prefix] = obj
    return out


def safe_get(d: Any, path: str, default: Any = None) -> Any:
    cur = d
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur


def first_present(d: Dict[str, Any], keys: Sequence[str], default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, "", "null"):
            return d[k]
    return default


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


# -----------------------------
# Rate limiter
# -----------------------------


class TokenBucketRateLimiter:
    """Simple process-local token bucket.

    max_rps controls steady-state request rate. A tiny random jitter reduces synchronized bursts.
    """

    def __init__(self, max_rps: float, burst: Optional[int] = None, jitter: float = 0.05):
        self.max_rps = max(float(max_rps), 0.1)
        self.capacity = float(burst if burst is not None and burst > 0 else max(1, math.ceil(max_rps)))
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self.lock = threading.Lock()
        self.jitter = max(0.0, float(jitter))

    def wait(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.updated
                self.updated = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.max_rps)
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    break
                needed = 1.0 - self.tokens
                sleep_for = needed / self.max_rps
            time.sleep(sleep_for + random.random() * self.jitter)



class GlobalThrottle:
    """Shared circuit breaker for HTTP 429 waves."""

    def __init__(
        self,
        enabled: bool = True,
        base_pause: float = 60.0,
        max_pause: float = 300.0,
        threshold: int = 3,
        window_seconds: float = 30.0,
    ):
        self.enabled = enabled
        self.base_pause = max(1.0, float(base_pause))
        self.max_pause = max(self.base_pause, float(max_pause))
        self.threshold = max(1, int(threshold))
        self.window_seconds = max(1.0, float(window_seconds))
        self.lock = threading.Lock()
        self.pause_until = 0.0
        self.recent_429s: List[float] = []
        self.penalty_level = 0

    def wait_if_paused(self) -> None:
        if not self.enabled:
            return
        while True:
            with self.lock:
                remaining = self.pause_until - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(remaining, 5.0))

    def register_429(self, retry_after: Optional[str], endpoint: str, page: Optional[int]) -> Optional[float]:
        if not self.enabled:
            return None

        now = time.monotonic()
        retry_after_seconds: Optional[float] = None
        if retry_after:
            try:
                retry_after_seconds = float(str(retry_after).strip())
            except Exception:
                retry_after_seconds = None

        with self.lock:
            self.recent_429s = [t for t in self.recent_429s if now - t <= self.window_seconds]
            self.recent_429s.append(now)

            should_pause = retry_after_seconds is not None or len(self.recent_429s) >= self.threshold
            if not should_pause:
                return None

            if retry_after_seconds is not None:
                pause_for = min(self.max_pause, max(self.base_pause, retry_after_seconds))
            else:
                self.penalty_level += 1
                pause_for = min(self.max_pause, self.base_pause * self.penalty_level)

            new_pause_until = now + pause_for
            if new_pause_until > self.pause_until:
                self.pause_until = new_pause_until
                log(
                    f"GLOBAL THROTTLE: HTTP 429 wave detected endpoint={endpoint} page={page} "
                    f"recent_429s={len(self.recent_429s)} retry_after={retry_after!r} "
                    f"sleep={pause_for:.0f}s penalty_level={self.penalty_level}"
                )
            return pause_for

    def register_success(self) -> None:
        if not self.enabled:
            return
        with self.lock:
            if self.penalty_level > 0 and time.monotonic() > self.pause_until:
                self.penalty_level = max(0, self.penalty_level - 1)


@dataclass
class ApiResult:
    endpoint: str
    page: Optional[int]
    url: str
    status_code: Optional[int]
    ok: bool
    duration_ms: int
    attempt: int
    error: Optional[str]
    data: Optional[Any]


class ApiClient:
    def __init__(
        self,
        language: str,
        max_rps: float,
        retries: int,
        backoff: float,
        timeout: int,
        user_agent: str = DEFAULT_USER_AGENT,
        global_throttle_enabled: bool = True,
        global_429_pause: float = 60.0,
        global_429_max_pause: float = 300.0,
        global_429_threshold: int = 3,
        global_429_window: float = 30.0,
    ):
        self.language = language
        self.retries = max(0, int(retries))
        self.backoff = max(0.1, float(backoff))
        self.timeout = int(timeout)
        self.limiter = TokenBucketRateLimiter(max_rps=max_rps)
        self.global_throttle = GlobalThrottle(
            enabled=global_throttle_enabled,
            base_pause=global_429_pause,
            max_pause=global_429_max_pause,
            threshold=global_429_threshold,
            window_seconds=global_429_window,
        )
        self.local = threading.local()
        self.user_agent = user_agent

    def session(self) -> requests.Session:
        if not hasattr(self.local, "session"):
            s = requests.Session()
            s.headers.update({
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            })
            self.local.session = s
        return self.local.session

    def get_json(self, endpoint: str, url: str, params: Optional[dict] = None, page: Optional[int] = None) -> ApiResult:
        last_result: Optional[ApiResult] = None
        for attempt in range(1, self.retries + 2):
            self.global_throttle.wait_if_paused()
            self.limiter.wait()
            started = time.monotonic()
            status_code = None
            final_url = url
            try:
                r = self.session().get(url, params=params, timeout=self.timeout)
                duration_ms = int((time.monotonic() - started) * 1000)
                status_code = r.status_code
                final_url = r.url
                retry_after = r.headers.get("Retry-After")
                try:
                    data = r.json()
                    parse_error = None
                except Exception as e:
                    data = None
                    parse_error = f"JSON parse failed: {e}"

                err = parse_error
                if not r.ok and err is None:
                    err = r.text[:500]

                result = ApiResult(
                    endpoint=endpoint,
                    page=page,
                    url=final_url,
                    status_code=status_code,
                    ok=bool(r.ok and data is not None),
                    duration_ms=duration_ms,
                    attempt=attempt,
                    error=err,
                    data=data,
                )
                last_result = result

                if result.ok:
                    self.global_throttle.register_success()
                    return result

                # Retry transient server/rate-limit errors, not 404.
                if status_code in (408, 409, 425, 429, 500, 502, 503, 504):
                    if status_code == 429:
                        self.global_throttle.register_429(retry_after, endpoint=endpoint, page=page)
                    sleep_for = self.backoff * attempt + random.random() * 0.2
                    log(
                        f"WARNING transient HTTP {status_code} endpoint={endpoint} page={page} "
                        f"attempt={attempt}/{self.retries + 1}; retry_after={retry_after!r}; backoff={sleep_for:.1f}s"
                    )
                    time.sleep(sleep_for)
                    continue
                return result
            except Exception as e:
                duration_ms = int((time.monotonic() - started) * 1000)
                last_result = ApiResult(
                    endpoint=endpoint,
                    page=page,
                    url=final_url,
                    status_code=status_code,
                    ok=False,
                    duration_ms=duration_ms,
                    attempt=attempt,
                    error=str(e),
                    data=None,
                )
                sleep_for = self.backoff * attempt + random.random() * 0.2
                log(f"WARNING request exception endpoint={endpoint} page={page} attempt={attempt}/{self.retries + 1}: {e}; backoff={sleep_for:.1f}s")
                time.sleep(sleep_for)

        assert last_result is not None
        return last_result


# -----------------------------
# API fetch logic
# -----------------------------


def discover_pages(client: ApiClient, page_size: int) -> Tuple[Dict[str, Any], ApiResult]:
    url = f"{BASE_URL}/devices/udiDiData"
    params = {
        "page": 0,
        "pageSize": page_size,
        "size": page_size,
        "iso2Code": client.language,
        "languageIso2Code": client.language,
    }
    res = client.get_json("devices_udiDiData_discovery", url, params=params, page=0)
    data = res.data if isinstance(res.data, dict) else {}

    # EUDAMED response has top-level and pageable metadata in observed responses.
    total_elements = first_present(data, ["totalElements", "page.totalElements", "pageable.totalElements"])
    total_pages = first_present(data, ["totalPages", "page.totalPages", "pageable.totalPages"])
    response_page_size = first_present(data, ["size", "pageable.pageSize", "pageable.size"], page_size)
    number_of_elements = first_present(data, ["numberOfElements"], len(data.get("content") or []))

    discovery = {
        "discovered_at_utc": now_utc(),
        "requested_page_size": page_size,
        "response_page_size": int(response_page_size) if response_page_size is not None else None,
        "total_elements": int(total_elements) if total_elements is not None else None,
        "total_pages": int(total_pages) if total_pages is not None else None,
        "first_page_number_of_elements": int(number_of_elements) if number_of_elements is not None else None,
        "first_page_content_length": len(data.get("content") or []),
        "first_page_status_code": res.status_code,
        "first_page_ok": res.ok,
        "first_page_url": res.url,
        "first_page_error": res.error,
        "raw_metadata": json.dumps({k: v for k, v in data.items() if k != "content"}, ensure_ascii=False, default=str),
    }
    return discovery, res


def parse_device_list_page(result: ApiResult) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    data = result.data if isinstance(result.data, dict) else {}
    content = data.get("content") or []
    rows: List[Dict[str, Any]] = []

    for idx, item in enumerate(content):
        flat = flatten_json(item)
        flat["_source_endpoint"] = "/devices/udiDiData"
        flat["_page"] = result.page
        flat["_page_index"] = idx
        flat["_request_url"] = result.url
        flat["_raw_hash"] = json_hash(item)
        rows.append(flat)

    audit = {
        "endpoint": result.endpoint,
        "page": result.page,
        "url": result.url,
        "status_code": result.status_code,
        "ok": result.ok,
        "duration_ms": result.duration_ms,
        "attempt": result.attempt,
        "error": result.error,
        "content_length": len(content),
        "number": data.get("number"),
        "numberOfElements": data.get("numberOfElements"),
        "size": data.get("size"),
        "first": data.get("first"),
        "last": data.get("last"),
        "empty": data.get("empty"),
        "pageable_offset": safe_get(data, "pageable.offset"),
        "pageable_pageSize": safe_get(data, "pageable.pageSize"),
        "pageable_pageNumber": safe_get(data, "pageable.pageNumber"),
        "totalElements": data.get("totalElements"),
        "totalPages": data.get("totalPages"),
    }

    raw_index = {
        "endpoint": result.endpoint,
        "page": result.page,
        "status_code": result.status_code,
        "ok": result.ok,
        "url": result.url,
        "raw_hash": json_hash(data) if data else None,
        "has_content": bool(content),
    }
    return rows, audit, raw_index


def fetch_one_device_page(client: ApiClient, page: int, page_size: int) -> ApiResult:
    url = f"{BASE_URL}/devices/udiDiData"
    params = {
        "page": page,
        "pageSize": page_size,
        "size": page_size,
        "iso2Code": client.language,
        "languageIso2Code": client.language,
    }
    return client.get_json("devices_udiDiData_list", url, params=params, page=page)


def fetch_device_pages_parallel(
    client: ApiClient,
    pages: Sequence[int],
    page_size: int,
    workers: int,
    progress_every: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    device_rows: List[Dict[str, Any]] = []
    page_audit: List[Dict[str, Any]] = []
    request_log: List[Dict[str, Any]] = []
    raw_index: List[Dict[str, Any]] = []

    total = len(pages)
    done = 0
    started = time.monotonic()

    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        fut_to_page = {ex.submit(fetch_one_device_page, client, p, page_size): p for p in pages}
        for fut in cf.as_completed(fut_to_page):
            page = fut_to_page[fut]
            try:
                result = fut.result()
            except Exception as e:
                result = ApiResult(
                    endpoint="devices_udiDiData_list",
                    page=page,
                    url="",
                    status_code=None,
                    ok=False,
                    duration_ms=0,
                    attempt=0,
                    error=str(e),
                    data=None,
                )

            rows, audit, raw = parse_device_list_page(result)
            device_rows.extend(rows)
            page_audit.append(audit)
            raw_index.append(raw)
            request_log.append({
                "endpoint": result.endpoint,
                "page": result.page,
                "url": result.url,
                "status_code": result.status_code,
                "ok": result.ok,
                "duration_ms": result.duration_ms,
                "attempt": result.attempt,
                "error": result.error,
            })

            done += 1
            if progress_every and (done % progress_every == 0 or done == total):
                elapsed = time.monotonic() - started
                rate = done / elapsed if elapsed else 0
                remaining = total - done
                eta = remaining / rate if rate else 0
                ok_pages = sum(1 for x in page_audit if x.get("ok") is True)
                failed_pages = done - ok_pages
                avg_ms = sum((x.get("duration_ms") or 0) for x in page_audit) / max(1, len(page_audit))
                status_counts: Dict[str, int] = {}
                for x in page_audit:
                    status_counts[str(x.get("status_code"))] = status_counts.get(str(x.get("status_code")), 0) + 1
                log(
                    f"Pages completed {done}/{total} ({done / total * 100:.2f}%) | "
                    f"ok={ok_pages} failed={failed_pages} | rows={len(device_rows):,} | "
                    f"rate={rate:.2f} pages/s | avg_response={avg_ms:.0f} ms | "
                    f"ETA={human_duration(eta)} | status={status_counts}"
                )

    device_rows.sort(key=lambda r: (int(r.get("_page") or 0), int(r.get("_page_index") or 0)))
    page_audit.sort(key=lambda r: int(r.get("page") or 0))
    request_log.sort(key=lambda r: (str(r.get("endpoint")), int(r.get("page") or 0)))
    raw_index.sort(key=lambda r: int(r.get("page") or 0))
    return device_rows, page_audit, request_log, raw_index


def retry_failed_or_missing_pages(
    client: ApiClient,
    pages: Sequence[int],
    page_size: int,
    existing_device_rows: List[Dict[str, Any]],
    existing_page_audit: List[Dict[str, Any]],
    existing_request_log: List[Dict[str, Any]],
    existing_raw_index: List[Dict[str, Any]],
    retry_rounds: int,
    workers: int,
    progress_every: int,
    retry_pause_seconds: float = 60.0,
    recovery_workers: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Retry failed/missing pages and replace rows/audit for recovered pages."""
    all_pages = set(int(p) for p in pages)

    for round_no in range(1, max(0, retry_rounds) + 1):
        successful = {
            int(a.get("page"))
            for a in existing_page_audit
            if a.get("page") is not None and a.get("ok") is True
        }
        explicit_failed = {
            int(a.get("page"))
            for a in existing_page_audit
            if a.get("page") is not None and a.get("ok") is not True
        }
        to_retry = sorted((all_pages - successful).union(explicit_failed))

        if not to_retry:
            log(f"Retry audit complete before round {round_no}: no failed/missing pages remain")
            break

        recovery_workers = max(1, int(recovery_workers or min(workers, 4)))
        pause_for = retry_pause_seconds * round_no
        log(
            f"=== Page recovery round {round_no}/{retry_rounds}: "
            f"{len(to_retry):,} failed/missing page(s); sleeping {pause_for:.0f}s before retry; "
            f"recovery_workers={recovery_workers} ==="
        )
        time.sleep(max(0.0, pause_for))
        retry_rows, retry_audit, retry_requests, retry_raw = fetch_device_pages_parallel(
            client=client,
            pages=to_retry,
            page_size=page_size,
            workers=recovery_workers,
            progress_every=max(1, min(progress_every, 50)),
        )

        recovered_pages = {
            int(a.get("page"))
            for a in retry_audit
            if a.get("page") is not None and a.get("ok") is True
        }

        if recovered_pages:
            existing_device_rows = [
                r for r in existing_device_rows
                if int(r.get("_page") or -1) not in recovered_pages
            ]
            existing_page_audit = [
                a for a in existing_page_audit
                if int(a.get("page") or -1) not in recovered_pages
            ]
            existing_raw_index = [
                r for r in existing_raw_index
                if int(r.get("page") or -1) not in recovered_pages
            ]

            existing_device_rows.extend([r for r in retry_rows if int(r.get("_page") or -1) in recovered_pages])
            existing_page_audit.extend([a for a in retry_audit if int(a.get("page") or -1) in recovered_pages])
            existing_raw_index.extend([r for r in retry_raw if int(r.get("page") or -1) in recovered_pages])
            log(f"Retry round {round_no}: recovered {len(recovered_pages):,} page(s)")

        existing_request_log.extend(retry_requests)

        still_failed_pages = {
            int(a.get("page"))
            for a in retry_audit
            if a.get("page") is not None and a.get("ok") is not True
        }
        if still_failed_pages:
            existing_page_audit = [
                a for a in existing_page_audit
                if not (int(a.get("page") or -1) in still_failed_pages and a.get("ok") is not True)
            ]
            existing_page_audit.extend([a for a in retry_audit if int(a.get("page") or -1) in still_failed_pages])

        successful_after = {
            int(a.get("page"))
            for a in existing_page_audit
            if a.get("page") is not None and a.get("ok") is True
        }
        remaining = sorted(all_pages - successful_after)
        log(f"Retry round {round_no} complete | remaining_failed_or_missing={len(remaining):,}")

    existing_device_rows.sort(key=lambda r: (int(r.get("_page") or 0), int(r.get("_page_index") or 0)))
    existing_page_audit.sort(key=lambda r: int(r.get("page") or 0))
    existing_request_log.sort(key=lambda r: (str(r.get("endpoint")), int(r.get("page") or 0) if r.get("page") is not None else -1))
    existing_raw_index.sort(key=lambda r: int(r.get("page") or 0) if r.get("page") is not None else -1)

    return existing_device_rows, existing_page_audit, existing_request_log, existing_raw_index


def select_rows(rows: List[Dict[str, Any]], limit: int, stable_key: str = "uuid") -> List[Dict[str, Any]]:
    if limit == 0:
        return []
    sorted_rows = sorted(rows, key=lambda r: stable_text(r.get(stable_key) or r.get("_raw_hash")))
    if limit < 0:
        return sorted_rows
    return sorted_rows[:limit]


def fetch_device_details(
    client: ApiClient,
    device_rows: List[Dict[str, Any]],
    limit: int,
    workers: int,
    progress_every: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    selected = select_rows(device_rows, limit, stable_key="uuid")
    if not selected:
        return [], [], [], []

    detail_rows: List[Dict[str, Any]] = []
    request_log: List[Dict[str, Any]] = []
    raw_index: List[Dict[str, Any]] = []
    raw_json_lines: List[str] = []

    def one(row: Dict[str, Any]) -> ApiResult:
        device_uuid = row.get("uuid")
        url = f"{BASE_URL}/devices/udiDiData/{device_uuid}"
        params = {"languageIso2Code": client.language}
        return client.get_json("devices_udiDiData_detail", url, params=params, page=None)

    total = len(selected)
    done = 0
    started = time.monotonic()
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        fut_to_uuid = {ex.submit(one, r): r.get("uuid") for r in selected if r.get("uuid")}
        for fut in cf.as_completed(fut_to_uuid):
            device_uuid = fut_to_uuid[fut]
            try:
                res = fut.result()
            except Exception as e:
                res = ApiResult("devices_udiDiData_detail", None, "", None, False, 0, 0, str(e), None)

            request_log.append({
                "endpoint": res.endpoint,
                "device_uuid": device_uuid,
                "url": res.url,
                "status_code": res.status_code,
                "ok": res.ok,
                "duration_ms": res.duration_ms,
                "attempt": res.attempt,
                "error": res.error,
            })
            if isinstance(res.data, dict):
                flat = flatten_json(res.data)
                flat["_source_endpoint"] = "/devices/udiDiData/{deviceId}"
                flat["_device_uuid_from_list"] = device_uuid
                flat["_request_url"] = res.url
                flat["_raw_hash"] = json_hash(res.data)
                detail_rows.append(flat)
                raw_index.append({
                    "endpoint": res.endpoint,
                    "device_uuid": device_uuid,
                    "status_code": res.status_code,
                    "ok": res.ok,
                    "url": res.url,
                    "raw_hash": json_hash(res.data),
                })
                raw_json_lines.append(json.dumps({
                    "endpoint": res.endpoint,
                    "device_uuid": device_uuid,
                    "url": res.url,
                    "data": res.data,
                }, ensure_ascii=False, default=str))

            done += 1
            if progress_every and (done % progress_every == 0 or done == total):
                elapsed = time.monotonic() - started
                rate = done / elapsed if elapsed else 0
                remaining = total - done
                eta = remaining / rate if rate else 0
                ok_req = sum(1 for x in request_log if x.get("ok") is True)
                failed_req = done - ok_req
                avg_ms = sum((x.get("duration_ms") or 0) for x in request_log) / max(1, len(request_log))
                status_counts: Dict[str, int] = {}
                for x in request_log:
                    status_counts[str(x.get("status_code"))] = status_counts.get(str(x.get("status_code")), 0) + 1
                log(
                    f"Details completed {done}/{total} ({done / total * 100:.2f}%) | "
                    f"ok={ok_req} failed={failed_req} | detail_rows={len(detail_rows):,} | "
                    f"rate={rate:.2f} req/s | avg_response={avg_ms:.0f} ms | "
                    f"ETA={human_duration(eta)} | status={status_counts}"
                )

    detail_rows.sort(key=lambda r: stable_text(r.get("_device_uuid_from_list")))
    return detail_rows, request_log, raw_index, raw_json_lines


# -----------------------------
# Derived tables
# -----------------------------


def extract_market_countries(device_detail_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for r in device_detail_rows:
        device_uuid = r.get("uuid") or r.get("_device_uuid_from_list")
        primary_di = r.get("primaryDi.code")
        trade_name_json = r.get("tradeName.texts")
        markets_raw = r.get("marketInfoLink.msWhereAvailable")
        if not markets_raw:
            continue
        try:
            markets = json.loads(markets_raw) if isinstance(markets_raw, str) else markets_raw
        except Exception:
            continue
        if not isinstance(markets, list):
            continue
        for m in markets:
            if not isinstance(m, dict):
                continue
            country = m.get("country") or {}
            rows.append({
                "device_uuid": device_uuid,
                "primary_di": primary_di,
                "trade_name_texts_json": trade_name_json,
                "market_info_link_uuid": r.get("marketInfoLink.uuid"),
                "market_info_link_ulid": r.get("marketInfoLink.ulid"),
                "market_country_entry_uuid": m.get("uuid"),
                "market_info_link_id": m.get("marketInfoLinkId"),
                "country_name": country.get("name"),
                "country_type": country.get("type"),
                "country_iso2": country.get("iso2Code"),
                "non_eu_member_state": country.get("nonEUMemberState"),
                "start_date": m.get("startDate"),
                "end_date": m.get("endDate"),
                "new": m.get("new"),
            })
    return rows


def make_field_inventory(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for table, df in dfs.items():
        for col in df.columns:
            rows.append({
                "table": table,
                "field": col,
                "row_count": int(len(df)),
                "non_null_count": int(df[col].notna().sum()) if len(df) else 0,
            })
    return pd.DataFrame(rows)


def compare_with_latest_duckdb(latest_duckdb: Optional[str], dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    if not latest_duckdb or not Path(latest_duckdb).exists():
        return pd.DataFrame([{
            "comparison": "latest_duckdb",
            "status": "skipped",
            "detail": "No latest DuckDB provided/found",
        }])
    try:
        con = duckdb.connect(latest_duckdb, read_only=True)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        for table in tables:
            try:
                count = con.execute(f'SELECT COUNT(*) AS n FROM "{table}"').fetchone()[0]
                cols = con.execute(f'DESCRIBE "{table}"').fetchdf()["column_name"].tolist()
                rows.append({
                    "comparison": "latest_duckdb_table",
                    "status": "ok",
                    "table": table,
                    "row_count": int(count),
                    "column_count": len(cols),
                    "columns": "|".join(cols),
                })
            except Exception as e:
                rows.append({
                    "comparison": "latest_duckdb_table",
                    "status": "error",
                    "table": table,
                    "detail": str(e),
                })
        con.close()
    except Exception as e:
        rows.append({"comparison": "latest_duckdb", "status": "error", "detail": str(e)})

    for name, df in dfs.items():
        rows.append({
            "comparison": "ui_api_output",
            "status": "ok",
            "table": name,
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "columns": "|".join(map(str, df.columns)),
        })
    return pd.DataFrame(rows)


# -----------------------------
# Export logic
# -----------------------------


def df_from_rows(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# -----------------------------
# ULID/state helpers v4.5
# -----------------------------

CROCKFORD_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
CROCKFORD_MAP = {c: i for i, c in enumerate(CROCKFORD_ALPHABET)}

def decode_ulid_timestamp(ulid_value: Any) -> Optional[str]:
    if not isinstance(ulid_value, str) or len(ulid_value) < 10:
        return None
    try:
        n = 0
        for ch in ulid_value[:10].upper():
            n = (n << 5) | CROCKFORD_MAP[ch]
        ms = n & ((1 << 48) - 1)
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return None

def latest_input_file(input_dir: Path) -> Optional[Path]:
    patterns = [
        "eudamed_ui_lab_v4_5.duckdb.zip",
        "eudamed_ui_lab.duckdb.zip",
        "*.duckdb.zip",
        "eudamed_ui_lab_v4_5.duckdb",
        "eudamed_ui_lab.duckdb",
        "*.duckdb",
    ]
    files = []
    for pat in patterns:
        files.extend(input_dir.glob(pat))
    files = [f for f in files if f.exists() and f.stat().st_size > 0]
    return sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)[0] if files else None

def materialize_input_db(input_dir: Path) -> Optional[Path]:
    src = latest_input_file(input_dir)
    if not src:
        return None
    if src.suffix == ".zip":
        with zipfile.ZipFile(src, "r") as zf:
            names = [n for n in zf.namelist() if n.endswith(".duckdb")]
            if not names:
                return None
            zf.extract(names[0], input_dir)
            return input_dir / names[0]
    return src

def read_state(input_dir: Path) -> Dict[str, Any]:
    for name in [STATE_FILENAME, "ui_api_state.json"]:
        path = input_dir / name
        if path.exists() and path.stat().st_size > 0:
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception as e:
                log(f"WARNING could not read state {path}: {e}")
    return {}

def read_previous_devices(input_dir: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    db = materialize_input_db(input_dir)
    if not db:
        return pd.DataFrame(), {}
    log(f"Previous raw DB found for bootstrap: {db}")
    con = duckdb.connect(str(db), read_only=True)
    try:
        df = con.execute('SELECT * FROM ui_devices_list_all').fetch_df()
    finally:
        con.close()
    if "ulid" in df.columns:
        max_ulid = None if df.empty else df["ulid"].dropna().max()
        min_ulid = None if df.empty else df["ulid"].dropna().min()
    else:
        max_ulid = min_ulid = None
    state = {
        "state_source": "db_bootstrap",
        "bootstrap_db": str(db),
        "row_count": int(len(df)),
        "max_ulid": max_ulid,
        "max_ulid_timestamp": decode_ulid_timestamp(max_ulid),
        "min_ulid": min_ulid,
        "min_ulid_timestamp": decode_ulid_timestamp(min_ulid),
    }
    return df, state

def build_state_payload(mode: str, discovery: Dict[str, Any], devices_df: pd.DataFrame, audit: Dict[str, Any]) -> Dict[str, Any]:
    max_ulid = devices_df["ulid"].dropna().max() if not devices_df.empty and "ulid" in devices_df.columns else None
    min_ulid = devices_df["ulid"].dropna().min() if not devices_df.empty and "ulid" in devices_df.columns else None
    total_pages = int(discovery.get("total_pages") or 0)
    row_count = int(len(devices_df))
    total_elements = int(discovery.get("total_elements") or 0)
    return {
        "pipeline_version": PIPELINE_VERSION,
        "crawl_mode": mode,
        "completed_at_utc": now_utc(),
        "max_ulid": max_ulid,
        "max_ulid_timestamp": decode_ulid_timestamp(max_ulid),
        "min_ulid": min_ulid,
        "min_ulid_timestamp": decode_ulid_timestamp(min_ulid),
        "row_count": row_count,
        "api_total_elements": total_elements,
        "api_total_pages": total_pages,
        "page_size": int(discovery.get("response_page_size") or discovery.get("requested_page_size") or 0),
        "first_page": 0,
        "last_page": total_pages - 1 if total_pages else None,
        "first_page_first_flag": safe_get(json.loads(discovery.get("raw_metadata") or "{}"), "first"),
        "first_page_last_flag": safe_get(json.loads(discovery.get("raw_metadata") or "{}"), "last"),
        "first_page_number": safe_get(json.loads(discovery.get("raw_metadata") or "{}"), "number"),
        "completeness_ratio": (row_count / total_elements) if total_elements else None,
        "audit": audit,
    }

def fetch_incremental_pages(client: ApiClient, known_max_ulid: str, page_size: int, total_pages: int, known_pages_to_stop: int, extra_pages_after_match: int, old_count: int, api_total: int, progress_every: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """Fetch only the newest frontier.

    v4.5 is deliberately conservative: after the first known ULID frontier, it does
    not stop on the first known-only page. It requires several consecutive
    known-only pages, and if API totalElements still does not match old+new it
    crawls extra known-only pages before stopping.
    """
    new_rows: List[Dict[str, Any]] = []
    page_audit: List[Dict[str, Any]] = []
    request_log: List[Dict[str, Any]] = []
    raw_index: List[Dict[str, Any]] = []
    consecutive_known_only_pages = 0
    known_or_mixed_pages_seen = 0
    fetched = 0
    frontier_reached = False
    stop_reason = None
    page = 0
    while page < total_pages:
        result = fetch_one_device_page(client, page, page_size)
        rows, audit, raw = parse_device_list_page(result)
        page_audit.append(audit)
        raw_index.append(raw)
        request_log.append({
            "endpoint": result.endpoint, "page": result.page, "url": result.url, "status_code": result.status_code,
            "ok": result.ok, "duration_ms": result.duration_ms, "attempt": result.attempt, "error": result.error,
        })
        fetched += 1
        page_new = [r for r in rows if str(r.get("ulid") or "") > known_max_ulid]
        page_known = len(rows) - len(page_new)
        new_rows.extend(page_new)
        if page_known > 0:
            frontier_reached = True
            known_or_mixed_pages_seen += 1
        if page_known > 0 and len(page_new) == 0:
            consecutive_known_only_pages += 1
        else:
            consecutive_known_only_pages = 0
        current_total = old_count + len(new_rows)
        diff = current_total - api_total
        log(
            f"Incremental page={page} rows={len(rows)} new={len(page_new)} "
            f"known_or_old={page_known} known_only_streak={consecutive_known_only_pages} "
            f"old_plus_new={current_total:,} api_total={api_total:,} diff={diff:,}"
        )

        if frontier_reached and current_total >= api_total and consecutive_known_only_pages >= known_pages_to_stop:
            stop_reason = "frontier_reached_and_total_matched"
            break

        if frontier_reached and consecutive_known_only_pages >= (known_pages_to_stop + extra_pages_after_match):
            stop_reason = "frontier_reached_extra_known_pages_exhausted"
            if current_total != api_total:
                log(
                    f"WARNING incremental frontier reached after extra pages but "
                    f"old+new={current_total:,} api_total={api_total:,}; stopping because repeated known-only pages were found"
                )
            break

        page += 1

    if stop_reason is None:
        stop_reason = "end_of_pages" if page >= total_pages else "unknown"

    audit = {
        "mode": "incremental",
        "known_max_ulid_before": known_max_ulid,
        "known_max_ulid_before_timestamp": decode_ulid_timestamp(known_max_ulid),
        "old_rows": old_count,
        "api_total_elements": api_total,
        "pages_fetched": fetched,
        "new_rows_appended": len(new_rows),
        "frontier_reached": frontier_reached,
        "known_or_mixed_pages_seen": known_or_mixed_pages_seen,
        "consecutive_known_only_pages_at_stop": consecutive_known_only_pages,
        "known_pages_to_stop": known_pages_to_stop,
        "extra_pages_after_match": extra_pages_after_match,
        "stop_reason": stop_reason,
        "expected_after_append": old_count + len(new_rows),
        "expected_after_append_minus_api_total": old_count + len(new_rows) - api_total,
    }
    return new_rows, page_audit, request_log, raw_index, audit


def write_csv_zip(zip_path: Path, dfs: Dict[str, pd.DataFrame]) -> None:
    ensure_dir(zip_path.parent)
    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for name, df in dfs.items():
            csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
            zf.writestr(f"{name}.csv", csv_bytes)


def write_duckdb(db_path: Path, dfs: Dict[str, pd.DataFrame]) -> Optional[str]:
    ensure_dir(db_path.parent)
    if db_path.exists():
        db_path.unlink()
    try:
        con = duckdb.connect(str(db_path))
        for name, df in dfs.items():
            if df.empty and len(df.columns) == 0:
                con.execute(f'CREATE TABLE "{name}" (_empty BOOLEAN)')
                continue
            con.register("_df", df)
            con.execute(f'CREATE TABLE "{name}" AS SELECT * FROM _df')
            con.unregister("_df")
        con.execute("CHECKPOINT")
        con.close()
        return None
    except Exception as e:
        return str(e)


def zip_file(src: Path, dst: Path) -> Optional[str]:
    try:
        ensure_dir(dst.parent)
        if dst.exists():
            dst.unlink()
        with zipfile.ZipFile(dst, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            zf.write(src, arcname=src.name)
        return None
    except Exception as e:
        return str(e)


# -----------------------------
# Main
# -----------------------------


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="dist/eudamed_ui_lab")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    parser.add_argument("--input-dir", default="inputs")
    parser.add_argument("--incremental-known-pages-to-stop", type=int, default=5, help="Stop incremental only after this many consecutive known-only pages")
    parser.add_argument("--incremental-extra-pages-after-match", type=int, default=10, help="If row count still differs from API total after frontier, crawl this many extra known-only pages before stopping")
    parser.add_argument("--language", default="en")
    parser.add_argument("--page-size", type=int, default=300)
    parser.add_argument("--max-pages", type=int, default=0, help="0 = all pages discovered")
    parser.add_argument("--max-device-detail", type=int, default=0, help="0=skip, -1=all fetched devices, >0 sample")
    parser.add_argument("--max-basic-udi", type=int, default=0, help="Reserved. 0=skip, -1=all candidates, >0 sample")
    parser.add_argument("--max-actor-detail", type=int, default=0, help="Reserved. 0=skip, -1=all candidates, >0 sample")
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--detail-workers", type=int, default=8)
    parser.add_argument("--max-rps", type=float, default=5.0, help="Global request rate cap across workers")
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--backoff", type=float, default=1.5)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--progress-every", type=int, default=50)
    parser.add_argument("--page-retry-rounds", type=int, default=5, help="Extra retry rounds for failed/missing list pages after first crawl")
    parser.add_argument("--page-retry-pause", type=float, default=60.0, help="Seconds to wait before page recovery; multiplied by recovery round")
    parser.add_argument("--recovery-workers", type=int, default=4, help="Workers used only for failed-page recovery passes")
    parser.add_argument("--global-429-pause", type=float, default=60.0, help="Global pause seconds when a 429 wave is detected")
    parser.add_argument("--global-429-max-pause", type=float, default=300.0, help="Maximum global pause seconds")
    parser.add_argument("--global-429-threshold", type=int, default=3, help="Number of 429s in window before global pause")
    parser.add_argument("--global-429-window", type=float, default=30.0, help="Window seconds for global 429 threshold")
    parser.add_argument("--disable-global-429-throttle", action="store_true", help="Disable global 429 circuit breaker")
    parser.add_argument("--skip-csv-zip", action="store_true", help="Skip CSV ZIP to save time on full crawls")
    parser.add_argument("--latest-duckdb", default=None)
    parser.add_argument("--keep-unzipped-duckdb", action="store_true")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    ensure_dir(out_dir)

    client = ApiClient(
        language=args.language,
        max_rps=args.max_rps,
        retries=args.retries,
        backoff=args.backoff,
        timeout=args.timeout,
        global_throttle_enabled=not args.disable_global_429_throttle,
        global_429_pause=args.global_429_pause,
        global_429_max_pause=args.global_429_max_pause,
        global_429_threshold=args.global_429_threshold,
        global_429_window=args.global_429_window,
    )

    log(f"=== EUDAMED UI API {PIPELINE_VERSION} raw acquisition started ===")
    log(f"out_dir={out_dir}")
    log(f"page_size={args.page_size} max_pages={args.max_pages} max_rps={args.max_rps} workers={args.workers} detail_workers={args.detail_workers}")
    log(f"mode={args.mode} details={args.max_device_detail} basic_udi={args.max_basic_udi} actor={args.max_actor_detail} retries={args.retries} backoff={args.backoff}s timeout={args.timeout}s")
    log("scope=raw_fetch_only cdc=0 canonical=0 dk_subset=0")

    discovery, discovery_result = discover_pages(client, args.page_size)
    log("=== Discovery complete ===")
    log(json.dumps(discovery, ensure_ascii=False))

    expected_total_pages = discovery.get("total_pages") or 0
    expected_total_elements = discovery.get("total_elements") or 0
    response_page_size = discovery.get("response_page_size") or args.page_size

    if not discovery_result.ok or not expected_total_pages:
        raise RuntimeError(f"Discovery failed: {discovery}")

    # v4.5: EUDAMED UI API uses zero-based pagination.
    # Valid pages are 0..totalPages-1.
    if args.max_pages == 0:
        pages = list(range(0, int(expected_total_pages)))
    else:
        pages = list(range(0, min(int(args.max_pages), int(expected_total_pages))))

    input_dir = Path(args.input_dir)
    ensure_dir(input_dir)
    incremental_audit: Dict[str, Any] = {}
    previous_df = pd.DataFrame()

    if args.mode == "incremental":
        state_file = read_state(input_dir)
        previous_df, db_state = read_previous_devices(input_dir)
        known_max_ulid = state_file.get("max_ulid") or db_state.get("max_ulid")
        if previous_df.empty or not known_max_ulid:
            log("WARNING incremental requested but previous DB/max_ulid was not found. Falling back to full mode.")
            args.mode = "full"
        else:
            log(f"=== Incremental from known max_ulid={known_max_ulid} ({decode_ulid_timestamp(known_max_ulid)}) ===")
            device_rows, page_audit, request_log, raw_index, incremental_audit = fetch_incremental_pages(
                client=client,
                known_max_ulid=known_max_ulid,
                page_size=args.page_size,
                total_pages=int(expected_total_pages),
                known_pages_to_stop=args.incremental_known_pages_to_stop,
                extra_pages_after_match=args.incremental_extra_pages_after_match,
                old_count=int(len(previous_df)),
                api_total=int(expected_total_elements),
                progress_every=args.progress_every,
            )

    if args.mode == "full":
        log(f"=== Fetching {len(pages):,} page(s), zero-based {pages[0] if pages else 0}..{pages[-1] if pages else 0} ===")
        device_rows, page_audit, request_log, raw_index = fetch_device_pages_parallel(
            client=client,
            pages=pages,
            page_size=args.page_size,
            workers=args.workers,
            progress_every=args.progress_every,
        )

    raw_json_lines: List[str] = []
    device_detail_rows: List[Dict[str, Any]] = []
    detail_request_log: List[Dict[str, Any]] = []
    detail_raw_index: List[Dict[str, Any]] = []

    if args.max_device_detail != 0:
        log(f"=== Fetching device details: limit={args.max_device_detail} ===")
        device_detail_rows, detail_request_log, detail_raw_index, raw_json_lines = fetch_device_details(
            client=client,
            device_rows=device_rows,
            limit=args.max_device_detail,
            workers=args.detail_workers,
            progress_every=max(1, min(args.progress_every, 500)),
        )
        request_log.extend(detail_request_log)
        raw_index.extend(detail_raw_index)
        log(f"Device detail fetch complete | detail_rows={len(device_detail_rows):,} | detail_requests={len(detail_request_log):,}")

    market_country_rows = extract_market_countries(device_detail_rows)
    log(f"Market country extraction complete | rows={len(market_country_rows):,}")

    # Basic UDI / Actor are intentionally skipped by default until correct ID format is known.
    basic_udi_attempts: List[Dict[str, Any]] = []
    actor_attempts: List[Dict[str, Any]] = []

    new_devices_df = df_from_rows(device_rows)
    if args.mode == "incremental" and not previous_df.empty:
        # Align columns and append only new rows to previous raw DB snapshot.
        for col in previous_df.columns:
            if col not in new_devices_df.columns:
                new_devices_df[col] = pd.NA
        for col in new_devices_df.columns:
            if col not in previous_df.columns:
                previous_df[col] = pd.NA
        devices_all_df = pd.concat([previous_df, new_devices_df[previous_df.columns]], ignore_index=True)
    else:
        devices_all_df = new_devices_df

    dfs: Dict[str, pd.DataFrame] = {
        "ui_discovery": pd.DataFrame([discovery]),
        "ui_page_audit": df_from_rows(page_audit),
        "ui_devices_list_all": devices_all_df,
        "ui_device_details_flat": df_from_rows(device_detail_rows),
        "ui_device_market_countries": df_from_rows(market_country_rows),
        "ui_basic_udi_attempts": df_from_rows(basic_udi_attempts),
        "ui_actor_attempts": df_from_rows(actor_attempts),
        "ui_api_request_log": df_from_rows(request_log),
        "ui_raw_response_index": df_from_rows(raw_index),
    }

    dfs["ui_field_inventory"] = make_field_inventory({k: v for k, v in dfs.items() if k != "ui_field_inventory"})
    dfs["ui_compare_with_latest_summary"] = compare_with_latest_duckdb(args.latest_duckdb, dfs)

    # Validation metrics
    page_df = dfs["ui_page_audit"]
    devices_df = dfs["ui_devices_list_all"]
    request_df = dfs["ui_api_request_log"]
    detail_df = dfs["ui_device_details_flat"]
    market_df = dfs["ui_device_market_countries"]

    successful_pages = int(page_df["ok"].sum()) if not page_df.empty and "ok" in page_df.columns else 0
    failed_pages = int((~page_df["ok"].astype(bool)).sum()) if not page_df.empty and "ok" in page_df.columns else 0
    # For full runs, requested_pages means the full discovered API scope.
    # For incremental runs, requested_pages means pages fetched in this run only;
    # non-fetched older pages are not missing pages.
    if args.mode == "incremental":
        requested_pages = int(len(page_df))
        fetched_scope_pages = set(page_df["page"].dropna().astype(int).tolist()) if not page_df.empty and "page" in page_df.columns else set()
    else:
        requested_pages = len(pages)
        fetched_scope_pages = set(pages)
    received_rows = int(len(devices_df))
    unique_uuid = int(devices_df["uuid"].nunique()) if not devices_df.empty and "uuid" in devices_df.columns else 0
    duplicate_uuid_rows = int(received_rows - unique_uuid) if unique_uuid else 0
    ok_pages = set(page_df.loc[page_df["ok"] == True, "page"].dropna().astype(int).tolist()) if not page_df.empty and "page" in page_df.columns else set()
    missing_pages = sorted(fetched_scope_pages - ok_pages)

    status_summary = []
    if not request_df.empty and "endpoint" in request_df.columns:
        grp = request_df.groupby(["endpoint", "status_code"], dropna=False).size().reset_index(name="count")
        status_summary = grp.to_dict(orient="records")

    stats = {
        "generated_at_utc": now_utc(),
        "pipeline_version": PIPELINE_VERSION,
        "mode": args.mode,
        "base_url": BASE_URL,
        "language": args.language,
        "page_size_requested": args.page_size,
        "page_size_response": response_page_size,
        "max_pages": args.max_pages,
        "max_device_detail": args.max_device_detail,
        "max_basic_udi": args.max_basic_udi,
        "max_actor_detail": args.max_actor_detail,
        "workers": args.workers,
        "detail_workers": args.detail_workers,
        "max_rps": args.max_rps,
        "retries": args.retries,
        "backoff": args.backoff,
        "skip_csv_zip": args.skip_csv_zip,
        "page_retry_rounds": args.page_retry_rounds,
        "page_retry_pause": args.page_retry_pause,
        "recovery_workers": args.recovery_workers,
        "global_429_throttle_enabled": not args.disable_global_429_throttle,
        "global_429_pause": args.global_429_pause,
        "global_429_max_pause": args.global_429_max_pause,
        "global_429_threshold": args.global_429_threshold,
        "global_429_window": args.global_429_window,
        "expected": {
            "total_elements": expected_total_elements,
            "total_pages": expected_total_pages,
            "first_page": 0,
            "last_page": int(expected_total_pages) - 1 if expected_total_pages else None,
        },
        "page_fetch_audit": {
            "requested_pages": requested_pages,
            "successful_pages": successful_pages,
            "failed_pages": failed_pages,
            "missing_pages_count": len(missing_pages),
            "missing_pages_sample": missing_pages[:100],
            "received_rows": received_rows,
            "unique_device_uuid": unique_uuid,
            "duplicate_device_uuid_rows": duplicate_uuid_rows,
            "expected_rows_for_scope": int(expected_total_elements if args.max_pages == 0 else min(expected_total_elements, requested_pages * response_page_size)),
            "received_rows_difference": int(received_rows - (expected_total_elements if args.max_pages == 0 else min(expected_total_elements, requested_pages * response_page_size))),
            "received_equals_expected_for_scope": received_rows == (expected_total_elements if args.max_pages == 0 else min(expected_total_elements, requested_pages * response_page_size)),
            "successful_pages_equals_requested_pages": successful_pages == requested_pages,
        },
        "incremental_audit": incremental_audit,
        "state": build_state_payload(args.mode, discovery, devices_df, incremental_audit or {"mode": "full"}),
        "detail_audit": {
            "device_detail_rows": int(len(detail_df)),
            "market_country_rows": int(len(market_df)),
            "devices_with_market_countries": int(market_df["device_uuid"].nunique()) if not market_df.empty and "device_uuid" in market_df.columns else 0,
            "dk_market_country_rows": int((market_df["country_iso2"] == "DK").sum()) if not market_df.empty and "country_iso2" in market_df.columns else 0,
            "market_entries_with_start_date": int(market_df["start_date"].notna().sum()) if not market_df.empty and "start_date" in market_df.columns else 0,
        },
        "rows": {k: int(len(v)) for k, v in dfs.items()},
        "columns": {k: int(len(v.columns)) for k, v in dfs.items()},
        "request_status_summary": status_summary,
        "output_files": [],
    }

    log(
        f"Validation summary | expected_elements={expected_total_elements:,} expected_pages={expected_total_pages:,} | "
        f"requested_pages={requested_pages:,} successful_pages={successful_pages:,} failed_pages={failed_pages:,} | "
        f"received_rows={received_rows:,} unique_uuid={unique_uuid:,} duplicate_uuid_rows={duplicate_uuid_rows:,} | "
        f"missing_pages={len(missing_pages):,}"
    )
    if received_rows != (expected_total_elements if args.max_pages == 0 else min(expected_total_elements, requested_pages * response_page_size)):
        log("WARNING completeness mismatch. This is warning-only in v4.5; raw DB will still be released.")

    # Write raw detail JSONL only if details were fetched; not zipped in CSV bundle, but useful in DB workflow if needed.
    raw_jsonl_path = out_dir / "ui_raw_detail_responses.jsonl"
    if raw_json_lines:
        raw_jsonl_path.write_text("\n".join(raw_json_lines) + "\n", encoding="utf-8")
        stats["output_files"].append(raw_jsonl_path.name)

    duckdb_path = out_dir / "eudamed_ui_lab_v4_5.duckdb"
    duckdb_zip_path = out_dir / "eudamed_ui_lab_v4_5.duckdb.zip"
    csv_zip_path = out_dir / "eudamed_ui_lab_csv_v4_5.zip"
    stats_path = out_dir / "ui_api_test_stats_v4_5.json"

    log(f"Writing DuckDB export: {duckdb_path}")
    export_started = time.monotonic()
    duckdb_error = write_duckdb(duckdb_path, dfs)
    log(f"DuckDB export finished in {human_duration(time.monotonic() - export_started)} | error={duckdb_error} | size={human_bytes(path_size(duckdb_path))}")
    stats["duckdb_export"] = str(duckdb_path)
    stats["duckdb_export_error"] = duckdb_error

    if duckdb_error is None:
        log(f"Compressing DuckDB ZIP: {duckdb_zip_path}")
        zip_started = time.monotonic()
        zip_err = zip_file(duckdb_path, duckdb_zip_path)
        log(f"DuckDB ZIP finished in {human_duration(time.monotonic() - zip_started)} | error={zip_err} | size={human_bytes(path_size(duckdb_zip_path))}")
        stats["duckdb_zip"] = str(duckdb_zip_path)
        stats["duckdb_zip_error"] = zip_err
        if zip_err is None:
            stats["output_files"].append(duckdb_zip_path.name)
        if not args.keep_unzipped_duckdb and duckdb_path.exists():
            duckdb_path.unlink()
    else:
        stats["duckdb_zip"] = None
        stats["duckdb_zip_error"] = "Skipped because DuckDB export failed"

    if args.skip_csv_zip:
        log("Skipping CSV ZIP because --skip-csv-zip is enabled")
        stats["csv_zip"] = None
        stats["csv_zip_skipped"] = True
    else:
        log(f"Compressing CSV ZIP: {csv_zip_path}")
        csv_started = time.monotonic()
        write_csv_zip(csv_zip_path, dfs)
        log(f"CSV ZIP finished in {human_duration(time.monotonic() - csv_started)} | size={human_bytes(path_size(csv_zip_path))}")
        stats["csv_zip"] = str(csv_zip_path)
        stats["csv_zip_skipped"] = False
        stats["output_files"].append(csv_zip_path.name)

    # State is deliberately unzipped.
    state_path = out_dir / STATE_FILENAME
    state_path.write_text(json.dumps(stats["state"], indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    stats["output_files"].append(state_path.name)

    # Stats are deliberately unzipped.
    stats["output_files"].append(stats_path.name)
    stats["output_file_sizes"] = {
        "duckdb_zip": human_bytes(path_size(duckdb_zip_path)),
        "csv_zip": human_bytes(path_size(csv_zip_path)),
        "stats_json": human_bytes(path_size(stats_path)),
        "out_dir_total": human_bytes(dir_size(out_dir)),
    }
    stats_path.write_text(json.dumps(stats, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    log(f"Stats JSON written: {stats_path} | size={human_bytes(path_size(stats_path))}")
    log(f"Output dir total size: {human_bytes(dir_size(out_dir))}")

    log("=== Final stats ===")
    log(json.dumps(stats, ensure_ascii=False, default=str))
    log(f"=== Done {PIPELINE_VERSION} ===")


if __name__ == "__main__":
    main()
