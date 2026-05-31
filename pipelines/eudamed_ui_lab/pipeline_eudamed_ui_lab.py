#!/usr/bin/env python3
"""
EUDAHUB EUDAMED UI API POC v4.1

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
DEFAULT_USER_AGENT = "EUDAHUB-Intelligence-EUDAMED-UI-API-Test/0.4.1"


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
    ):
        self.language = language
        self.retries = max(0, int(retries))
        self.backoff = max(0.1, float(backoff))
        self.timeout = int(timeout)
        self.limiter = TokenBucketRateLimiter(max_rps=max_rps)
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
            self.limiter.wait()
            started = time.monotonic()
            status_code = None
            final_url = url
            try:
                r = self.session().get(url, params=params, timeout=self.timeout)
                duration_ms = int((time.monotonic() - started) * 1000)
                status_code = r.status_code
                final_url = r.url
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
                    return result

                # Retry transient server/rate-limit errors, not 404.
                if status_code in (408, 409, 425, 429, 500, 502, 503, 504):
                    sleep_for = self.backoff * attempt + random.random() * 0.2
                    log(f"WARNING transient HTTP {status_code} endpoint={endpoint} page={page} attempt={attempt}/{self.retries + 1}; backoff={sleep_for:.1f}s")
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
        "page": 1,
        "pageSize": page_size,
        "size": page_size,
        "iso2Code": client.language,
        "languageIso2Code": client.language,
    }
    res = client.get_json("devices_udiDiData_discovery", url, params=params, page=1)
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
    log(f"Device list fetch complete | rows={len(device_rows):,} | page_audit_rows={len(page_audit):,} | request_log_rows={len(request_log):,}")

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
            # DuckDB can create table from pandas relation.
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
    parser.add_argument("--language", default="en")
    parser.add_argument("--page-size", type=int, default=300)
    parser.add_argument("--max-pages", type=int, default=1, help="0 = all pages discovered")
    parser.add_argument("--max-device-detail", type=int, default=0, help="0=skip, -1=all fetched devices, >0 sample")
    parser.add_argument("--max-basic-udi", type=int, default=0, help="Reserved. 0=skip, -1=all candidates, >0 sample")
    parser.add_argument("--max-actor-detail", type=int, default=0, help="Reserved. 0=skip, -1=all candidates, >0 sample")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--detail-workers", type=int, default=6)
    parser.add_argument("--max-rps", type=float, default=5.0, help="Global request rate cap across workers")
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--backoff", type=float, default=1.5)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--progress-every", type=int, default=250)
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
    )

    log("=== EUDAMED UI API v4.1 test started ===")
    log(f"out_dir={out_dir}")
    log(f"page_size={args.page_size} max_pages={args.max_pages} max_rps={args.max_rps} workers={args.workers} detail_workers={args.detail_workers}")
    log(f"details={args.max_device_detail} basic_udi={args.max_basic_udi} actor={args.max_actor_detail} retries={args.retries} backoff={args.backoff}s timeout={args.timeout}s")

    discovery, discovery_result = discover_pages(client, args.page_size)
    log("=== Discovery complete ===")
    log(json.dumps(discovery, ensure_ascii=False))

    expected_total_pages = discovery.get("total_pages") or 0
    expected_total_elements = discovery.get("total_elements") or 0
    response_page_size = discovery.get("response_page_size") or args.page_size

    if not discovery_result.ok or not expected_total_pages:
        raise RuntimeError(f"Discovery failed: {discovery}")

    if args.max_pages == 0:
        pages = list(range(1, int(expected_total_pages) + 1))
    else:
        pages = list(range(1, min(int(args.max_pages), int(expected_total_pages)) + 1))

    log(f"=== Fetching {len(pages):,} page(s) ===")
    device_rows, page_audit, request_log, raw_index = fetch_device_pages_parallel(
        client=client,
        pages=pages,
        page_size=args.page_size,
        workers=args.workers,
        progress_every=args.progress_every,
    )

    log(f"Device list fetch complete | rows={len(device_rows):,} | page_audit_rows={len(page_audit):,} | request_log_rows={len(request_log):,}")

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

    dfs: Dict[str, pd.DataFrame] = {
        "ui_discovery": pd.DataFrame([discovery]),
        "ui_page_audit": df_from_rows(page_audit),
        "ui_devices_list_all": df_from_rows(device_rows),
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
    requested_pages = len(pages)
    received_rows = int(len(devices_df))
    unique_uuid = int(devices_df["uuid"].nunique()) if not devices_df.empty and "uuid" in devices_df.columns else 0
    duplicate_uuid_rows = int(received_rows - unique_uuid) if unique_uuid else 0
    missing_pages = sorted(set(pages) - set(page_df.loc[page_df["ok"] == True, "page"].dropna().astype(int).tolist())) if not page_df.empty and "page" in page_df.columns else pages

    status_summary = []
    if not request_df.empty and "endpoint" in request_df.columns:
        grp = request_df.groupby(["endpoint", "status_code"], dropna=False).size().reset_index(name="count")
        status_summary = grp.to_dict(orient="records")

    stats = {
        "generated_at_utc": now_utc(),
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
        "expected": {
            "total_elements": expected_total_elements,
            "total_pages": expected_total_pages,
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
            "received_equals_expected_for_scope": received_rows == (expected_total_elements if args.max_pages == 0 else min(expected_total_elements, requested_pages * response_page_size)),
            "successful_pages_equals_requested_pages": successful_pages == requested_pages,
        },
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

    # Write raw detail JSONL only if details were fetched; not zipped in CSV bundle, but useful in DB workflow if needed.
    raw_jsonl_path = out_dir / "ui_raw_detail_responses.jsonl"
    if raw_json_lines:
        raw_jsonl_path.write_text("\n".join(raw_json_lines) + "\n", encoding="utf-8")
        stats["output_files"].append(raw_jsonl_path.name)

    duckdb_path = out_dir / "eudamed_ui_lab.duckdb"
    duckdb_zip_path = out_dir / "eudamed_ui_lab.duckdb.zip"
    csv_zip_path = out_dir / "eudamed_ui_lab_csv.zip"
    stats_path = out_dir / "ui_api_test_stats.json"

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

    log(f"Compressing CSV ZIP: {csv_zip_path}")
    csv_started = time.monotonic()
    write_csv_zip(csv_zip_path, dfs)
    log(f"CSV ZIP finished in {human_duration(time.monotonic() - csv_started)} | size={human_bytes(path_size(csv_zip_path))}")
    stats["csv_zip"] = str(csv_zip_path)
    stats["output_files"].append(csv_zip_path.name)

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
    log("=== Done ===")


if __name__ == "__main__":
    main()
