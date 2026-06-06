#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EUDAMED Platform UDI Raw acquisition pipeline.

Pipeline version: v4.8.1

Scope
-----
- Raw acquisition only.
- Current domain implemented: UDI list endpoint.
- No CDC, no canonical merge, no DK subset.

Design
------
- Default mode is incremental.
- Manual mode can be incremental or full.
- page=0 is the initial data page and also contains all metadata needed
  (totalElements, totalPages, page size, first/last flags).
- Previous latest is optional. If incremental is requested and no previous DB
  exists, the run becomes BOOTSTRAP and performs a full crawl.
- COMPLETE, PARTIAL, BOOTSTRAP publish a merged latest.
- FAILED means zero usable rows were received and latest should not be updated.
- Partial runs never delete or shrink latest: they merge received rows into the
  previous latest by UUID and retain unseen previous rows.

Release naming
--------------
Latest tag: eudamed-platform-udi-raw-latest
Latest assets:
  eudamed_platform_udi_raw_latest.duckdb
  eudamed_platform_udi_raw_latest_csv.zip
  eudamed_platform_udi_raw_latest.metadata.json
  RELEASE_NOTES_EUDAMED_PLATFORM_UDI_RAW.md

Dated tag: eudamed-platform-udi-raw-YYYYMMDD_HHMMSS
Dated assets:
  eudamed_platform_udi_raw_YYYYMMDD_HHMMSS.duckdb
  eudamed_platform_udi_raw_YYYYMMDD_HHMMSS_csv.zip
  eudamed_platform_udi_raw_YYYYMMDD_HHMMSS.metadata.json
  RELEASE_NOTES_EUDAMED_PLATFORM_UDI_RAW_YYYYMMDD_HHMMSS.md
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import random
import re
import shutil
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import duckdb
import pandas as pd
import requests

PIPELINE_VERSION = "v4.8.1"
BASE_URL_DEFAULT = "https://ec.europa.eu/tools/eudamed/api"
UDI_ENDPOINT = "/devices/udiDiData"
CROCKFORD32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

RUN_BOOTSTRAP = "BOOTSTRAP"
RUN_COMPLETE = "COMPLETE"
RUN_PARTIAL = "PARTIAL"
RUN_FAILED = "FAILED"


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc_now() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log(msg: str) -> None:
    print(f"[{utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}] {msg}", flush=True)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def safe_unlink(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass


def file_size(path: Path) -> str:
    if not path.exists():
        return "n/a"
    size = path.stat().st_size
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024 or unit == "TB":
            return f"{size:.2f} {unit}" if unit != "B" else f"{size} B"
        size /= 1024
    return f"{size:.2f} TB"


def fmt_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "n/a"
    try:
        seconds = max(0, int(seconds))
    except Exception:
        return "n/a"
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def build_progress(prefix: str, pages_done: int, pages_total: int, rows: int, started_at: float, response_times_ms: List[float], status_counts: Dict[int, int], throttle_count: int) -> str:
    elapsed = max(0.001, time.monotonic() - started_at)
    rate = pages_done / elapsed if pages_done else 0.0
    remaining_pages = max(0, pages_total - pages_done)
    eta = remaining_pages / rate if rate > 0 else None
    avg_ms = sum(response_times_ms) / len(response_times_ms) if response_times_ms else 0.0
    failed = sum(v for k, v in status_counts.items() if k != 200)
    pct = (pages_done / pages_total * 100.0) if pages_total else 0.0
    return (
        f"{prefix} progress={pages_done}/{pages_total} ({pct:.2f}%) | "
        f"rows={rows:,} | rate={rate:.3f} pages/s | avg_response={avg_ms:.0f} ms | "
        f"429_count={throttle_count} | failed_count={failed} | "
        f"elapsed={fmt_duration(elapsed)} | ETA={fmt_duration(eta)}"
    )


def release_timestamp(value: Optional[str]) -> str:
    if value and re.fullmatch(r"\d{8}_\d{6}", value):
        return value
    return utc_now().strftime("%Y%m%d_%H%M%S")


def release_title_time(ts: str) -> str:
    # YYYYMMDD_HHMMSS -> YYYY-MM-DD HH:MM:SS UTC
    try:
        d = dt.datetime.strptime(ts, "%Y%m%d_%H%M%S")
        return d.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")


def ulid_timestamp_ms(ulid: Optional[str]) -> Optional[int]:
    if not ulid:
        return None
    s = str(ulid).strip().upper()
    if len(s) < 10:
        return None
    try:
        value = 0
        for ch in s[:10]:
            value = value * 32 + CROCKFORD32.index(ch)
        return value
    except ValueError:
        return None


def decode_ulid(ulid: Optional[str]) -> Optional[str]:
    ms = ulid_timestamp_ms(ulid)
    if ms is None:
        return None
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).isoformat()


def min_ulid(values: Iterable[Optional[str]]) -> Optional[str]:
    vals = [str(v) for v in values if v]
    return min(vals) if vals else None


def max_ulid(values: Iterable[Optional[str]]) -> Optional[str]:
    vals = [str(v) for v in values if v]
    return max(vals) if vals else None


def get_code(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get("code") or obj.get("value") or obj.get("id")
    return str(obj)


def jdump(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def flatten_udi(row: Dict[str, Any], page_number: int, extract_date: str, extract_ts: str) -> Dict[str, Any]:
    return {
        "EXTRACT_DATE": extract_date,
        "EXTRACT_DATETIME_UTC": extract_ts,
        "basic_udi": row.get("basicUdi"),
        "primary_di": row.get("primaryDi"),
        "uuid": row.get("uuid"),
        "ulid": row.get("ulid"),
        "ulid_timestamp": decode_ulid(row.get("ulid")),
        "basic_udi_di_data_ulid": row.get("basicUdiDiDataUlid"),
        "basic_udi_di_data_ulid_timestamp": decode_ulid(row.get("basicUdiDiDataUlid")),
        "risk_class_code": get_code(row.get("riskClass")),
        "trade_name": row.get("tradeName"),
        "manufacturer_name": row.get("manufacturerName"),
        "manufacturer_srn": row.get("manufacturerSrn"),
        "device_status_code": get_code(row.get("deviceStatusType")),
        "manufacturer_names_json": jdump(row.get("manufacturerNames")),
        "manufacturer_status_code": get_code(row.get("manufacturerStatus")),
        "latest_version": row.get("latestVersion"),
        "version_number": row.get("versionNumber"),
        "basic_udi_data_uuid": row.get("basicUdiDataUuid"),
        "basic_udi_data_ulid": row.get("basicUdiDataUlid"),
        "basic_udi_data_ulid_timestamp": decode_ulid(row.get("basicUdiDataUlid")),
        "basic_udi_data_version_state": row.get("basicUdiDataVersionState"),
        "version_state": row.get("versionState"),
        "device_name": row.get("deviceName"),
        "device_model": row.get("deviceModel"),
        "last_update_date": row.get("lastUpdateDate"),
        "reference": row.get("reference"),
        "basic_udi_data_version_number": row.get("basicUdiDataVersionNumber"),
        "issuing_agency": row.get("issuingAgency"),
        "container_package_count": row.get("containerPackageCount"),
        "mf_or_pr_srn": row.get("mfOrPrSrn"),
        "applicable_legislation": row.get("applicableLegislation"),
        "authorised_representative_srn": row.get("authorisedRepresentativeSrn"),
        "authorised_representative_name": row.get("authorisedRepresentativeName"),
        "sterile": row.get("sterile"),
        "multi_component": row.get("multiComponent"),
        "device_criterion": row.get("deviceCriterion"),
        "raw_json": jdump(row),
        "source_endpoint": "devices/udiDiData",
        "source_page": page_number,
    }


UDI_COLUMNS = list(flatten_udi({}, 0, "", "").keys())


class EudamedClient:
    def __init__(self, base_url: str, language: str, page_size: int, timeout: int, retries: int, backoff: float, max_rps: float):
        self.base_url = base_url.rstrip("/")
        self.language = language
        self.page_size = page_size
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.max_rps = max_rps
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": f"EUDAHUB-Intelligence Platform UDI Raw {PIPELINE_VERSION}",
            "Accept": "application/json,text/plain,*/*",
            "Cache-Control": "no-cache",
        })
        self._last_request_at = 0.0
        self.throttle_events: List[Dict[str, Any]] = []
        self.throttle_events: List[Dict[str, Any]] = []

    def _rate_limit(self) -> None:
        if self.max_rps <= 0:
            return
        gap = 1.0 / self.max_rps
        now = time.monotonic()
        wait = self._last_request_at + gap - now
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def fetch_page(self, page: int) -> Tuple[int, int, Optional[Dict[str, Any]], Optional[str], float, Optional[str]]:
        url = f"{self.base_url}{UDI_ENDPOINT}"
        params = {"page": page, "size": self.page_size, "languageIso2Code": self.language}
        last_error = None
        for attempt in range(self.retries + 1):
            self._rate_limit()
            start = time.monotonic()
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                elapsed_ms = (time.monotonic() - start) * 1000
                retry_after = resp.headers.get("Retry-After")
                text = resp.text or ""
                if page == 0 and attempt == 0:
                    log(f"INITIAL REQUEST URL: {resp.request.url}")
                    log(f"INITIAL REQUEST HEADERS: {dict(resp.request.headers)}")
                if resp.status_code == 200:
                    return page, resp.status_code, resp.json(), None, elapsed_ms, retry_after
                if "Web Filter" in text or "Access Denied" in text or "security reason" in text:
                    return page, resp.status_code, None, f"WEB_FILTER_ACCESS_DENIED: HTTP {resp.status_code}: {text[:500]}", elapsed_ms, retry_after
                if resp.status_code == 429 and attempt < self.retries:
                    if retry_after and retry_after.isdigit():
                        sleep_s = float(retry_after)
                    else:
                        sleep_s = min(300.0, 30.0 * (attempt + 1))
                    sleep_s += random.random() * 0.5
                    last_error = f"HTTP 429 on page={page}; retry_after={retry_after}; sleep={sleep_s:.1f}s; elapsed_ms={elapsed_ms:.0f}"
                    self.throttle_events.append({
                        "page": page,
                        "attempt": attempt + 1,
                        "status_code": resp.status_code,
                        "retry_after": retry_after,
                        "sleep_s": sleep_s,
                        "elapsed_ms": elapsed_ms,
                        "at_utc": iso_utc_now(),
                    })
                    log(f"WARNING {last_error}")
                    time.sleep(sleep_s)
                    continue
                if resp.status_code in {500, 502, 503, 504} and attempt < self.retries:
                    sleep_s = min(120.0, self.backoff * (attempt + 1)) + random.random() * 0.25
                    last_error = f"HTTP {resp.status_code} on page={page}; retry_after={retry_after}; sleep={sleep_s:.1f}s; elapsed_ms={elapsed_ms:.0f}"
                    log(f"WARNING {last_error}")
                    time.sleep(sleep_s)
                    continue
                return page, resp.status_code, None, f"HTTP {resp.status_code}: {text[:500]}", elapsed_ms, retry_after
            except Exception as e:
                elapsed_ms = (time.monotonic() - start) * 1000
                last_error = repr(e)
                if attempt < self.retries:
                    time.sleep(self.backoff * (attempt + 1) + random.random() * 0.25)
                    continue
                return page, 0, None, last_error, elapsed_ms, None
        return page, 0, None, last_error or "unknown_error", 0.0, None


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]).fetchone()[0] > 0


def existing_udi_table(con: duckdb.DuckDBPyConnection) -> Optional[str]:
    for t in ["udi", "ui_devices_list_all"]:
        if table_exists(con, t):
            return t
    return None


def find_existing_db(inputs_dir: Path) -> Optional[Path]:
    candidates = [
        inputs_dir / "eudamed_platform_udi_raw_latest.duckdb",
        inputs_dir / "eudamed_platform_udi_raw_latest.duckdb.zip",
    ]
    for p in candidates:
        if p.exists() and p.suffix == ".duckdb":
            return p
        if p.exists() and p.suffix == ".zip":
            extracted = unzip_duckdb(p, inputs_dir)
            if extracted:
                return extracted
    return None


def unzip_duckdb(zip_path: Path, out_dir: Path) -> Optional[Path]:
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = [n for n in zf.namelist() if n.endswith(".duckdb")]
            if not names:
                return None
            name = names[0]
            zf.extract(name, out_dir)
            extracted = out_dir / name
            target = out_dir / Path(name).name
            if extracted != target:
                shutil.move(str(extracted), str(target))
            return target
    except Exception as e:
        log(f"WARNING could not unzip {zip_path}: {e}")
        return None


def read_existing_rows(db_path: Path) -> List[Dict[str, Any]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        table = existing_udi_table(con)
        if not table:
            return []
        df = con.execute(f"SELECT * FROM {table}").fetchdf()
        records = df.to_dict("records")
        out: List[Dict[str, Any]] = []
        mapping = {
            "basic_udi": ["basicUdi", "basic_udi"],
            "primary_di": ["primaryDi", "primary_di"],
            "uuid": ["uuid"],
            "ulid": ["ulid"],
            "basic_udi_di_data_ulid": ["basicUdiDiDataUlid", "basic_udi_di_data_ulid"],
            "risk_class_code": ["risk_class_code"],
            "trade_name": ["tradeName", "trade_name"],
            "manufacturer_name": ["manufacturerName", "manufacturer_name"],
            "manufacturer_srn": ["manufacturerSrn", "manufacturer_srn"],
            "device_status_code": ["device_status_code"],
            "latest_version": ["latestVersion", "latest_version"],
            "version_number": ["versionNumber", "version_number"],
            "device_name": ["deviceName", "device_name"],
            "device_model": ["deviceModel", "device_model"],
            "last_update_date": ["lastUpdateDate", "last_update_date"],
            "reference": ["reference"],
            "authorised_representative_srn": ["authorisedRepresentativeSrn", "authorised_representative_srn"],
            "authorised_representative_name": ["authorisedRepresentativeName", "authorised_representative_name"],
        }
        for r in records:
            d = {c: None for c in UDI_COLUMNS}
            for c in UDI_COLUMNS:
                if c in r:
                    d[c] = r.get(c)
            for new_key, old_keys in mapping.items():
                if d.get(new_key) is None:
                    for old_key in old_keys:
                        if old_key in r:
                            d[new_key] = r.get(old_key)
                            break
            # Old UI Lab did not have these two columns; retain old records with their old extract value if available.
            d["EXTRACT_DATE"] = d.get("EXTRACT_DATE") or str(r.get("EXTRACT_DATE") or r.get("extract_date") or "") or None
            d["EXTRACT_DATETIME_UTC"] = d.get("EXTRACT_DATETIME_UTC") or str(r.get("EXTRACT_DATETIME_UTC") or r.get("extract_timestamp_utc") or r.get("extract_datetime_utc") or "") or None
            d["ulid_timestamp"] = d.get("ulid_timestamp") or decode_ulid(d.get("ulid"))
            d["basic_udi_di_data_ulid_timestamp"] = d.get("basic_udi_di_data_ulid_timestamp") or decode_ulid(d.get("basic_udi_di_data_ulid"))
            d["basic_udi_data_ulid_timestamp"] = d.get("basic_udi_data_ulid_timestamp") or decode_ulid(d.get("basic_udi_data_ulid"))
            d["source_endpoint"] = d.get("source_endpoint") or "devices/udiDiData"
            out.append(d)
        return out
    finally:
        con.close()


def parse_page(data: Dict[str, Any], page: int, extract_date: str, extract_ts: str) -> List[Dict[str, Any]]:
    return [flatten_udi(item, page, extract_date, extract_ts) for item in data.get("content", [])]


def dedupe_by_uuid_choose_latest(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Dict[str, Dict[str, Any]] = {}
    no_uuid: List[Dict[str, Any]] = []
    for r in rows:
        u = r.get("uuid")
        if not u:
            no_uuid.append(r)
            continue
        old = seen.get(u)
        if old is None:
            seen[u] = r
            continue
        # Prefer newer ULID. If ULID is equal/missing, prefer row with newer extract datetime.
        r_key = (str(r.get("ulid") or ""), str(r.get("EXTRACT_DATETIME_UTC") or ""))
        old_key = (str(old.get("ulid") or ""), str(old.get("EXTRACT_DATETIME_UTC") or ""))
        if r_key >= old_key:
            seen[u] = r
    return list(seen.values()) + no_uuid


def merge_rows(previous_rows: List[Dict[str, Any]], received_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    prev_by_uuid = {r.get("uuid"): r for r in previous_rows if r.get("uuid")}
    recv_by_uuid = {r.get("uuid"): r for r in received_rows if r.get("uuid")}
    previous_uuid_set = set(prev_by_uuid)
    received_uuid_set = set(recv_by_uuid)
    inserted = len(received_uuid_set - previous_uuid_set)
    refreshed = len(received_uuid_set & previous_uuid_set)
    retained = len(previous_uuid_set - received_uuid_set)
    merged = dedupe_by_uuid_choose_latest(previous_rows + received_rows)
    stats = {
        "previous_rows": len(previous_rows),
        "received_rows": len(received_rows),
        "previous_uuid_count": len(previous_uuid_set),
        "received_uuid_count": len(received_uuid_set),
        "inserted_uuid_count": inserted,
        "refreshed_uuid_count": refreshed,
        "retained_uuid_count": retained,
        "merged_rows": len(merged),
    }
    return merged, stats


def initial_page_record(data: Optional[Dict[str, Any]], status: int, error: Optional[str], elapsed_ms: float, retry_after: Optional[str], page_size: int) -> Dict[str, Any]:
    content = data.get("content", []) if data else []
    return {
        "fetched_at_utc": iso_utc_now(),
        "requested_page_size": page_size,
        "response_page_size": data.get("size") if data else None,
        "total_elements": data.get("totalElements") if data else None,
        "total_pages": data.get("totalPages") if data else None,
        "number_of_elements": data.get("numberOfElements") if data else None,
        "content_length": len(content),
        "status_code": status,
        "ok": status == 200,
        "first_flag": data.get("first") if data else None,
        "last_flag": data.get("last") if data else None,
        "page_number": data.get("number") if data else None,
        "raw_metadata": json.dumps({k: v for k, v in (data or {}).items() if k != "content"}, ensure_ascii=False) if data else None,
        "error": error,
        "elapsed_ms": elapsed_ms,
        "retry_after": retry_after,
    }


def request_log_row(page: int, status: int, elapsed_ms: Optional[float], retry_after: Optional[str], error: Optional[str], initial_page: bool = False) -> Dict[str, Any]:
    return {
        "endpoint": "devices_udiDiData_list",
        "page": page,
        "status_code": status,
        "elapsed_ms": elapsed_ms,
        "retry_after": retry_after,
        "error": error,
        "requested_at_utc": iso_utc_now(),
        "probe": False,
        "initial_page": initial_page,
    }


def page_audit_row(page: int, status: int, data: Optional[Dict[str, Any]], rows: List[Dict[str, Any]], elapsed_ms: Optional[float], error: Optional[str], initial_page: bool = False) -> Dict[str, Any]:
    return {
        "page": page,
        "status_code": status,
        "ok": status == 200,
        "rows_returned": len(rows),
        "api_number": data.get("number") if data else None,
        "api_first": data.get("first") if data else None,
        "api_last": data.get("last") if data else None,
        "api_total_elements": data.get("totalElements") if data else None,
        "api_total_pages": data.get("totalPages") if data else None,
        "elapsed_ms": elapsed_ms,
        "error": error,
        "initial_page": initial_page,
    }


def fetch_incremental(client: EudamedClient, initial_data: Dict[str, Any], initial_meta: Dict[str, Any], known_max_ulid: str, extract_date: str, extract_ts: str, known_pages_to_stop: int, extra_pages_after_match: int, max_pages: int = 0) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], bool]:
    total_pages = int(initial_meta.get("total_pages") or 0)
    if max_pages and max_pages > 0:
        total_pages = min(total_pages, max_pages)
    received_rows: List[Dict[str, Any]] = []
    page_audit: List[Dict[str, Any]] = []
    request_log: List[Dict[str, Any]] = []
    frontier_reached = False
    consecutive_known_only = 0
    known_or_mixed_pages_seen = 0
    normal_completion = False
    stop_reason = None
    started_at = time.monotonic()
    response_times_ms: List[float] = []
    status_counts: Dict[int, int] = {}
    detailed_logging = False
    detailed_logging_reason = None

    log(f"=== Incremental from known max_ulid={known_max_ulid} ({decode_ulid(known_max_ulid)}) ===")

    def process_page(page: int, data: Optional[Dict[str, Any]] = None, initial: bool = False) -> Tuple[bool, int, int, int, Optional[float], Optional[str], Optional[str], int]:
        if data is None:
            _, status, data, error, elapsed_ms, retry_after = client.fetch_page(page)
        else:
            status, error, elapsed_ms, retry_after = 200, None, initial_meta.get("elapsed_ms"), initial_meta.get("retry_after")
        rows = parse_page(data, page, extract_date, extract_ts) if data else []
        status_counts[int(status or 0)] = status_counts.get(int(status or 0), 0) + 1
        if elapsed_ms is not None:
            response_times_ms.append(float(elapsed_ms))
        new_candidates = [r for r in rows if (str(r.get("ulid") or "") > str(known_max_ulid or ""))]
        received_rows.extend(new_candidates)
        page_audit.append(page_audit_row(page, status, data, rows, elapsed_ms, error, initial_page=initial))
        request_log.append(request_log_row(page, status, elapsed_ms, retry_after, error, initial_page=initial))
        ok = status == 200
        return ok, len(rows), len(new_candidates), len(rows) - len(new_candidates), elapsed_ms, retry_after, error, status

    page = 0
    while page < total_pages:
        throttles_before = len(client.throttle_events)
        ok, rows_n, new_n, known_n, elapsed_ms, retry_after, error, status = process_page(page, data=initial_data if page == 0 else None, initial=(page == 0))
        if len(client.throttle_events) > throttles_before:
            detailed_logging = True
            detailed_logging_reason = detailed_logging_reason or f"429 observed around page={page}"
        if not ok:
            stop_reason = f"page_{page}_fetch_failed"
            break
        if detailed_logging:
            log(
                f"Incremental detail page={page}/{total_pages-1} | rows={rows_n} | new={new_n} | known_or_old={known_n} | "
                f"status={status} | elapsed_ms={(elapsed_ms or 0):.0f} | retry_after={retry_after} | "
                f"429_count={len(client.throttle_events)} | reason={detailed_logging_reason}"
            )
            log(build_progress("Incremental", len(page_audit), total_pages, len(received_rows), started_at, response_times_ms, status_counts, len(client.throttle_events)))
        else:
            log(f"Incremental page={page} rows={rows_n} new={new_n} added={new_n} known_or_old={known_n}")
        if rows_n == 0:
            stop_reason = "empty_page"
            break
        if known_n > 0:
            frontier_reached = True
            known_or_mixed_pages_seen += 1
        consecutive_known_only = consecutive_known_only + 1 if new_n == 0 else 0
        if frontier_reached and consecutive_known_only >= known_pages_to_stop + extra_pages_after_match:
            stop_reason = "frontier_reached_extra_known_pages_exhausted"
            normal_completion = True
            break
        page += 1
    else:
        stop_reason = "all_pages_scanned"
        normal_completion = True

    elapsed_total = time.monotonic() - started_at
    pages_ok = len([r for r in page_audit if r.get("ok")])
    audit = {
        "mode": "incremental",
        "known_max_ulid_before": known_max_ulid,
        "known_max_ulid_before_timestamp": decode_ulid(known_max_ulid),
        "api_total_elements": initial_meta.get("total_elements"),
        "api_total_pages": initial_meta.get("total_pages"),
        "pages_fetched": pages_ok,
        "new_rows_received": len(received_rows),
        "frontier_reached": frontier_reached,
        "known_or_mixed_pages_seen": known_or_mixed_pages_seen,
        "consecutive_known_only_pages_at_stop": consecutive_known_only,
        "known_pages_to_stop": known_pages_to_stop,
        "extra_pages_after_match": extra_pages_after_match,
        "stop_reason": stop_reason,
        "normal_completion": normal_completion,
        "telemetry": {
            "elapsed_seconds": elapsed_total,
            "pages_per_second": pages_ok / max(0.001, elapsed_total),
            "avg_response_ms": (sum(response_times_ms) / len(response_times_ms)) if response_times_ms else None,
            "status_counts": {str(k): v for k, v in sorted(status_counts.items())},
            "throttle_429_count": len(client.throttle_events),
            "throttle_events": client.throttle_events,
            "detailed_logging_enabled": detailed_logging,
            "detailed_logging_reason": detailed_logging_reason,
        },
    }
    return received_rows, page_audit, request_log, audit, normal_completion


def fetch_full(client: EudamedClient, initial_data: Dict[str, Any], initial_meta: Dict[str, Any], extract_date: str, extract_ts: str, max_pages: int = 0) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], bool]:
    total_pages = int(initial_meta.get("total_pages") or 0)
    if max_pages and max_pages > 0:
        total_pages = min(total_pages, max_pages)
    received_rows: List[Dict[str, Any]] = []
    page_audit: List[Dict[str, Any]] = []
    request_log: List[Dict[str, Any]] = []
    failed_pages: List[int] = []
    normal_completion = True
    started_at = time.monotonic()
    response_times_ms: List[float] = []
    status_counts: Dict[int, int] = {}
    detailed_logging = False
    detailed_logging_reason = None
    last_successful_page: Optional[int] = None

    log(f"=== Full fetch pages 0..{total_pages - 1} ({total_pages} pages) ===")
    for page in range(total_pages):
        throttles_before = len(client.throttle_events)
        if page == 0:
            status, data, error, elapsed_ms, retry_after = 200, initial_data, None, initial_meta.get("elapsed_ms"), initial_meta.get("retry_after")
        else:
            _, status, data, error, elapsed_ms, retry_after = client.fetch_page(page)
        rows = parse_page(data, page, extract_date, extract_ts) if data else []
        status_counts[int(status or 0)] = status_counts.get(int(status or 0), 0) + 1
        if elapsed_ms is not None:
            response_times_ms.append(float(elapsed_ms))
        if len(client.throttle_events) > throttles_before:
            detailed_logging = True
            detailed_logging_reason = detailed_logging_reason or f"429 observed around page={page}"
        if status == 200:
            received_rows.extend(rows)
            last_successful_page = page
        else:
            failed_pages.append(page)
            normal_completion = False
            log(f"WARNING full fetch stopped at page={page}; status={status}; error={error}; elapsed_ms={elapsed_ms}")
            break
        page_audit.append(page_audit_row(page, status, data, rows, elapsed_ms, error, initial_page=(page == 0)))
        request_log.append(request_log_row(page, status, elapsed_ms, retry_after, error, initial_page=(page == 0)))

        pages_done = page + 1
        if detailed_logging:
            log(
                f"Full fetch detail page={page}/{total_pages-1} | rows={len(rows)} | status={status} | "
                f"elapsed_ms={(elapsed_ms or 0):.0f} | retry_after={retry_after} | received_rows={len(received_rows):,} | "
                f"429_count={len(client.throttle_events)} | reason={detailed_logging_reason}"
            )
            if page % 10 == 0 or page == total_pages - 1:
                log(build_progress("Full fetch", pages_done, total_pages, len(received_rows), started_at, response_times_ms, status_counts, len(client.throttle_events)))
        elif page % 50 == 0 or page == total_pages - 1:
            log(build_progress("Full fetch", pages_done, total_pages, len(received_rows), started_at, response_times_ms, status_counts, len(client.throttle_events)))

    elapsed_total = time.monotonic() - started_at
    pages_ok = len([r for r in page_audit if r.get("ok")])
    audit = {
        "mode": "full",
        "api_total_elements": initial_meta.get("total_elements"),
        "api_total_pages": initial_meta.get("total_pages"),
        "pages_fetched": pages_ok,
        "failed_pages": failed_pages,
        "failed_pages_count": len(failed_pages),
        "received_rows": len(received_rows),
        "normal_completion": normal_completion,
        "stop_reason": "all_pages_fetched" if normal_completion else "page_fetch_failed",
        "last_successful_page": last_successful_page,
        "next_page_to_fetch": (last_successful_page + 1) if last_successful_page is not None else 0,
        "telemetry": {
            "elapsed_seconds": elapsed_total,
            "pages_per_second": pages_ok / max(0.001, elapsed_total),
            "avg_response_ms": (sum(response_times_ms) / len(response_times_ms)) if response_times_ms else None,
            "status_counts": {str(k): v for k, v in sorted(status_counts.items())},
            "throttle_429_count": len(client.throttle_events),
            "throttle_events": client.throttle_events,
            "detailed_logging_enabled": detailed_logging,
            "detailed_logging_reason": detailed_logging_reason,
        },
    }
    return received_rows, page_audit, request_log, audit, normal_completion


def write_duckdb(out_db: Path, rows: List[Dict[str, Any]], initial_page: Dict[str, Any], page_audit: List[Dict[str, Any]], request_log: List[Dict[str, Any]], metadata: Dict[str, Any]) -> None:
    safe_unlink(out_db)
    con = duckdb.connect(str(out_db))
    try:
        con.register("udi_df", pd.DataFrame(rows, columns=UDI_COLUMNS))
        con.execute("CREATE TABLE udi AS SELECT * FROM udi_df")
        con.register("initial_page_df", pd.DataFrame([initial_page]))
        con.execute("CREATE TABLE initial_page AS SELECT * FROM initial_page_df")
        con.register("page_audit_df", pd.DataFrame(page_audit))
        con.execute("CREATE TABLE page_audit AS SELECT * FROM page_audit_df")
        con.register("api_request_log_df", pd.DataFrame(request_log))
        con.execute("CREATE TABLE api_request_log AS SELECT * FROM api_request_log_df")
        field_rows = []
        for col in UDI_COLUMNS:
            field_rows.append({"table_name": "udi", "field_name": col, "non_null_rows": sum(1 for r in rows if r.get(col) is not None), "total_rows": len(rows)})
        con.register("field_inventory_df", pd.DataFrame(field_rows))
        con.execute("CREATE TABLE field_inventory AS SELECT * FROM field_inventory_df")
        con.register("metadata_df", pd.DataFrame([flatten_for_table(metadata)]))
        con.execute("CREATE TABLE pipeline_metadata AS SELECT * FROM metadata_df")
    finally:
        con.close()


def flatten_for_table(obj: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in obj.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False)
        else:
            out[k] = v
    return out


def write_csv_zip(zip_path: Path, rows: List[Dict[str, Any]]) -> None:
    safe_unlink(zip_path)
    tmp = zip_path.parent / "_csv_tmp_platform"
    if tmp.exists():
        shutil.rmtree(tmp)
    ensure_dir(tmp)
    csv_path = tmp / "udi.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=UDI_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow({c: r.get(c) for c in UDI_COLUMNS})
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        zf.write(csv_path, "udi.csv")
    shutil.rmtree(tmp)


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_release_notes(path: Path, metadata: Dict[str, Any]) -> None:
    title = "# EUDAMED Platform UDI Raw"
    lines = [
        title,
        "",
        f"Pipeline version: `{metadata.get('pipeline_version')}`",
        f"Generated at UTC: `{metadata.get('generated_at_utc')}`",
        f"Run status: `{metadata.get('run_status')}`",
        f"Requested mode: `{metadata.get('requested_mode')}`",
        f"Effective mode: `{metadata.get('effective_mode')}`",
        "",
        "## Snapshot",
        "",
        f"- API total elements: `{metadata.get('api_total_elements')}`",
        f"- API total pages: `{metadata.get('api_total_pages')}`",
        f"- Rows in latest DB: `{metadata.get('row_count')}`",
        f"- Completeness ratio vs API total: `{metadata.get('completeness_ratio')}`",
        "",
        "## Merge",
        "",
        f"- Previous rows: `{metadata.get('merge', {}).get('previous_rows')}`",
        f"- Received rows this run: `{metadata.get('merge', {}).get('received_rows')}`",
        f"- Inserted UUIDs: `{metadata.get('merge', {}).get('inserted_uuid_count')}`",
        f"- Refreshed UUIDs: `{metadata.get('merge', {}).get('refreshed_uuid_count')}`",
        f"- Retained UUIDs from previous latest: `{metadata.get('merge', {}).get('retained_uuid_count')}`",
        f"- Merged rows: `{metadata.get('merge', {}).get('merged_rows')}`",
        "",
        "## ULID range",
        "",
        f"- Min ULID: `{metadata.get('min_ulid')}` → `{metadata.get('min_ulid_timestamp')}`",
        f"- Max ULID: `{metadata.get('max_ulid')}` → `{metadata.get('max_ulid_timestamp')}`",
        "",
        "## Audit",
        "",
        f"- Normal completion: `{metadata.get('normal_completion')}`",
        f"- Stop reason: `{metadata.get('audit', {}).get('stop_reason')}`",
        f"- Pages fetched: `{metadata.get('audit', {}).get('pages_fetched')}`",
        f"- Failed pages: `{metadata.get('audit', {}).get('failed_pages_count', 0)}`",
        "",
        "## Telemetry",
        "",
        f"- Pages/sec: `{(metadata.get('telemetry') or {}).get('pages_per_second')}`",
        f"- Avg response ms: `{(metadata.get('telemetry') or {}).get('avg_response_ms')}`",
        f"- 429 count: `{(metadata.get('telemetry') or {}).get('throttle_429_count')}`",
        f"- Detailed logging enabled: `{(metadata.get('telemetry') or {}).get('detailed_logging_enabled')}`",
        "",
        "## Interpretation",
        "",
        "- `COMPLETE` means the selected mode reached normal completion.",
        "- `PARTIAL` means some useful data was received, but the selected mode did not reach normal completion. Latest is still merged with previous latest so data is not lost.",
        "- `BOOTSTRAP` means no previous latest DB was available and a full base was created.",
        "- `FAILED` means no usable data was received; latest should not be updated.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def copy_file(src: Path, dst: Path) -> None:
    safe_unlink(dst)
    shutil.copy2(src, dst)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    p.add_argument("--out-dir", default="dist/eudamed_platform_udi_raw")
    p.add_argument("--inputs-dir", default="inputs")
    p.add_argument("--base-url", default=BASE_URL_DEFAULT)
    p.add_argument("--language", default="en")
    p.add_argument("--page-size", type=int, default=300)
    p.add_argument("--max-pages", type=int, default=0)
    p.add_argument("--max-rps", type=float, default=1.0)
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--retries", type=int, default=4)
    p.add_argument("--backoff", type=float, default=1.5)
    p.add_argument("--known-pages-to-stop", type=int, default=5)
    p.add_argument("--extra-pages-after-match", type=int, default=10)
    p.add_argument("--release-timestamp", default=None)
    p.add_argument("--skip-csv-zip", action="store_true")
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    out_dir = Path(args.out_dir)
    inputs_dir = Path(args.inputs_dir)
    ensure_dir(out_dir)
    ensure_dir(inputs_dir)
    rel_ts = release_timestamp(args.release_timestamp)
    extract_ts = iso_utc_now()
    extract_date = extract_ts[:10]

    log(f"=== EUDAMED Platform UDI Raw {PIPELINE_VERSION} acquisition started ===")
    log(f"requested_mode={args.mode} out_dir={out_dir} page_size={args.page_size}")
    log("scope=raw_fetch_only cdc=0 canonical=0 dk_subset=0 domain=udi")

    existing_db = find_existing_db(inputs_dir)
    existing_rows: List[Dict[str, Any]] = []
    if existing_db:
        log(f"Previous Platform Raw latest DB found: {existing_db}")
        existing_rows = read_existing_rows(existing_db)
        log(f"Loaded previous rows: {len(existing_rows):,}")
    else:
        log("No previous Platform Raw latest DB found")

    previous_max_ulid = max_ulid(r.get("ulid") for r in existing_rows)
    effective_mode = args.mode
    bootstrap = False
    if args.mode == "incremental" and not existing_rows:
        effective_mode = "full"
        bootstrap = True
        log("Incremental requested but no previous DB exists. Falling back to BOOTSTRAP full crawl.")
    elif args.mode == "incremental" and not previous_max_ulid:
        effective_mode = "full"
        bootstrap = True
        log("Incremental requested but previous DB has no max ULID. Falling back to BOOTSTRAP full crawl.")

    client = EudamedClient(args.base_url, args.language, args.page_size, args.timeout, args.retries, args.backoff, args.max_rps)

    log("=== Initial page fetch page=0 ===")
    page, status, initial_data, error, elapsed_ms, retry_after = client.fetch_page(0)
    initial_meta = initial_page_record(initial_data, status, error, elapsed_ms, retry_after, args.page_size)
    log(json.dumps(initial_meta, ensure_ascii=False))

    if status != 200 or not initial_data:
        metadata = {
            "pipeline_version": PIPELINE_VERSION,
            "release_timestamp": rel_ts,
            "generated_at_utc": iso_utc_now(),
            "run_status": RUN_FAILED,
            "requested_mode": args.mode,
            "effective_mode": effective_mode,
            "error": error,
            "initial_page": initial_meta,
            "row_count": 0,
        }
        write_json(out_dir / f"eudamed_platform_udi_raw_{rel_ts}.metadata.json", metadata)
        write_json(out_dir / "eudamed_platform_udi_raw_latest.metadata.json", metadata)
        write_release_notes(out_dir / "RELEASE_NOTES_EUDAMED_PLATFORM_UDI_RAW.md", metadata)
        log("ERROR page 0 failed. RUN_STATUS=FAILED. Latest should not be updated.")
        return 2

    api_total_elements = int(initial_meta.get("total_elements") or 0)
    api_total_pages = int(initial_meta.get("total_pages") or 0)

    if effective_mode == "incremental":
        received_rows, page_audit, request_log, audit, normal_completion = fetch_incremental(
            client, initial_data, initial_meta, str(previous_max_ulid), extract_date, extract_ts,
            args.known_pages_to_stop, args.extra_pages_after_match, max_pages=args.max_pages
        )
    else:
        received_rows, page_audit, request_log, audit, normal_completion = fetch_full(
            client, initial_data, initial_meta, extract_date, extract_ts, max_pages=args.max_pages
        )

    # A manual max_pages cap is a test/partial scope unless the cap is at or above the API total pages.
    if args.max_pages and args.max_pages > 0 and args.max_pages < api_total_pages:
        if audit.get("stop_reason") == "all_pages_scanned":
            audit["stop_reason"] = "max_pages_cap_reached"
        if effective_mode == "full":
            normal_completion = False

    usable_pages_received = len([r for r in page_audit if r.get("ok")])
    # IMPORTANT: 0 new/received rows in an incremental run can be a perfectly valid COMPLETE run
    # when page 0 responded OK and the frontier check found no new UDI records.
    # FAILED is reserved for cases where no usable API page/data was received.
    if usable_pages_received == 0:
        run_status = RUN_FAILED
    elif bootstrap and normal_completion:
        run_status = RUN_BOOTSTRAP
    elif normal_completion:
        run_status = RUN_COMPLETE
    else:
        run_status = RUN_PARTIAL

    if run_status == RUN_FAILED:
        merged_rows = existing_rows
        merge_stats = {
            "previous_rows": len(existing_rows),
            "received_rows": 0,
            "previous_uuid_count": len({r.get("uuid") for r in existing_rows if r.get("uuid")}),
            "received_uuid_count": 0,
            "inserted_uuid_count": 0,
            "refreshed_uuid_count": 0,
            "retained_uuid_count": len({r.get("uuid") for r in existing_rows if r.get("uuid")}),
            "merged_rows": len(existing_rows),
        }
    elif run_status in {RUN_COMPLETE, RUN_BOOTSTRAP} and effective_mode == "full":
        # A complete full run is the new full truth. No previous rows need to be retained.
        merged_rows = dedupe_by_uuid_choose_latest(received_rows)
        merge_stats = {
            "previous_rows": len(existing_rows),
            "received_rows": len(received_rows),
            "previous_uuid_count": len({r.get("uuid") for r in existing_rows if r.get("uuid")}),
            "received_uuid_count": len({r.get("uuid") for r in received_rows if r.get("uuid")}),
            "inserted_uuid_count": len({r.get("uuid") for r in received_rows if r.get("uuid")} - {r.get("uuid") for r in existing_rows if r.get("uuid")}),
            "refreshed_uuid_count": len({r.get("uuid") for r in received_rows if r.get("uuid")} & {r.get("uuid") for r in existing_rows if r.get("uuid")}),
            "retained_uuid_count": 0,
            "merged_rows": len(merged_rows),
        }
    else:
        # Incremental complete/partial, or full partial: merge safely with previous latest.
        merged_rows, merge_stats = merge_rows(existing_rows, received_rows)

    row_count = len(merged_rows)
    min_u = min_ulid(r.get("ulid") for r in merged_rows)
    max_u = max_ulid(r.get("ulid") for r in merged_rows)
    completeness_ratio = (row_count / api_total_elements) if api_total_elements else None
    status_summary: Dict[Tuple[str, int], int] = {}
    for r in request_log:
        key = (r.get("endpoint", ""), int(r.get("status_code") or 0))
        status_summary[key] = status_summary.get(key, 0) + 1

    metadata = {
        "pipeline_version": PIPELINE_VERSION,
        "release_timestamp": rel_ts,
        "release_time_utc": release_title_time(rel_ts),
        "generated_at_utc": iso_utc_now(),
        "run_status": run_status,
        "requested_mode": args.mode,
        "effective_mode": effective_mode,
        "bootstrap": bootstrap,
        "base_url": args.base_url,
        "language": args.language,
        "page_size": args.page_size,
        "max_pages": args.max_pages,
        "max_rps": args.max_rps,
        "retries": args.retries,
        "api_total_elements": api_total_elements,
        "api_total_pages": api_total_pages,
        "row_count": row_count,
        "completeness_ratio": completeness_ratio,
        "min_ulid": min_u,
        "min_ulid_timestamp": decode_ulid(min_u),
        "max_ulid": max_u,
        "max_ulid_timestamp": decode_ulid(max_u),
        "previous_db_found": str(existing_db) if existing_db else None,
        "previous_rows": len(existing_rows),
        "received_rows": len(received_rows),
        "normal_completion": normal_completion,
        "initial_page": initial_meta,
        "audit": audit,
        "merge": merge_stats,
        "request_status_summary": [{"endpoint": k[0], "status_code": k[1], "count": v} for k, v in sorted(status_summary.items())],
        "telemetry": (audit or {}).get("telemetry"),
    }

    latest_db = out_dir / "eudamed_platform_udi_raw_latest.duckdb"
    latest_csv = out_dir / "eudamed_platform_udi_raw_latest_csv.zip"
    latest_meta = out_dir / "eudamed_platform_udi_raw_latest.metadata.json"
    latest_notes = out_dir / "RELEASE_NOTES_EUDAMED_PLATFORM_UDI_RAW.md"
    dated_db = out_dir / f"eudamed_platform_udi_raw_{rel_ts}.duckdb"
    dated_csv = out_dir / f"eudamed_platform_udi_raw_{rel_ts}_csv.zip"
    dated_meta = out_dir / f"eudamed_platform_udi_raw_{rel_ts}.metadata.json"
    dated_notes = out_dir / f"RELEASE_NOTES_EUDAMED_PLATFORM_UDI_RAW_{rel_ts}.md"

    if run_status == RUN_FAILED:
        log("RUN_STATUS=FAILED. Writing metadata/notes only; not writing latest DB.")
        write_json(latest_meta, metadata)
        write_json(dated_meta, metadata)
        write_release_notes(latest_notes, metadata)
        write_release_notes(dated_notes, metadata)
        return 2

    log(f"Writing DuckDB: {latest_db}")
    write_duckdb(latest_db, merged_rows, initial_meta, page_audit, request_log, metadata)
    if not args.skip_csv_zip:
        log(f"Writing CSV ZIP: {latest_csv}")
        write_csv_zip(latest_csv, merged_rows)
    write_json(latest_meta, metadata)
    write_release_notes(latest_notes, metadata)

    copy_file(latest_db, dated_db)
    if latest_csv.exists():
        copy_file(latest_csv, dated_csv)
    copy_file(latest_meta, dated_meta)
    copy_file(latest_notes, dated_notes)

    log("=== EUDAMED Platform UDI Raw complete ===")
    log(json.dumps({
        "run_status": run_status,
        "requested_mode": args.mode,
        "effective_mode": effective_mode,
        "api_total_elements": api_total_elements,
        "row_count": row_count,
        "received_rows": len(received_rows),
        "previous_rows": len(existing_rows),
        "merge": merge_stats,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
