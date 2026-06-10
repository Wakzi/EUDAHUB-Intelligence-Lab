#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EUDAMED Platform UDI Raw acquisition pipeline.

Pipeline version: v4.8.3

Scope
-----
- Raw acquisition only.
- Current domain implemented: UDI list endpoint.
- No CDC, no canonical merge, no DK subset.

Design
------
- Default mode is incremental.
- Manual mode can be incremental, full, or resume_full.
- page=0 is the initial data page and also contains all metadata needed
  (totalElements, totalPages, page size, first/last flags).
- Previous latest is optional. If incremental is requested and no previous DB
  exists, the run becomes BOOTSTRAP and performs a full crawl.
- COMPLETE, PARTIAL, BOOTSTRAP publish a merged latest.
- FAILED means zero usable API pages/rows were received and latest should not be updated.
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

PIPELINE_VERSION = "v4.8.4"
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

    def _rate_limit(self) -> None:
        if self.max_rps <= 0:
            return
        gap = 1.0 / self.max_rps
        now = time.monotonic()
        wait = self._last_request_at + gap - now
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def fetch_page(self, page: int, include_historical: bool = False) -> Tuple[int, int, Optional[Dict[str, Any]], Optional[str], float, Optional[str]]:
        url = f"{self.base_url}{UDI_ENDPOINT}"
        params = {"page": page, "size": self.page_size, "languageIso2Code": self.language}
        # IMPORTANT: only add includeHistoricalVersion when explicitly true.
        # For current/head/tail calls the parameter is omitted completely, matching the old URL.
        if include_historical:
            params["includeHistoricalVersion"] = "true"
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
    uuid_diag = uuid_page_diagnostics(rows)
    return {
        "page": page,
        "status_code": status,
        "ok": status == 200,
        "rows_returned": len(rows),
        "uuid_rows": uuid_diag["uuid_rows"],
        "distinct_uuid_rows": uuid_diag["distinct_uuid_rows"],
        "missing_uuid_rows": uuid_diag["missing_uuid_rows"],
        "duplicate_uuid_on_page": uuid_diag["duplicate_uuid_on_page"],
        "api_number": data.get("number") if data else None,
        "api_first": data.get("first") if data else None,
        "api_last": data.get("last") if data else None,
        "api_total_elements": data.get("totalElements") if data else None,
        "api_total_pages": data.get("totalPages") if data else None,
        "elapsed_ms": elapsed_ms,
        "error": error,
        "initial_page": initial_page,
    }



def make_phase_stats(name: str) -> Dict[str, Any]:
    return {
        "phase": name,
        "pages_fetched": 0,
        "rows_received": 0,
        "new_uuid_count": 0,
        "refreshed_uuid_count": 0,
        "uuid_rows_received": 0,
        "missing_uuid_rows": 0,
        "duplicate_uuid_rows": 0,
        "known_pages_streak_at_stop": 0,
        "stop_reason": None,
        "start_page": None,
        "last_successful_page": None,
        "next_page_to_fetch": None,
    }


def uuid_page_diagnostics(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    page_uuids = [str(r.get("uuid")) for r in rows if r.get("uuid")]
    uuid_rows = len(page_uuids)
    distinct_uuid_rows = len(set(page_uuids))
    return {
        "uuid_rows": uuid_rows,
        "distinct_uuid_rows": distinct_uuid_rows,
        "missing_uuid_rows": max(0, len(rows) - uuid_rows),
        "duplicate_uuid_on_page": max(0, uuid_rows - distinct_uuid_rows),
    }


def aggregate_uuid_diagnostics_from_audit(page_audit: List[Dict[str, Any]]) -> Dict[str, int]:
    return {
        "uuid_rows_received": int(sum(r.get("uuid_rows") or 0 for r in page_audit)),
        "missing_uuid_rows": int(sum(r.get("missing_uuid_rows") or 0 for r in page_audit)),
        "duplicate_uuid_rows": int(sum(r.get("duplicate_uuid_on_page") or 0 for r in page_audit)),
    }


def progress_values(pages_done: int, pages_total: int, rows: int, started_at: float, response_times_ms: List[float], status_counts: Dict[int, int], throttle_count: int, eta_remaining_pages: Optional[int] = None) -> Dict[str, Any]:
    elapsed = max(0.001, time.monotonic() - started_at)
    rate = pages_done / elapsed if pages_done else 0.0
    remaining_pages = max(0, pages_total - pages_done) if eta_remaining_pages is None else max(0, int(eta_remaining_pages))
    eta = remaining_pages / rate if rate > 0 else None
    avg_ms = sum(response_times_ms) / len(response_times_ms) if response_times_ms else 0.0
    recent_ms = response_times_ms[-50:]
    recent_avg_ms = sum(recent_ms) / len(recent_ms) if recent_ms else 0.0
    # Response-limited recent rate. The existing global rate remains wall-clock based.
    recent_rate = (len(recent_ms) / (sum(recent_ms) / 1000.0)) if recent_ms and sum(recent_ms) > 0 else 0.0
    failed = sum(v for k, v in status_counts.items() if k != 200)
    pct = (pages_done / pages_total * 100.0) if pages_total else 0.0
    return {
        "elapsed_seconds": elapsed,
        "pages_per_second": rate,
        "remaining_pages": remaining_pages,
        "eta_seconds": eta,
        "avg_response_ms": avg_ms,
        "recent_rate_50": recent_rate,
        "recent_avg_response_ms_50": recent_avg_ms,
        "failed_count": failed,
        "pct": pct,
        "rows": rows,
        "throttle_429_count": throttle_count,
    }


def phase_log_line(phase: str, page: int, total_pages: int, rows_n: int, new_n: int, refreshed_n: int, known_streak: Optional[int], known_stop: Optional[int], status: int, elapsed_ms: Optional[float], retry_after: Optional[str], received_rows: int, pages_done: int, pages_total_for_progress: int, started_at: float, response_times_ms: List[float], status_counts: Dict[int, int], throttle_count: int, stop_reason: Optional[str] = None, eta_remaining_pages: Optional[int] = None, uuid_rows: Optional[int] = None, missing_uuid_rows: Optional[int] = None, duplicate_uuid_on_page: Optional[int] = None, total_new: Optional[int] = None) -> str:
    pv = progress_values(pages_done, pages_total_for_progress, received_rows, started_at, response_times_ms, status_counts, throttle_count, eta_remaining_pages=eta_remaining_pages)
    known_part = ""
    if known_streak is not None and known_stop is not None:
        known_part = f" | known_streak={known_streak}/{known_stop}"
    uuid_part = ""
    if uuid_rows is not None:
        uuid_part = (
            f" | uuid_rows={uuid_rows}"
            f" | missing_uuid_rows={missing_uuid_rows or 0}"
            f" | duplicate_uuid_on_page={duplicate_uuid_on_page or 0}"
        )
    total_new_part = f" | total_new={total_new}" if total_new is not None else ""
    reason_part = f" | reason={stop_reason}" if stop_reason else ""
    return (
        f"{phase} page={page}/{max(0, total_pages-1)} | rows={rows_n}{uuid_part} | new={new_n}{total_new_part} | refreshed={refreshed_n}"
        f"{known_part} | status={status} | elapsed_ms={(elapsed_ms or 0):.0f} | retry_after={retry_after} | "
        f"received_rows={received_rows:,} | rate={pv['pages_per_second']:.3f} pages/s | "
        f"avg_response={pv['avg_response_ms']:.0f} ms | recent_rate_50={pv['recent_rate_50']:.3f} pages/s | "
        f"recent_avg_response_50={pv['recent_avg_response_ms_50']:.0f} ms | 429_count={throttle_count} | failed_count={pv['failed_count']} | "
        f"elapsed={fmt_duration(pv['elapsed_seconds'])} | ETA={fmt_duration(pv['eta_seconds'])}{reason_part}"
    )


def runtime_exceeded(started_at: float, max_runtime_hours: float) -> bool:
    return bool(max_runtime_hours and max_runtime_hours > 0 and ((time.monotonic() - started_at) / 3600.0) >= max_runtime_hours)


def count_new_and_refreshed(rows: List[Dict[str, Any]], existing_uuid_set: set) -> Tuple[int, int]:
    uuids = {r.get("uuid") for r in rows if r.get("uuid")}
    new_n = len(uuids - existing_uuid_set)
    refreshed_n = len(uuids & existing_uuid_set)
    return new_n, refreshed_n


def fetch_page_and_record(
    client: EudamedClient,
    page: int,
    extract_date: str,
    extract_ts: str,
    page_audit: List[Dict[str, Any]],
    request_log: List[Dict[str, Any]],
    response_times_ms: List[float],
    status_counts: Dict[int, int],
    initial_data: Optional[Dict[str, Any]] = None,
    initial_meta: Optional[Dict[str, Any]] = None,
    include_historical: bool = False,
) -> Tuple[bool, int, Optional[Dict[str, Any]], List[Dict[str, Any]], Optional[float], Optional[str], Optional[str]]:
    if initial_data is not None:
        status, data, error, elapsed_ms, retry_after = 200, initial_data, None, (initial_meta or {}).get("elapsed_ms"), (initial_meta or {}).get("retry_after")
        initial = True
    else:
        _, status, data, error, elapsed_ms, retry_after = client.fetch_page(page, include_historical=include_historical)
        initial = False
    rows = parse_page(data, page, extract_date, extract_ts) if data else []
    status_counts[int(status or 0)] = status_counts.get(int(status or 0), 0) + 1
    if elapsed_ms is not None:
        response_times_ms.append(float(elapsed_ms))
    page_audit.append(page_audit_row(page, status, data, rows, elapsed_ms, error, initial_page=initial))
    request_log.append(request_log_row(page, status, elapsed_ms, retry_after, error, initial_page=initial))
    return status == 200, status, data, rows, elapsed_ms, retry_after, error


def run_known_pages_scan(
    client: EudamedClient,
    phase_name: str,
    direction: str,
    start_page: int,
    total_pages: int,
    extract_date: str,
    extract_ts: str,
    existing_uuid_set: set,
    known_pages_to_stop: int,
    max_pages: int,
    started_at: float,
    response_times_ms: List[float],
    status_counts: Dict[int, int],
    page_audit: List[Dict[str, Any]],
    request_log: List[Dict[str, Any]],
    received_rows: List[Dict[str, Any]],
    fetched_pages: set,
    initial_data: Optional[Dict[str, Any]] = None,
    initial_meta: Optional[Dict[str, Any]] = None,
    max_runtime_hours: float = 0.0,
    log_every_page: bool = True,
    include_historical: bool = False,
) -> Tuple[Dict[str, Any], bool]:
    stats = make_phase_stats(phase_name)
    stats["start_page"] = start_page
    consecutive_known = 0
    pages_done_phase = 0
    normal_completion = False
    page = start_page
    step = 1 if direction == "forward" else -1
    log(f"=== {phase_name} {direction} start_page={start_page} known_pages_to_stop={known_pages_to_stop} ===")

    while 0 <= page < total_pages:
        if max_pages and pages_done_phase >= max_pages:
            stats["stop_reason"] = "max_pages_cap_reached"
            normal_completion = True
            break
        if runtime_exceeded(started_at, max_runtime_hours):
            stats["stop_reason"] = "runtime_limit"
            normal_completion = False
            break
        if page in fetched_pages:
            page += step
            continue
        use_initial = initial_data is not None and page == 0
        ok, status, data, rows, elapsed_ms, retry_after, error = fetch_page_and_record(
            client, page, extract_date, extract_ts, page_audit, request_log, response_times_ms, status_counts,
            initial_data=initial_data if use_initial else None,
            initial_meta=initial_meta if use_initial else None,
            include_historical=include_historical,
        )
        fetched_pages.add(page)
        pages_done_phase += 1
        stats["pages_fetched"] += 1
        uuid_diag = uuid_page_diagnostics(rows)
        if ok:
            received_rows.extend(rows)
            stats["rows_received"] += len(rows)
            stats["uuid_rows_received"] += uuid_diag["uuid_rows"]
            stats["missing_uuid_rows"] += uuid_diag["missing_uuid_rows"]
            stats["duplicate_uuid_rows"] += uuid_diag["duplicate_uuid_on_page"]
            stats["last_successful_page"] = page
            stats["next_page_to_fetch"] = page + step
        else:
            stats["stop_reason"] = f"page_{page}_fetch_failed"
            normal_completion = False
            log(phase_log_line(phase_name, page, total_pages, len(rows), 0, 0, consecutive_known, known_pages_to_stop, status, elapsed_ms, retry_after, len(received_rows), len(page_audit), total_pages, started_at, response_times_ms, status_counts, len(client.throttle_events), stats["stop_reason"], uuid_rows=uuid_diag["uuid_rows"], missing_uuid_rows=uuid_diag["missing_uuid_rows"], duplicate_uuid_on_page=uuid_diag["duplicate_uuid_on_page"], total_new=stats["new_uuid_count"]))
            break
        new_n, refreshed_n = count_new_and_refreshed(rows, existing_uuid_set)
        stats["new_uuid_count"] += new_n
        stats["refreshed_uuid_count"] += refreshed_n
        consecutive_known = consecutive_known + 1 if new_n == 0 else 0
        stats["known_pages_streak_at_stop"] = consecutive_known
        if log_every_page:
            log(phase_log_line(phase_name, page, total_pages, len(rows), new_n, refreshed_n, consecutive_known, known_pages_to_stop, status, elapsed_ms, retry_after, len(received_rows), len(page_audit), total_pages, started_at, response_times_ms, status_counts, len(client.throttle_events), uuid_rows=uuid_diag["uuid_rows"], missing_uuid_rows=uuid_diag["missing_uuid_rows"], duplicate_uuid_on_page=uuid_diag["duplicate_uuid_on_page"], total_new=stats["new_uuid_count"]))
        if len(rows) == 0:
            stats["stop_reason"] = "empty_page"
            normal_completion = True
            break
        if consecutive_known >= known_pages_to_stop:
            stats["stop_reason"] = f"{known_pages_to_stop}_known_pages_in_a_row"
            normal_completion = True
            break
        page += step
    else:
        stats["stop_reason"] = "boundary_reached"
        normal_completion = True

    if stats["stop_reason"] is None:
        stats["stop_reason"] = "loop_complete"
    return stats, normal_completion


def fetch_incremental(
    client: EudamedClient,
    initial_data: Dict[str, Any],
    initial_meta: Dict[str, Any],
    existing_uuid_set: set,
    extract_date: str,
    extract_ts: str,
    head_known_pages_to_stop: int,
    tail_known_pages_to_stop: int,
    max_pages: int = 0,
    max_runtime_hours: float = 0.0,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], bool]:
    total_pages = int(initial_meta.get("total_pages") or 0)
    received_rows: List[Dict[str, Any]] = []
    page_audit: List[Dict[str, Any]] = []
    request_log: List[Dict[str, Any]] = []
    started_at = time.monotonic()
    response_times_ms: List[float] = []
    status_counts: Dict[int, int] = {}
    fetched_pages: set = set()

    log("=== Incremental mode: HEAD + TAIL ===")
    head_stats, head_ok = run_known_pages_scan(
        client=client,
        phase_name="Incremental Head",
        direction="forward",
        start_page=0,
        total_pages=total_pages,
        extract_date=extract_date,
        extract_ts=extract_ts,
        existing_uuid_set=existing_uuid_set,
        known_pages_to_stop=head_known_pages_to_stop,
        max_pages=max_pages if max_pages and max_pages > 0 else 0,
        started_at=started_at,
        response_times_ms=response_times_ms,
        status_counts=status_counts,
        page_audit=page_audit,
        request_log=request_log,
        received_rows=received_rows,
        fetched_pages=fetched_pages,
        initial_data=initial_data,
        initial_meta=initial_meta,
        max_runtime_hours=max_runtime_hours,
        log_every_page=True,
    )

    tail_ok = True
    tail_stats = make_phase_stats("Incremental Tail")
    if head_ok and not runtime_exceeded(started_at, max_runtime_hours):
        tail_stats, tail_ok = run_known_pages_scan(
            client=client,
            phase_name="Incremental Tail",
            direction="backward",
            start_page=max(0, total_pages - 1),
            total_pages=total_pages,
            extract_date=extract_date,
            extract_ts=extract_ts,
            existing_uuid_set=existing_uuid_set,
            known_pages_to_stop=tail_known_pages_to_stop,
            max_pages=0,
            started_at=started_at,
            response_times_ms=response_times_ms,
            status_counts=status_counts,
            page_audit=page_audit,
            request_log=request_log,
            received_rows=received_rows,
            fetched_pages=fetched_pages,
            max_runtime_hours=max_runtime_hours,
            log_every_page=True,
        )
    elif runtime_exceeded(started_at, max_runtime_hours):
        tail_ok = False
        tail_stats["stop_reason"] = "skipped_runtime_limit"
    else:
        tail_ok = False
        tail_stats["stop_reason"] = "skipped_head_failed"

    elapsed_total = time.monotonic() - started_at
    pages_ok = len([r for r in page_audit if r.get("ok")])
    normal_completion = bool(head_ok and tail_ok)
    uuid_diag_totals = aggregate_uuid_diagnostics_from_audit(page_audit)
    total_new_uuid_count = int((head_stats or {}).get("new_uuid_count") or 0) + int((tail_stats or {}).get("new_uuid_count") or 0)
    audit = {
        "mode": "incremental",
        "api_total_elements": initial_meta.get("total_elements"),
        "api_total_pages": initial_meta.get("total_pages"),
        "current_total_elements": initial_meta.get("total_elements"),
        "current_total_pages": initial_meta.get("total_pages"),
        "pages_fetched": pages_ok,
        "received_rows": len(received_rows),
        "new_uuid_count": total_new_uuid_count,
        "uuid_rows_received": uuid_diag_totals["uuid_rows_received"],
        "missing_uuid_rows": uuid_diag_totals["missing_uuid_rows"],
        "duplicate_uuid_rows": uuid_diag_totals["duplicate_uuid_rows"],
        "normal_completion": normal_completion,
        "stop_reason": "incremental_head_tail_complete" if normal_completion else "incremental_partial",
        "phases": {"head": head_stats, "tail": tail_stats},
        "telemetry": {
            "elapsed_seconds": elapsed_total,
            "pages_per_second": pages_ok / max(0.001, elapsed_total),
            "avg_response_ms": (sum(response_times_ms) / len(response_times_ms)) if response_times_ms else None,
            "recent_rate_50": ((len(response_times_ms[-50:]) / (sum(response_times_ms[-50:]) / 1000.0)) if response_times_ms[-50:] and sum(response_times_ms[-50:]) > 0 else None),
            "recent_avg_response_ms_50": ((sum(response_times_ms[-50:]) / len(response_times_ms[-50:])) if response_times_ms[-50:] else None),
            "status_counts": {str(k): v for k, v in sorted(status_counts.items())},
            "throttle_429_count": len(client.throttle_events),
            "throttle_events": client.throttle_events,
        },
    }
    return received_rows, page_audit, request_log, audit, normal_completion


def fetch_full(
    client: EudamedClient,
    initial_data: Dict[str, Any],
    initial_meta: Dict[str, Any],
    existing_uuid_set: set,
    extract_date: str,
    extract_ts: str,
    max_pages: int = 0,
    max_429_before_partial: int = 7,
    max_body_runtime_hours: float = 0.0,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], bool]:
    total_pages = int(initial_meta.get("total_pages") or 0)
    if max_pages and max_pages > 0:
        total_pages = min(total_pages, max_pages)
    received_rows: List[Dict[str, Any]] = []
    page_audit: List[Dict[str, Any]] = []
    request_log: List[Dict[str, Any]] = []
    failed_pages: List[int] = []
    normal_completion = True
    stop_reason = "all_pages_fetched"
    started_at = time.monotonic()
    response_times_ms: List[float] = []
    status_counts: Dict[int, int] = {}
    last_successful_page: Optional[int] = None
    total_new_uuid_count = 0
    total_uuid_rows_received = 0
    total_missing_uuid_rows = 0
    total_duplicate_uuid_rows = 0

    log(f"=== Full fetch pages 0..{total_pages - 1} ({total_pages} pages) ===")
    for page in range(total_pages):
        ok, status, data, rows, elapsed_ms, retry_after, error = fetch_page_and_record(
            client, page, extract_date, extract_ts, page_audit, request_log, response_times_ms, status_counts,
            initial_data=initial_data if page == 0 else None,
            initial_meta=initial_meta if page == 0 else None,
            include_historical=True,
        )
        uuid_diag = uuid_page_diagnostics(rows)
        new_n, refreshed_n = count_new_and_refreshed(rows, existing_uuid_set)
        if ok:
            received_rows.extend(rows)
            last_successful_page = page
            total_new_uuid_count += new_n
            total_uuid_rows_received += uuid_diag["uuid_rows"]
            total_missing_uuid_rows += uuid_diag["missing_uuid_rows"]
            total_duplicate_uuid_rows += uuid_diag["duplicate_uuid_on_page"]
        else:
            failed_pages.append(page)
            normal_completion = False
            stop_reason = "page_fetch_failed"
        pages_done = page + 1
        detailed = len(client.throttle_events) > 0
        should_log = detailed or page % 50 == 0 or page == total_pages - 1 or not ok
        if should_log:
            log(phase_log_line("Full", page, total_pages, len(rows), new_n, refreshed_n, None, None, status, elapsed_ms, retry_after, len(received_rows), pages_done, total_pages, started_at, response_times_ms, status_counts, len(client.throttle_events), None if ok else stop_reason, eta_remaining_pages=max(0, total_pages - page - 1), uuid_rows=uuid_diag["uuid_rows"], missing_uuid_rows=uuid_diag["missing_uuid_rows"], duplicate_uuid_on_page=uuid_diag["duplicate_uuid_on_page"], total_new=total_new_uuid_count))
        if not ok:
            break
        if max_429_before_partial and len(client.throttle_events) >= max_429_before_partial:
            normal_completion = False
            stop_reason = "429_limit"
            log(f"FULL CONTROLLED PARTIAL STOP: 429_count={len(client.throttle_events)} max_429_before_partial={max_429_before_partial} last_successful_page={last_successful_page}")
            break
        if runtime_exceeded(started_at, max_body_runtime_hours):
            normal_completion = False
            stop_reason = "runtime_limit"
            log(f"FULL CONTROLLED PARTIAL STOP: runtime_limit max_body_runtime_hours={max_body_runtime_hours} last_successful_page={last_successful_page}")
            break

    elapsed_total = time.monotonic() - started_at
    pages_ok = len([r for r in page_audit if r.get("ok")])
    audit = {
        "mode": "full",
        "api_total_elements": initial_meta.get("total_elements"),
        "api_total_pages": initial_meta.get("total_pages"),
        "historical_total_elements": initial_meta.get("total_elements"),
        "historical_total_pages": initial_meta.get("total_pages"),
        "current_total_elements": None,
        "current_total_pages": None,
        "current_initial_page": None,
        "pages_fetched": pages_ok,
        "failed_pages": failed_pages,
        "failed_pages_count": len(failed_pages),
        "received_rows": len(received_rows),
        "new_uuid_count": total_new_uuid_count,
        "uuid_rows_received": total_uuid_rows_received,
        "missing_uuid_rows": total_missing_uuid_rows,
        "duplicate_uuid_rows": total_duplicate_uuid_rows,
        "normal_completion": normal_completion,
        "stop_reason": stop_reason,
        "last_successful_page": last_successful_page,
        "next_page_to_fetch": (last_successful_page + 1) if last_successful_page is not None else 0,
        "resume_state": None if normal_completion else {
            "resume_mode": "resume_full",
            "last_successful_page": last_successful_page,
            "next_page_to_fetch": (last_successful_page + 1) if last_successful_page is not None else 0,
            "api_total_pages": initial_meta.get("total_pages"),
            "stop_reason": stop_reason,
        },
        "telemetry": {
            "elapsed_seconds": elapsed_total,
            "pages_per_second": pages_ok / max(0.001, elapsed_total),
            "avg_response_ms": (sum(response_times_ms) / len(response_times_ms)) if response_times_ms else None,
            "recent_rate_50": ((len(response_times_ms[-50:]) / (sum(response_times_ms[-50:]) / 1000.0)) if response_times_ms[-50:] and sum(response_times_ms[-50:]) > 0 else None),
            "recent_avg_response_ms_50": ((sum(response_times_ms[-50:]) / len(response_times_ms[-50:])) if response_times_ms[-50:] else None),
            "status_counts": {str(k): v for k, v in sorted(status_counts.items())},
            "throttle_429_count": len(client.throttle_events),
            "throttle_events": client.throttle_events,
        },
    }
    return received_rows, page_audit, request_log, audit, normal_completion


def read_previous_metadata(inputs_dir: Path) -> Dict[str, Any]:
    path = inputs_dir / "eudamed_platform_udi_raw_latest.metadata.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"WARNING could not read previous metadata {path}: {e}")
        return {}


def extract_resume_state(previous_metadata: Dict[str, Any]) -> Dict[str, Any]:
    if not previous_metadata:
        return {}
    audit = previous_metadata.get("audit") or {}
    state = previous_metadata.get("resume_state") or audit.get("resume_state") or {}
    if state:
        return state
    if audit.get("next_page_to_fetch") is not None:
        return {
            "resume_mode": "resume_full",
            "last_successful_page": audit.get("last_successful_page"),
            "next_page_to_fetch": audit.get("next_page_to_fetch"),
            "api_total_pages": audit.get("api_total_pages") or previous_metadata.get("api_total_pages"),
            "stop_reason": audit.get("stop_reason"),
        }
    return {}


def fetch_resume_full(
    client: EudamedClient,
    initial_data: Dict[str, Any],
    initial_meta: Dict[str, Any],
    previous_metadata: Dict[str, Any],
    existing_uuid_set: set,
    extract_date: str,
    extract_ts: str,
    resume_overlap_pages: int,
    head_known_pages_to_stop: int,
    tail_known_pages_to_stop: int,
    max_pages: int = 0,
    max_429_before_partial: int = 7,
    max_body_runtime_hours: float = 0.0,
    max_total_runtime_hours: float = 0.0,
    slow_rps: float = 0.3,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], bool]:
    total_pages = int(initial_meta.get("total_pages") or 0)
    resume_state_in = extract_resume_state(previous_metadata)
    next_page = int(resume_state_in.get("next_page_to_fetch") or 0)
    body_start_page = max(0, next_page - max(0, resume_overlap_pages))
    body_total_pages = total_pages
    received_rows: List[Dict[str, Any]] = []
    page_audit: List[Dict[str, Any]] = []
    request_log: List[Dict[str, Any]] = []
    failed_pages: List[int] = []
    started_at = time.monotonic()
    response_times_ms: List[float] = []
    status_counts: Dict[int, int] = {}
    fetched_pages: set = set()
    last_successful_body_page: Optional[int] = None
    body_new_uuid_count = 0
    body_uuid_rows_received = 0
    body_missing_uuid_rows = 0
    body_duplicate_uuid_rows = 0
    normal_completion = False
    stop_reason = "resume_body_complete"

    log(f"=== Resume Full body start_page={body_start_page} next_page_from_state={next_page} overlap={resume_overlap_pages} ===")
    page = body_start_page
    body_pages_done = 0
    while page < body_total_pages:
        if max_pages and max_pages > 0 and body_pages_done >= max_pages:
            stop_reason = "max_pages_cap_reached"
            break
        ok, status, data, rows, elapsed_ms, retry_after, error = fetch_page_and_record(
            client, page, extract_date, extract_ts, page_audit, request_log, response_times_ms, status_counts,
            initial_data=initial_data if page == 0 else None,
            initial_meta=initial_meta if page == 0 else None,
            include_historical=True,
        )
        fetched_pages.add(page)
        body_pages_done += 1
        uuid_diag = uuid_page_diagnostics(rows)
        new_n, refreshed_n = count_new_and_refreshed(rows, existing_uuid_set)
        if ok:
            received_rows.extend(rows)
            last_successful_body_page = page
            body_new_uuid_count += new_n
            body_uuid_rows_received += uuid_diag["uuid_rows"]
            body_missing_uuid_rows += uuid_diag["missing_uuid_rows"]
            body_duplicate_uuid_rows += uuid_diag["duplicate_uuid_on_page"]
        else:
            failed_pages.append(page)
            stop_reason = "page_fetch_failed"
        log(phase_log_line("Resume Body", page, total_pages, len(rows), new_n, refreshed_n, None, None, status, elapsed_ms, retry_after, len(received_rows), body_pages_done, body_total_pages, started_at, response_times_ms, status_counts, len(client.throttle_events), None if ok else stop_reason, eta_remaining_pages=max(0, body_total_pages - page - 1), uuid_rows=uuid_diag["uuid_rows"], missing_uuid_rows=uuid_diag["missing_uuid_rows"], duplicate_uuid_on_page=uuid_diag["duplicate_uuid_on_page"], total_new=body_new_uuid_count))
        if not ok:
            break
        if max_429_before_partial and len(client.throttle_events) >= max_429_before_partial:
            stop_reason = "429_limit"
            log(f"RESUME BODY CONTROLLED PARTIAL STOP: 429_count={len(client.throttle_events)} max_429_before_partial={max_429_before_partial} last_successful_body_page={last_successful_body_page}")
            break
        if runtime_exceeded(started_at, max_body_runtime_hours):
            stop_reason = "runtime_limit"
            log(f"RESUME BODY CONTROLLED PARTIAL STOP: runtime_limit max_body_runtime_hours={max_body_runtime_hours} last_successful_body_page={last_successful_body_page}")
            break
        page += 1
    else:
        normal_completion = True
        stop_reason = "all_pages_fetched"

    body_stats = {
        "phase": "Resume Body",
        "start_page": body_start_page,
        "pages_fetched": body_pages_done,
        "rows_received": sum(r.get("rows_returned") or 0 for r in page_audit),
        "new_uuid_count": body_new_uuid_count,
        "uuid_rows_received": body_uuid_rows_received,
        "missing_uuid_rows": body_missing_uuid_rows,
        "duplicate_uuid_rows": body_duplicate_uuid_rows,
        "last_successful_page": last_successful_body_page,
        "next_page_to_fetch": (last_successful_body_page + 1) if last_successful_body_page is not None else body_start_page,
        "stop_reason": stop_reason,
    }

    # After body, run slow current head and tail incremental probes. They refresh rows but do not move body checkpoint.
    # Body uses includeHistoricalVersion=true. Head/tail intentionally use the current endpoint with the parameter omitted.
    current_initial_meta: Dict[str, Any] = {}
    head_stats = make_phase_stats("Resume Head")
    tail_stats = make_phase_stats("Resume Tail")
    head_ok = True
    tail_ok = True
    old_rps = client.max_rps
    if slow_rps and slow_rps > 0:
        client.max_rps = slow_rps
    if runtime_exceeded(started_at, max_total_runtime_hours):
        head_ok = False
        tail_ok = False
        head_stats["stop_reason"] = "skipped_total_runtime_limit"
        tail_stats["stop_reason"] = "skipped_total_runtime_limit"
    else:
        log("=== Resume current head/tail initial page fetch page=0 ===")
        _, current_status, current_initial_data, current_error, current_elapsed_ms, current_retry_after = client.fetch_page(0, include_historical=False)
        current_initial_meta = initial_page_record(current_initial_data, current_status, current_error, current_elapsed_ms, current_retry_after, client.page_size)
        log(json.dumps({"current_initial_page": current_initial_meta}, ensure_ascii=False))
        if current_status != 200 or not current_initial_data:
            head_ok = False
            tail_ok = False
            head_stats["stop_reason"] = "current_initial_page_failed"
            tail_stats["stop_reason"] = "current_initial_page_failed"
        else:
            current_total_pages = int(current_initial_meta.get("total_pages") or 0)
            current_fetched_pages: set = set()
            head_stats, head_ok = run_known_pages_scan(
                client=client,
                phase_name="Resume Head",
                direction="forward",
                start_page=0,
                total_pages=current_total_pages,
                extract_date=extract_date,
                extract_ts=extract_ts,
                existing_uuid_set=existing_uuid_set,
                known_pages_to_stop=head_known_pages_to_stop,
                max_pages=0,
                started_at=started_at,
                response_times_ms=response_times_ms,
                status_counts=status_counts,
                page_audit=page_audit,
                request_log=request_log,
                received_rows=received_rows,
                fetched_pages=current_fetched_pages,
                initial_data=current_initial_data,
                initial_meta=current_initial_meta,
                max_runtime_hours=max_total_runtime_hours,
                log_every_page=True,
                include_historical=False,
            )
            if head_ok and not runtime_exceeded(started_at, max_total_runtime_hours):
                tail_stats, tail_ok = run_known_pages_scan(
                    client=client,
                    phase_name="Resume Tail",
                    direction="backward",
                    start_page=max(0, current_total_pages - 1),
                    total_pages=current_total_pages,
                    extract_date=extract_date,
                    extract_ts=extract_ts,
                    existing_uuid_set=existing_uuid_set,
                    known_pages_to_stop=tail_known_pages_to_stop,
                    max_pages=0,
                    started_at=started_at,
                    response_times_ms=response_times_ms,
                    status_counts=status_counts,
                    page_audit=page_audit,
                    request_log=request_log,
                    received_rows=received_rows,
                    fetched_pages=current_fetched_pages,
                    max_runtime_hours=max_total_runtime_hours,
                    log_every_page=True,
                    include_historical=False,
                )
            elif runtime_exceeded(started_at, max_total_runtime_hours):
                tail_ok = False
                tail_stats["stop_reason"] = "skipped_total_runtime_limit"
            else:
                tail_ok = False
                tail_stats["stop_reason"] = "skipped_head_failed"
    client.max_rps = old_rps

    elapsed_total = time.monotonic() - started_at
    pages_ok = len([r for r in page_audit if r.get("ok")])
    uuid_diag_totals = aggregate_uuid_diagnostics_from_audit(page_audit)
    total_new_uuid_count = (
        int((body_stats or {}).get("new_uuid_count") or 0)
        + int((head_stats or {}).get("new_uuid_count") or 0)
        + int((tail_stats or {}).get("new_uuid_count") or 0)
    )
    full_normal_completion = bool(normal_completion and head_ok and tail_ok)
    resume_state_out = None if full_normal_completion else {
        "resume_mode": "resume_full",
        "last_successful_page": last_successful_body_page,
        "next_page_to_fetch": (last_successful_body_page + 1) if last_successful_body_page is not None else body_start_page,
        "api_total_pages": total_pages,
        "resume_overlap_pages": resume_overlap_pages,
        "recommended_resume_start_page": max(0, ((last_successful_body_page + 1) if last_successful_body_page is not None else body_start_page) - resume_overlap_pages),
        "stop_reason": stop_reason,
    }
    audit = {
        "mode": "resume_full",
        "api_total_elements": initial_meta.get("total_elements"),
        "api_total_pages": initial_meta.get("total_pages"),
        "historical_total_elements": initial_meta.get("total_elements"),
        "historical_total_pages": initial_meta.get("total_pages"),
        "current_total_elements": current_initial_meta.get("total_elements") if current_initial_meta else None,
        "current_total_pages": current_initial_meta.get("total_pages") if current_initial_meta else None,
        "current_initial_page": current_initial_meta,
        "pages_fetched": pages_ok,
        "failed_pages": failed_pages,
        "failed_pages_count": len(failed_pages),
        "received_rows": len(received_rows),
        "new_uuid_count": total_new_uuid_count,
        "uuid_rows_received": uuid_diag_totals["uuid_rows_received"],
        "missing_uuid_rows": uuid_diag_totals["missing_uuid_rows"],
        "duplicate_uuid_rows": uuid_diag_totals["duplicate_uuid_rows"],
        "normal_completion": full_normal_completion,
        "stop_reason": "resume_full_complete" if full_normal_completion else stop_reason,
        "last_successful_page": last_successful_body_page,
        "next_page_to_fetch": (last_successful_body_page + 1) if last_successful_body_page is not None else body_start_page,
        "resume_state": resume_state_out,
        "phases": {"body": body_stats, "head": head_stats, "tail": tail_stats},
        "telemetry": {
            "elapsed_seconds": elapsed_total,
            "pages_per_second": pages_ok / max(0.001, elapsed_total),
            "avg_response_ms": (sum(response_times_ms) / len(response_times_ms)) if response_times_ms else None,
            "recent_rate_50": ((len(response_times_ms[-50:]) / (sum(response_times_ms[-50:]) / 1000.0)) if response_times_ms[-50:] and sum(response_times_ms[-50:]) > 0 else None),
            "recent_avg_response_ms_50": ((sum(response_times_ms[-50:]) / len(response_times_ms[-50:])) if response_times_ms[-50:] else None),
            "status_counts": {str(k): v for k, v in sorted(status_counts.items())},
            "throttle_429_count": len(client.throttle_events),
            "throttle_events": client.throttle_events,
        },
    }
    return received_rows, page_audit, request_log, audit, full_normal_completion

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
        f"- Historical API total elements: `{metadata.get('historical_total_elements')}`",
        f"- Historical API total pages: `{metadata.get('historical_total_pages')}`",
        f"- Current API total elements: `{metadata.get('current_total_elements')}`",
        f"- Current API total pages: `{metadata.get('current_total_pages')}`",
        f"- Rows in latest DB: `{metadata.get('row_count')}`",
        f"- Distinct UUIDs: `{metadata.get('distinct_uuid_count')}`",
        f"- Distinct PRIMARY_DI: `{metadata.get('distinct_primary_di_count')}`",
        f"- Distinct ULID: `{metadata.get('distinct_ulid_count')}`",
        f"- latest_version=true rows: `{metadata.get('latest_version_true_count')}`",
        f"- Completeness ratio vs API total: `{metadata.get('completeness_ratio')}`",
        "",
        "## Merge",
        "",
        f"- Previous rows: `{metadata.get('merge', {}).get('previous_rows')}`",
        f"- Received rows this run: `{metadata.get('merge', {}).get('received_rows')}`",
        f"- New UUIDs seen this run: `{metadata.get('new_uuid_count')}`",
        f"- UUID rows received this run: `{metadata.get('uuid_rows_received')}`",
        f"- Missing UUID rows this run: `{metadata.get('missing_uuid_rows')}`",
        f"- Duplicate UUID rows this run: `{metadata.get('duplicate_uuid_rows')}`",
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
        "## Phases",
        "",
        f"- Phase stats JSON: `{json.dumps(metadata.get('phases') or {}, ensure_ascii=False)}`",
        "",
        "## Resume state",
        "",
        f"- Resume state JSON: `{json.dumps(metadata.get('resume_state') or {}, ensure_ascii=False)}`",
        "",
        "## Telemetry",
        "",
        f"- Pages/sec: `{(metadata.get('telemetry') or {}).get('pages_per_second')}`",
        f"- Avg response ms: `{(metadata.get('telemetry') or {}).get('avg_response_ms')}`",
        f"- 429 count: `{(metadata.get('telemetry') or {}).get('throttle_429_count')}`",
        f"- Recent rate 50: `{(metadata.get('telemetry') or {}).get('recent_rate_50')}`",
        f"- Recent avg response 50 ms: `{(metadata.get('telemetry') or {}).get('recent_avg_response_ms_50')}`",
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
    p.add_argument("--mode", choices=["incremental", "full", "resume_full"], default="incremental")
    p.add_argument("--out-dir", default="dist/eudamed_platform_udi_raw")
    p.add_argument("--inputs-dir", default="inputs")
    p.add_argument("--base-url", default=BASE_URL_DEFAULT)
    p.add_argument("--language", default="en")
    p.add_argument("--page-size", type=int, default=300)
    p.add_argument("--max-pages", type=int, default=0)
    p.add_argument("--max-rps", type=float, default=1.0)
    p.add_argument("--slow-rps", type=float, default=0.3, help="Slow RPS used for resume_full head/tail refresh")
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--retries", type=int, default=4)
    p.add_argument("--backoff", type=float, default=1.5)
    p.add_argument("--head-known-pages-to-stop", type=int, default=20)
    p.add_argument("--tail-known-pages-to-stop", type=int, default=20)
    p.add_argument("--resume-overlap-pages", type=int, default=250)
    p.add_argument("--max-429-before-partial", type=int, default=7)
    p.add_argument("--max-runtime-hours", type=float, default=None, help="Backward compatible alias for --max-body-runtime-hours")
    p.add_argument("--max-body-runtime-hours", type=float, default=2.5)
    p.add_argument("--max-total-runtime-hours", type=float, default=3.0)
    p.add_argument("--release-timestamp", default=None)
    p.add_argument("--skip-csv-zip", action="store_true")
    return p.parse_args(argv)

def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    out_dir = Path(args.out_dir)
    inputs_dir = Path(args.inputs_dir)
    ensure_dir(out_dir)
    ensure_dir(inputs_dir)
    if args.max_runtime_hours is not None:
        args.max_body_runtime_hours = args.max_runtime_hours
        if not args.max_total_runtime_hours or args.max_total_runtime_hours < args.max_body_runtime_hours:
            args.max_total_runtime_hours = args.max_body_runtime_hours
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

    previous_metadata = read_previous_metadata(inputs_dir)
    previous_max_ulid = max_ulid(r.get("ulid") for r in existing_rows)
    existing_uuid_set = {r.get("uuid") for r in existing_rows if r.get("uuid")}
    effective_mode = args.mode
    bootstrap = False
    if args.mode in {"incremental", "resume_full"} and not existing_rows:
        effective_mode = "full"
        bootstrap = True
        log(f"{args.mode} requested but no previous DB exists. Falling back to BOOTSTRAP full crawl.")
    elif args.mode in {"incremental", "resume_full"} and not previous_max_ulid:
        effective_mode = "full"
        bootstrap = True
        log(f"{args.mode} requested but previous DB has no max ULID. Falling back to BOOTSTRAP full crawl.")

    client = EudamedClient(args.base_url, args.language, args.page_size, args.timeout, args.retries, args.backoff, args.max_rps)

    body_include_historical = effective_mode in {"full", "resume_full"}
    log(f"=== Initial page fetch page=0 includeHistoricalVersion={'true' if body_include_historical else 'omitted'} ===")
    page, status, initial_data, error, elapsed_ms, retry_after = client.fetch_page(0, include_historical=body_include_historical)
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
            client, initial_data, initial_meta, existing_uuid_set, extract_date, extract_ts,
            args.head_known_pages_to_stop, args.tail_known_pages_to_stop,
            max_pages=args.max_pages, max_runtime_hours=args.max_total_runtime_hours
        )
    elif effective_mode == "resume_full":
        received_rows, page_audit, request_log, audit, normal_completion = fetch_resume_full(
            client, initial_data, initial_meta, previous_metadata, existing_uuid_set, extract_date, extract_ts,
            resume_overlap_pages=args.resume_overlap_pages,
            head_known_pages_to_stop=args.head_known_pages_to_stop,
            tail_known_pages_to_stop=args.tail_known_pages_to_stop,
            max_pages=args.max_pages,
            max_429_before_partial=args.max_429_before_partial,
            max_body_runtime_hours=args.max_body_runtime_hours,
            max_total_runtime_hours=args.max_total_runtime_hours,
            slow_rps=args.slow_rps,
        )
    else:
        received_rows, page_audit, request_log, audit, normal_completion = fetch_full(
            client, initial_data, initial_meta, existing_uuid_set, extract_date, extract_ts,
            max_pages=args.max_pages,
            max_429_before_partial=args.max_429_before_partial,
            max_body_runtime_hours=args.max_body_runtime_hours,
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
    distinct_uuid_count = len({r.get("uuid") for r in merged_rows if r.get("uuid")})
    distinct_primary_di_count = len({r.get("primary_di") for r in merged_rows if r.get("primary_di")})
    distinct_ulid_count = len({r.get("ulid") for r in merged_rows if r.get("ulid")})
    latest_version_true_count = sum(1 for r in merged_rows if str(r.get("latest_version")).lower() == "true" or r.get("latest_version") is True)
    previous_resume_state = extract_resume_state(previous_metadata)
    output_resume_state = previous_resume_state if effective_mode == "incremental" else (audit or {}).get("resume_state")
    if effective_mode == "incremental" and previous_resume_state:
        audit["preserved_resume_state"] = previous_resume_state
    historical_total_elements = (audit or {}).get("historical_total_elements") if body_include_historical else None
    historical_total_pages = (audit or {}).get("historical_total_pages") if body_include_historical else None
    current_total_elements = (audit or {}).get("current_total_elements") or (api_total_elements if not body_include_historical else None)
    current_total_pages = (audit or {}).get("current_total_pages") or (api_total_pages if not body_include_historical else None)

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
        "slow_rps": args.slow_rps,
        "max_429_before_partial": args.max_429_before_partial,
        "max_runtime_hours": args.max_runtime_hours,
        "max_body_runtime_hours": args.max_body_runtime_hours,
        "max_total_runtime_hours": args.max_total_runtime_hours,
        "body_include_historical": body_include_historical,
        "head_known_pages_to_stop": args.head_known_pages_to_stop,
        "tail_known_pages_to_stop": args.tail_known_pages_to_stop,
        "resume_overlap_pages": args.resume_overlap_pages,
        "retries": args.retries,
        "api_total_elements": api_total_elements,
        "api_total_pages": api_total_pages,
        "historical_total_elements": historical_total_elements,
        "historical_total_pages": historical_total_pages,
        "current_total_elements": current_total_elements,
        "current_total_pages": current_total_pages,
        "distinct_uuid_count": distinct_uuid_count,
        "distinct_primary_di_count": distinct_primary_di_count,
        "distinct_ulid_count": distinct_ulid_count,
        "latest_version_true_count": latest_version_true_count,
        "row_count": row_count,
        "completeness_ratio": completeness_ratio,
        "min_ulid": min_u,
        "min_ulid_timestamp": decode_ulid(min_u),
        "max_ulid": max_u,
        "max_ulid_timestamp": decode_ulid(max_u),
        "previous_db_found": str(existing_db) if existing_db else None,
        "previous_rows": len(existing_rows),
        "received_rows": len(received_rows),
        "new_uuid_count": (audit or {}).get("new_uuid_count"),
        "uuid_rows_received": (audit or {}).get("uuid_rows_received"),
        "missing_uuid_rows": (audit or {}).get("missing_uuid_rows"),
        "duplicate_uuid_rows": (audit or {}).get("duplicate_uuid_rows"),
        "normal_completion": normal_completion,
        "initial_page": initial_meta,
        "audit": audit,
        "resume_state": output_resume_state,
        "phases": (audit or {}).get("phases"),
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
        "new_uuid_count": metadata.get("new_uuid_count"),
        "uuid_rows_received": metadata.get("uuid_rows_received"),
        "missing_uuid_rows": metadata.get("missing_uuid_rows"),
        "duplicate_uuid_rows": metadata.get("duplicate_uuid_rows"),
        "previous_rows": len(existing_rows),
        "merge": merge_stats,
        "phases": metadata.get("phases"),
        "resume_state": metadata.get("resume_state"),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
