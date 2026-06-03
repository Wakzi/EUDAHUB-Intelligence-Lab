#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EUDAMED Platform Raw acquisition pipeline.

Pipeline version: v4.7

Scope:
- Raw acquisition only.
- Current domain implemented: UDI.
- Future domain planned: Actors.
- No CDC, no canonical merge, no DK subset.

Important:
- Repo path expected by workflow:
  pipelines/eudamed/pipeline_eudamed_platform_raw.py

Release naming:
- Latest release: eudamed-platform-raw-latest
- Latest assets:
  eudamed_platform_raw.duckdb
  eudamed_platform_raw.csv.zip
  eudamed_platform_raw_state.json
  eudamed_platform_raw_stats.json
  release_notes.md

Backward compatibility:
- First run after rename can bootstrap from legacy eudamed-ui-lab-latest assets/state.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
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


PIPELINE_VERSION = "v4.7"
BASE_URL_DEFAULT = "https://ec.europa.eu/tools/eudamed/api"
UDI_ENDPOINT = "/devices/udiDiData"
CROCKFORD32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc_now() -> str:
    return utc_now().isoformat()


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


def release_timestamp(value: Optional[str]) -> str:
    if value and re.fullmatch(r"\d{8}_\d{6}", value):
        return value
    return utc_now().strftime("%Y%m%d_%H%M%S")


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
    vals = [v for v in values if v]
    return min(vals) if vals else None


def max_ulid(values: Iterable[Optional[str]]) -> Optional[str]:
    vals = [v for v in values if v]
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


def flatten_udi(row: Dict[str, Any], page_number: int, extract_ts: str) -> Dict[str, Any]:
    return {
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
        "extract_timestamp_utc": extract_ts,
    }


UDI_COLUMNS = list(flatten_udi({}, 0, "").keys())


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
        self._last_request_at = 0.0

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
        params = {
            "page": page,
            "pageSize": self.page_size,
            "size": self.page_size,
            "iso2Code": self.language,
            "languageIso2Code": self.language,
        }
        last_error = None
        for attempt in range(self.retries + 1):
            self._rate_limit()
            start = time.monotonic()
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                elapsed_ms = (time.monotonic() - start) * 1000
                retry_after = resp.headers.get("Retry-After")
                if resp.status_code == 200:
                    return page, resp.status_code, resp.json(), None, elapsed_ms, retry_after
                if resp.status_code in {429, 500, 502, 503, 504} and attempt < self.retries:
                    sleep_s = float(retry_after) if retry_after and retry_after.isdigit() else self.backoff * (attempt + 1)
                    sleep_s += random.random() * 0.25
                    last_error = f"HTTP {resp.status_code}; retry_after={retry_after}; sleep={sleep_s:.1f}s"
                    time.sleep(sleep_s)
                    continue
                return page, resp.status_code, None, f"HTTP {resp.status_code}: {resp.text[:500]}", elapsed_ms, retry_after
            except Exception as e:
                elapsed_ms = (time.monotonic() - start) * 1000
                last_error = repr(e)
                if attempt < self.retries:
                    time.sleep(self.backoff * (attempt + 1) + random.random() * 0.25)
                    continue
                return page, 0, None, last_error, elapsed_ms, None
        return page, 0, None, last_error or "unknown_error", 0.0, None

    def discover(self) -> Dict[str, Any]:
        page, status, data, error, elapsed, retry_after = self.fetch_page(0)
        content = data.get("content", []) if data else []
        return {
            "discovered_at_utc": iso_utc_now(),
            "requested_page_size": self.page_size,
            "response_page_size": data.get("size") if data else None,
            "total_elements": data.get("totalElements") if data else None,
            "total_pages": data.get("totalPages") if data else None,
            "first_page_number_of_elements": data.get("numberOfElements") if data else None,
            "first_page_content_length": len(content),
            "first_page_status_code": status,
            "first_page_ok": status == 200,
            "first_page_first_flag": data.get("first") if data else None,
            "first_page_last_flag": data.get("last") if data else None,
            "first_page_number": data.get("number") if data else None,
            "raw_metadata": json.dumps({k: v for k, v in (data or {}).items() if k != "content"}, ensure_ascii=False) if data else None,
            "first_page_error": error,
        }


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


def find_existing_db(inputs_dir: Path) -> Optional[Path]:
    candidates = [
        inputs_dir / "eudamed_platform_raw.duckdb",
        inputs_dir / "eudamed_platform_raw.duckdb.zip",
        inputs_dir / "eudamed_ui_lab.duckdb",
        inputs_dir / "eudamed_ui_lab.duckdb.zip",
        inputs_dir / "eudamed_ui_lab_v4_6.duckdb",
        inputs_dir / "eudamed_ui_lab_v4_6.duckdb.zip",
        inputs_dir / "eudamed_ui_lab_v4_5.duckdb",
        inputs_dir / "eudamed_ui_lab_v4_5.duckdb.zip",
        inputs_dir / "eudamed_ui_lab_v4_4.duckdb",
        inputs_dir / "eudamed_ui_lab_v4_4.duckdb.zip",
    ]
    for p in candidates:
        if p.exists() and p.suffix == ".duckdb":
            return p
        if p.exists() and p.suffix == ".zip":
            extracted = unzip_duckdb(p, inputs_dir)
            if extracted and extracted.exists():
                return extracted
    for p in sorted(inputs_dir.glob("*.duckdb")):
        return p
    for p in sorted(inputs_dir.glob("*.duckdb.zip")):
        extracted = unzip_duckdb(p, inputs_dir)
        if extracted:
            return extracted
    return None


def read_state_file(inputs_dir: Path) -> Optional[Dict[str, Any]]:
    candidates = [
        inputs_dir / "eudamed_platform_raw_state.json",
        inputs_dir / "ui_api_state_v4_6.json",
        inputs_dir / "ui_api_state_v4_5.json",
        inputs_dir / "ui_api_state_v4_4.json",
        inputs_dir / "ui_api_state.json",
    ]
    for p in candidates:
        if p.exists():
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                if data.get("max_ulid"):
                    log(f"State bootstrap found: {p}")
                    return data
            except Exception as e:
                log(f"WARNING could not read state {p}: {e}")
    return None


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]).fetchone()[0] > 0


def existing_udi_table(con: duckdb.DuckDBPyConnection) -> Optional[str]:
    for t in ["udi", "ui_devices_list_all"]:
        if table_exists(con, t):
            return t
    return None


def bootstrap_from_db(db_path: Path) -> Dict[str, Any]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        table = existing_udi_table(con)
        if not table:
            return {}
        cols = {r[1].lower(): r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
        ulid_col = cols.get("ulid") or cols.get("udi_di_data_ulid")
        if not ulid_col:
            return {}
        n, min_u, max_u = con.execute(f"SELECT COUNT(*), MIN({ulid_col}), MAX({ulid_col}) FROM {table} WHERE {ulid_col} IS NOT NULL").fetchone()
        return {
            "source": "db",
            "db_path": str(db_path),
            "table": table,
            "row_count": int(n or 0),
            "min_ulid": min_u,
            "min_ulid_timestamp": decode_ulid(min_u),
            "max_ulid": max_u,
            "max_ulid_timestamp": decode_ulid(max_u),
        }
    finally:
        con.close()


def read_existing_rows(db_path: Path) -> List[Dict[str, Any]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        table = existing_udi_table(con)
        if not table:
            return []
        df = con.execute(f"SELECT * FROM {table}").fetchdf()
        records = df.to_dict("records")
        out: List[Dict[str, Any]] = []
        if table == "udi":
            for r in records:
                d = {c: r.get(c) for c in UDI_COLUMNS}
                out.append(d)
            return out

        mapping = {
            "basic_udi": ["basicUdi", "basic_udi"],
            "primary_di": ["primaryDi", "primary_di"],
            "uuid": ["uuid"],
            "ulid": ["ulid"],
            "basic_udi_di_data_ulid": ["basicUdiDiDataUlid", "basic_udi_di_data_ulid"],
            "trade_name": ["tradeName", "trade_name"],
            "manufacturer_name": ["manufacturerName", "manufacturer_name"],
            "manufacturer_srn": ["manufacturerSrn", "manufacturer_srn"],
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
            d["ulid_timestamp"] = d.get("ulid_timestamp") or decode_ulid(d.get("ulid"))
            d["basic_udi_di_data_ulid_timestamp"] = d.get("basic_udi_di_data_ulid_timestamp") or decode_ulid(d.get("basic_udi_di_data_ulid"))
            d["basic_udi_data_ulid_timestamp"] = d.get("basic_udi_data_ulid_timestamp") or decode_ulid(d.get("basic_udi_data_ulid"))
            d["source_endpoint"] = d.get("source_endpoint") or "devices/udiDiData"
            out.append(d)
        return out
    finally:
        con.close()


def parse_page(data: Dict[str, Any], page: int, extract_ts: str) -> List[Dict[str, Any]]:
    return [flatten_udi(item, page, extract_ts) for item in data.get("content", [])]


def dedupe_by_uuid(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Dict[str, Dict[str, Any]] = {}
    no_uuid = []
    for r in rows:
        u = r.get("uuid")
        if not u:
            no_uuid.append(r)
            continue
        old = seen.get(u)
        if old is None or (r.get("ulid") or "") > (old.get("ulid") or ""):
            seen[u] = r
    return list(seen.values()) + no_uuid


def fetch_full(client: EudamedClient, total_pages: int, workers: int, extract_ts: str):
    all_rows, page_audit, request_log = [], [], []
    log(f"=== Full fetch pages 0..{total_pages - 1} ({total_pages} pages) ===")

    def task(page: int):
        c = EudamedClient(client.base_url, client.language, client.page_size, client.timeout, client.retries, client.backoff, client.max_rps)
        return c.fetch_page(page)

    done = 0
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(task, p): p for p in range(total_pages)}
        for fut in cf.as_completed(futures):
            page, status, data, error, elapsed_ms, retry_after = fut.result()
            done += 1
            rows = parse_page(data, page, extract_ts) if data else []
            all_rows.extend(rows)
            page_audit.append({
                "page": page, "status_code": status, "ok": status == 200,
                "rows_returned": len(rows), "api_number": data.get("number") if data else None,
                "api_first": data.get("first") if data else None, "api_last": data.get("last") if data else None,
                "api_total_elements": data.get("totalElements") if data else None,
                "api_total_pages": data.get("totalPages") if data else None,
                "elapsed_ms": elapsed_ms, "error": error,
            })
            request_log.append({
                "endpoint": "devices_udiDiData_list", "page": page, "status_code": status,
                "elapsed_ms": elapsed_ms, "retry_after": retry_after, "error": error,
                "requested_at_utc": iso_utc_now(), "probe": False,
            })
            if done % 50 == 0 or done == total_pages:
                log(f"Pages completed {done}/{total_pages} | rows={len(all_rows):,}")
    page_audit.sort(key=lambda r: r["page"])
    request_log.sort(key=lambda r: r["page"])
    return all_rows, page_audit, request_log


def fetch_incremental(client: EudamedClient, existing_rows: List[Dict[str, Any]], known_max_ulid: str, api_total_elements: int, total_pages: int, extract_ts: str, known_pages_to_stop: int, extra_pages_after_match: int, mismatch_probe_head_pages: int, mismatch_probe_tail_pages: int):
    existing_uuid = {r.get("uuid") for r in existing_rows if r.get("uuid")}
    new_rows, page_audit, request_log = [], [], []
    consecutive_known_only, frontier_reached, known_or_mixed_pages_seen = 0, False, 0
    stop_reason = None
    log(f"=== Incremental from known max_ulid={known_max_ulid} ({decode_ulid(known_max_ulid)}) ===")

    def process_page(page: int, probe: bool = False):
        nonlocal new_rows
        _, status, data, error, elapsed_ms, retry_after = client.fetch_page(page)
        rows = parse_page(data, page, extract_ts) if data else []
        new_candidates = [r for r in rows if (r.get("ulid") or "") > known_max_ulid]
        added = 0
        if not probe:
            for r in new_candidates:
                u = r.get("uuid")
                if u and u not in existing_uuid:
                    existing_uuid.add(u)
                    new_rows.append(r)
                    added += 1
        page_audit.append({
            "page": page, "status_code": status, "ok": status == 200, "rows_returned": len(rows),
            "new_candidates": len(new_candidates), "new_added": added,
            "known_or_old": len(rows) - len(new_candidates),
            "api_number": data.get("number") if data else None,
            "api_first": data.get("first") if data else None,
            "api_last": data.get("last") if data else None,
            "api_total_elements": data.get("totalElements") if data else None,
            "api_total_pages": data.get("totalPages") if data else None,
            "elapsed_ms": elapsed_ms, "error": error, "probe": probe,
        })
        request_log.append({
            "endpoint": "devices_udiDiData_list", "page": page, "status_code": status,
            "elapsed_ms": elapsed_ms, "retry_after": retry_after, "error": error,
            "requested_at_utc": iso_utc_now(), "probe": probe,
        })
        return len(rows), len(new_candidates), added

    page = 0
    while page < total_pages:
        rows_n, new_n, added = process_page(page, probe=False)
        known_n = rows_n - new_n
        log(f"Incremental page={page} rows={rows_n} new={new_n} added={added} known_or_old={known_n}")
        if rows_n == 0:
            stop_reason = "empty_page"
            break
        if known_n > 0:
            frontier_reached = True
            known_or_mixed_pages_seen += 1
        consecutive_known_only = consecutive_known_only + 1 if new_n == 0 else 0
        if frontier_reached and consecutive_known_only >= known_pages_to_stop + extra_pages_after_match:
            stop_reason = "frontier_reached_extra_known_pages_exhausted"
            break
        page += 1
    if stop_reason is None:
        stop_reason = "max_pages_guard_reached"

    diff = len(existing_rows) + len(new_rows) - api_total_elements
    mismatch_probe = {
        "executed": False, "head_probe_pages_requested": mismatch_probe_head_pages,
        "tail_probe_pages_requested": mismatch_probe_tail_pages, "head_probe_pages_fetched": 0,
        "tail_probe_pages_fetched": 0, "head_probe_new_rows": 0, "tail_probe_new_rows": 0,
        "head_probe_new_rows_added": 0, "tail_probe_new_rows_added": 0,
        "tail_first_page": None, "tail_last_page": None, "tail_last_page_rows": None,
        "difference_before_probe": diff, "difference_after_probe": diff, "api_drift_suspected": False,
    }

    if diff != 0:
        mismatch_probe["executed"] = True
        log(f"WARNING mismatch before probes old+new={len(existing_rows)+len(new_rows):,} api_total={api_total_elements:,} diff={diff}")

        for p in range(0, min(mismatch_probe_head_pages, total_pages)):
            before = len(new_rows)
            rows_n, new_n, added = process_page(p, probe=False)
            mismatch_probe["head_probe_pages_fetched"] += 1
            mismatch_probe["head_probe_new_rows"] += new_n
            mismatch_probe["head_probe_new_rows_added"] += len(new_rows) - before

        tail_start = max(0, total_pages - mismatch_probe_tail_pages)
        mismatch_probe["tail_first_page"] = tail_start
        mismatch_probe["tail_last_page"] = total_pages - 1
        for p in range(tail_start, total_pages):
            before = len(new_rows)
            rows_n, new_n, added = process_page(p, probe=False)
            mismatch_probe["tail_probe_pages_fetched"] += 1
            mismatch_probe["tail_probe_new_rows"] += new_n
            mismatch_probe["tail_probe_new_rows_added"] += len(new_rows) - before
            if p == total_pages - 1:
                mismatch_probe["tail_last_page_rows"] = rows_n

        diff = len(existing_rows) + len(new_rows) - api_total_elements
        mismatch_probe["difference_after_probe"] = diff
        mismatch_probe["api_drift_suspected"] = diff != 0 and abs(diff) < client.page_size

    audit = {
        "mode": "incremental",
        "known_max_ulid_before": known_max_ulid,
        "known_max_ulid_before_timestamp": decode_ulid(known_max_ulid),
        "old_rows": len(existing_rows),
        "api_total_elements": api_total_elements,
        "pages_fetched": page + 1,
        "new_rows_appended": len(new_rows),
        "frontier_reached": frontier_reached,
        "known_or_mixed_pages_seen": known_or_mixed_pages_seen,
        "consecutive_known_only_pages_at_stop": consecutive_known_only,
        "known_pages_to_stop": known_pages_to_stop,
        "extra_pages_after_match": extra_pages_after_match,
        "mismatch_probe_head_pages": mismatch_probe_head_pages,
        "mismatch_probe_tail_pages": mismatch_probe_tail_pages,
        "stop_reason": stop_reason,
        "mismatch_probe": mismatch_probe,
        "expected_after_append": len(existing_rows) + len(new_rows),
        "expected_after_append_minus_api_total": len(existing_rows) + len(new_rows) - api_total_elements,
    }
    return dedupe_by_uuid(existing_rows + new_rows), page_audit, request_log, audit


def write_duckdb(out_db: Path, rows: List[Dict[str, Any]], discovery: Dict[str, Any], page_audit: List[Dict[str, Any]], request_log: List[Dict[str, Any]], stats: Dict[str, Any]) -> None:
    safe_unlink(out_db)
    con = duckdb.connect(str(out_db))
    try:
        con.register("udi_df", pd.DataFrame(rows, columns=UDI_COLUMNS))
        con.execute("CREATE TABLE udi AS SELECT * FROM udi_df")
        con.register("discovery_df", pd.DataFrame([discovery]))
        con.execute("CREATE TABLE discovery AS SELECT * FROM discovery_df")
        con.register("page_audit_df", pd.DataFrame(page_audit))
        con.execute("CREATE TABLE page_audit AS SELECT * FROM page_audit_df")
        con.register("api_request_log_df", pd.DataFrame(request_log))
        con.execute("CREATE TABLE api_request_log AS SELECT * FROM api_request_log_df")

        field_rows = []
        for col in UDI_COLUMNS:
            field_rows.append({"table_name": "udi", "field_name": col, "non_null_rows": sum(1 for r in rows if r.get(col) is not None), "total_rows": len(rows)})
        con.register("field_inventory_df", pd.DataFrame(field_rows))
        con.execute("CREATE TABLE field_inventory AS SELECT * FROM field_inventory_df")

        con.register("pipeline_stats_df", pd.DataFrame([{
            "pipeline_version": stats.get("pipeline_version"),
            "mode": stats.get("mode"),
            "generated_at_utc": stats.get("generated_at_utc"),
            "row_count": len(rows),
            "api_total_elements": stats.get("expected", {}).get("total_elements"),
            "difference": len(rows) - (stats.get("expected", {}).get("total_elements") or 0),
        }]))
        con.execute("CREATE TABLE pipeline_stats AS SELECT * FROM pipeline_stats_df")
    finally:
        con.close()


def write_csv_zip(zip_path: Path, rows: List[Dict[str, Any]]) -> None:
    safe_unlink(zip_path)
    tmp = zip_path.parent / "_csv_tmp"
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


def write_release_notes(path: Path, stats: Dict[str, Any], state: Dict[str, Any]) -> None:
    inc = stats.get("incremental_audit") or {}
    exp = stats.get("expected", {})
    audit = stats.get("page_fetch_audit", {})
    lines = [
        "# EUDAMED Platform Raw",
        "",
        f"Pipeline version: `{stats.get('pipeline_version')}`",
        f"Mode: `{stats.get('mode')}`",
        f"Generated at UTC: `{stats.get('generated_at_utc')}`",
        "",
        "## Scope",
        "",
        "Raw acquisition only. Current domain: UDI.",
        "",
        "## Validation",
        "",
        f"- API total elements: `{exp.get('total_elements')}`",
        f"- API total pages: `{exp.get('total_pages')}`",
        f"- First page: `{exp.get('first_page')}`",
        f"- Last page: `{exp.get('last_page')}`",
        f"- Rows in DB: `{state.get('row_count')}`",
        f"- Difference: `{audit.get('received_rows_difference')}`",
        f"- Completeness ratio: `{state.get('completeness_ratio')}`",
        "",
        "## ULID range",
        "",
        f"- Min ULID: `{state.get('min_ulid')}` → `{state.get('min_ulid_timestamp')}`",
        f"- Max ULID: `{state.get('max_ulid')}` → `{state.get('max_ulid_timestamp')}`",
    ]
    if inc:
        lines.extend([
            "",
            "## Incremental audit",
            "",
            f"- Known max ULID before: `{inc.get('known_max_ulid_before')}` → `{inc.get('known_max_ulid_before_timestamp')}`",
            f"- Old rows: `{inc.get('old_rows')}`",
            f"- New rows appended: `{inc.get('new_rows_appended')}`",
            f"- Frontier reached: `{inc.get('frontier_reached')}`",
            f"- Stop reason: `{inc.get('stop_reason')}`",
            f"- Expected after append minus API total: `{inc.get('expected_after_append_minus_api_total')}`",
        ])
        mp = inc.get("mismatch_probe") or {}
        if mp:
            lines.extend([
                "",
                "## Mismatch probes",
                "",
                f"- Executed: `{mp.get('executed')}`",
                f"- Head probe pages fetched: `{mp.get('head_probe_pages_fetched')}`",
                f"- Head probe new rows added: `{mp.get('head_probe_new_rows_added')}`",
                f"- Tail probe pages fetched: `{mp.get('tail_probe_pages_fetched')}`",
                f"- Tail probe new rows added: `{mp.get('tail_probe_new_rows_added')}`",
                f"- Tail last page rows: `{mp.get('tail_last_page_rows')}`",
                f"- Difference before probe: `{mp.get('difference_before_probe')}`",
                f"- Difference after probe: `{mp.get('difference_after_probe')}`",
                f"- API drift suspected: `{mp.get('api_drift_suspected')}`",
            ])
    lines.extend([
        "",
        "## Notes",
        "",
        "- CDC, canonical merge and DK subset are intentionally out of scope.",
        "- Latest release is recreated on every successful run.",
    ])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def copy_file(src: Path, dst: Path) -> None:
    safe_unlink(dst)
    shutil.copy2(src, dst)


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    p.add_argument("--out-dir", default="dist/eudamed_platform_raw")
    p.add_argument("--inputs-dir", default="inputs")
    p.add_argument("--base-url", default=BASE_URL_DEFAULT)
    p.add_argument("--language", default="en")
    p.add_argument("--page-size", type=int, default=300)
    p.add_argument("--max-pages", type=int, default=0)
    p.add_argument("--workers", type=int, default=12)
    p.add_argument("--max-rps", type=float, default=3.0)
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--retries", type=int, default=4)
    p.add_argument("--backoff", type=float, default=1.5)
    p.add_argument("--known-pages-to-stop", type=int, default=5)
    p.add_argument("--extra-pages-after-match", type=int, default=10)
    p.add_argument("--mismatch-probe-head-pages", type=int, default=50)
    p.add_argument("--mismatch-probe-tail-pages", type=int, default=50)
    p.add_argument("--release-timestamp", default=None)
    p.add_argument("--skip-csv-zip", action="store_true")
    args = p.parse_args(argv)

    out_dir = Path(args.out_dir)
    inputs_dir = Path(args.inputs_dir)
    ensure_dir(out_dir)
    ensure_dir(inputs_dir)
    rel_ts = release_timestamp(args.release_timestamp)
    extract_ts = iso_utc_now()

    log(f"=== EUDAMED Platform Raw {PIPELINE_VERSION} acquisition started ===")
    log(f"mode={args.mode} out_dir={out_dir} page_size={args.page_size}")
    log("scope=raw_fetch_only cdc=0 canonical=0 dk_subset=0 domain=udi")

    client = EudamedClient(args.base_url, args.language, args.page_size, args.timeout, args.retries, args.backoff, args.max_rps)
    discovery = client.discover()
    log("=== Discovery complete ===")
    log(json.dumps(discovery, ensure_ascii=False))
    if not discovery.get("first_page_ok"):
        log("ERROR discovery failed")
        return 2

    total_elements = int(discovery.get("total_elements") or 0)
    total_pages = int(discovery.get("total_pages") or 0)
    if args.max_pages and args.max_pages > 0:
        total_pages = min(total_pages, args.max_pages)

    previous_state = read_state_file(inputs_dir)
    existing_db = find_existing_db(inputs_dir)
    existing_rows: List[Dict[str, Any]] = []
    bootstrap = {}
    if existing_db:
        log(f"Previous raw DB found for bootstrap: {existing_db}")
        bootstrap = bootstrap_from_db(existing_db)
        log(json.dumps(bootstrap, ensure_ascii=False))
        if args.mode == "incremental":
            existing_rows = read_existing_rows(existing_db)
            log(f"Loaded existing rows: {len(existing_rows):,}")

    known_max = None
    if previous_state and previous_state.get("max_ulid"):
        known_max = previous_state.get("max_ulid")
        log(f"Using max_ulid from state: {known_max} ({decode_ulid(known_max)})")
    elif bootstrap.get("max_ulid"):
        known_max = bootstrap.get("max_ulid")
        log(f"Using max_ulid from DB: {known_max} ({decode_ulid(known_max)})")

    if args.mode == "incremental" and not known_max:
        log("WARNING no max_ulid found; fallback to full mode")
        args.mode = "full"

    if args.mode == "full":
        rows, page_audit, request_log = fetch_full(client, total_pages, args.workers, extract_ts)
        incremental_audit = None
    else:
        rows, page_audit, request_log, incremental_audit = fetch_incremental(
            client, existing_rows, known_max, total_elements, total_pages, extract_ts,
            args.known_pages_to_stop, args.extra_pages_after_match,
            args.mismatch_probe_head_pages, args.mismatch_probe_tail_pages,
        )

    rows = dedupe_by_uuid(rows)
    row_count = len(rows)
    uuids = [r.get("uuid") for r in rows if r.get("uuid")]
    unique_uuid_count = len(set(uuids))
    duplicate_uuid_rows = len(uuids) - unique_uuid_count
    min_u = min_ulid(r.get("ulid") for r in rows)
    max_u = max_ulid(r.get("ulid") for r in rows)
    fetched_pages = sorted({r["page"] for r in page_audit if r.get("ok")})
    missing_pages = [] if args.mode == "incremental" else sorted(set(range(total_pages)) - set(fetched_pages))

    page_fetch_audit = {
        "requested_pages": len(page_audit),
        "successful_pages": len(fetched_pages),
        "failed_pages": sum(1 for r in page_audit if not r.get("ok")),
        "missing_pages_count": len(missing_pages),
        "missing_pages_sample": missing_pages[:100],
        "received_rows": row_count,
        "unique_device_uuid": unique_uuid_count,
        "duplicate_device_uuid_rows": duplicate_uuid_rows,
        "expected_rows_for_scope": total_elements,
        "received_rows_difference": row_count - total_elements,
        "received_equals_expected_for_scope": row_count == total_elements,
        "successful_pages_equals_requested_pages": len(fetched_pages) == len(page_audit),
    }

    state = {
        "pipeline_version": PIPELINE_VERSION,
        "release_timestamp": rel_ts,
        "crawl_mode": args.mode,
        "completed_at_utc": iso_utc_now(),
        "max_ulid": max_u,
        "max_ulid_timestamp": decode_ulid(max_u),
        "min_ulid": min_u,
        "min_ulid_timestamp": decode_ulid(min_u),
        "row_count": row_count,
        "api_total_elements": total_elements,
        "api_total_pages": total_pages,
        "page_size": args.page_size,
        "first_page": 0,
        "last_page": total_pages - 1,
        "first_page_first_flag": discovery.get("first_page_first_flag"),
        "first_page_last_flag": discovery.get("first_page_last_flag"),
        "first_page_number": discovery.get("first_page_number"),
        "completeness_ratio": (row_count / total_elements) if total_elements else None,
        "bootstrap": {
            "state_found": bool(previous_state),
            "db_found": str(existing_db) if existing_db else None,
            "legacy_compatible": True,
            "bootstrap_max_ulid": known_max,
            "bootstrap_max_ulid_timestamp": decode_ulid(known_max),
        },
        "audit": incremental_audit,
    }

    status_summary = {}
    for r in request_log:
        key = (r.get("endpoint"), int(r.get("status_code") or 0))
        status_summary[key] = status_summary.get(key, 0) + 1

    stats = {
        "generated_at_utc": iso_utc_now(),
        "pipeline_version": PIPELINE_VERSION,
        "release_timestamp": rel_ts,
        "mode": args.mode,
        "base_url": args.base_url,
        "language": args.language,
        "page_size_requested": args.page_size,
        "page_size_response": discovery.get("response_page_size"),
        "max_pages": args.max_pages,
        "workers": args.workers,
        "max_rps": args.max_rps,
        "retries": args.retries,
        "backoff": args.backoff,
        "csv_zip_skipped": bool(args.skip_csv_zip),
        "expected": {"total_elements": total_elements, "total_pages": total_pages, "first_page": 0, "last_page": total_pages - 1},
        "page_fetch_audit": page_fetch_audit,
        "incremental_audit": incremental_audit,
        "state": state,
        "rows": {"discovery": 1, "page_audit": len(page_audit), "udi": row_count, "actors": 0, "api_request_log": len(request_log), "field_inventory": len(UDI_COLUMNS)},
        "columns": {"udi": len(UDI_COLUMNS)},
        "request_status_summary": [{"endpoint": k[0], "status_code": k[1], "count": v} for k, v in sorted(status_summary.items())],
    }

    if row_count != total_elements:
        log("WARNING completeness mismatch. Warning-only; release continues.")
    log(f"Validation summary | api_total={total_elements:,} db_rows={row_count:,} diff={row_count-total_elements}")

    latest_db = out_dir / "eudamed_platform_raw.duckdb"
    latest_csv = out_dir / "eudamed_platform_raw.csv.zip"
    latest_state = out_dir / "eudamed_platform_raw_state.json"
    latest_stats = out_dir / "eudamed_platform_raw_stats.json"
    latest_notes = out_dir / "release_notes.md"

    log(f"Writing DuckDB: {latest_db}")
    t0 = time.monotonic()
    write_duckdb(latest_db, rows, discovery, page_audit, request_log, stats)
    log(f"DuckDB finished in {time.monotonic()-t0:.0f}s | size={file_size(latest_db)}")

    if not args.skip_csv_zip:
        log(f"Writing CSV ZIP: {latest_csv}")
        t0 = time.monotonic()
        write_csv_zip(latest_csv, rows)
        log(f"CSV ZIP finished in {time.monotonic()-t0:.0f}s | size={file_size(latest_csv)}")

    write_json(latest_state, state)
    write_json(latest_stats, stats)
    write_release_notes(latest_notes, stats, state)

    dated_db = out_dir / f"eudamed_platform_raw_{rel_ts}.duckdb"
    dated_csv = out_dir / f"eudamed_platform_raw_{rel_ts}.csv.zip"
    dated_state = out_dir / f"eudamed_platform_raw_state_{rel_ts}.json"
    dated_stats = out_dir / f"eudamed_platform_raw_stats_{rel_ts}.json"
    dated_notes = out_dir / f"release_notes_{rel_ts}.md"

    copy_file(latest_db, dated_db)
    if latest_csv.exists():
        copy_file(latest_csv, dated_csv)
    copy_file(latest_state, dated_state)
    copy_file(latest_stats, dated_stats)
    copy_file(latest_notes, dated_notes)

    stats["output_files"] = [p.name for p in [latest_db, latest_csv, latest_state, latest_stats, latest_notes, dated_db, dated_csv, dated_state, dated_stats, dated_notes] if p.exists()]
    stats["duckdb_export"] = str(latest_db)
    stats["csv_zip"] = str(latest_csv) if latest_csv.exists() else None
    stats["output_file_sizes"] = {"duckdb": file_size(latest_db), "csv_zip": file_size(latest_csv) if latest_csv.exists() else "n/a", "stats_json": file_size(latest_stats)}
    write_json(latest_stats, stats)
    copy_file(latest_stats, dated_stats)

    log("=== Final stats ===")
    log(json.dumps(stats, ensure_ascii=False))
    log(f"=== Done {PIPELINE_VERSION} ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
