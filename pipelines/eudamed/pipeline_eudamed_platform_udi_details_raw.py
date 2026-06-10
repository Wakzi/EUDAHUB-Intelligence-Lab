#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EUDAMED Platform UDI Details Raw acquisition pipeline.

Pipeline version: v1.0.1

Scope
-----
- Raw acquisition only.
- Domain: UDI-DI detail endpoint.
- Input master list: EUDAMED Platform UDI Raw latest DuckDB.
- No Basic UDI details in this pipeline.
- No CDC, no canonical merge, no DK subset.

Design
------
- Detail endpoint: /devices/udiDiData/{uuid}
- udi_raw is the authority for which device UUID/version records exist.
- udi_details_raw stores one raw detail response per UDI UUID.
- Queue based processing is used because UDI scale can be ~2.7M UUID calls.
- Priority 1 = current/latest UDI UUIDs.
- Priority 2 = historical/non-latest UDI UUIDs.
- Full modes refresh oldest EXTRACT_DATETIME_UTC first rather than starting from one end.

Release naming
--------------
Latest tag: eudamed-platform-udi-details-raw-latest
Latest assets:
  eudamed_platform_udi_details_raw_latest.duckdb
  eudamed_platform_udi_details_raw_latest_csv.zip
  eudamed_platform_udi_details_raw_latest.metadata.json
  RELEASE_NOTES_EUDAMED_PLATFORM_UDI_DETAILS_RAW.md

Dated tag: eudamed-platform-udi-details-raw-YYYYMMDD_HHMMSS
Dated assets:
  eudamed_platform_udi_details_raw_YYYYMMDD_HHMMSS.duckdb
  eudamed_platform_udi_details_raw_YYYYMMDD_HHMMSS_csv.zip
  eudamed_platform_udi_details_raw_YYYYMMDD_HHMMSS.metadata.json
  RELEASE_NOTES_EUDAMED_PLATFORM_UDI_DETAILS_RAW_YYYYMMDD_HHMMSS.md
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
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

PIPELINE_VERSION = "v1.0.1"
BASE_URL_DEFAULT = "https://ec.europa.eu/tools/eudamed/api"
UDI_DETAILS_ENDPOINT_TEMPLATE = "/devices/udiDiData/{uuid}"
CROCKFORD32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

RUN_BOOTSTRAP = "BOOTSTRAP"
RUN_COMPLETE = "COMPLETE"
RUN_PARTIAL = "PARTIAL"
RUN_FAILED = "FAILED"

DETAIL_MODES = {
    "incremental", "resume_incremental",
    "incremental_current", "resume_incremental_current",
    "incremental_historical", "resume_incremental_historical",
    "full_current", "resume_full_current",
    "full_historical", "resume_full_historical",
    "full", "resume_full",
}

QUEUE_PENDING = "pending"
QUEUE_DONE = "done"
QUEUE_FAILED = "failed"


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


def release_timestamp(value: Optional[str]) -> str:
    if value and re.fullmatch(r"\d{8}_\d{6}", value):
        return value
    return utc_now().strftime("%Y%m%d_%H%M%S")


def release_title_time(ts: str) -> str:
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
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def clean_values(values: Iterable[Optional[str]]) -> List[str]:
    vals: List[str] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if not s or s.lower() in {"nan", "none", "null", "nat"}:
            continue
        vals.append(s)
    return vals


def min_value(values: Iterable[Optional[str]]) -> Optional[str]:
    vals = clean_values(values)
    return min(vals) if vals else None


def max_value(values: Iterable[Optional[str]]) -> Optional[str]:
    vals = clean_values(values)
    return max(vals) if vals else None


def jdump(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def code_of(obj: Any) -> Optional[str]:
    if isinstance(obj, dict):
        return obj.get("code") or obj.get("value") or obj.get("id")
    if obj is None:
        return None
    return str(obj)


def text_value(obj: Any) -> Optional[str]:
    if isinstance(obj, dict):
        if obj.get("textByDefaultLanguage"):
            return obj.get("textByDefaultLanguage")
        texts = obj.get("texts")
        if isinstance(texts, list) and texts:
            # Prefer English, otherwise first text.
            for t in texts:
                lang = t.get("language") if isinstance(t, dict) else None
                if isinstance(lang, dict) and lang.get("isoCode") == "en" and t.get("text"):
                    return t.get("text")
            first = texts[0]
            if isinstance(first, dict):
                return first.get("text")
    if obj is None:
        return None
    return str(obj)


def country_fields(obj: Any) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[bool]]:
    if not isinstance(obj, dict):
        return None, None, None, None
    return obj.get("iso2Code"), obj.get("name"), obj.get("type"), obj.get("nonEUMemberState")


def flatten_detail(response: Dict[str, Any], requested_uuid: str, extract_date: str, extract_ts: str) -> Dict[str, Any]:
    primary_di = response.get("primaryDi") or {}
    issuing_agency = primary_di.get("issuingAgency") if isinstance(primary_di, dict) else {}
    market = response.get("marketInfoLink") or {}
    ms = market.get("msWhereAvailable") if isinstance(market, dict) else None
    placed = response.get("placedOnTheMarket") or {}
    device_status = response.get("deviceStatus") or {}
    device_status_type = device_status.get("type") if isinstance(device_status, dict) else {}
    version_state = response.get("versionState") or {}
    udi_pi_type = response.get("udiPiType") or {}
    cnd = response.get("cndNomenclatures")
    primary_cnd = cnd[0] if isinstance(cnd, list) and cnd and isinstance(cnd[0], dict) else {}
    primary_cnd_status = primary_cnd.get("status") if isinstance(primary_cnd, dict) else {}
    primary_cnd_version_state = primary_cnd.get("versionState") if isinstance(primary_cnd, dict) else {}
    pi, pn, pt, pneu = country_fields(placed)
    return {
        "EXTRACT_DATE": extract_date,
        "EXTRACT_DATETIME_UTC": extract_ts,
        "requested_uuid": requested_uuid,
        "uuid": response.get("uuid"),
        "ulid": response.get("ulid"),
        "ulid_datetime_utc": decode_ulid(response.get("ulid")),
        "primary_di_uuid": primary_di.get("uuid") if isinstance(primary_di, dict) else None,
        "primary_di_code": primary_di.get("code") if isinstance(primary_di, dict) else None,
        "primary_di_issuing_agency_code": code_of(issuing_agency),
        "primary_di_type": primary_di.get("type") if isinstance(primary_di, dict) else None,
        "contained_item_json": jdump(response.get("containedItem")),
        "secondary_di_json": jdump(response.get("secondaryDi")),
        "secondary_di_applicable": response.get("secondaryDiApplicable"),
        "reference": response.get("reference"),
        "trade_name": text_value(response.get("tradeName")),
        "trade_name_json": jdump(response.get("tradeName")),
        "trade_name_applicable": response.get("tradeNameApplicable"),
        "additional_description": text_value(response.get("additionalDescription")),
        "additional_description_json": jdump(response.get("additionalDescription")),
        "placed_on_market_iso2_code": pi,
        "placed_on_market_name": pn,
        "placed_on_market_type": pt,
        "placed_on_market_non_eu_member_state": pneu,
        "device_status_uuid": device_status.get("uuid") if isinstance(device_status, dict) else None,
        "device_status_code": code_of(device_status_type),
        "device_status_date": device_status.get("statusDate") if isinstance(device_status, dict) else None,
        "latest_version": response.get("latestVersion"),
        "version_number": response.get("versionNumber"),
        "version_date": response.get("versionDate"),
        "last_updated": response.get("lastUpdated"),
        "version_state_code": code_of(version_state),
        "discarded_date": response.get("discardedDate"),
        "new_device": response.get("newDevice"),
        "annex_xvi_applicable": response.get("annexXVIApplicable"),
        "cmr_substance": response.get("cmrSubstance"),
        "endocrine_disruptor": response.get("endocrineDisruptor"),
        "latex": response.get("latex"),
        "oem_applicable": response.get("oemApplicable"),
        "reprocessed": response.get("reprocessed"),
        "single_use": response.get("singleUse"),
        "sterile": response.get("sterile"),
        "sterilization": response.get("sterilization"),
        "direct_marking": response.get("directMarking"),
        "direct_marking_same_as_udi_di": response.get("directMarkingSameAsUdiDi"),
        "base_quantity": response.get("baseQuantity"),
        "unit_of_use": response.get("unitOfUse"),
        "clinical_size_applicable": response.get("clinicalSizeApplicable"),
        "critical_warnings_applicable": response.get("criticalWarningsApplicable"),
        "storage_applicable": response.get("storageApplicable"),
        "max_number_of_reuses": response.get("maxNumberOfReuses"),
        "max_number_of_reuses_applicable": response.get("maxNumberOfReusesApplicable"),
        "additional_information_url": response.get("additionalInformationUrl"),
        "udi_pi_batch_number": udi_pi_type.get("batchNumber") if isinstance(udi_pi_type, dict) else None,
        "udi_pi_serialization_number": udi_pi_type.get("serializationNumber") if isinstance(udi_pi_type, dict) else None,
        "udi_pi_manufacturing_date": udi_pi_type.get("manufacturingDate") if isinstance(udi_pi_type, dict) else None,
        "udi_pi_expiration_date": udi_pi_type.get("expirationDate") if isinstance(udi_pi_type, dict) else None,
        "udi_pi_software_identification": udi_pi_type.get("softwareIdentification") if isinstance(udi_pi_type, dict) else None,
        "market_info_link_uuid": market.get("uuid") if isinstance(market, dict) else None,
        "market_info_link_ulid": market.get("ulid") if isinstance(market, dict) else None,
        "market_info_link_ulid_datetime_utc": decode_ulid(market.get("ulid") if isinstance(market, dict) else None),
        "market_info_link_latest_version": market.get("latestVersion") if isinstance(market, dict) else None,
        "market_info_link_version_number": market.get("versionNumber") if isinstance(market, dict) else None,
        "market_info_link_version_date": market.get("versionDate") if isinstance(market, dict) else None,
        "market_country_count": len(ms) if isinstance(ms, list) else (0 if ms is None else None),
        "market_countries": "|".join([str(((x.get("country") or {}).get("iso2Code"))) for x in ms if isinstance(x, dict) and isinstance(x.get("country"), dict) and (x.get("country") or {}).get("iso2Code")]) if isinstance(ms, list) else None,
        "primary_cnd_uuid": primary_cnd.get("uuid") if isinstance(primary_cnd, dict) else None,
        "primary_cnd_code": primary_cnd.get("code") if isinstance(primary_cnd, dict) else None,
        "primary_cnd_description": text_value(primary_cnd.get("description")) if isinstance(primary_cnd, dict) else None,
        "primary_cnd_ulid": primary_cnd.get("ulid") if isinstance(primary_cnd, dict) else None,
        "primary_cnd_ulid_datetime_utc": decode_ulid(primary_cnd.get("ulid") if isinstance(primary_cnd, dict) else None),
        "primary_cnd_status_code": code_of(primary_cnd_status),
        "primary_cnd_version_state_code": code_of(primary_cnd_version_state),
        "primary_cnd_version_number": primary_cnd.get("versionNumber") if isinstance(primary_cnd, dict) else None,
        "primary_cnd_latest_version": primary_cnd.get("latestVersion") if isinstance(primary_cnd, dict) else None,
        "primary_di_json": jdump(primary_di),
        "market_info_link_json": jdump(market),
        "ms_where_available_json": jdump(ms),
        "udi_pi_type_json": jdump(udi_pi_type),
        "cnd_nomenclatures_json": jdump(cnd),
        "cmr_substances_json": jdump(response.get("cmrSubstances")),
        "component_dis_json": jdump(response.get("componentDis")),
        "clinical_sizes_json": jdump(response.get("clinicalSizes")),
        "critical_warnings_json": jdump(response.get("criticalWarnings")),
        "storage_handling_conditions_json": jdump(response.get("storageHandlingConditions")),
        "sub_statuses_json": jdump(response.get("subStatuses")),
        "linked_udi_di_view_json": jdump(response.get("linkedUdiDiView")),
        "product_designer_json": jdump(response.get("productDesigner")),
        "raw_json": jdump(response),
        "source_endpoint": "devices/udiDiData",
    }


DETAIL_COLUMNS = list(flatten_detail({}, "", "", "").keys())
CSV_EXCLUDED_COLUMNS = {
    "raw_json", "primary_di_json", "market_info_link_json", "ms_where_available_json",
    "udi_pi_type_json", "cnd_nomenclatures_json", "cmr_substances_json", "component_dis_json",
    "clinical_sizes_json", "critical_warnings_json", "storage_handling_conditions_json",
    "sub_statuses_json", "linked_udi_di_view_json", "product_designer_json", "contained_item_json",
    "secondary_di_json", "trade_name_json", "additional_description_json",
}
CSV_COLUMNS = [c for c in DETAIL_COLUMNS if c not in CSV_EXCLUDED_COLUMNS]

QUEUE_COLUMNS = [
    "candidate_uuid", "primary_di_code", "primary_di_uuid", "ulid", "ulid_datetime_utc",
    "latest_version", "version_number", "queue_source", "priority", "status", "attempt_count",
    "last_status_code", "last_error", "queued_at_utc", "last_attempt_at_utc", "completed_at_utc",
    "last_detail_extract_datetime_utc",
]


class EudamedClient:
    def __init__(self, base_url: str, language: str, timeout: int, retries: int, backoff: float, max_rps: float):
        self.base_url = base_url.rstrip("/")
        self.language = language
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.max_rps = max_rps
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": f"EUDAHUB-Intelligence Platform UDI Details Raw {PIPELINE_VERSION}",
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

    def fetch_udi_detail(self, uuid: str) -> Tuple[int, Optional[Dict[str, Any]], Optional[str], float, Optional[str], str]:
        url = f"{self.base_url}{UDI_DETAILS_ENDPOINT_TEMPLATE.format(uuid=uuid)}"
        params = {"languageIso2Code": self.language}
        last_error = None
        last_url = url
        for attempt in range(self.retries + 1):
            self._rate_limit()
            start = time.monotonic()
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                elapsed_ms = (time.monotonic() - start) * 1000
                retry_after = resp.headers.get("Retry-After")
                text = resp.text or ""
                last_url = resp.request.url
                if resp.status_code == 200:
                    return resp.status_code, resp.json(), None, elapsed_ms, retry_after, last_url
                if "Web Filter" in text or "Access Denied" in text or "security reason" in text:
                    return resp.status_code, None, f"WEB_FILTER_ACCESS_DENIED: HTTP {resp.status_code}: {text[:500]}", elapsed_ms, retry_after, last_url
                if resp.status_code == 429 and attempt < self.retries:
                    sleep_s = float(retry_after) if retry_after and retry_after.isdigit() else min(300.0, 30.0 * (attempt + 1))
                    sleep_s += random.random() * 0.5
                    last_error = f"HTTP 429 uuid={uuid}; retry_after={retry_after}; sleep={sleep_s:.1f}s; elapsed_ms={elapsed_ms:.0f}"
                    self.throttle_events.append({"uuid": uuid, "attempt": attempt + 1, "status_code": resp.status_code, "retry_after": retry_after, "sleep_s": sleep_s, "elapsed_ms": elapsed_ms, "at_utc": iso_utc_now()})
                    log(f"WARNING {last_error}")
                    time.sleep(sleep_s)
                    continue
                if resp.status_code in {500, 502, 503, 504} and attempt < self.retries:
                    sleep_s = min(120.0, self.backoff * (attempt + 1)) + random.random() * 0.25
                    last_error = f"HTTP {resp.status_code} uuid={uuid}; retry_after={retry_after}; sleep={sleep_s:.1f}s; elapsed_ms={elapsed_ms:.0f}"
                    log(f"WARNING {last_error}")
                    time.sleep(sleep_s)
                    continue
                return resp.status_code, None, f"HTTP {resp.status_code}: {text[:500]}", elapsed_ms, retry_after, last_url
            except Exception as e:
                elapsed_ms = (time.monotonic() - start) * 1000
                last_error = repr(e)
                if attempt < self.retries:
                    time.sleep(self.backoff * (attempt + 1) + random.random() * 0.25)
                    continue
                return 0, None, last_error, elapsed_ms, None, last_url
        return 0, None, last_error or "unknown_error", 0.0, None, last_url


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]).fetchone()[0] > 0


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


def find_db(inputs_dir: Path, filenames: Sequence[str]) -> Optional[Path]:
    for name in filenames:
        p = inputs_dir / name
        if p.exists() and p.suffix == ".duckdb":
            return p
        if p.exists() and p.suffix == ".zip":
            extracted = unzip_duckdb(p, inputs_dir)
            if extracted:
                return extracted
    return None


def find_udi_raw_db(inputs_dir: Path) -> Optional[Path]:
    return find_db(inputs_dir, ["eudamed_platform_udi_raw_latest.duckdb", "eudamed_platform_udi_raw_latest.duckdb.zip"])


def find_existing_details_db(inputs_dir: Path) -> Optional[Path]:
    return find_db(inputs_dir, ["eudamed_platform_udi_details_raw_latest.duckdb", "eudamed_platform_udi_details_raw_latest.duckdb.zip"])


def boolish_true(v: Any) -> bool:
    return v is True or str(v).strip().lower() == "true"


def int_or_minus(v: Any) -> int:
    try:
        if pd.isna(v):
            return -1
    except Exception:
        pass
    try:
        return int(v)
    except Exception:
        return -1


def read_udi_raw_rows(db_path: Path) -> List[Dict[str, Any]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        table = None
        for t in ["udi_raw", "udi", "devices", "eudamed_platform_udi_raw"]:
            if table_exists(con, t):
                table = t
                break
        if not table:
            # fallback to largest table with uuid column
            tables = [r[0] for r in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()]
            best = None
            best_count = -1
            for t in tables:
                cols = {r[1] for r in con.execute(f"PRAGMA table_info('{t}')").fetchall()}
                if "uuid" in cols:
                    cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    if cnt > best_count:
                        best, best_count = t, cnt
            table = best
        if not table:
            return []
        cols = {r[1].lower(): r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
        def c(*names: str) -> Optional[str]:
            for n in names:
                if n.lower() in cols:
                    return cols[n.lower()]
            return None
        wanted_map = {
            "uuid": c("uuid", "UUID"),
            "ulid": c("ulid", "ULID"),
            "primary_di_code": c("primary_di", "primary_di_code", "PRIMARY_DI", "PRIMARY_DI_CODE"),
            "primary_di_uuid": c("primary_di_uuid", "PRIMARY_DI_UUID"),
            "latest_version": c("latest_version", "LATEST_VERSION"),
            "version_number": c("version_number", "VERSION_NUMBER"),
        }
        select_exprs = []
        for alias, col in wanted_map.items():
            if col:
                select_exprs.append(f'"{col}" AS {alias}')
            else:
                select_exprs.append(f'NULL AS {alias}')
        df = con.execute(f"SELECT {', '.join(select_exprs)} FROM {table} WHERE {wanted_map['uuid'] or 'uuid'} IS NOT NULL").fetchdf()
        return df.to_dict("records")
    finally:
        con.close()


def read_existing_detail_rows(db_path: Optional[Path]) -> List[Dict[str, Any]]:
    if not db_path or not db_path.exists():
        return []
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        table = None
        for t in ["udi_details_raw", "details_raw"]:
            if table_exists(con, t):
                table = t
                break
        if not table:
            return []
        df = con.execute(f"SELECT * FROM {table}").fetchdf()
        records = df.to_dict("records")
        out = []
        for r in records:
            d = {c: None for c in DETAIL_COLUMNS}
            for c in DETAIL_COLUMNS:
                if c in r:
                    d[c] = r.get(c)
            out.append(d)
        return out
    finally:
        con.close()


def read_existing_queue(db_path: Optional[Path]) -> List[Dict[str, Any]]:
    if not db_path or not db_path.exists():
        return []
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "udi_details_queue"):
            return []
        df = con.execute("SELECT * FROM udi_details_queue").fetchdf()
        records = df.to_dict("records")
        return [{c: r.get(c) for c in QUEUE_COLUMNS} for r in records]
    finally:
        con.close()


def read_previous_metadata(inputs_dir: Path) -> Dict[str, Any]:
    path = inputs_dir / "eudamed_platform_udi_details_raw_latest.metadata.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"WARNING could not read previous metadata {path}: {e}")
        return {}


def canonical_base_mode(mode: str) -> str:
    return mode.replace("resume_", "")


def resume_mode_for_base(base_mode: str) -> str:
    return f"resume_{base_mode}"


def current_candidate_uuids(rows: List[Dict[str, Any]]) -> set:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        u = r.get("uuid")
        pd_code = r.get("primary_di_code") or u
        if u:
            groups.setdefault(str(pd_code), []).append(r)
    chosen = set()
    for _, items in groups.items():
        s = sorted(items, key=lambda r: (1 if boolish_true(r.get("latest_version")) else 0, int_or_minus(r.get("version_number")), str(r.get("ulid") or ""), str(r.get("uuid") or "")), reverse=True)
        if s:
            chosen.add(s[0].get("uuid"))
    return chosen


def build_queue(mode: str, raw_rows: List[Dict[str, Any]], existing_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    base_mode = canonical_base_mode(mode)
    now = iso_utc_now()
    existing_by_uuid = {r.get("uuid"): r for r in existing_rows if r.get("uuid")}
    dedup: Dict[str, Dict[str, Any]] = {}
    for r in raw_rows:
        u = r.get("uuid")
        if not u:
            continue
        old = dedup.get(u)
        if old is None or (str(r.get("ulid") or ""), int_or_minus(r.get("version_number"))) >= (str(old.get("ulid") or ""), int_or_minus(old.get("version_number"))):
            dedup[u] = r
    all_rows = list(dedup.values())
    current_uuids = current_candidate_uuids(all_rows)
    rows: List[Dict[str, Any]] = []
    for r in all_rows:
        u = r.get("uuid")
        is_current = u in current_uuids
        exists = u in existing_by_uuid
        include = False
        queue_source = base_mode.upper()
        priority = 1 if is_current else 2
        if base_mode == "incremental":
            include = not exists
            queue_source = "INCREMENTAL_CURRENT" if is_current else "INCREMENTAL_HISTORICAL"
        elif base_mode == "incremental_current":
            include = is_current and not exists
            queue_source = "INCREMENTAL_CURRENT"
            priority = 1
        elif base_mode == "incremental_historical":
            include = (not is_current) and not exists
            queue_source = "INCREMENTAL_HISTORICAL"
            priority = 2
        elif base_mode == "full_current":
            include = is_current
            queue_source = "FULL_CURRENT"
            priority = 1
        elif base_mode == "full_historical":
            include = not is_current
            queue_source = "FULL_HISTORICAL"
            priority = 2
        elif base_mode == "full":
            include = True
            queue_source = "FULL_CURRENT" if is_current else "FULL_HISTORICAL"
            priority = 1 if is_current else 2
        else:
            raise ValueError(f"Unsupported mode: {mode}")
        if not include:
            continue
        prev = existing_by_uuid.get(u) or {}
        last_extract = prev.get("EXTRACT_DATETIME_UTC")
        rows.append({
            "candidate_uuid": u,
            "primary_di_code": r.get("primary_di_code"),
            "primary_di_uuid": r.get("primary_di_uuid"),
            "ulid": r.get("ulid"),
            "ulid_datetime_utc": decode_ulid(r.get("ulid")),
            "latest_version": r.get("latest_version"),
            "version_number": r.get("version_number"),
            "queue_source": queue_source,
            "priority": priority,
            "status": QUEUE_PENDING,
            "attempt_count": 0,
            "last_status_code": None,
            "last_error": None,
            "queued_at_utc": now,
            "last_attempt_at_utc": None,
            "completed_at_utc": None,
            "last_detail_extract_datetime_utc": last_extract,
        })
    # Full modes refresh oldest details first; incremental prioritizes current and stable ULID/UUID.
    if base_mode.startswith("full"):
        rows = sorted(rows, key=lambda q: (int_or_minus(q.get("priority")), str(q.get("last_detail_extract_datetime_utc") or "0000"), str(q.get("primary_di_code") or ""), str(q.get("candidate_uuid") or "")))
    else:
        rows = sorted(rows, key=lambda q: (int_or_minus(q.get("priority")), str(q.get("ulid") or ""), int_or_minus(q.get("version_number")), str(q.get("candidate_uuid") or "")))
    stats = {
        "udi_raw_rows": len(raw_rows),
        "udi_raw_distinct_uuid_count": len(all_rows),
        "current_candidate_count": len(current_uuids),
        "historical_candidate_count": len(all_rows) - len(current_uuids),
        "existing_detail_uuid_count": len(existing_by_uuid),
        "candidate_count": len(rows),
        "priority_1_count": sum(1 for q in rows if int_or_minus(q.get("priority")) == 1),
        "priority_2_count": sum(1 for q in rows if int_or_minus(q.get("priority")) == 2),
        "base_mode": base_mode,
    }
    return rows, stats


def queue_compatible(previous_metadata: Dict[str, Any], requested_mode: str) -> bool:
    state = previous_metadata.get("resume_state") or {}
    if not state:
        return False
    return state.get("base_mode") == canonical_base_mode(requested_mode)


def progress_line(mode: str, processed: int, total: int, ok: int, failed: int, started_at: float, response_ms: List[float], throttle_count: int, total_new: int, total_refreshed: int, batch_new: int, batch_refreshed: int) -> str:
    elapsed = max(0.001, time.monotonic() - started_at)
    rate = processed / elapsed if processed else 0.0
    remaining = max(0, total - processed)
    eta = remaining / rate if rate > 0 else None
    avg_ms = sum(response_ms) / len(response_ms) if response_ms else 0.0
    recent = response_ms[-100:]
    recent_elapsed = sum(recent) / 1000.0 if recent else 0.0
    recent_rate_response_limited = len(recent) / recent_elapsed if recent_elapsed > 0 else 0.0
    return (f"{mode} detail={processed}/{total} ({(processed/total*100.0 if total else 0):.2f}%) | "
            f"remaining={remaining:,} | new={batch_new:,} | refreshed={batch_refreshed:,} | total_new={total_new:,} | total_refreshed={total_refreshed:,} | "
            f"ok={ok:,} | failed={failed:,} | rate={rate:.3f} req/s | avg_response={avg_ms:.0f} ms | "
            f"recent_response_rate_100={recent_rate_response_limited:.3f} req/s | 429_count={throttle_count} | elapsed={fmt_duration(elapsed)} | ETA={fmt_duration(eta)}")


def runtime_exceeded(started_at: float, max_runtime_hours: float) -> bool:
    return bool(max_runtime_hours and max_runtime_hours > 0 and ((time.monotonic() - started_at) / 3600.0) >= max_runtime_hours)


def fetch_details(client: EudamedClient, queue_rows: List[Dict[str, Any]], extract_date: str, extract_ts: str, mode: str, max_records: int, max_runtime_hours: float, max_429_before_partial: int, max_failed_records: int, log_every_records: int, existing_detail_uuid_set: Optional[set] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], bool]:
    total = len([q for q in queue_rows if q.get("status") == QUEUE_PENDING])
    rows: List[Dict[str, Any]] = []
    request_log: List[Dict[str, Any]] = []
    started_at = time.monotonic()
    response_ms: List[float] = []
    status_counts: Dict[int, int] = {}
    ok_count = fail_count = processed = 0
    total_new = total_refreshed = batch_new = batch_refreshed = 0
    existing_detail_uuid_set = existing_detail_uuid_set or set()
    normal_completion = True
    stop_reason = "all_queue_items_fetched"
    last_successful_uuid = None
    pending_indices = [i for i, q in enumerate(queue_rows) if q.get("status") == QUEUE_PENDING]
    log(f"=== UDI Details fetch mode={mode} pending_queue={total} ===")
    for pos, qidx in enumerate(pending_indices):
        if max_records and max_records > 0 and processed >= max_records:
            normal_completion = False; stop_reason = "max_records_cap_reached"; break
        if runtime_exceeded(started_at, max_runtime_hours):
            normal_completion = False; stop_reason = "runtime_limit"; break
        q = queue_rows[qidx]
        uuid = str(q.get("candidate_uuid"))
        q["attempt_count"] = int_or_minus(q.get("attempt_count")) + 1
        q["last_attempt_at_utc"] = iso_utc_now()
        status, data, error, elapsed_ms, retry_after, url = client.fetch_udi_detail(uuid)
        status_counts[int(status or 0)] = status_counts.get(int(status or 0), 0) + 1
        response_ms.append(float(elapsed_ms or 0.0))
        processed += 1
        request_log.append({"endpoint": "udi_detail", "uuid": uuid, "queue_index": qidx, "priority": q.get("priority"), "queue_source": q.get("queue_source"), "status_code": status, "elapsed_ms": elapsed_ms, "retry_after": retry_after, "error": error, "requested_url": url, "requested_at_utc": iso_utc_now()})
        q["last_status_code"] = status
        q["last_error"] = error
        if status == 200 and data:
            row = flatten_detail(data, uuid, extract_date, extract_ts)
            if not row.get("uuid"):
                row["uuid"] = uuid
            rows.append(row)
            ok_count += 1
            row_uuid = row.get("uuid") or uuid
            if row_uuid in existing_detail_uuid_set:
                total_refreshed += 1; batch_refreshed += 1
            else:
                total_new += 1; batch_new += 1
            q["status"] = QUEUE_DONE
            q["completed_at_utc"] = iso_utc_now()
            q["last_detail_extract_datetime_utc"] = extract_ts
            last_successful_uuid = uuid
        else:
            fail_count += 1
            q["status"] = QUEUE_FAILED
            q["completed_at_utc"] = None
            log(f"WARNING detail fetch failed uuid={uuid} status={status} error={error}; marking queue item as failed and continuing")
            if max_failed_records and max_failed_records > 0 and fail_count >= max_failed_records:
                normal_completion = False
                stop_reason = "failed_records_limit"
                break
        if max_429_before_partial and len(client.throttle_events) >= max_429_before_partial:
            normal_completion = False
            stop_reason = "429_limit"
            break
        if processed == 1 or processed % max(1, log_every_records) == 0 or pos == len(pending_indices) - 1:
            log(progress_line(mode, processed, total, ok_count, fail_count, started_at, response_ms, len(client.throttle_events), total_new, total_refreshed, batch_new, batch_refreshed))
            batch_new = batch_refreshed = 0
    remaining_pending = sum(1 for q in queue_rows if q.get("status") == QUEUE_PENDING)
    if remaining_pending == 0 and stop_reason not in {"max_records_cap_reached", "runtime_limit", "429_limit", "failed_records_limit"}:
        normal_completion = True
        stop_reason = "all_queue_items_fetched"
    audit = {
        "mode": mode,
        "base_mode": canonical_base_mode(mode),
        "total_queue_items": total,
        "processed_queue_items": processed,
        "successful_details": ok_count,
        "failed_details": fail_count,
        "remaining_pending_queue_items": remaining_pending,
        "normal_completion": normal_completion,
        "stop_reason": stop_reason,
        "last_successful_uuid": last_successful_uuid,
        "resume_state": None if normal_completion else {"resume_mode": resume_mode_for_base(canonical_base_mode(mode)), "base_mode": canonical_base_mode(mode), "remaining_pending_queue_items": remaining_pending, "last_successful_uuid": last_successful_uuid, "stop_reason": stop_reason},
        "telemetry": {"elapsed_seconds": time.monotonic() - started_at, "requests_per_second": processed / max(0.001, time.monotonic() - started_at), "avg_response_ms": (sum(response_ms) / len(response_ms)) if response_ms else None, "status_counts": {str(k): v for k, v in sorted(status_counts.items())}, "throttle_429_count": len(client.throttle_events), "throttle_events": client.throttle_events, "new_details": total_new, "refreshed_details": total_refreshed},
        "new_details": total_new,
        "refreshed_details": total_refreshed,
    }
    return rows, request_log, queue_rows, audit, normal_completion


def dedupe_by_uuid_choose_latest(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Dict[str, Dict[str, Any]] = {}
    no_uuid: List[Dict[str, Any]] = []
    for r in rows:
        u = r.get("uuid")
        if not u:
            no_uuid.append(r); continue
        old = seen.get(u)
        if old is None or str(r.get("EXTRACT_DATETIME_UTC") or "") >= str(old.get("EXTRACT_DATETIME_UTC") or ""):
            seen[u] = r
    return list(seen.values()) + no_uuid


def merge_rows(previous_rows: List[Dict[str, Any]], received_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    prev = {r.get("uuid") for r in previous_rows if r.get("uuid")}
    recv = {r.get("uuid") for r in received_rows if r.get("uuid")}
    merged = dedupe_by_uuid_choose_latest(previous_rows + received_rows)
    return merged, {"previous_rows": len(previous_rows), "received_rows": len(received_rows), "previous_uuid_count": len(prev), "received_uuid_count": len(recv), "inserted_uuid_count": len(recv - prev), "refreshed_uuid_count": len(recv & prev), "retained_uuid_count": len(prev - recv), "merged_rows": len(merged)}


def flatten_for_table(obj: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in obj.items():
        out[k] = json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
    return out


def write_duckdb(out_db: Path, rows: List[Dict[str, Any]], request_log: List[Dict[str, Any]], queue_rows: List[Dict[str, Any]], metadata: Dict[str, Any]) -> None:
    safe_unlink(out_db)
    con = duckdb.connect(str(out_db))
    try:
        con.register("detail_df", pd.DataFrame(rows, columns=DETAIL_COLUMNS))
        con.execute("CREATE TABLE udi_details_raw AS SELECT * FROM detail_df")
        con.register("queue_df", pd.DataFrame(queue_rows, columns=QUEUE_COLUMNS))
        con.execute("CREATE TABLE udi_details_queue AS SELECT * FROM queue_df")
        con.register("request_log_df", pd.DataFrame(request_log))
        con.execute("CREATE TABLE api_request_log AS SELECT * FROM request_log_df")
        con.register("candidate_df", pd.DataFrame(queue_rows, columns=QUEUE_COLUMNS))
        con.execute("CREATE TABLE candidate_inventory AS SELECT * FROM candidate_df")
        field_rows = [{"table_name": "udi_details_raw", "field_name": col, "non_null_rows": sum(1 for r in rows if r.get(col) is not None), "total_rows": len(rows)} for col in DETAIL_COLUMNS]
        con.register("field_inventory_df", pd.DataFrame(field_rows))
        con.execute("CREATE TABLE field_inventory AS SELECT * FROM field_inventory_df")
        con.register("metadata_df", pd.DataFrame([flatten_for_table(metadata)]))
        con.execute("CREATE TABLE pipeline_metadata AS SELECT * FROM metadata_df")
    finally:
        con.close()


def write_csv_zip(zip_path: Path, rows: List[Dict[str, Any]]) -> None:
    safe_unlink(zip_path)
    tmp = zip_path.parent / "_csv_tmp_udi_details"
    if tmp.exists(): shutil.rmtree(tmp)
    ensure_dir(tmp)
    csv_path = tmp / "udi_details_raw.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore", quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for r in rows:
            writer.writerow({c: r.get(c) for c in CSV_COLUMNS})
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        zf.write(csv_path, "udi_details_raw.csv")
    shutil.rmtree(tmp)


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_release_notes(path: Path, metadata: Dict[str, Any]) -> None:
    lines = [
        "# EUDAMED Platform UDI Details Raw", "",
        f"Pipeline version: `{metadata.get('pipeline_version')}`",
        f"Generated at UTC: `{metadata.get('generated_at_utc')}`",
        f"Run status: `{metadata.get('run_status')}`",
        f"Requested mode: `{metadata.get('requested_mode')}`",
        f"Effective mode: `{metadata.get('effective_mode')}`", "",
        "## Snapshot", "",
        f"- Rows in latest DB: `{metadata.get('row_count')}`",
        f"- Distinct UUIDs: `{metadata.get('distinct_uuid_count')}`",
        f"- Distinct Primary DI codes: `{metadata.get('distinct_primary_di_count')}`",
        f"- Distinct ULIDs: `{metadata.get('distinct_ulid_count')}`",
        f"- latest_version=true rows: `{metadata.get('latest_version_true_count')}`",
        f"- latest_version=false rows: `{metadata.get('latest_version_false_count')}`", "",
        "## Queue", "",
        f"- Queue rows: `{metadata.get('queue_rows')}`",
        f"- Queue pending rows: `{metadata.get('queue_pending_rows')}`",
        f"- Queue done rows: `{metadata.get('queue_done_rows')}`",
        f"- Queue failed rows: `{metadata.get('queue_failed_rows')}`",
        f"- Priority 1 rows: `{metadata.get('queue_priority_1_rows')}`",
        f"- Priority 2 rows: `{metadata.get('queue_priority_2_rows')}`", "",
        "## Candidate selection", "",
        f"- UDI Raw rows: `{metadata.get('candidate_stats', {}).get('udi_raw_rows')}`",
        f"- UDI Raw distinct UUIDs: `{metadata.get('candidate_stats', {}).get('udi_raw_distinct_uuid_count')}`",
        f"- Current candidates: `{metadata.get('candidate_stats', {}).get('current_candidate_count')}`",
        f"- Historical candidates: `{metadata.get('candidate_stats', {}).get('historical_candidate_count')}`",
        f"- Existing detail UUIDs before run: `{metadata.get('candidate_stats', {}).get('existing_detail_uuid_count')}`",
        f"- Selected candidates: `{metadata.get('candidate_stats', {}).get('candidate_count')}`", "",
        "## Merge", "",
        f"- Previous rows: `{metadata.get('merge', {}).get('previous_rows')}`",
        f"- Received rows this run: `{metadata.get('merge', {}).get('received_rows')}`",
        f"- Inserted UUIDs: `{metadata.get('merge', {}).get('inserted_uuid_count')}`",
        f"- Refreshed UUIDs: `{metadata.get('merge', {}).get('refreshed_uuid_count')}`",
        f"- New details this run: `{metadata.get('new_details_this_run')}`",
        f"- Refreshed details this run: `{metadata.get('refreshed_details_this_run')}`",
        f"- Retained UUIDs from previous latest: `{metadata.get('merge', {}).get('retained_uuid_count')}`",
        f"- Merged rows: `{metadata.get('merge', {}).get('merged_rows')}`", "",
        "## ULID range", "",
        f"- Min ULID: `{metadata.get('min_ulid')}` → `{metadata.get('min_ulid_datetime_utc')}`",
        f"- Max ULID: `{metadata.get('max_ulid')}` → `{metadata.get('max_ulid_datetime_utc')}`", "",
        "## Audit", "",
        f"- Normal completion: `{metadata.get('normal_completion')}`",
        f"- Stop reason: `{metadata.get('audit', {}).get('stop_reason')}`",
        f"- Processed queue items: `{metadata.get('audit', {}).get('processed_queue_items')}`",
        f"- Successful details: `{metadata.get('audit', {}).get('successful_details')}`",
        f"- Failed details: `{metadata.get('audit', {}).get('failed_details')}`", "",
        "## Resume state", "",
        f"- Resume state JSON: `{json.dumps(metadata.get('resume_state') or {}, ensure_ascii=False)}`", "",
        "## Telemetry", "",
        f"- Requests/sec: `{(metadata.get('telemetry') or {}).get('requests_per_second')}`",
        f"- Avg response ms: `{(metadata.get('telemetry') or {}).get('avg_response_ms')}`",
        f"- 429 count: `{(metadata.get('telemetry') or {}).get('throttle_429_count')}`", "",
        "## CSV export", "",
        f"- CSV excluded columns: `{json.dumps(metadata.get('csv_excluded_columns') or [], ensure_ascii=False)}`", "",
        "## Interpretation", "",
        "- `COMPLETE` means the selected queue reached normal completion.",
        "- `PARTIAL` means useful data was received, but pending queue items remain. Latest is merged with previous latest so data is not lost.",
        "- `BOOTSTRAP` means no previous latest DB was available and the selected queue completed.",
        "- `FAILED` means no usable details were received; latest DB is not updated.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def copy_file(src: Path, dst: Path) -> None:
    safe_unlink(dst); shutil.copy2(src, dst)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=sorted(DETAIL_MODES), default="incremental_current")
    p.add_argument("--out-dir", default="dist/eudamed_platform_udi_details_raw")
    p.add_argument("--inputs-dir", default="inputs")
    p.add_argument("--base-url", default=BASE_URL_DEFAULT)
    p.add_argument("--language", default="en")
    p.add_argument("--max-records", type=int, default=0)
    p.add_argument("--max-rps", type=float, default=0.8)
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--retries", type=int, default=4)
    p.add_argument("--backoff", type=float, default=1.5)
    p.add_argument("--max-429-before-partial", type=int, default=7)
    p.add_argument("--max-failed-records", type=int, default=100, help="Controlled PARTIAL stop after this many failed detail records. 0 = never stop for failed records")
    p.add_argument("--max-runtime-hours", type=float, default=5.5)
    p.add_argument("--log-every-records", type=int, default=100)
    p.add_argument("--release-timestamp", default=None)
    p.add_argument("--skip-csv-zip", action="store_true")
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    out_dir = Path(args.out_dir); inputs_dir = Path(args.inputs_dir)
    ensure_dir(out_dir); ensure_dir(inputs_dir)
    rel_ts = release_timestamp(args.release_timestamp)
    extract_ts = iso_utc_now(); extract_date = extract_ts[:10]
    log(f"=== EUDAMED Platform UDI Details Raw {PIPELINE_VERSION} acquisition started ===")
    log(f"requested_mode={args.mode} out_dir={out_dir} max_rps={args.max_rps}")
    log("scope=raw_fetch_only cdc=0 canonical=0 dk_subset=0 domain=udi_details")

    raw_db = find_udi_raw_db(inputs_dir)
    if not raw_db:
        log("ERROR: Missing UDI Raw latest DB in inputs"); return 2
    log(f"UDI Raw DB found: {raw_db}")
    raw_rows = read_udi_raw_rows(raw_db)
    log(f"Loaded udi_raw rows: {len(raw_rows):,}")
    if not raw_rows:
        log("ERROR: No UDI raw rows found"); return 2

    existing_db = find_existing_details_db(inputs_dir)
    previous_rows: List[Dict[str, Any]] = []
    previous_queue: List[Dict[str, Any]] = []
    if existing_db:
        log(f"Previous UDI Details Raw latest DB found: {existing_db}")
        previous_rows = read_existing_detail_rows(existing_db)
        previous_queue = read_existing_queue(existing_db)
        log(f"Loaded previous detail rows: {len(previous_rows):,}; previous queue rows: {len(previous_queue):,}")
    else:
        log("No previous UDI Details Raw latest DB found")

    previous_metadata = read_previous_metadata(inputs_dir)
    base_mode = canonical_base_mode(args.mode)
    if args.mode.startswith("resume_") and previous_queue and queue_compatible(previous_metadata, args.mode):
        queue_rows = previous_queue
        candidate_stats = previous_metadata.get("candidate_stats") or {"base_mode": base_mode, "candidate_count": len(queue_rows)}
        log(f"Compatible previous queue found for resume. pending={sum(1 for q in queue_rows if q.get('status') == QUEUE_PENDING):,}")
    else:
        if args.mode.startswith("resume_"):
            log("No compatible previous queue found. Rebuilding queue from selected base mode.")
        queue_rows, candidate_stats = build_queue(base_mode, raw_rows, previous_rows)
    if not queue_rows:
        log("No queue candidates selected. Run can complete without API calls.")

    client = EudamedClient(args.base_url, args.language, args.timeout, args.retries, args.backoff, args.max_rps)
    received_rows, request_log, queue_rows, audit, normal_completion = fetch_details(
        client, queue_rows, extract_date, extract_ts, base_mode, args.max_records, args.max_runtime_hours,
        args.max_429_before_partial, args.max_failed_records, args.log_every_records, {r.get("uuid") for r in previous_rows if r.get("uuid")},
    )

    if len(received_rows) == 0 and len(previous_rows) == 0 and len(queue_rows) > 0 and not normal_completion and audit.get("stop_reason") not in {"runtime_limit", "max_records_cap_reached", "failed_records_limit", "429_limit"}:
        run_status = RUN_FAILED
    elif not previous_rows and normal_completion:
        run_status = RUN_BOOTSTRAP
    elif normal_completion:
        run_status = RUN_COMPLETE
    else:
        run_status = RUN_PARTIAL

    if run_status == RUN_FAILED:
        merged_rows = previous_rows
        merge_stats = {"previous_rows": len(previous_rows), "received_rows": 0, "previous_uuid_count": len({r.get('uuid') for r in previous_rows if r.get('uuid')}), "received_uuid_count": 0, "inserted_uuid_count": 0, "refreshed_uuid_count": 0, "retained_uuid_count": len({r.get('uuid') for r in previous_rows if r.get('uuid')}), "merged_rows": len(previous_rows)}
    else:
        merged_rows, merge_stats = merge_rows(previous_rows, received_rows)

    min_u = min_value(r.get("ulid") for r in merged_rows); max_u = max_value(r.get("ulid") for r in merged_rows)
    status_summary: Dict[Tuple[str, int], int] = {}
    for r in request_log:
        key = (r.get("endpoint", ""), int(r.get("status_code") or 0)); status_summary[key] = status_summary.get(key, 0) + 1
    metadata = {
        "pipeline_version": PIPELINE_VERSION, "release_timestamp": rel_ts, "release_time_utc": release_title_time(rel_ts), "generated_at_utc": iso_utc_now(),
        "run_status": run_status, "requested_mode": args.mode, "effective_mode": base_mode, "base_url": args.base_url, "language": args.language,
        "max_records": args.max_records, "max_rps": args.max_rps, "max_429_before_partial": args.max_429_before_partial, "max_failed_records": args.max_failed_records, "max_runtime_hours": args.max_runtime_hours,
        "log_every_records": args.log_every_records, "retries": args.retries, "udi_raw_db_found": str(raw_db), "previous_detail_db_found": str(existing_db) if existing_db else None,
        "candidate_stats": candidate_stats, "row_count": len(merged_rows), "distinct_uuid_count": len({r.get('uuid') for r in merged_rows if r.get('uuid')}),
        "distinct_primary_di_count": len({r.get('primary_di_code') for r in merged_rows if r.get('primary_di_code')}), "distinct_ulid_count": len({r.get('ulid') for r in merged_rows if r.get('ulid')}),
        "latest_version_true_count": sum(1 for r in merged_rows if boolish_true(r.get("latest_version"))), "latest_version_false_count": sum(1 for r in merged_rows if str(r.get("latest_version")).strip().lower() == "false" or r.get("latest_version") is False),
        "queue_rows": len(queue_rows), "queue_pending_rows": sum(1 for q in queue_rows if q.get("status") == QUEUE_PENDING), "queue_done_rows": sum(1 for q in queue_rows if q.get("status") == QUEUE_DONE), "queue_failed_rows": sum(1 for q in queue_rows if q.get("status") == QUEUE_FAILED),
        "queue_priority_1_rows": sum(1 for q in queue_rows if int_or_minus(q.get("priority")) == 1), "queue_priority_2_rows": sum(1 for q in queue_rows if int_or_minus(q.get("priority")) == 2),
        "min_ulid": min_u, "min_ulid_datetime_utc": decode_ulid(min_u), "max_ulid": max_u, "max_ulid_datetime_utc": decode_ulid(max_u),
        "previous_rows": len(previous_rows), "received_rows": len(received_rows), "new_details_this_run": (audit or {}).get("new_details"), "refreshed_details_this_run": (audit or {}).get("refreshed_details"),
        "csv_excluded_columns": sorted(CSV_EXCLUDED_COLUMNS), "normal_completion": normal_completion, "audit": audit, "resume_state": audit.get("resume_state"), "merge": merge_stats,
        "request_status_summary": [{"endpoint": k[0], "status_code": k[1], "count": v} for k, v in sorted(status_summary.items())], "telemetry": audit.get("telemetry"),
    }

    latest_db = out_dir / "eudamed_platform_udi_details_raw_latest.duckdb"
    latest_csv = out_dir / "eudamed_platform_udi_details_raw_latest_csv.zip"
    latest_meta = out_dir / "eudamed_platform_udi_details_raw_latest.metadata.json"
    latest_notes = out_dir / "RELEASE_NOTES_EUDAMED_PLATFORM_UDI_DETAILS_RAW.md"
    dated_db = out_dir / f"eudamed_platform_udi_details_raw_{rel_ts}.duckdb"
    dated_csv = out_dir / f"eudamed_platform_udi_details_raw_{rel_ts}_csv.zip"
    dated_meta = out_dir / f"eudamed_platform_udi_details_raw_{rel_ts}.metadata.json"
    dated_notes = out_dir / f"RELEASE_NOTES_EUDAMED_PLATFORM_UDI_DETAILS_RAW_{rel_ts}.md"

    if run_status == RUN_FAILED:
        log("RUN_STATUS=FAILED. Writing metadata/notes only; not writing latest DB.")
        write_json(latest_meta, metadata); write_json(dated_meta, metadata); write_release_notes(latest_notes, metadata); write_release_notes(dated_notes, metadata); return 2

    log(f"Writing DuckDB: {latest_db}")
    write_duckdb(latest_db, merged_rows, request_log, queue_rows, metadata)
    if not args.skip_csv_zip:
        log(f"Writing CSV ZIP: {latest_csv}"); write_csv_zip(latest_csv, merged_rows)
    write_json(latest_meta, metadata); write_release_notes(latest_notes, metadata)
    copy_file(latest_db, dated_db)
    if latest_csv.exists(): copy_file(latest_csv, dated_csv)
    copy_file(latest_meta, dated_meta); copy_file(latest_notes, dated_notes)

    log("=== EUDAMED Platform UDI Details Raw complete ===")
    log(json.dumps({"run_status": run_status, "requested_mode": args.mode, "effective_mode": base_mode, "row_count": len(merged_rows), "received_rows": len(received_rows), "new_details_this_run": metadata.get("new_details_this_run"), "refreshed_details_this_run": metadata.get("refreshed_details_this_run"), "previous_rows": len(previous_rows), "candidate_stats": candidate_stats, "queue_pending_rows": metadata.get("queue_pending_rows"), "merge": merge_stats, "resume_state": metadata.get("resume_state")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
