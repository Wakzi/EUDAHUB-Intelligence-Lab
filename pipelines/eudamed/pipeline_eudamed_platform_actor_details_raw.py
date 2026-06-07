#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EUDAMED Platform Actor Details Raw acquisition pipeline.

Pipeline version: v1.0.0

Scope
-----
- Raw acquisition only.
- Domain: Actor detail endpoint.
- Input master list: EUDAMED Platform Actors Raw latest DuckDB.
- No CDC, no canonical merge, no DK subset.

Design
------
- Detail endpoint: /actors/{uuid}/publicInformation.
- actors_raw is the authority for which actor UUIDs exist.
- actor_details_raw stores one raw detail response per actor UUID.
- Incremental means: fetch actor UUIDs present in actors_raw but missing from actor_details_raw.
- Current means: fetch one current candidate per actor identity.
- Historical means: fetch non-current actor UUIDs only.
- Full means: fetch all actor UUIDs from actors_raw.
- Resume modes continue a previous partial candidate list by deterministic index.

Current actor selection rule
----------------------------
1. Prefer latest_version=true within actor identity.
2. Fallback to highest version_number within actor identity.
3. Fallback to stable UUID ordering when needed.

Release naming
--------------
Latest tag: eudamed-platform-actor-details-raw-latest
Latest assets:
  eudamed_platform_actor_details_raw_latest.duckdb
  eudamed_platform_actor_details_raw_latest_csv.zip
  eudamed_platform_actor_details_raw_latest.metadata.json
  RELEASE_NOTES_EUDAMED_PLATFORM_ACTOR_DETAILS_RAW.md

Dated tag: eudamed-platform-actor-details-raw-YYYYMMDD_HHMMSS
Dated assets:
  eudamed_platform_actor_details_raw_YYYYMMDD_HHMMSS.duckdb
  eudamed_platform_actor_details_raw_YYYYMMDD_HHMMSS_csv.zip
  eudamed_platform_actor_details_raw_YYYYMMDD_HHMMSS.metadata.json
  RELEASE_NOTES_EUDAMED_PLATFORM_ACTOR_DETAILS_RAW_YYYYMMDD_HHMMSS.md
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

PIPELINE_VERSION = "v1.0.0"
BASE_URL_DEFAULT = "https://ec.europa.eu/tools/eudamed/api"
ACTOR_DETAILS_ENDPOINT_TEMPLATE = "/actors/{uuid}/publicInformation"
CROCKFORD32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

RUN_BOOTSTRAP = "BOOTSTRAP"
RUN_COMPLETE = "COMPLETE"
RUN_PARTIAL = "PARTIAL"
RUN_FAILED = "FAILED"

DETAIL_MODES = {
    "incremental", "resume_incremental",
    "current", "resume_current",
    "historical", "resume_historical",
    "full", "resume_full",
}


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
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).isoformat()


def clean_ulid_values(values: Iterable[Optional[str]]) -> List[str]:
    vals: List[str] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if not s or s.lower() in {"nan", "none", "null", "nat"}:
            continue
        vals.append(s)
    return vals


def min_ulid(values: Iterable[Optional[str]]) -> Optional[str]:
    vals = clean_ulid_values(values)
    return min(vals) if vals else None


def max_ulid(values: Iterable[Optional[str]]) -> Optional[str]:
    vals = clean_ulid_values(values)
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


def first_text_value(obj: Any) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    return obj.get("textByDefaultLanguage") or obj.get("text")


def first_list_item(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, list) and value:
        item = value[0]
        return item if isinstance(item, dict) else None
    return None


def nested_country(prefix_obj: Any) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[bool]]:
    if not isinstance(prefix_obj, dict):
        return None, None, None, None
    country = prefix_obj.get("country") if isinstance(prefix_obj.get("country"), dict) else prefix_obj
    if not isinstance(country, dict):
        return None, None, None, None
    return country.get("iso2Code"), country.get("name"), country.get("type"), country.get("nonEUMemberState")


def flatten_detail(response: Dict[str, Any], requested_uuid: str, extract_date: str, extract_ts: str) -> Dict[str, Any]:
    view = response.get("actorDataPublicView") or {}
    actor_type = view.get("type") or {}
    actor_status = view.get("actorStatus") or {}
    country = view.get("country") or {}
    actor_address = view.get("actorAddress") or {}
    version_state = view.get("versionState") or {}
    validator_type = view.get("validatorType") or {}
    validator_address = view.get("validatorAddress") or {}
    validator_address_country = validator_address.get("country") if isinstance(validator_address, dict) else {}
    ar = first_list_item(view.get("authorisedRepresentatives")) or {}
    ar_mandate_status = ar.get("mandateStatus") or {}
    ar_actor_status = ar.get("actorStatus") or {}
    ar_version_state = ar.get("versionState") or {}
    prrcs = view.get("regulatoryComplianceResponsibles")
    prrc = first_list_item(prrcs) or {}
    p_addr = prrc.get("geographicalAddress") or {}
    p_country = p_addr.get("country") if isinstance(p_addr, dict) else {}
    org_docs = view.get("organizationIdentificationDocuments")
    accuracy = view.get("accuracyData")

    return {
        "EXTRACT_DATE": extract_date,
        "EXTRACT_DATETIME_UTC": extract_ts,
        "requested_uuid": requested_uuid,
        "uuid": view.get("uuid"),
        "ulid": view.get("ulid"),
        "ulid_timestamp": decode_ulid(view.get("ulid")),
        "srn": view.get("eudamedIdentifier"),
        "eudamed_identifier": view.get("eudamedIdentifier"),
        "name": first_text_value(view.get("name")),
        "names_json": jdump(view.get("name")),
        "abbreviated_name": first_text_value(view.get("abbreviatedName")) if isinstance(view.get("abbreviatedName"), dict) else view.get("abbreviatedName"),
        "actor_type_code": actor_type.get("code") if isinstance(actor_type, dict) else None,
        "actor_type_srn_code": actor_type.get("srnCode") if isinstance(actor_type, dict) else None,
        "actor_type_category": actor_type.get("category") if isinstance(actor_type, dict) else None,
        "actor_status_code": actor_status.get("code") if isinstance(actor_status, dict) else None,
        "actor_status_from_date": view.get("actorStatusFromDate"),
        "country_iso2_code": country.get("iso2Code") if isinstance(country, dict) else None,
        "country_name": country.get("name") if isinstance(country, dict) else None,
        "country_type": country.get("type") if isinstance(country, dict) else None,
        "country_non_eu_member_state": country.get("nonEUMemberState") if isinstance(country, dict) else None,
        "european_vat_number_applicable": view.get("europeanVatNumberApplicable"),
        "european_vat_number": view.get("europeanVatNumber"),
        "trade_register": view.get("tradeRegister"),
        "eori": view.get("eori"),
        "telephone": view.get("telephone"),
        "electronic_mail": view.get("electronicMail"),
        "website": view.get("website"),
        "street_name": actor_address.get("streetName") if isinstance(actor_address, dict) else None,
        "street_info_applicable": actor_address.get("streetInfoApplicable") if isinstance(actor_address, dict) else None,
        "building_number": actor_address.get("buildingNumber") if isinstance(actor_address, dict) else None,
        "address_complement": actor_address.get("complement") if isinstance(actor_address, dict) else None,
        "postbox": actor_address.get("postbox") if isinstance(actor_address, dict) else None,
        "gps": actor_address.get("gps") if isinstance(actor_address, dict) else None,
        "postal_zone": actor_address.get("postalZone") if isinstance(actor_address, dict) else None,
        "city_name": actor_address.get("cityName") if isinstance(actor_address, dict) else None,
        "latest_version": view.get("latestVersion"),
        "version_number": view.get("versionNumber"),
        "version_state_code": version_state.get("code") if isinstance(version_state, dict) else None,
        "last_update_date": view.get("lastUpdateDate"),
        "last_accuracy_date": view.get("lastAccuracyDate"),
        "competent_authority_responsibility": view.get("competentAuthorityResponsibility"),
        "latest_subsidiary": view.get("latestSubsidiary"),
        "validator_uuid": view.get("validatorUuid"),
        "validator_srn": view.get("validatorSrn"),
        "validator_name": view.get("validatorName"),
        "validator_type_code": validator_type.get("code") if isinstance(validator_type, dict) else None,
        "validator_type_srn_code": validator_type.get("srnCode") if isinstance(validator_type, dict) else None,
        "validator_type_category": validator_type.get("category") if isinstance(validator_type, dict) else None,
        "validator_email": view.get("validatorEmail"),
        "validator_telephone": view.get("validatorTelephone"),
        "validator_street_name": validator_address.get("streetName") if isinstance(validator_address, dict) else None,
        "validator_street_info_applicable": validator_address.get("streetInfoApplicable") if isinstance(validator_address, dict) else None,
        "validator_building_number": validator_address.get("buildingNumber") if isinstance(validator_address, dict) else None,
        "validator_address_complement": validator_address.get("complement") if isinstance(validator_address, dict) else None,
        "validator_postbox": validator_address.get("postbox") if isinstance(validator_address, dict) else None,
        "validator_gps": validator_address.get("gps") if isinstance(validator_address, dict) else None,
        "validator_postal_zone": validator_address.get("postalZone") if isinstance(validator_address, dict) else None,
        "validator_city_name": validator_address.get("cityName") if isinstance(validator_address, dict) else None,
        "validator_country_iso2_code": validator_address_country.get("iso2Code") if isinstance(validator_address_country, dict) else None,
        "validator_country_name": validator_address_country.get("name") if isinstance(validator_address_country, dict) else None,
        "validator_country_type": validator_address_country.get("type") if isinstance(validator_address_country, dict) else None,
        "authorised_representative_uuid": ar.get("authorisedRepresentativeUuid"),
        "authorised_representative_ulid": ar.get("authorisedRepresentativeUlid") or ar.get("ulid"),
        "authorised_representative_ulid_timestamp": decode_ulid(ar.get("authorisedRepresentativeUlid") or ar.get("ulid")),
        "authorised_representative_srn": ar.get("srn"),
        "authorised_representative_name": ar.get("name"),
        "authorised_representative_address": ar.get("address"),
        "authorised_representative_country_name": ar.get("countryName"),
        "authorised_representative_start_date": ar.get("startDate"),
        "authorised_representative_end_date": ar.get("endDate"),
        "authorised_representative_termination_date": ar.get("terminationDate"),
        "authorised_representative_email": ar.get("email"),
        "authorised_representative_telephone": ar.get("telephone"),
        "authorised_representative_mandate_status_code": ar_mandate_status.get("code") if isinstance(ar_mandate_status, dict) else None,
        "authorised_representative_actor_status_code": ar_actor_status.get("code") if isinstance(ar_actor_status, dict) else None,
        "authorised_representative_actor_status_from_date": ar.get("actorStatusFromDate"),
        "authorised_representative_version_number": ar.get("versionNumber"),
        "authorised_representative_version_state_code": ar_version_state.get("code") if isinstance(ar_version_state, dict) else None,
        "authorised_representative_latest_version": ar.get("latestVersion"),
        "authorised_representative_last_update_date": ar.get("lastUpdateDate"),
        "prrc_count": len(prrcs) if isinstance(prrcs, list) else (0 if prrcs is None else None),
        "primary_prrc_first_name": prrc.get("firstName"),
        "primary_prrc_family_name": prrc.get("familyName"),
        "primary_prrc_email": prrc.get("electronicMail"),
        "primary_prrc_telephone": prrc.get("telephone"),
        "primary_prrc_position": prrc.get("position"),
        "primary_prrc_street_name": p_addr.get("streetName") if isinstance(p_addr, dict) else None,
        "primary_prrc_building_number": p_addr.get("buildingNumber") if isinstance(p_addr, dict) else None,
        "primary_prrc_address_complement": p_addr.get("complement") if isinstance(p_addr, dict) else None,
        "primary_prrc_postal_zone": p_addr.get("postalZone") if isinstance(p_addr, dict) else None,
        "primary_prrc_city_name": p_addr.get("cityName") if isinstance(p_addr, dict) else None,
        "primary_prrc_country_iso2_code": p_country.get("iso2Code") if isinstance(p_country, dict) else None,
        "primary_prrc_country_name": p_country.get("name") if isinstance(p_country, dict) else None,
        "regulatory_compliance_responsibles_json": jdump(prrcs),
        "accuracy_data_json": jdump(accuracy),
        "organization_identification_documents_json": jdump(org_docs),
        "importers_json": jdump(response.get("importers")),
        "non_eu_manufacturers_json": jdump(response.get("nonEuManufacturers")),
        "acquisitions_json": jdump(view.get("acquisitions")),
        "certificates_json": jdump(view.get("certificates")),
        "legislation_links_json": jdump(view.get("legislationLinks")),
        "raw_json": jdump(response),
        "source_endpoint": "actors",
    }


DETAIL_COLUMNS = list(flatten_detail({}, "", "", "").keys())


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
            "User-Agent": f"EUDAHUB-Intelligence Platform Actor Details Raw {PIPELINE_VERSION}",
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

    def fetch_actor_detail(self, actor_uuid: str) -> Tuple[int, Optional[Dict[str, Any]], Optional[str], float, Optional[str], str]:
        path = ACTOR_DETAILS_ENDPOINT_TEMPLATE.format(uuid=actor_uuid)
        url = f"{self.base_url}{path}"
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
                    if retry_after and retry_after.isdigit():
                        sleep_s = float(retry_after)
                    else:
                        sleep_s = min(300.0, 30.0 * (attempt + 1))
                    sleep_s += random.random() * 0.5
                    last_error = f"HTTP 429 uuid={actor_uuid}; retry_after={retry_after}; sleep={sleep_s:.1f}s; elapsed_ms={elapsed_ms:.0f}"
                    self.throttle_events.append({
                        "uuid": actor_uuid,
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
                    last_error = f"HTTP {resp.status_code} uuid={actor_uuid}; retry_after={retry_after}; sleep={sleep_s:.1f}s; elapsed_ms={elapsed_ms:.0f}"
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


def find_actors_raw_db(inputs_dir: Path) -> Optional[Path]:
    return find_db(inputs_dir, [
        "eudamed_platform_actors_raw_latest.duckdb",
        "eudamed_platform_actors_raw_latest.duckdb.zip",
    ])


def find_existing_details_db(inputs_dir: Path) -> Optional[Path]:
    return find_db(inputs_dir, [
        "eudamed_platform_actor_details_raw_latest.duckdb",
        "eudamed_platform_actor_details_raw_latest.duckdb.zip",
    ])


def read_actors_raw_rows(db_path: Path) -> List[Dict[str, Any]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        table = None
        for t in ["actors_raw", "actors", "eos"]:
            if table_exists(con, t):
                table = t
                break
        if not table:
            return []
        cols = {r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
        wanted = [
            "uuid", "ulid", "srn", "eudamed_identifier", "latest_version", "version_number",
            "actor_type_code", "actor_type_srn_code", "actor_type_category", "country_iso2_code", "name",
        ]
        select_cols = [c for c in wanted if c in cols]
        df = con.execute(f"SELECT {', '.join(select_cols)} FROM {table} WHERE uuid IS NOT NULL").fetchdf()
        records = df.to_dict("records")
        out: List[Dict[str, Any]] = []
        for r in records:
            d = {c: r.get(c) for c in wanted}
            if d.get("eudamed_identifier") is None:
                d["eudamed_identifier"] = d.get("srn")
            if d.get("srn") is None:
                d["srn"] = d.get("eudamed_identifier")
            out.append(d)
        return out
    finally:
        con.close()


def read_existing_detail_rows(db_path: Optional[Path]) -> List[Dict[str, Any]]:
    if not db_path or not db_path.exists():
        return []
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        table = None
        for t in ["actor_details_raw", "details_raw"]:
            if table_exists(con, t):
                table = t
                break
        if not table:
            return []
        df = con.execute(f"SELECT * FROM {table}").fetchdf()
        records = df.to_dict("records")
        out: List[Dict[str, Any]] = []
        for r in records:
            d = {c: None for c in DETAIL_COLUMNS}
            for c in DETAIL_COLUMNS:
                if c in r:
                    d[c] = r.get(c)
            out.append(d)
        return out
    finally:
        con.close()


def read_previous_metadata(inputs_dir: Path) -> Dict[str, Any]:
    path = inputs_dir / "eudamed_platform_actor_details_raw_latest.metadata.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"WARNING could not read previous metadata {path}: {e}")
        return {}


def extract_resume_state(previous_metadata: Dict[str, Any], requested_mode: str) -> Dict[str, Any]:
    if not previous_metadata:
        return {}
    state = previous_metadata.get("resume_state") or (previous_metadata.get("audit") or {}).get("resume_state") or {}
    if not state:
        return {}
    base_mode = requested_mode.replace("resume_", "")
    if state.get("base_mode") and state.get("base_mode") != base_mode:
        return {}
    return state


def boolish_true(v: Any) -> bool:
    return v is True or str(v).strip().lower() == "true"


def identity_key(row: Dict[str, Any]) -> str:
    return str(row.get("ulid") or row.get("srn") or row.get("eudamed_identifier") or row.get("uuid") or "")


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


def current_candidate_uuids(actors: List[Dict[str, Any]]) -> set:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in actors:
        u = r.get("uuid")
        if not u:
            continue
        groups.setdefault(identity_key(r), []).append(r)
    chosen = set()
    for _, rows in groups.items():
        rows_sorted = sorted(
            rows,
            key=lambda r: (
                1 if boolish_true(r.get("latest_version")) else 0,
                int_or_minus(r.get("version_number")),
                str(r.get("ulid") or ""),
                str(r.get("uuid") or ""),
            ),
            reverse=True,
        )
        if rows_sorted:
            chosen.add(rows_sorted[0].get("uuid"))
    return chosen


def choose_candidates(mode: str, actors: List[Dict[str, Any]], existing_detail_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    dedup: Dict[str, Dict[str, Any]] = {}
    for r in actors:
        u = r.get("uuid")
        if not u:
            continue
        old = dedup.get(u)
        if old is None:
            dedup[u] = r
        else:
            if (str(r.get("ulid") or ""), int_or_minus(r.get("version_number"))) >= (str(old.get("ulid") or ""), int_or_minus(old.get("version_number"))):
                dedup[u] = r
    all_rows = list(dedup.values())
    current_uuids = current_candidate_uuids(all_rows)
    existing_uuids = {r.get("uuid") for r in existing_detail_rows if r.get("uuid")}
    base_mode = mode.replace("resume_", "")
    if base_mode == "full":
        candidates = all_rows
    elif base_mode == "current":
        candidates = [r for r in all_rows if r.get("uuid") in current_uuids]
    elif base_mode == "historical":
        candidates = [r for r in all_rows if r.get("uuid") not in current_uuids]
    elif base_mode == "incremental":
        candidates = [r for r in all_rows if r.get("uuid") not in existing_uuids]
    else:
        raise ValueError(f"Unsupported mode: {mode}")
    candidates = sorted(candidates, key=lambda r: (str(r.get("ulid") or ""), int_or_minus(r.get("version_number")), str(r.get("uuid") or "")))
    stats = {
        "actors_raw_rows": len(actors),
        "actors_raw_distinct_uuid_count": len(all_rows),
        "current_candidate_count": len(current_uuids),
        "existing_detail_uuid_count": len(existing_uuids),
        "candidate_count": len(candidates),
        "base_mode": base_mode,
    }
    return candidates, stats


def progress_line(mode: str, idx: int, total: int, ok_count: int, fail_count: int, started_at: float, response_ms: List[float], throttle_count: int) -> str:
    elapsed = max(0.001, time.monotonic() - started_at)
    done = idx + 1
    rate = done / elapsed
    remaining = max(0, total - done)
    eta = remaining / rate if rate > 0 else None
    avg_ms = sum(response_ms) / len(response_ms) if response_ms else 0.0
    return (
        f"{mode} detail={done}/{total} ({(done/total*100.0 if total else 0):.2f}%) | "
        f"ok={ok_count:,} | failed={fail_count:,} | rate={rate:.3f} req/s | avg_response={avg_ms:.0f} ms | "
        f"429_count={throttle_count} | elapsed={fmt_duration(elapsed)} | ETA={fmt_duration(eta)}"
    )


def runtime_exceeded(started_at: float, max_runtime_hours: float) -> bool:
    return bool(max_runtime_hours and max_runtime_hours > 0 and ((time.monotonic() - started_at) / 3600.0) >= max_runtime_hours)


def fetch_details(
    client: EudamedClient,
    candidates: List[Dict[str, Any]],
    start_index: int,
    extract_date: str,
    extract_ts: str,
    mode: str,
    max_records: int,
    max_runtime_hours: float,
    max_429_before_partial: int,
    log_every_records: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], bool]:
    total = len(candidates)
    rows: List[Dict[str, Any]] = []
    request_log: List[Dict[str, Any]] = []
    started_at = time.monotonic()
    response_ms: List[float] = []
    status_counts: Dict[int, int] = {}
    ok_count = 0
    fail_count = 0
    normal_completion = True
    stop_reason = "all_candidates_fetched"
    last_successful_index: Optional[int] = None
    processed = 0

    log(f"=== Actor Details fetch mode={mode} start_index={start_index} total_candidates={total} ===")
    for idx in range(start_index, total):
        if max_records and max_records > 0 and processed >= max_records:
            normal_completion = False
            stop_reason = "max_records_cap_reached"
            break
        if runtime_exceeded(started_at, max_runtime_hours):
            normal_completion = False
            stop_reason = "runtime_limit"
            break
        actor_uuid = str(candidates[idx].get("uuid"))
        status, data, error, elapsed_ms, retry_after, url = client.fetch_actor_detail(actor_uuid)
        status_counts[int(status or 0)] = status_counts.get(int(status or 0), 0) + 1
        response_ms.append(float(elapsed_ms or 0.0))
        processed += 1
        request_log.append({
            "endpoint": "actors_detail",
            "uuid": actor_uuid,
            "candidate_index": idx,
            "status_code": status,
            "elapsed_ms": elapsed_ms,
            "retry_after": retry_after,
            "error": error,
            "requested_url": url,
            "requested_at_utc": iso_utc_now(),
        })
        if status == 200 and data:
            row = flatten_detail(data, actor_uuid, extract_date, extract_ts)
            # Defensive fallback if API returns no embedded uuid.
            if not row.get("uuid"):
                row["uuid"] = actor_uuid
            rows.append(row)
            ok_count += 1
            last_successful_index = idx
        else:
            fail_count += 1
            normal_completion = False
            stop_reason = "detail_fetch_failed"
            log(f"WARNING detail fetch failed uuid={actor_uuid} status={status} error={error}")
            break
        if max_429_before_partial and len(client.throttle_events) >= max_429_before_partial:
            normal_completion = False
            stop_reason = "429_limit"
            break
        should_log = (processed == 1 or processed % max(1, log_every_records) == 0 or idx == total - 1)
        if should_log:
            log(progress_line(mode, idx, total, ok_count, fail_count, started_at, response_ms, len(client.throttle_events)))

    next_index = (last_successful_index + 1) if last_successful_index is not None else start_index
    if next_index >= total and fail_count == 0:
        normal_completion = True
        stop_reason = "all_candidates_fetched"
    audit = {
        "mode": mode,
        "base_mode": mode.replace("resume_", ""),
        "start_index": start_index,
        "total_candidates": total,
        "processed_candidates": processed,
        "successful_details": ok_count,
        "failed_details": fail_count,
        "normal_completion": normal_completion,
        "stop_reason": stop_reason,
        "last_successful_index": last_successful_index,
        "next_index": next_index,
        "resume_state": None if normal_completion else {
            "resume_mode": f"resume_{mode.replace('resume_', '')}",
            "base_mode": mode.replace("resume_", ""),
            "last_successful_index": last_successful_index,
            "next_index": next_index,
            "total_candidates": total,
            "stop_reason": stop_reason,
        },
        "telemetry": {
            "elapsed_seconds": time.monotonic() - started_at,
            "requests_per_second": processed / max(0.001, time.monotonic() - started_at),
            "avg_response_ms": (sum(response_ms) / len(response_ms)) if response_ms else None,
            "status_counts": {str(k): v for k, v in sorted(status_counts.items())},
            "throttle_429_count": len(client.throttle_events),
            "throttle_events": client.throttle_events,
        },
    }
    return rows, request_log, audit, normal_completion


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
        r_key = str(r.get("EXTRACT_DATETIME_UTC") or "")
        old_key = str(old.get("EXTRACT_DATETIME_UTC") or "")
        if r_key >= old_key:
            seen[u] = r
    return list(seen.values()) + no_uuid


def merge_rows(previous_rows: List[Dict[str, Any]], received_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    prev = {r.get("uuid") for r in previous_rows if r.get("uuid")}
    recv = {r.get("uuid") for r in received_rows if r.get("uuid")}
    merged = dedupe_by_uuid_choose_latest(previous_rows + received_rows)
    return merged, {
        "previous_rows": len(previous_rows),
        "received_rows": len(received_rows),
        "previous_uuid_count": len(prev),
        "received_uuid_count": len(recv),
        "inserted_uuid_count": len(recv - prev),
        "refreshed_uuid_count": len(recv & prev),
        "retained_uuid_count": len(prev - recv),
        "merged_rows": len(merged),
    }


def flatten_for_table(obj: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in obj.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False)
        else:
            out[k] = v
    return out


def write_duckdb(out_db: Path, rows: List[Dict[str, Any]], request_log: List[Dict[str, Any]], candidates: List[Dict[str, Any]], metadata: Dict[str, Any]) -> None:
    safe_unlink(out_db)
    con = duckdb.connect(str(out_db))
    try:
        con.register("detail_df", pd.DataFrame(rows, columns=DETAIL_COLUMNS))
        con.execute("CREATE TABLE actor_details_raw AS SELECT * FROM detail_df")
        con.register("request_log_df", pd.DataFrame(request_log))
        con.execute("CREATE TABLE api_request_log AS SELECT * FROM request_log_df")
        con.register("candidate_df", pd.DataFrame(candidates))
        con.execute("CREATE TABLE candidate_inventory AS SELECT * FROM candidate_df")
        field_rows = []
        for col in DETAIL_COLUMNS:
            field_rows.append({"table_name": "actor_details_raw", "field_name": col, "non_null_rows": sum(1 for r in rows if r.get(col) is not None), "total_rows": len(rows)})
        con.register("field_inventory_df", pd.DataFrame(field_rows))
        con.execute("CREATE TABLE field_inventory AS SELECT * FROM field_inventory_df")
        con.register("metadata_df", pd.DataFrame([flatten_for_table(metadata)]))
        con.execute("CREATE TABLE pipeline_metadata AS SELECT * FROM metadata_df")
    finally:
        con.close()


def write_csv_zip(zip_path: Path, rows: List[Dict[str, Any]]) -> None:
    safe_unlink(zip_path)
    tmp = zip_path.parent / "_csv_tmp_actor_details"
    if tmp.exists():
        shutil.rmtree(tmp)
    ensure_dir(tmp)
    csv_path = tmp / "actor_details_raw.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=DETAIL_COLUMNS, extrasaction="ignore", quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for r in rows:
            writer.writerow({c: r.get(c) for c in DETAIL_COLUMNS})
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        zf.write(csv_path, "actor_details_raw.csv")
    shutil.rmtree(tmp)


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_release_notes(path: Path, metadata: Dict[str, Any]) -> None:
    lines = [
        "# EUDAMED Platform Actor Details Raw",
        "",
        f"Pipeline version: `{metadata.get('pipeline_version')}`",
        f"Generated at UTC: `{metadata.get('generated_at_utc')}`",
        f"Run status: `{metadata.get('run_status')}`",
        f"Requested mode: `{metadata.get('requested_mode')}`",
        f"Effective mode: `{metadata.get('effective_mode')}`",
        "",
        "## Snapshot",
        "",
        f"- Rows in latest DB: `{metadata.get('row_count')}`",
        f"- Distinct UUIDs: `{metadata.get('distinct_uuid_count')}`",
        f"- Distinct ULIDs: `{metadata.get('distinct_ulid_count')}`",
        f"- Distinct SRNs: `{metadata.get('distinct_srn_count')}`",
        f"- latest_version=true rows: `{metadata.get('latest_version_true_count')}`",
        f"- latest_version=false rows: `{metadata.get('latest_version_false_count')}`",
        f"- PRRC count > 0 rows: `{metadata.get('prrc_present_count')}`",
        f"- Authorised representative present rows: `{metadata.get('authorised_representative_present_count')}`",
        f"- Validator present rows: `{metadata.get('validator_present_count')}`",
        "",
        "## Candidate selection",
        "",
        f"- Actors Raw rows: `{metadata.get('candidate_stats', {}).get('actors_raw_rows')}`",
        f"- Actors Raw distinct UUIDs: `{metadata.get('candidate_stats', {}).get('actors_raw_distinct_uuid_count')}`",
        f"- Current candidates: `{metadata.get('candidate_stats', {}).get('current_candidate_count')}`",
        f"- Existing detail UUIDs before run: `{metadata.get('candidate_stats', {}).get('existing_detail_uuid_count')}`",
        f"- Selected candidates: `{metadata.get('candidate_stats', {}).get('candidate_count')}`",
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
        f"- Processed candidates: `{metadata.get('audit', {}).get('processed_candidates')}`",
        f"- Successful details: `{metadata.get('audit', {}).get('successful_details')}`",
        f"- Failed details: `{metadata.get('audit', {}).get('failed_details')}`",
        "",
        "## Resume state",
        "",
        f"- Resume state JSON: `{json.dumps(metadata.get('resume_state') or {}, ensure_ascii=False)}`",
        "",
        "## Telemetry",
        "",
        f"- Requests/sec: `{(metadata.get('telemetry') or {}).get('requests_per_second')}`",
        f"- Avg response ms: `{(metadata.get('telemetry') or {}).get('avg_response_ms')}`",
        f"- 429 count: `{(metadata.get('telemetry') or {}).get('throttle_429_count')}`",
        "",
        "## Current actor selection rule",
        "",
        "- Primary rule: `latest_version=true`.",
        "- Fallback rule: highest `version_number` within the same ULID/SRN.",
        "- Actor Details Raw itself stores only raw API response fields and does not add derived current flags.",
        "",
        "## Interpretation",
        "",
        "- `COMPLETE` means the selected mode reached normal completion.",
        "- `PARTIAL` means useful data was received, but the selected mode did not reach normal completion. Latest is merged with previous latest so data is not lost.",
        "- `BOOTSTRAP` means no previous latest DB was available and a base was created.",
        "- `FAILED` means no usable details were received; latest DB is not updated.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def copy_file(src: Path, dst: Path) -> None:
    safe_unlink(dst)
    shutil.copy2(src, dst)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=sorted(DETAIL_MODES), default="incremental")
    p.add_argument("--out-dir", default="dist/eudamed_platform_actor_details_raw")
    p.add_argument("--inputs-dir", default="inputs")
    p.add_argument("--base-url", default=BASE_URL_DEFAULT)
    p.add_argument("--language", default="en")
    p.add_argument("--max-records", type=int, default=0, help="Optional candidate cap for testing. 0 = no cap")
    p.add_argument("--max-rps", type=float, default=1.0)
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--retries", type=int, default=4)
    p.add_argument("--backoff", type=float, default=1.5)
    p.add_argument("--max-429-before-partial", type=int, default=7)
    p.add_argument("--max-runtime-hours", type=float, default=5.5)
    p.add_argument("--resume-overlap-records", type=int, default=25)
    p.add_argument("--log-every-records", type=int, default=100)
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

    log(f"=== EUDAMED Platform Actor Details Raw {PIPELINE_VERSION} acquisition started ===")
    log(f"requested_mode={args.mode} out_dir={out_dir} max_rps={args.max_rps}")
    log("scope=raw_fetch_only cdc=0 canonical=0 dk_subset=0 domain=actor_details")

    actors_db = find_actors_raw_db(inputs_dir)
    if not actors_db:
        log("ERROR: Missing actors raw latest DB in inputs")
        return 2
    log(f"Actors Raw DB found: {actors_db}")
    actors = read_actors_raw_rows(actors_db)
    log(f"Loaded actors_raw rows: {len(actors):,}")
    if not actors:
        log("ERROR: No actors raw rows found")
        return 2

    existing_db = find_existing_details_db(inputs_dir)
    previous_rows: List[Dict[str, Any]] = []
    if existing_db:
        log(f"Previous Actor Details Raw latest DB found: {existing_db}")
        previous_rows = read_existing_detail_rows(existing_db)
        log(f"Loaded previous detail rows: {len(previous_rows):,}")
    else:
        log("No previous Actor Details Raw latest DB found")

    previous_metadata = read_previous_metadata(inputs_dir)
    candidates, candidate_stats = choose_candidates(args.mode, actors, previous_rows)
    if not candidates:
        log("No candidates selected. Run can complete without API calls.")

    start_index = 0
    effective_mode = args.mode
    if args.mode.startswith("resume_"):
        state = extract_resume_state(previous_metadata, args.mode)
        if state:
            start_index = max(0, int(state.get("next_index") or 0) - max(0, int(args.resume_overlap_records or 0)))
            log(f"Resume state found: next_index={state.get('next_index')} overlap={args.resume_overlap_records} start_index={start_index}")
        else:
            effective_mode = args.mode.replace("resume_", "")
            log(f"No compatible resume state found. Falling back to effective_mode={effective_mode}")
    elif not previous_rows and args.mode == "incremental":
        # Incremental without previous detail DB is a bootstrap detail run over all missing UUIDs.
        log("Incremental requested and no previous detail DB exists. This is a bootstrap incremental over all actors_raw UUIDs.")

    client = EudamedClient(args.base_url, args.language, args.timeout, args.retries, args.backoff, args.max_rps)
    received_rows, request_log, audit, normal_completion = fetch_details(
        client=client,
        candidates=candidates,
        start_index=start_index,
        extract_date=extract_date,
        extract_ts=extract_ts,
        mode=effective_mode,
        max_records=args.max_records,
        max_runtime_hours=args.max_runtime_hours,
        max_429_before_partial=args.max_429_before_partial,
        log_every_records=args.log_every_records,
    )

    usable_received = len(received_rows)
    if usable_received == 0 and len(candidates) > 0:
        run_status = RUN_FAILED
    elif not previous_rows and normal_completion:
        run_status = RUN_BOOTSTRAP
    elif normal_completion:
        run_status = RUN_COMPLETE
    else:
        run_status = RUN_PARTIAL

    if run_status == RUN_FAILED:
        merged_rows = previous_rows
        merge_stats = {
            "previous_rows": len(previous_rows),
            "received_rows": 0,
            "previous_uuid_count": len({r.get("uuid") for r in previous_rows if r.get("uuid")}),
            "received_uuid_count": 0,
            "inserted_uuid_count": 0,
            "refreshed_uuid_count": 0,
            "retained_uuid_count": len({r.get("uuid") for r in previous_rows if r.get("uuid")}),
            "merged_rows": len(previous_rows),
        }
    elif run_status in {RUN_COMPLETE, RUN_BOOTSTRAP} and effective_mode == "full":
        merged_rows = dedupe_by_uuid_choose_latest(received_rows)
        previous_uuid_set = {r.get("uuid") for r in previous_rows if r.get("uuid")}
        received_uuid_set = {r.get("uuid") for r in received_rows if r.get("uuid")}
        merge_stats = {
            "previous_rows": len(previous_rows),
            "received_rows": len(received_rows),
            "previous_uuid_count": len(previous_uuid_set),
            "received_uuid_count": len(received_uuid_set),
            "inserted_uuid_count": len(received_uuid_set - previous_uuid_set),
            "refreshed_uuid_count": len(received_uuid_set & previous_uuid_set),
            "retained_uuid_count": 0,
            "merged_rows": len(merged_rows),
        }
    else:
        merged_rows, merge_stats = merge_rows(previous_rows, received_rows)

    row_count = len(merged_rows)
    min_u = min_ulid(r.get("ulid") for r in merged_rows)
    max_u = max_ulid(r.get("ulid") for r in merged_rows)
    latest_true = sum(1 for r in merged_rows if boolish_true(r.get("latest_version")))
    latest_false = sum(1 for r in merged_rows if str(r.get("latest_version")).strip().lower() == "false" or r.get("latest_version") is False)

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
        "base_url": args.base_url,
        "language": args.language,
        "max_records": args.max_records,
        "max_rps": args.max_rps,
        "max_429_before_partial": args.max_429_before_partial,
        "max_runtime_hours": args.max_runtime_hours,
        "resume_overlap_records": args.resume_overlap_records,
        "log_every_records": args.log_every_records,
        "retries": args.retries,
        "actors_raw_db_found": str(actors_db),
        "previous_detail_db_found": str(existing_db) if existing_db else None,
        "candidate_stats": candidate_stats,
        "row_count": row_count,
        "distinct_uuid_count": len({r.get("uuid") for r in merged_rows if r.get("uuid")}),
        "distinct_ulid_count": len({r.get("ulid") for r in merged_rows if r.get("ulid")}),
        "distinct_srn_count": len({r.get("srn") for r in merged_rows if r.get("srn")}),
        "latest_version_true_count": latest_true,
        "latest_version_false_count": latest_false,
        "prrc_present_count": sum(1 for r in merged_rows if int_or_minus(r.get("prrc_count")) > 0),
        "authorised_representative_present_count": sum(1 for r in merged_rows if r.get("authorised_representative_uuid") or r.get("authorised_representative_srn")),
        "validator_present_count": sum(1 for r in merged_rows if r.get("validator_uuid") or r.get("validator_srn")),
        "min_ulid": min_u,
        "min_ulid_timestamp": decode_ulid(min_u),
        "max_ulid": max_u,
        "max_ulid_timestamp": decode_ulid(max_u),
        "previous_rows": len(previous_rows),
        "received_rows": len(received_rows),
        "normal_completion": normal_completion,
        "audit": audit,
        "resume_state": audit.get("resume_state"),
        "merge": merge_stats,
        "request_status_summary": [{"endpoint": k[0], "status_code": k[1], "count": v} for k, v in sorted(status_summary.items())],
        "telemetry": audit.get("telemetry"),
    }

    latest_db = out_dir / "eudamed_platform_actor_details_raw_latest.duckdb"
    latest_csv = out_dir / "eudamed_platform_actor_details_raw_latest_csv.zip"
    latest_meta = out_dir / "eudamed_platform_actor_details_raw_latest.metadata.json"
    latest_notes = out_dir / "RELEASE_NOTES_EUDAMED_PLATFORM_ACTOR_DETAILS_RAW.md"
    dated_db = out_dir / f"eudamed_platform_actor_details_raw_{rel_ts}.duckdb"
    dated_csv = out_dir / f"eudamed_platform_actor_details_raw_{rel_ts}_csv.zip"
    dated_meta = out_dir / f"eudamed_platform_actor_details_raw_{rel_ts}.metadata.json"
    dated_notes = out_dir / f"RELEASE_NOTES_EUDAMED_PLATFORM_ACTOR_DETAILS_RAW_{rel_ts}.md"

    if run_status == RUN_FAILED:
        log("RUN_STATUS=FAILED. Writing metadata/notes only; not writing latest DB.")
        write_json(latest_meta, metadata)
        write_json(dated_meta, metadata)
        write_release_notes(latest_notes, metadata)
        write_release_notes(dated_notes, metadata)
        return 2

    log(f"Writing DuckDB: {latest_db}")
    write_duckdb(latest_db, merged_rows, request_log, candidates, metadata)
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

    log("=== EUDAMED Platform Actor Details Raw complete ===")
    log(json.dumps({
        "run_status": run_status,
        "requested_mode": args.mode,
        "effective_mode": effective_mode,
        "row_count": row_count,
        "received_rows": len(received_rows),
        "previous_rows": len(previous_rows),
        "candidate_stats": candidate_stats,
        "merge": merge_stats,
        "resume_state": metadata.get("resume_state"),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
