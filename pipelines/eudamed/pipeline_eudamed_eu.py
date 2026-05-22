
from __future__ import annotations

import argparse
import zipfile
import gc
import shutil
import hashlib
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Any

import duckdb
import pandas as pd
import requests


# =============================================================================
# EUDAHUB INTELLIGENCE - EUDAMED EU PIPELINE
# =============================================================================
#
# Main goals in this version:
# - Canonical EU layer:
#     reference, actors, udi
#     reference_change_events, actors_change_events, udi_change_events
#
# - Danish intelligence layer:
#     actor_dk_intel
#     udi_dk_intel
#     udi_dk_intel_change_events
#
# - CDC architecture:
#     EXTRACT_DATE
#     EXTRACT_DATETIME_UTC
#     FIRST_SEEN_DATE
#     FIRST_SEEN_DATETIME_UTC
#     LAST_SEEN_DATE
#     LAST_SEEN_DATETIME_UTC
#     CHANGE_TYPE
#     CHANGED_COLUMNS
#     CHANGED_COLUMNS_COUNT
#     CHANGE_SUMMARY
#     BUSINESS_KEY
#     ENTITY_VARIANT_KEY
#     ROW_HASH
#     [source columns...]
#
# - Important implementation rules:
#     * No MASTER_DB.
#     * Previous state is downloaded directly as eudamed_latest.duckdb.
#     * Current run is streamed into eudamed_temp.duckdb.
#     * Final release artifact is eudamed.duckdb.
#     * Large CDC transitions are DuckDB SQL only, not Pandas.
#     * Pandas is only used for small batches/reporting.
#     * Change details are only populated for real update events, not NEW.
#     * Change severity is calculated from canonical IDs/column names, not labels.
#     * Danish labels are derived from reference using ID + CODE + LANGUAGE='da'.
#
# =============================================================================


# =============================================================================
# CONFIG
# =============================================================================

BASE_URL = "https://api.datalake.sante.service.ec.europa.eu/eudamed"

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_FILE = SCRIPT_DIR / "config" / "eudamed_filters.json"

STATE_DB = "eudamed_latest.duckdb"
TEMP_DB = "eudamed_temp.duckdb"
EXPORT_DB = "eudamed.duckdb"
NEXTLINK_DB = "eudamed_nextlinks.duckdb"

RUN_STATS_FILE = "run_stats.json"
RELEASE_NOTES_FILE = "RELEASE_NOTES.md"
NEXTLINK_RELEASE_NOTES_FILE = "NEXTLINK_RELEASE_NOTES.md"

MAX_WORKERS = 4
PROCESS_WORKERS = 1
BATCH_SIZE = 25_000
PROCESS_QUEUE_MAXSIZE = 20
WRITE_QUEUE_MAXSIZE = 8
LOG_PAGES_EVERY = 100
LOG_WRITES_EVERY = 5

RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
REQUEST_TIMEOUT = 180

TABLES = ["reference", "actors", "udi"]

PIPELINE_VERSION = "1.0.21"

# =============================================================================
# CHANGE EVENT CLEANUP CONFIG
# =============================================================================
#
# Default is False: pipeline never deletes change history unless explicitly enabled.
# When True, only rows matching CHANGE_EVENT_CLEANUP_DATES + CHANGE_EVENT_CLEANUP_TYPES
# in CHANGE_EVENT_CLEANUP_TABLES are deleted. Nothing else is touched.
#
# Use this for controlled schema/hash migration cleanup only.
ENABLE_CHANGE_EVENT_CLEANUP = False

CHANGE_EVENT_CLEANUP_DATES = [
    # "2026-05-20",
]

CHANGE_EVENT_CLEANUP_TYPES = [
    "NEW",
    "UPDATED",
    "UPDATED_PRRC",
    "UPDATED_RISK_CLASS",
    "UPDATED_LEGISLATION",
    "UPDATED_STATUS",
    "UPDATED_DEVICE_STATUS_TYPE",
    "UPDATED_ACTOR_RELATION",
    "UPDATED_DEVICE_NAME",
    # "KEY_MISSING",  # Usually important; include only for full migration cleanup.
]

CHANGE_EVENT_CLEANUP_TABLES = [
    "reference_change_events",
    "actors_change_events",
    "udi_change_events",
    "udi_dk_intel_change_events",
]

# Only EUDAMED reference/enumeration IDs.
# These are values that map against reference.ID + reference.CODE + reference.LANGUAGE.
# They are stored as VARCHAR while preserving the source representation, e.g. "-203.0".
REFERENCE_ID_COLUMNS = [
    "ID",
    "PARENT_ID",
    "REFERENCE_ID",
    "RISK_CLASS_ID",
    "APPLICABLE_LEGISLATION_ID",
    "DEVICE_STATUS_ID",
    "DEVICE_STATUS_TYPE_ID",
    "STATUS_ID",
    "PLACED_ON_THE_MARKET_ID",
    "SPECIAL_DEVICE_TYPE_ID",
    "MULTI_COMPONENT_ID",
    "STORAGE_CONDITIONS_ID",
    "STERILE_ID",
]

# Stable entity identity
BUSINESS_KEYS = {
    "reference": ["ID", "CODE", "LANGUAGE"],
    "actors": ["ACTOR_ID"],
    "udi": ["UUID"],
}

# Variant/sub-entity identity
ENTITY_VARIANT_KEYS = {
    "reference": ["ID", "CODE", "LANGUAGE"],
    "actors": ["ACTOR_ID", "PRRC_FIRST_NAME", "PRRC_FAMILY_NAME"],
    "udi": ["UUID"],
}

REQUIRED_BUSINESS_KEYS = {
    "reference": ["ID", "CODE", "LANGUAGE"],
    "actors": ["ACTOR_ID"],
    "udi": ["UUID"],
}

CDC_COLUMNS = [
    "EXTRACT_DATE",
    "EXTRACT_DATETIME_UTC",
    "FIRST_SEEN_DATE",
    "FIRST_SEEN_DATETIME_UTC",
    "LAST_SEEN_DATE",
    "LAST_SEEN_DATETIME_UTC",
    "CHANGE_TYPE",
    "CHANGED_COLUMNS",
    "CHANGED_COLUMNS_COUNT",
    "CHANGE_SUMMARY",
    "BUSINESS_KEY",
    "ENTITY_VARIANT_KEY",
    "ROW_HASH",
]

ACTOR_DK_INTEL_PREVIOUS_METRIC_COLUMNS = [
    ("UDI_DEVICE_COUNT_FIRST_SEEN", "BIGINT"),
    ("UDI_DEVICE_COUNT", "BIGINT"),
    ("DOMINANT_RISK_CLASS_DEVICE_COUNT", "BIGINT"),
    ("HIGHEST_MDR_MDD_RISK_CLASS_DEVICE_COUNT", "BIGINT"),
    ("HIGHEST_IVDR_IVDD_RISK_CLASS_DEVICE_COUNT", "BIGINT"),
    ("DOMINANT_LEGISLATION", "VARCHAR"),
    ("DOMINANT_LEGISLATION_DEVICE_COUNT", "BIGINT"),
    ("LATEST_MDR_MDD_LEGISLATION_DEVICE_COUNT", "BIGINT"),
    ("LATEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT", "BIGINT"),
    ("OLDEST_MDR_MDD_LEGISLATION_DEVICE_COUNT", "BIGINT"),
    ("OLDEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT", "BIGINT"),
    ("MDR_MIGRATION_SCORE", "DOUBLE"),
    ("IVDR_MIGRATION_SCORE", "DOUBLE"),
]

TRACKING_COLUMNS = set(CDC_COLUMNS)

RISK_CLASS_COLUMN_CANDIDATES = [
    "RISK_CLASS_ID",
    "RISK_CLASS",
    "DEVICE_RISK_CLASS",
    "RISK_CLASS_CODE",
    "RISK_CLASS_VALUE",
    "MD_RISK_CLASS",
    "MDR_RISK_CLASS",
    "RISK_CLASSIFICATION",
    "RISK_CLASSIFICATION_CODE",
    "RISK_CLASS_TEXT",
]

LEGISLATION_COLUMN_CANDIDATES = [
    "APPLICABLE_LEGISLATION_ID",
    "LEGISLATION_ID",
]

STATUS_COLUMN_CANDIDATES = [
    "STATUS_ID",
    "DEVICE_STATUS_TYPE_ID",
    "DEVICE_STATUS_ID",
]

DEVICE_NAME_CANDIDATES = [
    "DEVICE_TRADE_NAME",
    "TRADE_NAME",
    "DEVICE_NAME",
    "NAME",
    "MODEL",
    "DEVICE_MODEL",
    "BRAND_NAME",
]

PRIMARY_DI_CANDIDATES = [
    "PRIMARY_DI",
    "PRIMARY_DI_CODE",
    "DI",
    "UDI_DI",
]

BASIC_UDI_CANDIDATES = [
    "BASIC_UDI_DI",
    "BASIC_UDI",
    "BASIC_UDI_DI_CODE",
]

MF_SRN_CANDIDATES = [
    "MF_SRN",
    "MANUFACTURER_SRN",
    "SRN",
    "SOURCE_ACTOR_ID",
]

MF_NAME_CANDIDATES = [
    "MF_NAME",
    "MANUFACTURER_NAME",
    "MANUFACTURER",
    "ACTOR_NAME",
    "ORGANISATION_NAME",
    "ORGANIZATION_NAME",
    "COMPANY_NAME",
]

AR_SRN_CANDIDATES = [
    "AR_SRN",
    "AUTHORISED_REPRESENTATIVE_SRN",
    "AUTHORIZED_REPRESENTATIVE_SRN",
    "REPRESENTATIVE_SRN",
]

AR_NAME_CANDIDATES = [
    "AR_NAME",
    "AUTHORISED_REPRESENTATIVE_NAME",
    "AUTHORIZED_REPRESENTATIVE_NAME",
    "REPRESENTATIVE_NAME",
]

ACTOR_NAME_CANDIDATES = [
    "NAME",
    "ACTOR_NAME",
    "ORGANISATION_NAME",
    "ORGANIZATION_NAME",
    "COMPANY_NAME",
    "LEGAL_NAME",
    "ACTOR_LEGAL_NAME",
]

ACTOR_TYPE_CANDIDATES = [
    "ACTOR_TYPE",
    "ACTOR_TYPE_CODE",
    "ACTOR_ROLE",
    "ROLE",
    "ACTOR_ROLE_CODE",
]

HEADERS = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
}

PROCESS_QUEUE: Queue = Queue(maxsize=PROCESS_QUEUE_MAXSIZE)
WRITE_QUEUE: Queue = Queue(maxsize=WRITE_QUEUE_MAXSIZE)
STOP = object()

NEXTLINK_EVENTS: list[dict[str, Any]] = []
NEXTLINK_LOCK = threading.Lock()


# =============================================================================
# TIME / SQL HELPERS
# =============================================================================

def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def utc_date_string(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def utc_datetime_string(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def now_date() -> str:
    return utc_date_string(utc_now_dt())


def q(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def safe_sql(value: str) -> str:
    return str(value).replace("'", "''")


def md5_text(value: str | None) -> str | None:
    if value is None:
        return None
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def id_norm_expr(expr: str) -> str:
    # EUDAMED reference IDs often appear as -203, while API parameters/data can be -203.0.
    return f"regexp_replace(trim(cast({expr} AS VARCHAR)), '\\\\.0$', '')"


def should_track_nextlinks(mode: str) -> bool:
    return mode in {"partitioned_nextlink", "unfiltered_nextlink"}


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {q(table_name)} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def attached_table_exists(
    con: duckdb.DuckDBPyConnection,
    schema_name: str,
    table_name: str,
) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {q(schema_name)}.{q(table_name)} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def get_table_columns(con, table_name, schema_name=None):
    """
    Return column names for a DuckDB table.

    Supports:
    - Main database tables
    - Attached databases/schemas
      Example:
          get_table_columns(con, "actor_dk_intel")
          get_table_columns(con, "actor_dk_intel", schema_name="state_db")
    """

    try:
        if schema_name:
            rows = con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = ?
                  AND table_name = ?
                ORDER BY ordinal_position
                """,
                [schema_name, table_name],
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = ?
                ORDER BY ordinal_position
                """,
                [table_name],
            ).fetchall()

        columns = [r[0] for r in rows]

        if not columns:
            print(
                f"WARNING get_table_columns: no columns found for "
                f"{schema_name + '.' if schema_name else ''}{table_name}"
            )

        return columns

    except Exception as e:
        print(
            f"ERROR get_table_columns failed for "
            f"{schema_name + '.' if schema_name else ''}{table_name}: {e}"
        )
        return []

def get_attached_table_columns(
    con: duckdb.DuckDBPyConnection,
    schema_name: str,
    table_name: str,
) -> list[str]:
    return [
        row[0]
        for row in con.execute(f"DESCRIBE {q(schema_name)}.{q(table_name)}").fetchall()
    ]


def reference_id_columns_in(columns: list[str]) -> list[str]:
    configured = {col.upper() for col in REFERENCE_ID_COLUMNS}
    return [col for col in columns if col.upper() in configured]


def normalize_reference_id_columns_in_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> None:
    """Force EUDAMED reference/enumeration IDs to VARCHAR without changing values.

    Important:
    - This does NOT strip ".0".
    - It preserves the source representation, e.g. "-203.0".
    - It only targets reference/enumeration IDs used for mapping to reference.
    - Natural identifiers such as ACTOR_ID, UUID, MF_SRN, AR_SRN are not touched.
    """
    if not table_exists(con, table_name):
        print(f"REFERENCE ID NORMALIZE {table_name}: skipped, table missing.", flush=True)
        return

    cols = get_table_columns(con, table_name)
    targets = reference_id_columns_in(cols)

    if not targets:
        print(f"REFERENCE ID NORMALIZE {table_name}: no configured reference-ID columns found.", flush=True)
        return

    started = time.perf_counter()

    for col in targets:
        con.execute(
            f"""
            UPDATE {q(table_name)}
            SET {q(col)} = NULLIF(TRIM(CAST({q(col)} AS VARCHAR)), '')
            WHERE {q(col)} IS NOT NULL
            """
        )

    elapsed = round(time.perf_counter() - started, 1)
    print(
        f"OK REFERENCE ID NORMALIZE {table_name}: cast to VARCHAR preserving values for {targets} in {elapsed}s",
        flush=True,
    )


def first_existing_column(columns: list[str], candidates: list[str]) -> str | None:
    lookup = {str(col).upper(): col for col in columns}
    for candidate in candidates:
        found = lookup.get(candidate.upper())
        if found:
            return found
    return None


def col_or_null(columns: list[str], candidates: list[str], alias: str, out_name: str) -> str:
    col = first_existing_column(columns, candidates)
    if not col:
        return f"CAST(NULL AS VARCHAR) AS {q(out_name)}"
    return f"CAST({alias}.{q(col)} AS VARCHAR) AS {q(out_name)}"


def normalize_value_expr(col: str, alias: str | None = None) -> str:
    prefix = f"{alias}." if alias else ""
    return f"COALESCE(NULLIF(TRIM(CAST({prefix}{q(col)} AS VARCHAR)), ''), '')"


def build_key_expr(
    key_cols: list[str],
    columns: list[str],
    alias: str | None = None,
) -> str | None:
    if not key_cols or any(col not in columns for col in key_cols):
        return None

    parts = []
    for col in key_cols:
        parts.append(f"'{col}='")
        parts.append(normalize_value_expr(col, alias))

    return f"concat_ws('|', {', '.join(parts)})"


def build_missing_key_expr(
    key_cols: list[str],
    columns: list[str],
    alias: str | None = None,
) -> str:
    if not key_cols or any(col not in columns for col in key_cols):
        return "TRUE"

    prefix = f"{alias}." if alias else ""
    checks = [
        (
            f"{prefix}{q(col)} IS NULL OR "
            f"TRIM(CAST({prefix}{q(col)} AS VARCHAR)) = '' OR "
            f"LOWER(TRIM(CAST({prefix}{q(col)} AS VARCHAR))) IN ('none', 'nan', 'nat')"
        )
        for col in key_cols
    ]
    return "(" + " OR ".join(checks) + ")"


def build_row_hash_expr(columns: list[str], alias: str | None = None) -> str:
    source_cols = [col for col in columns if col not in TRACKING_COLUMNS]
    prefix = f"{alias}." if alias else ""
    parts = [
        f"'{safe_sql(col)}=', COALESCE(CAST({prefix}{q(col)} AS VARCHAR), '')"
        for col in sorted(source_cols)
    ]
    if not parts:
        return "md5('')"
    return "md5(concat_ws('|', " + ", ".join(parts) + "))"


def build_changed_columns_expr(current_cols: list[str], previous_cols: list[str]) -> str:
    comparable = [
        col
        for col in current_cols
        if col in previous_cols and col not in TRACKING_COLUMNS
    ]

    if not comparable:
        return "CAST(NULL AS VARCHAR)"

    pieces = []
    for col in comparable:
        pieces.append(
            f"""
            CASE
                WHEN CAST(c.{q(col)} AS VARCHAR) IS DISTINCT FROM CAST(p.{q(col)} AS VARCHAR)
                THEN '{safe_sql(col)}'
                ELSE NULL
            END
            """
        )

    return "CAST(NULLIF(concat_ws('|', " + ", ".join(pieces) + "), '') AS VARCHAR)"


def changed_count_expr(changed_col_expr: str) -> str:
    return (
        f"CASE WHEN {changed_col_expr} IS NULL OR TRIM(CAST({changed_col_expr} AS VARCHAR)) = '' "
        f"THEN 0 ELSE LENGTH(CAST({changed_col_expr} AS VARCHAR)) "
        f"- LENGTH(REPLACE(CAST({changed_col_expr} AS VARCHAR), '|', '')) + 1 END"
    )


def ensure_state_db_exists() -> None:
    if os.path.exists(STATE_DB) and os.path.getsize(STATE_DB) > 0:
        print(f"OK Using existing state DB: {STATE_DB}", flush=True)
        return

    print(
        f"INFO No previous state DB found: {STATE_DB}. First run will mark rows as NEW.",
        flush=True,
    )


# =============================================================================
# CONFIG / HTTP
# =============================================================================

def load_config() -> dict[str, Any]:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def request_json(
    url: str,
    params: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, int | None, int | None, str | None, str | None]:
    last_status_code = None
    last_duration_ms = None
    last_request_url = url
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        started = time.perf_counter()
        try:
            response = requests.get(
                url,
                params=params,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )

            duration_ms = int((time.perf_counter() - started) * 1000)
            request_url = response.url
            status_code = response.status_code

            last_status_code = status_code
            last_duration_ms = duration_ms
            last_request_url = request_url

            if status_code in RETRY_STATUS_CODES:
                sleep_seconds = min(60, 2 ** attempt)
                print(
                    f"WARNING HTTP {status_code}. Retry {attempt}/{MAX_RETRIES}. "
                    f"Sleeping {sleep_seconds}s.",
                    flush=True,
                )
                time.sleep(sleep_seconds)
                continue

            if status_code >= 400:
                error_message = f"HTTP {status_code}"
                print(f"WARNING {error_message} for URL: {request_url}", flush=True)
                return None, status_code, duration_ms, request_url, error_message

            return response.json(), status_code, duration_ms, request_url, None

        except requests.exceptions.RequestException as e:
            duration_ms = int((time.perf_counter() - started) * 1000)
            last_duration_ms = duration_ms
            last_error = str(e)

            sleep_seconds = min(60, 2 ** attempt)
            print(
                f"WARNING Request error: {e}. Retry {attempt}/{MAX_RETRIES}. "
                f"Sleeping {sleep_seconds}s.",
                flush=True,
            )
            time.sleep(sleep_seconds)

    print(f"ERROR Failed after {MAX_RETRIES} retries: {url}", flush=True)
    return None, last_status_code, last_duration_ms, last_request_url, last_error


# =============================================================================
# NEXTLINK TRACKING
# =============================================================================

def collect_nextlink_event(
    enabled: bool,
    endpoint: str,
    label: str,
    page: int,
    rows_count: int,
    status_code: int | None,
    request_duration_ms: int | None,
    request_url: str | None,
    next_link: str | None,
    error_message: str | None = None,
) -> None:
    if not enabled:
        return

    page_hash_source = "|".join(
        [str(endpoint), str(label), str(page), str(request_url), str(next_link)]
    )

    event = {
        "EXTRACT_DATE": now_date(),
        "PAGE_HASH": md5_text(page_hash_source),
        "REQUEST_HASH": md5_text(request_url),
        "NEXT_LINK_HASH": md5_text(next_link),
        "ENDPOINT": endpoint,
        "LABEL": label,
        "PAGE": page,
        "ROWS_COUNT": rows_count,
        "STATUS_CODE": status_code,
        "REQUEST_DURATION_MS": request_duration_ms,
        "REQUEST_URL": request_url,
        "NEXT_LINK": next_link,
        "ERROR_MESSAGE": error_message,
    }

    with NEXTLINK_LOCK:
        NEXTLINK_EVENTS.append(event)


def write_nextlink_events_to_db() -> None:
    if not NEXTLINK_EVENTS:
        print("INFO No nextLink events to write.", flush=True)
        return

    con = duckdb.connect(NEXTLINK_DB)

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS nextlinks (
            EXTRACT_DATE VARCHAR,
            FIRST_SEEN_DATE VARCHAR,
            LAST_SEEN_DATE VARCHAR,
            CHANGE_TYPE VARCHAR,
            PAGE_HASH VARCHAR,
            REQUEST_HASH VARCHAR,
            NEXT_LINK_HASH VARCHAR,
            ENDPOINT VARCHAR,
            LABEL VARCHAR,
            PAGE INTEGER,
            ROWS_COUNT INTEGER,
            STATUS_CODE INTEGER,
            REQUEST_DURATION_MS INTEGER,
            REQUEST_URL VARCHAR,
            NEXT_LINK VARCHAR,
            ERROR_MESSAGE VARCHAR
        )
        """
    )

    df = pd.DataFrame(NEXTLINK_EVENTS)

    existing_hashes = {}
    try:
        existing = con.execute(
            """
            SELECT PAGE_HASH, MIN(FIRST_SEEN_DATE) AS FIRST_SEEN_DATE
            FROM nextlinks
            GROUP BY PAGE_HASH
            """
        ).df()
        existing_hashes = dict(zip(existing["PAGE_HASH"], existing["FIRST_SEEN_DATE"]))
    except Exception:
        existing_hashes = {}

    df["FIRST_SEEN_DATE"] = df["PAGE_HASH"].map(existing_hashes).fillna(df["EXTRACT_DATE"])
    df["LAST_SEEN_DATE"] = df["EXTRACT_DATE"]
    df["CHANGE_TYPE"] = df["PAGE_HASH"].apply(
        lambda x: "UNCHANGED" if x in existing_hashes else "NEW"
    )

    df = df[
        [
            "EXTRACT_DATE",
            "FIRST_SEEN_DATE",
            "LAST_SEEN_DATE",
            "CHANGE_TYPE",
            "PAGE_HASH",
            "REQUEST_HASH",
            "NEXT_LINK_HASH",
            "ENDPOINT",
            "LABEL",
            "PAGE",
            "ROWS_COUNT",
            "STATUS_CODE",
            "REQUEST_DURATION_MS",
            "REQUEST_URL",
            "NEXT_LINK",
            "ERROR_MESSAGE",
        ]
    ]

    con.register("nextlink_events_view", df)
    con.execute("INSERT INTO nextlinks SELECT * FROM nextlink_events_view")
    con.unregister("nextlink_events_view")
    con.close()

    print(f"OK Wrote {len(df):,} nextLink events to {NEXTLINK_DB}", flush=True)


# =============================================================================
# STREAMING PIPELINE
# =============================================================================

def normalize_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.astype(str)


def append_df_to_temp_db(
    table_name: str,
    df: pd.DataFrame,
    con_temp: duckdb.DuckDBPyConnection,
) -> int:
    if df.empty:
        return 0

    if table_exists(con_temp, table_name):
        existing_cols = [
            row[1]
            for row in con_temp.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        ]

        for col in df.columns:
            if col not in existing_cols:
                con_temp.execute(f"ALTER TABLE {q(table_name)} ADD COLUMN {q(col)} VARCHAR")
                existing_cols.append(col)
                print(f"INFO Added new column to {table_name}: {col}", flush=True)

        for col in existing_cols:
            if col not in df.columns:
                df[col] = None

        df = df[existing_cols]

    view_name = f"{table_name}_append_view"
    con_temp.register(view_name, df)

    if not table_exists(con_temp, table_name):
        con_temp.execute(
            f"""
            CREATE TABLE {q(table_name)} AS
            SELECT *
            FROM {q(view_name)}
            """
        )
    else:
        con_temp.execute(
            f"""
            INSERT INTO {q(table_name)}
            SELECT *
            FROM {q(view_name)}
            """
        )

    con_temp.unregister(view_name)
    return len(df)


def enqueue_rows_for_processing(
    table_name: str,
    rows: list[dict[str, Any]],
    label: str,
) -> int:
    if not rows:
        return 0

    PROCESS_QUEUE.put(
        {
            "table_name": table_name,
            "rows": rows,
            "label": label,
        }
    )
    return len(rows)


def processor_worker(worker_id: int) -> None:
    print(f"PROCESSOR-{worker_id} started", flush=True)

    while True:
        item = PROCESS_QUEUE.get()

        try:
            if item is STOP:
                WRITE_QUEUE.put(STOP)
                print(f"PROCESSOR-{worker_id} stopping", flush=True)
                return

            table_name = item["table_name"]
            rows = item["rows"]
            label = item.get("label", table_name)

            df = normalize_df(rows)
            if not df.empty:
                WRITE_QUEUE.put(
                    {
                        "table_name": table_name,
                        "df": df,
                        "label": label,
                    }
                )

        except Exception as e:
            print(f"ERROR PROCESSOR-{worker_id} failed: {e}", flush=True)
            raise

        finally:
            PROCESS_QUEUE.task_done()


def writer_worker(expected_processor_stops: int) -> None:
    print("WRITER started", flush=True)

    con_temp = duckdb.connect(TEMP_DB)
    stopped_processors = 0
    write_batches = 0
    rows_written_total = 0
    rows_by_table: dict[str, int] = {}

    try:
        while True:
            item = WRITE_QUEUE.get()

            try:
                if item is STOP:
                    stopped_processors += 1
                    print(
                        f"WRITER received stop signal "
                        f"{stopped_processors}/{expected_processor_stops}",
                        flush=True,
                    )

                    if stopped_processors >= expected_processor_stops:
                        print(
                            f"WRITER stopping. Batches written: {write_batches:,}. "
                            f"Rows written: {rows_written_total:,}. By table: {rows_by_table}",
                            flush=True,
                        )
                        return

                    continue

                table_name = item["table_name"]
                df = item["df"]
                label = item.get("label", table_name)

                inserted = append_df_to_temp_db(table_name, df, con_temp)

                write_batches += 1
                rows_written_total += inserted
                rows_by_table[table_name] = rows_by_table.get(table_name, 0) + inserted

                if write_batches == 1 or write_batches % LOG_WRITES_EVERY == 0:
                    print(
                        f"WRITER batch {write_batches:,}: {label} -> {inserted:,} rows | "
                        f"total written: {rows_written_total:,}",
                        flush=True,
                    )

            except Exception as e:
                print(f"ERROR WRITER failed: {e}", flush=True)
                raise

            finally:
                WRITE_QUEUE.task_done()

    finally:
        con_temp.close()


def start_streaming_pipeline() -> tuple[list[Thread], Thread]:
    processor_threads = [
        Thread(target=processor_worker, args=(i + 1,), daemon=True)
        for i in range(PROCESS_WORKERS)
    ]

    writer_thread = Thread(
        target=writer_worker,
        args=(PROCESS_WORKERS,),
        daemon=True,
    )

    for thread in processor_threads:
        thread.start()

    writer_thread.start()
    return processor_threads, writer_thread


def wait_for_pipeline_idle(stage_name: str) -> None:
    print(f"WAIT Pipeline drain before CDC/export: {stage_name}", flush=True)
    PROCESS_QUEUE.join()
    WRITE_QUEUE.join()
    print(f"OK Pipeline drained: {stage_name}", flush=True)


def stop_streaming_pipeline(
    processor_threads: list[Thread],
    writer_thread: Thread,
) -> None:
    print("Stopping streaming pipeline...", flush=True)

    for _ in range(PROCESS_WORKERS):
        PROCESS_QUEUE.put(STOP)

    PROCESS_QUEUE.join()
    WRITE_QUEUE.join()

    for thread in processor_threads:
        thread.join()

    writer_thread.join()
    print("OK Streaming pipeline stopped", flush=True)


# =============================================================================
# FETCH REFERENCE / ACTORS / UDI
# =============================================================================

def stream_pages_to_processing_queue(
    endpoint: str,
    params: dict[str, Any],
    label: str,
    table_name: str,
    track_nextlinks: bool,
) -> int:
    total_rows = 0
    buffer: list[dict[str, Any]] = []

    url = f"{BASE_URL}/{endpoint}"
    page = 1
    next_params = params

    while url:
        data, status_code, duration_ms, request_url, error_message = request_json(
            url,
            next_params,
        )

        if data is None:
            collect_nextlink_event(
                enabled=track_nextlinks,
                endpoint=endpoint,
                label=label,
                page=page,
                rows_count=0,
                status_code=status_code,
                request_duration_ms=duration_ms,
                request_url=request_url,
                next_link=None,
                error_message=error_message,
            )
            print(
                f"WARNING Stopping pagination for {label}. Rows queued: {total_rows:,}",
                flush=True,
            )
            break

        rows = data.get("value", [])
        next_link = data.get("nextLink")

        collect_nextlink_event(
            enabled=track_nextlinks,
            endpoint=endpoint,
            label=label,
            page=page,
            rows_count=len(rows),
            status_code=status_code,
            request_duration_ms=duration_ms,
            request_url=request_url,
            next_link=next_link,
            error_message=None,
        )

        if rows:
            buffer.extend(rows)

        if len(buffer) >= BATCH_SIZE:
            queued = enqueue_rows_for_processing(table_name, buffer, label)
            total_rows += queued
            buffer = []
            print(f"QUEUED {label}: {total_rows:,} rows", flush=True)

        if page == 1 or page % LOG_PAGES_EVERY == 0:
            print(
                f"{label}: page {page:,}, rows seen: {total_rows + len(buffer):,}",
                flush=True,
            )

        url = next_link
        next_params = None
        page += 1

    if buffer:
        queued = enqueue_rows_for_processing(table_name, buffer, label)
        total_rows += queued
        print(f"QUEUED {label}: final batch. Total queued: {total_rows:,}", flush=True)

    return total_rows


def fetch_reference(track_nextlinks: bool) -> int:
    total_rows = 0

    for language in ["da", "en"]:
        url = f"{BASE_URL}/reference"
        page = 1
        buffer: list[dict[str, Any]] = []
        label = f"reference_{language}"

        params = {
            "LANGUAGE": language,
            "format": "json",
            "api-version": "v1.0",
        }

        while url:
            data, status_code, duration_ms, request_url, error_message = request_json(
                url,
                params,
            )

            if data is None:
                collect_nextlink_event(
                    enabled=track_nextlinks,
                    endpoint="reference",
                    label=label,
                    page=page,
                    rows_count=0,
                    status_code=status_code,
                    request_duration_ms=duration_ms,
                    request_url=request_url,
                    next_link=None,
                    error_message=error_message,
                )
                break

            page_rows = data.get("value", [])
            next_link = data.get("nextLink")

            collect_nextlink_event(
                enabled=track_nextlinks,
                endpoint="reference",
                label=label,
                page=page,
                rows_count=len(page_rows),
                status_code=status_code,
                request_duration_ms=duration_ms,
                request_url=request_url,
                next_link=next_link,
                error_message=None,
            )

            # Audit/debug field: what we requested.
            # LANGUAGE is still returned by the API and used as part of reference key.
            for row in page_rows:
                row["REFERENCE_LANGUAGE_REQUESTED"] = language

            buffer.extend(page_rows)

            if len(buffer) >= BATCH_SIZE:
                queued = enqueue_rows_for_processing("reference", buffer, label)
                total_rows += queued
                buffer = []

            if page == 1 or page % LOG_PAGES_EVERY == 0:
                print(
                    f"{label}: page {page:,}, rows seen: {total_rows + len(buffer):,}",
                    flush=True,
                )

            url = next_link
            params = None
            page += 1

        if buffer:
            queued = enqueue_rows_for_processing("reference", buffer, label)
            total_rows += queued

    print(f"OK Queued {total_rows:,} reference rows", flush=True)
    return total_rows


def active_parameter_values(parameter_block: dict[str, Any]) -> list[Any]:
    return [
        value["id"]
        for value in parameter_block.get("parameter_values", [])
        if value.get("active", True)
    ]


def fetch_partition_stream_safe(
    endpoint: str,
    params: dict[str, Any],
    label: str,
    table_name: str,
    track_nextlinks: bool,
) -> tuple[str, int, str | None]:
    try:
        row_count = stream_pages_to_processing_queue(
            endpoint=endpoint,
            params=params,
            label=label,
            table_name=table_name,
            track_nextlinks=track_nextlinks,
        )
        return label, row_count, None
    except Exception as e:
        return label, 0, str(e)


def fetch_actors_partitioned(
    config: dict[str, Any],
    track_nextlinks: bool,
) -> int:
    actors_config = config["actors_parameters"]

    if not actors_config.get("active", True):
        print("WARNING actors_parameters is inactive. Skipping actors.", flush=True)
        return 0

    request_parameter = actors_config["request_parameter"]
    values = active_parameter_values(actors_config)

    total_rows = 0
    completed = 0

    print("=" * 80, flush=True)
    print("FETCHING ACTORS PARTITIONED WITH QUEUED STREAMING", flush=True)
    print(f"Parameter: {request_parameter}", flush=True)
    print(f"Partitions: {len(values)}", flush=True)
    print(f"Fetch workers: {MAX_WORKERS}", flush=True)
    print(f"Process workers: {PROCESS_WORKERS}", flush=True)
    print("=" * 80, flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}

        for value in values:
            params = {
                request_parameter: value,
                "format": "json",
                "api-version": "v1.0",
            }
            label = f"actors_{request_parameter}_{value}"

            futures[
                executor.submit(
                    fetch_partition_stream_safe,
                    "actors",
                    params,
                    label,
                    "actors",
                    track_nextlinks,
                )
            ] = label

        for future in as_completed(futures):
            label, row_count, error = future.result()
            completed += 1

            if error:
                print(f"WARNING [{completed}/{len(futures)}] Failed {label}: {error}", flush=True)
                continue

            total_rows += row_count
            print(
                f"OK [{completed}/{len(futures)}] {label}: {row_count:,} rows queued | "
                f"Total: {total_rows:,}",
                flush=True,
            )

    return total_rows


def fetch_actors_unfiltered(track_nextlinks: bool) -> int:
    return stream_pages_to_processing_queue(
        endpoint="actors",
        params={"format": "json", "api-version": "v1.0"},
        label="actors_unfiltered",
        table_name="actors",
        track_nextlinks=track_nextlinks,
    )


def build_udi_partitions(config: dict[str, Any]) -> list[dict[str, Any]]:
    partitions = []
    covered_parameter_values = set()

    for rule in config.get("udi_combination_rules", []):
        if not rule.get("active", True):
            continue

        rule_name = rule.get("name", "combination")

        for pair in rule.get("pairs", []):
            pair_params = {
                key: value
                for key, value in pair.items()
                if key != "label" and value is not None
            }

            if not pair_params:
                continue

            partitions.append(pair_params)

            for key, value in pair_params.items():
                covered_parameter_values.add((key, value))

            label = pair.get("label")
            if label:
                print(f"Configured UDI combination partition: {rule_name}/{label}", flush=True)

    for block in config["udi_parameters"]:
        if not block.get("active", True):
            continue

        request_parameter = block["request_parameter"]

        for value in block.get("parameter_values", []):
            if not value.get("active", True):
                continue

            if (request_parameter, value["id"]) in covered_parameter_values:
                continue

            partitions.append({request_parameter: value["id"]})

    return partitions


def fetch_udi_partitioned(
    config: dict[str, Any],
    track_nextlinks: bool,
) -> int:
    partitions = build_udi_partitions(config)

    if not partitions:
        print("WARNING No active UDI parameters. Skipping UDI.", flush=True)
        return 0

    total_rows = 0
    completed = 0

    print("=" * 80, flush=True)
    print("FETCHING UDI PARTITIONED WITH QUEUED STREAMING", flush=True)
    print(f"Partitions: {len(partitions)}", flush=True)
    print(f"Fetch workers: {MAX_WORKERS}", flush=True)
    print(f"Process workers: {PROCESS_WORKERS}", flush=True)
    print(f"Batch size: {BATCH_SIZE}", flush=True)
    print("=" * 80, flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}

        for partition in partitions:
            params = {**partition, "format": "json", "api-version": "v1.0"}
            label = "udi_" + "_".join(f"{key}_{value}" for key, value in partition.items())

            futures[
                executor.submit(
                    fetch_partition_stream_safe,
                    "udi",
                    params,
                    label,
                    "udi",
                    track_nextlinks,
                )
            ] = label

        for future in as_completed(futures):
            label, row_count, error = future.result()
            completed += 1

            if error:
                print(f"WARNING [{completed}/{len(futures)}] Failed {label}: {error}", flush=True)
                continue

            total_rows += row_count
            print(
                f"OK [{completed}/{len(futures)}] {label}: {row_count:,} rows queued | "
                f"Total: {total_rows:,}",
                flush=True,
            )

    return total_rows


def fetch_udi_unfiltered(track_nextlinks: bool) -> int:
    return stream_pages_to_processing_queue(
        endpoint="udi",
        params={"format": "json", "api-version": "v1.0"},
        label="udi_unfiltered",
        table_name="udi",
        track_nextlinks=track_nextlinks,
    )


# =============================================================================
# CDC SQL
# =============================================================================

def create_state_compare_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    source_cols: list[str],
) -> list[str]:
    """Create temp previous compare table from attached state_db.

    This is migration-tolerant:
    - If old state has BUSINESS_KEY, use it.
    - Otherwise rebuild BUSINESS_KEY from source columns.
    - If old state has ROW_HASH, use it.
    - Otherwise rebuild ROW_HASH from source columns.
    """
    if not attached_table_exists(con, "state_db", table_name):
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {q(table_name + "_previous_compare")} AS
            SELECT
                CAST(NULL AS VARCHAR) AS BUSINESS_KEY,
                CAST(NULL AS VARCHAR) AS ENTITY_VARIANT_KEY,
                CAST(NULL AS VARCHAR) AS ROW_HASH,
                CAST(NULL AS VARCHAR) AS EXTRACT_DATE,
                CAST(NULL AS VARCHAR) AS EXTRACT_DATETIME_UTC,
                CAST(NULL AS VARCHAR) AS FIRST_SEEN_DATE,
                CAST(NULL AS VARCHAR) AS FIRST_SEEN_DATETIME_UTC,
                CAST(NULL AS VARCHAR) AS LAST_SEEN_DATE,
                CAST(NULL AS VARCHAR) AS LAST_SEEN_DATETIME_UTC
            WHERE FALSE
            """
        )
        return [
            "BUSINESS_KEY",
            "ENTITY_VARIANT_KEY",
            "ROW_HASH",
            "EXTRACT_DATE",
            "EXTRACT_DATETIME_UTC",
            "FIRST_SEEN_DATE",
            "FIRST_SEEN_DATETIME_UTC",
            "LAST_SEEN_DATE",
            "LAST_SEEN_DATETIME_UTC",
        ]

    prev_cols = get_attached_table_columns(con, "state_db", table_name)

    prev_business_expr = (
        "s.BUSINESS_KEY"
        if "BUSINESS_KEY" in prev_cols
        else build_key_expr(BUSINESS_KEYS.get(table_name, []), prev_cols, alias="s") or "NULL"
    )

    prev_variant_expr = (
        "s.ENTITY_VARIANT_KEY"
        if "ENTITY_VARIANT_KEY" in prev_cols
        else build_key_expr(
            ENTITY_VARIANT_KEYS.get(table_name, BUSINESS_KEYS.get(table_name, [])),
            prev_cols,
            alias="s",
        )
        or prev_business_expr
    )

    prev_hash_expr = (
        "s.ROW_HASH"
        if "ROW_HASH" in prev_cols
        else build_row_hash_expr(prev_cols, alias="s")
    )

    previous_extract_date_expr = (
        "s.EXTRACT_DATE"
        if "EXTRACT_DATE" in prev_cols
        else ("s.LAST_SEEN_DATE" if "LAST_SEEN_DATE" in prev_cols else "NULL")
    )
    previous_extract_datetime_expr = (
        "s.EXTRACT_DATETIME_UTC"
        if "EXTRACT_DATETIME_UTC" in prev_cols
        else ("s.LAST_SEEN_DATETIME_UTC" if "LAST_SEEN_DATETIME_UTC" in prev_cols else "NULL")
    )
    first_seen_date_expr = (
        "s.FIRST_SEEN_DATE"
        if "FIRST_SEEN_DATE" in prev_cols
        else "NULL"
    )
    first_seen_datetime_expr = (
        "s.FIRST_SEEN_DATETIME_UTC"
        if "FIRST_SEEN_DATETIME_UTC" in prev_cols
        else "NULL"
    )
    last_seen_date_expr = (
        "s.LAST_SEEN_DATE"
        if "LAST_SEEN_DATE" in prev_cols
        else "NULL"
    )
    last_seen_datetime_expr = (
        "s.LAST_SEEN_DATETIME_UTC"
        if "LAST_SEEN_DATETIME_UTC" in prev_cols
        else "NULL"
    )

    source_passthrough = []
    for col in source_cols:
        if col in prev_cols:
            source_passthrough.append(f"CAST(s.{q(col)} AS VARCHAR) AS {q(col)}")
        else:
            source_passthrough.append(f"CAST(NULL AS VARCHAR) AS {q(col)}")

    source_sql = ",\n                ".join(source_passthrough)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {q(table_name + "_previous_compare")} AS
        SELECT *
        FROM (
            SELECT
                CAST({prev_business_expr} AS VARCHAR) AS BUSINESS_KEY,
                CAST({prev_variant_expr} AS VARCHAR) AS ENTITY_VARIANT_KEY,
                CAST({prev_hash_expr} AS VARCHAR) AS ROW_HASH,
                CAST({previous_extract_date_expr} AS VARCHAR) AS EXTRACT_DATE,
                CAST({previous_extract_datetime_expr} AS VARCHAR) AS EXTRACT_DATETIME_UTC,
                CAST({first_seen_date_expr} AS VARCHAR) AS FIRST_SEEN_DATE,
                CAST({first_seen_datetime_expr} AS VARCHAR) AS FIRST_SEEN_DATETIME_UTC,
                CAST({last_seen_date_expr} AS VARCHAR) AS LAST_SEEN_DATE,
                CAST({last_seen_datetime_expr} AS VARCHAR) AS LAST_SEEN_DATETIME_UTC,
                {source_sql},
                ROW_NUMBER() OVER (
                    PARTITION BY CAST({prev_business_expr} AS VARCHAR)
                    ORDER BY CAST({last_seen_datetime_expr} AS VARCHAR) DESC NULLS LAST,
                             CAST({prev_hash_expr} AS VARCHAR)
                ) AS rn
            FROM state_db.{q(table_name)} s
        )
        WHERE rn = 1
          AND BUSINESS_KEY IS NOT NULL
        """
    )

    return get_table_columns(con, table_name + "_previous_compare")


def has_compatible_previous_schema(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    source_cols: list[str],
) -> bool:
    """Return True if previous table can be compared normally.

    Older state DBs may have ROW_HASH values calculated with a different source
    schema. If current source columns are not all present in previous state, then
    ROW_HASH comparison can mark every row as UPDATED. In that migration case we
    still refresh the main state table, but matched keys are treated as UNCHANGED
    and bulk UPDATE events are suppressed.
    """
    if not attached_table_exists(con, "state_db", table_name):
        return False

    prev_cols = set(get_attached_table_columns(con, "state_db", table_name))

    if "ROW_HASH" not in prev_cols:
        print(
            f"CDC {table_name}: previous schema has no ROW_HASH. "
            "Treating matched rows as migration-compatible UNCHANGED.",
            flush=True,
        )
        return False

    missing_source_cols = [col for col in source_cols if col not in prev_cols]
    if missing_source_cols:
        preview = ", ".join(missing_source_cols[:10])
        print(
            f"CDC {table_name}: previous schema missing {len(missing_source_cols)} current source columns. "
            f"Treating matched rows as migration-compatible UNCHANGED to avoid false bulk updates. "
            f"Examples: {preview}",
            flush=True,
        )
        return False

    return True


def build_change_type_case(table_name: str) -> str:
    """Return SQL CASE tail for domain-specific change types.

    Priority for UDI:
    UPDATED_LEGISLATION >
    UPDATED_RISK_CLASS >
    UPDATED_STATUS >
    UPDATED_DEVICE_STATUS_TYPE >
    UPDATED_ACTOR_RELATION >
    UPDATED_DEVICE_NAME >
    UPDATED

    Priority for actors:
    UPDATED_PRRC >
    UPDATED
    """
    if table_name == "actors":
        return """
                WHEN p.BUSINESS_KEY IS NULL THEN 'NEW'
                WHEN c.ROW_HASH = p.ROW_HASH THEN 'UNCHANGED'
                WHEN c.ENTITY_VARIANT_KEY IS DISTINCT FROM p.ENTITY_VARIANT_KEY THEN 'UPDATED_PRRC'
                ELSE 'UPDATED'
        """

    if table_name == "udi":
        return """
                WHEN p.BUSINESS_KEY IS NULL THEN 'NEW'
                WHEN c.ROW_HASH = p.ROW_HASH THEN 'UNCHANGED'
                WHEN c.NEW_LEGISLATION_ID IS DISTINCT FROM p.OLD_LEGISLATION_ID THEN 'UPDATED_LEGISLATION'
                WHEN c.NEW_RISK_CLASS_ID IS DISTINCT FROM p.OLD_RISK_CLASS_ID THEN 'UPDATED_RISK_CLASS'
                WHEN c.NEW_STATUS_ID IS DISTINCT FROM p.OLD_STATUS_ID THEN 'UPDATED_STATUS'
                WHEN c.NEW_DEVICE_STATUS_TYPE_ID IS DISTINCT FROM p.OLD_DEVICE_STATUS_TYPE_ID THEN 'UPDATED_DEVICE_STATUS_TYPE'
                WHEN c.NEW_MF_SRN IS DISTINCT FROM p.OLD_MF_SRN
                  OR c.NEW_AR_SRN IS DISTINCT FROM p.OLD_AR_SRN THEN 'UPDATED_ACTOR_RELATION'
                WHEN c.NEW_DEVICE_NAME IS DISTINCT FROM p.OLD_DEVICE_NAME THEN 'UPDATED_DEVICE_NAME'
                ELSE 'UPDATED'
        """

    return """
                WHEN p.BUSINESS_KEY IS NULL THEN 'NEW'
                WHEN c.ROW_HASH = p.ROW_HASH THEN 'UNCHANGED'
                ELSE 'UPDATED'
    """





def risk_rank_expr(value_expr: str) -> str:
    normalized = id_norm_expr(value_expr)
    return f"""
        CASE {normalized}
            -- MDR/MDD
            WHEN '-10' THEN 100
            WHEN '-205' THEN 80
            WHEN '-204' THEN 60
            WHEN '-203' THEN 40
            WHEN '-154' THEN 5

            -- IVDR/IVDD
            WHEN '-202' THEN 95
            WHEN '-201' THEN 75
            WHEN '-155' THEN 70
            WHEN '-200' THEN 55
            WHEN '-156' THEN 50
            WHEN '-219' THEN 45
            WHEN '-199' THEN 35
            WHEN '-157' THEN 20
            ELSE NULL
        END
    """


def legislation_rank_expr(value_expr: str) -> str:
    normalized = id_norm_expr(value_expr)
    return f"""
        CASE {normalized}
            -- MDR/MDD side
            WHEN '-197' THEN 40
            WHEN '-53' THEN 30
            WHEN '-54' THEN 20

            -- IVDR/IVDD side
            WHEN '-198' THEN 40
            WHEN '-55' THEN 30
            ELSE NULL
        END
    """


def build_change_severity_expr(table_name: str) -> str:
    """Build SQL expression for event severity.

    Kept self-contained so it does not depend on helper functions declared later
    in the file.
    """
    def local_id_norm_expr(value_expr: str) -> str:
        return f"regexp_replace(trim(cast({value_expr} AS VARCHAR)), '\\\\.0$', '')"

    def local_risk_rank_expr(value_expr: str) -> str:
        normalized = local_id_norm_expr(value_expr)
        return f"""
            CASE {normalized}
                WHEN '-10' THEN 100
                WHEN '-205' THEN 80
                WHEN '-204' THEN 60
                WHEN '-203' THEN 40
                WHEN '-154' THEN 5
                WHEN '-202' THEN 95
                WHEN '-201' THEN 75
                WHEN '-155' THEN 70
                WHEN '-200' THEN 55
                WHEN '-156' THEN 50
                WHEN '-219' THEN 45
                WHEN '-199' THEN 35
                WHEN '-157' THEN 20
                ELSE NULL
            END
        """

    if table_name == "udi":
        old_rank = local_risk_rank_expr("p.OLD_RISK_CLASS_ID")
        new_rank = local_risk_rank_expr("c.NEW_RISK_CLASS_ID")

        return f"""
            CASE
                WHEN CHANGE_TYPE = 'KEY_MISSING' THEN 'HIGH'
                WHEN CHANGE_TYPE = 'UPDATED_LEGISLATION' THEN 'HIGH'
                WHEN CHANGE_TYPE = 'UPDATED_RISK_CLASS'
                     AND ({new_rank}) > ({old_rank}) THEN 'HIGH'
                WHEN CHANGE_TYPE = 'UPDATED_RISK_CLASS' THEN 'MEDIUM'
                WHEN CHANGE_TYPE = 'UPDATED_STATUS' THEN 'MEDIUM'
                WHEN CHANGE_TYPE = 'UPDATED_DEVICE_STATUS_TYPE' THEN 'MEDIUM'
                WHEN CHANGE_TYPE = 'UPDATED_ACTOR_RELATION' THEN 'MEDIUM'
                WHEN CHANGE_TYPE = 'UPDATED_DEVICE_NAME' THEN 'LOW'
                WHEN CHANGE_TYPE = 'UPDATED' THEN 'LOW'
                ELSE NULL
            END
        """

    if table_name == "actors":
        return """
            CASE
                WHEN CHANGE_TYPE = 'KEY_MISSING' THEN 'HIGH'
                WHEN CHANGE_TYPE = 'UPDATED_PRRC' THEN 'MEDIUM'
                WHEN CHANGE_TYPE = 'UPDATED' THEN 'LOW'
                ELSE NULL
            END
        """

    return """
        CASE
            WHEN CHANGE_TYPE = 'KEY_MISSING' THEN 'HIGH'
            WHEN CHANGE_TYPE = 'UPDATED' THEN 'LOW'
            ELSE NULL
        END
    """


def ensure_event_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    source_cols: list[str],
) -> list[str]:
    event_table = f"{table_name}_change_events"

    fixed_cols = [
        "EVENT_DATE",
        "EVENT_DATETIME_UTC",
        "PREVIOUS_EXTRACT_DATE",
        "PREVIOUS_EXTRACT_DATETIME_UTC",
        "CURRENT_EXTRACT_DATE",
        "CURRENT_EXTRACT_DATETIME_UTC",
        "CHANGE_TYPE",
        "CHANGE_SEVERITY",
        "CHANGED_COLUMNS",
        "CHANGED_COLUMNS_COUNT",
        "CHANGE_SUMMARY",
        "BUSINESS_KEY",
        "ENTITY_VARIANT_KEY",
        "ROW_HASH",
        "OLD_ROW_HASH",
        "OLD_ENTITY_VARIANT_KEY",
        "OLD_RISK_CLASS_ID",
        "NEW_RISK_CLASS_ID",
        "OLD_LEGISLATION_ID",
        "NEW_LEGISLATION_ID",
        "OLD_STATUS_ID",
        "NEW_STATUS_ID",
        "OLD_DEVICE_STATUS_TYPE_ID",
        "NEW_DEVICE_STATUS_TYPE_ID",
    ]

    all_cols = []
    seen = set()
    for col in fixed_cols + source_cols:
        if col not in seen:
            all_cols.append(col)
            seen.add(col)

    if not attached_table_exists(con, "state_db", event_table):
        col_defs = ",\n                ".join(f"{q(col)} VARCHAR" for col in all_cols)
        con.execute(
            f"""
            CREATE TABLE state_db.{q(event_table)} (
                {col_defs}
            )
            """
        )
        print(f"OK Created event table: {event_table}", flush=True)
        return all_cols

    existing = get_attached_table_columns(con, "state_db", event_table)
    for col in all_cols:
        if col not in existing:
            con.execute(f"ALTER TABLE state_db.{q(event_table)} ADD COLUMN {q(col)} VARCHAR")
            existing.append(col)
            print(f"INFO Added column to {event_table}: {col}", flush=True)

    return existing


def compute_hashes_and_tracking(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    extract_date: str,
    extract_datetime_utc: str,
) -> dict[str, Any]:
    if not table_exists(con, table_name):
        print(f"WARNING No current table for {table_name}. Skipping tracking.", flush=True)
        return skipped_stats("SKIPPED")

    print(f"CDC {table_name}: deduplicating exact duplicate rows...", flush=True)
    started = time.perf_counter()
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {q(table_name)} AS
        SELECT DISTINCT *
        FROM {q(table_name)}
        """
    )
    dedupe_rows = con.execute(f"SELECT COUNT(*) FROM {q(table_name)}").fetchone()[0]
    print(
        f"OK CDC {table_name}: dedupe complete with {dedupe_rows:,} rows "
        f"in {round(time.perf_counter() - started, 1)}s",
        flush=True,
    )
    log_resource_usage(f"after dedupe {table_name}")

    source_cols = [
        col for col in get_table_columns(con, table_name)
        if col not in TRACKING_COLUMNS
    ]

    current_cols = get_table_columns(con, table_name)

    previous_schema_compatible = has_compatible_previous_schema(con, table_name, source_cols)

    business_key_expr = build_key_expr(
        BUSINESS_KEYS.get(table_name, []),
        current_cols,
        alias="t",
    ) or "NULL"

    entity_variant_key_expr = build_key_expr(
        ENTITY_VARIANT_KEYS.get(table_name, BUSINESS_KEYS.get(table_name, [])),
        current_cols,
        alias="t",
    ) or business_key_expr

    missing_key_expr = build_missing_key_expr(
        REQUIRED_BUSINESS_KEYS.get(table_name, BUSINESS_KEYS.get(table_name, [])),
        current_cols,
        alias="t",
    )

    row_hash_expr = build_row_hash_expr(current_cols, alias="t")

    risk_col = first_existing_column(current_cols, RISK_CLASS_COLUMN_CANDIDATES)
    legislation_col = first_existing_column(current_cols, LEGISLATION_COLUMN_CANDIDATES)
    status_col = first_existing_column(current_cols, ["STATUS_ID"])
    device_status_col = first_existing_column(current_cols, ["DEVICE_STATUS_TYPE_ID", "DEVICE_STATUS_ID"])
    mf_srn_col = first_existing_column(current_cols, ["MF_SRN"])
    ar_srn_col = first_existing_column(current_cols, ["AR_SRN"])
    device_name_col = first_existing_column(current_cols, ["DEVICE_NAME", "TRADE_NAME"])

    current_select_cols = ",\n                ".join(f"t.{q(col)}" for col in source_cols)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {q(table_name + "_current_hashed_all")} AS
        SELECT
            {missing_key_expr} AS KEY_MISSING_FLAG,
            CASE WHEN {missing_key_expr} THEN NULL ELSE CAST({business_key_expr} AS VARCHAR) END AS BUSINESS_KEY,
            CASE WHEN {missing_key_expr} THEN NULL ELSE CAST({entity_variant_key_expr} AS VARCHAR) END AS ENTITY_VARIANT_KEY,
            CAST({row_hash_expr} AS VARCHAR) AS ROW_HASH,
            {f"CAST(t.{q(risk_col)} AS VARCHAR)" if risk_col else "CAST(NULL AS VARCHAR)"} AS NEW_RISK_CLASS_ID,
            {f"CAST(t.{q(legislation_col)} AS VARCHAR)" if legislation_col else "CAST(NULL AS VARCHAR)"} AS NEW_LEGISLATION_ID,
            {f"CAST(t.{q(status_col)} AS VARCHAR)" if status_col else "CAST(NULL AS VARCHAR)"} AS NEW_STATUS_ID,
            {f"CAST(t.{q(device_status_col)} AS VARCHAR)" if device_status_col else "CAST(NULL AS VARCHAR)"} AS NEW_DEVICE_STATUS_TYPE_ID,
            {f"CAST(t.{q(mf_srn_col)} AS VARCHAR)" if mf_srn_col else "CAST(NULL AS VARCHAR)"} AS NEW_MF_SRN,
            {f"CAST(t.{q(ar_srn_col)} AS VARCHAR)" if ar_srn_col else "CAST(NULL AS VARCHAR)"} AS NEW_AR_SRN,
            {f"CAST(t.{q(device_name_col)} AS VARCHAR)" if device_name_col else "CAST(NULL AS VARCHAR)"} AS NEW_DEVICE_NAME,
            {current_select_cols}
        FROM {q(table_name)} t
        """
    )

    duplicate_business_keys = con.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT BUSINESS_KEY, COUNT(*) AS cnt
            FROM {q(table_name + "_current_hashed_all")}
            WHERE BUSINESS_KEY IS NOT NULL
            GROUP BY BUSINESS_KEY
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    if duplicate_business_keys:
        print(
            f"WARNING {table_name}: {duplicate_business_keys:,} duplicate BUSINESS_KEY groups. "
            "Keeping deterministic first row per key.",
            flush=True,
        )

    hashed_cols = [
        "KEY_MISSING_FLAG",
        "BUSINESS_KEY",
        "ENTITY_VARIANT_KEY",
        "ROW_HASH",
        "NEW_RISK_CLASS_ID",
        "NEW_LEGISLATION_ID",
        "NEW_STATUS_ID",
        "NEW_DEVICE_STATUS_TYPE_ID",
        "NEW_MF_SRN",
        "NEW_AR_SRN",
        "NEW_DEVICE_NAME",
    ] + source_cols
    hashed_cols_sql = ", ".join(q(col) for col in hashed_cols)

    started = time.perf_counter()
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {q(table_name + "_current_hashed")} AS
        SELECT {hashed_cols_sql}
        FROM (
            SELECT
                {hashed_cols_sql},
                ROW_NUMBER() OVER (
                    PARTITION BY BUSINESS_KEY
                    ORDER BY ROW_HASH, ENTITY_VARIANT_KEY
                ) AS rn
            FROM {q(table_name + "_current_hashed_all")}
            WHERE BUSINESS_KEY IS NOT NULL
        )
        WHERE rn = 1

        UNION ALL

        SELECT {hashed_cols_sql}
        FROM {q(table_name + "_current_hashed_all")}
        WHERE BUSINESS_KEY IS NULL
        """
    )
    current_hashed_rows = con.execute(
        f"SELECT COUNT(*) FROM {q(table_name + '_current_hashed')}"
    ).fetchone()[0]
    print(
        f"OK CDC {table_name}: current hashed table has {current_hashed_rows:,} rows "
        f"in {round(time.perf_counter() - started, 1)}s",
        flush=True,
    )
    log_resource_usage(f"after current hashed {table_name}")

    previous_cols = create_state_compare_table(con, table_name, source_cols)

    # Add old semantic fields to previous compare table for severity and DK summaries.
    # Missing columns become NULL.
    prev_base_cols = get_table_columns(con, table_name + "_previous_compare")
    prev_risk_col = first_existing_column(prev_base_cols, RISK_CLASS_COLUMN_CANDIDATES)
    prev_legislation_col = first_existing_column(prev_base_cols, LEGISLATION_COLUMN_CANDIDATES)
    prev_status_col = first_existing_column(prev_base_cols, ["STATUS_ID"])
    prev_device_status_col = first_existing_column(prev_base_cols, ["DEVICE_STATUS_TYPE_ID", "DEVICE_STATUS_ID"])
    prev_mf_srn_col = first_existing_column(prev_base_cols, ["MF_SRN"])
    prev_ar_srn_col = first_existing_column(prev_base_cols, ["AR_SRN"])
    prev_device_name_col = first_existing_column(prev_base_cols, ["DEVICE_NAME", "TRADE_NAME"])

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {q(table_name + "_previous_compare_aug")} AS
        SELECT
            p.*,
            {f"CAST(p.{q(prev_risk_col)} AS VARCHAR)" if prev_risk_col else "CAST(NULL AS VARCHAR)"} AS OLD_RISK_CLASS_ID,
            {f"CAST(p.{q(prev_legislation_col)} AS VARCHAR)" if prev_legislation_col else "CAST(NULL AS VARCHAR)"} AS OLD_LEGISLATION_ID,
            {f"CAST(p.{q(prev_status_col)} AS VARCHAR)" if prev_status_col else "CAST(NULL AS VARCHAR)"} AS OLD_STATUS_ID,
            {f"CAST(p.{q(prev_device_status_col)} AS VARCHAR)" if prev_device_status_col else "CAST(NULL AS VARCHAR)"} AS OLD_DEVICE_STATUS_TYPE_ID,
            {f"CAST(p.{q(prev_mf_srn_col)} AS VARCHAR)" if prev_mf_srn_col else "CAST(NULL AS VARCHAR)"} AS OLD_MF_SRN,
            {f"CAST(p.{q(prev_ar_srn_col)} AS VARCHAR)" if prev_ar_srn_col else "CAST(NULL AS VARCHAR)"} AS OLD_AR_SRN,
            {f"CAST(p.{q(prev_device_name_col)} AS VARCHAR)" if prev_device_name_col else "CAST(NULL AS VARCHAR)"} AS OLD_DEVICE_NAME
        FROM {q(table_name + "_previous_compare")} p
        """
    )

    prev_aug_cols = get_table_columns(con, table_name + "_previous_compare_aug")
    changed_cols_expr = build_changed_columns_expr(source_cols, prev_aug_cols)
    change_type_case = build_change_type_case(table_name)
    severity_expr = (
        build_change_severity_expr(table_name)
        if previous_schema_compatible
        else "CAST(NULL AS VARCHAR)"
    )


    if table_name == "udi":
        domain_change_summary_expr = """
                                WHEN c.NEW_LEGISLATION_ID IS DISTINCT FROM p.OLD_LEGISLATION_ID
                                     THEN concat('APPLICABLE_LEGISLATION_ID changed from ', COALESCE(p.OLD_LEGISLATION_ID, ''), ' to ', COALESCE(c.NEW_LEGISLATION_ID, ''))
                                WHEN c.NEW_RISK_CLASS_ID IS DISTINCT FROM p.OLD_RISK_CLASS_ID
                                     THEN concat('RISK_CLASS_ID changed from ', COALESCE(p.OLD_RISK_CLASS_ID, ''), ' to ', COALESCE(c.NEW_RISK_CLASS_ID, ''))
                                WHEN c.NEW_STATUS_ID IS DISTINCT FROM p.OLD_STATUS_ID
                                     THEN concat('STATUS_ID changed from ', COALESCE(p.OLD_STATUS_ID, ''), ' to ', COALESCE(c.NEW_STATUS_ID, ''))
                                WHEN c.NEW_DEVICE_STATUS_TYPE_ID IS DISTINCT FROM p.OLD_DEVICE_STATUS_TYPE_ID
                                     THEN concat('DEVICE_STATUS_TYPE_ID changed from ', COALESCE(p.OLD_DEVICE_STATUS_TYPE_ID, ''), ' to ', COALESCE(c.NEW_DEVICE_STATUS_TYPE_ID, ''))
                                WHEN c.NEW_MF_SRN IS DISTINCT FROM p.OLD_MF_SRN
                                  OR c.NEW_AR_SRN IS DISTINCT FROM p.OLD_AR_SRN
                                     THEN 'Manufacturer or authorised representative relation changed'
                                WHEN c.NEW_DEVICE_NAME IS DISTINCT FROM p.OLD_DEVICE_NAME
                                     THEN 'Device name changed'
                                ELSE 'Row fields changed'
        """
    elif table_name == "actors":
        domain_change_summary_expr = """
                                WHEN c.ENTITY_VARIANT_KEY IS DISTINCT FROM p.ENTITY_VARIANT_KEY
                                     THEN 'PRRC information changed'
                                ELSE 'Row fields changed'
        """
    else:
        domain_change_summary_expr = """
                                ELSE 'Row fields changed'
        """

    raw_source_select = ",\n                ".join(f"c.{q(col)}" for col in source_cols)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {q(table_name + "_compared_pre")} AS
        SELECT
            '{safe_sql(extract_date)}' AS EXTRACT_DATE,
            '{safe_sql(extract_datetime_utc)}' AS EXTRACT_DATETIME_UTC,
            CASE
                WHEN c.KEY_MISSING_FLAG THEN '{safe_sql(extract_date)}'
                ELSE COALESCE(p.FIRST_SEEN_DATE, '{safe_sql(extract_date)}')
            END AS FIRST_SEEN_DATE,
            CASE
                WHEN c.KEY_MISSING_FLAG THEN '{safe_sql(extract_datetime_utc)}'
                ELSE COALESCE(p.FIRST_SEEN_DATETIME_UTC, '{safe_sql(extract_datetime_utc)}')
            END AS FIRST_SEEN_DATETIME_UTC,
            '{safe_sql(extract_date)}' AS LAST_SEEN_DATE,
            '{safe_sql(extract_datetime_utc)}' AS LAST_SEEN_DATETIME_UTC,
            CASE
                WHEN c.KEY_MISSING_FLAG THEN 'KEY_MISSING'
                WHEN p.BUSINESS_KEY IS NOT NULL AND {str(not previous_schema_compatible).upper()} THEN 'UNCHANGED'
                {change_type_case}
            END AS CHANGE_TYPE,
            CASE
                WHEN {str(previous_schema_compatible).upper()}
                     AND NOT c.KEY_MISSING_FLAG
                     AND p.BUSINESS_KEY IS NOT NULL
                     AND c.ROW_HASH IS DISTINCT FROM p.ROW_HASH
                THEN {changed_cols_expr}
                ELSE NULL
            END AS CHANGED_COLUMNS,
            CASE
                WHEN {str(previous_schema_compatible).upper()}
                     AND NOT c.KEY_MISSING_FLAG
                     AND p.BUSINESS_KEY IS NOT NULL
                     AND c.ROW_HASH IS DISTINCT FROM p.ROW_HASH
                THEN
                    CASE
                        WHEN c.ROW_HASH IS DISTINCT FROM p.ROW_HASH THEN
                            CASE
                                WHEN '{safe_sql(table_name)}' = 'actors'
                                     AND c.ENTITY_VARIANT_KEY IS DISTINCT FROM p.ENTITY_VARIANT_KEY
                                     THEN 'PRRC information changed'
                                {domain_change_summary_expr}
                            END
                        ELSE NULL
                    END
                ELSE NULL
            END AS CHANGE_SUMMARY,
            c.BUSINESS_KEY,
            c.ENTITY_VARIANT_KEY,
            c.ROW_HASH,
            p.ROW_HASH AS OLD_ROW_HASH,
            p.ENTITY_VARIANT_KEY AS OLD_ENTITY_VARIANT_KEY,
            p.OLD_RISK_CLASS_ID,
            c.NEW_RISK_CLASS_ID,
            p.OLD_LEGISLATION_ID,
            c.NEW_LEGISLATION_ID,
            p.OLD_STATUS_ID,
            c.NEW_STATUS_ID,
            p.OLD_DEVICE_STATUS_TYPE_ID,
            c.NEW_DEVICE_STATUS_TYPE_ID,
            {severity_expr} AS CHANGE_SEVERITY,
            p.EXTRACT_DATE AS PREVIOUS_EXTRACT_DATE,
            p.EXTRACT_DATETIME_UTC AS PREVIOUS_EXTRACT_DATETIME_UTC,
            {raw_source_select}
        FROM {q(table_name + "_current_hashed")} c
        LEFT JOIN {q(table_name + "_previous_compare_aug")} p
            ON c.BUSINESS_KEY = p.BUSINESS_KEY
        """
    )

    started = time.perf_counter()
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {q(table_name + "_compared")} AS
        SELECT
            EXTRACT_DATE,
            EXTRACT_DATETIME_UTC,
            FIRST_SEEN_DATE,
            FIRST_SEEN_DATETIME_UTC,
            LAST_SEEN_DATE,
            LAST_SEEN_DATETIME_UTC,
            CHANGE_TYPE,
            CHANGED_COLUMNS,
            {changed_count_expr("CHANGED_COLUMNS")} AS CHANGED_COLUMNS_COUNT,
            CHANGE_SUMMARY,
            BUSINESS_KEY,
            ENTITY_VARIANT_KEY,
            ROW_HASH,
            OLD_ROW_HASH,
            OLD_ENTITY_VARIANT_KEY,
            OLD_RISK_CLASS_ID,
            NEW_RISK_CLASS_ID,
            OLD_LEGISLATION_ID,
            NEW_LEGISLATION_ID,
            OLD_STATUS_ID,
            NEW_STATUS_ID,
            OLD_DEVICE_STATUS_TYPE_ID,
            NEW_DEVICE_STATUS_TYPE_ID,
            CHANGE_SEVERITY,
            PREVIOUS_EXTRACT_DATE,
            PREVIOUS_EXTRACT_DATETIME_UTC,
            {", ".join(q(col) for col in source_cols)}
        FROM {q(table_name + "_compared_pre")}
        """
    )
    compared_rows = con.execute(
        f"SELECT COUNT(*) FROM {q(table_name + '_compared')}"
    ).fetchone()[0]
    print(
        f"OK CDC {table_name}: compared table has {compared_rows:,} rows "
        f"in {round(time.perf_counter() - started, 1)}s",
        flush=True,
    )
    log_resource_usage(f"after compared {table_name}")

    # Append change events using DuckDB SQL only.
    event_cols = ensure_event_table(con, table_name, source_cols)
    event_select = []
    for col in event_cols:
        if col == "EVENT_DATE":
            event_select.append("EXTRACT_DATE AS EVENT_DATE")
        elif col == "EVENT_DATETIME_UTC":
            event_select.append("EXTRACT_DATETIME_UTC AS EVENT_DATETIME_UTC")
        elif col == "CURRENT_EXTRACT_DATE":
            event_select.append("EXTRACT_DATE AS CURRENT_EXTRACT_DATE")
        elif col == "CURRENT_EXTRACT_DATETIME_UTC":
            event_select.append("EXTRACT_DATETIME_UTC AS CURRENT_EXTRACT_DATETIME_UTC")
        elif col in get_table_columns(con, table_name + "_compared"):
            event_select.append(q(col))
        else:
            event_select.append(f"CAST(NULL AS VARCHAR) AS {q(col)}")

    # Event tables are append-only histories.
    # Important: when previous state is missing, do NOT append massive first-run NEW rows.
    # Main tables still get CHANGE_TYPE='NEW'; event history starts with real changes.
    previous_state_rows = con.execute(
        f"SELECT COUNT(*) FROM {q(table_name + '_previous_compare_aug')}"
    ).fetchone()[0]

    event_filter = (
        "CHANGE_TYPE LIKE 'UPDATED%' OR CHANGE_TYPE = 'KEY_MISSING'"
        if previous_state_rows == 0
        else "CHANGE_TYPE <> 'UNCHANGED'"
    )

    event_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {q(table_name + "_compared")}
        WHERE {event_filter}
        """
    ).fetchone()[0]

    print(
        f"CDC {table_name}: previous_state_rows={previous_state_rows:,}; "
        f"appending {event_count:,} event rows using filter: {event_filter}",
        flush=True,
    )

    if event_count > 0:
        con.execute(
            f"""
            INSERT INTO state_db.{q(table_name + "_change_events")} ({", ".join(q(c) for c in event_cols)})
            SELECT {", ".join(event_select)}
            FROM {q(table_name + "_compared")}
            WHERE {event_filter}
            """
        )
        print(f"OK Appended {event_count:,} change events for {table_name}", flush=True)
    else:
        print(f"OK No change events appended for {table_name}", flush=True)

    final_cols = CDC_COLUMNS + source_cols
    con.execute(
        f"""
        CREATE OR REPLACE TABLE state_db.{q(table_name)} AS
        SELECT {", ".join(q(col) for col in final_cols)}
        FROM {q(table_name + "_compared")}
        """
    )

    status_counts = dict(
        con.execute(
            f"""
            SELECT CHANGE_TYPE, COUNT(*) AS row_count
            FROM {q(table_name + "_compared")}
            GROUP BY CHANGE_TYPE
            """
        ).fetchall()
    )

    total_count = con.execute(
        f"SELECT COUNT(*) FROM {q(table_name + '_compared')}"
    ).fetchone()[0]

    changed_columns_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {q(table_name + "_compared")}
        WHERE CHANGED_COLUMNS IS NOT NULL
          AND TRIM(CAST(CHANGED_COLUMNS AS VARCHAR)) <> ''
        """
    ).fetchone()[0]

    stats = {
        "total": total_count,
        "new": status_counts.get("NEW", 0),
        "updated": status_counts.get("UPDATED", 0),
        "updated_prrc": status_counts.get("UPDATED_PRRC", 0),
        "updated_risk_class": status_counts.get("UPDATED_RISK_CLASS", 0),
        "unchanged": status_counts.get("UNCHANGED", 0),
        "key_missing": status_counts.get("KEY_MISSING", 0),
        "duplicate_business_keys": duplicate_business_keys,
        "changed_columns_rows": changed_columns_count,
        "events_appended": event_count,
        "status": "OK",
    }

    print(
        f"OK {table_name}: {stats['total']:,} total | "
        f"{stats['new']:,} NEW | "
        f"{stats['updated']:,} UPDATED | "
        f"{stats['updated_prrc']:,} UPDATED_PRRC | "
        f"{stats['updated_risk_class']:,} UPDATED_RISK_CLASS | "
        f"{stats['unchanged']:,} UNCHANGED | "
        f"{stats['key_missing']:,} KEY_MISSING | "
        f"{stats['duplicate_business_keys']:,} duplicate key groups | "
        f"{stats['changed_columns_rows']:,} changed-column rows | "
        f"{stats['events_appended']:,} events appended",
        flush=True,
    )

    return stats


# =============================================================================
# DANISH INTELLIGENCE LAYER
# =============================================================================

def ref_value_subquery(id_expr: str, code: str) -> str:
    return f"""
        (
            SELECT r.VALUE
            FROM state_db.reference r
            WHERE {id_norm_expr("r.ID")} = {id_norm_expr(id_expr)}
              AND r.CODE = '{safe_sql(code)}'
              AND r.LANGUAGE = 'da'
            LIMIT 1
        )
    """


def label_select_if_col(
    source_cols: list[str],
    source_col: str,
    out_col: str,
    code: str,
    alias: str = "u",
) -> str:
    if source_col not in source_cols:
        return f"CAST(NULL AS VARCHAR) AS {q(out_col)}"
    return f"CAST({ref_value_subquery(f'{alias}.{q(source_col)}', code)} AS VARCHAR) AS {q(out_col)}"


def build_udi_dk_intel(con: duckdb.DuckDBPyConnection) -> None:
    print("DK INTEL: building udi_dk_intel...", flush=True)

    if not attached_table_exists(con, "state_db", "udi"):
        print("WARNING DK INTEL: state_db.udi missing. Skipping udi_dk_intel.", flush=True)
        return

    if not attached_table_exists(con, "state_db", "actor_dk_intel"):
        print("WARNING DK INTEL: actor_dk_intel missing. Skipping udi_dk_intel.", flush=True)
        return

    udi_cols = get_attached_table_columns(con, "state_db", "udi")
    mf_srn = first_existing_column(udi_cols, MF_SRN_CANDIDATES)
    ar_srn = first_existing_column(udi_cols, AR_SRN_CANDIDATES)

    if not mf_srn and not ar_srn:
        print("WARNING DK INTEL: no MF/AR SRN columns found in udi. udi_dk_intel will be empty.", flush=True)

    relation_clauses = []
    if mf_srn:
        relation_clauses.append(f"u.{q(mf_srn)} LIKE 'DK-%'")
        relation_clauses.append(f"u.{q(mf_srn)} IN (SELECT ACTOR_ID FROM state_db.actor_dk_intel WHERE ACTOR_ID IS NOT NULL)")
    if ar_srn:
        relation_clauses.append(f"u.{q(ar_srn)} LIKE 'DK-%'")
        relation_clauses.append(f"u.{q(ar_srn)} IN (SELECT ACTOR_ID FROM state_db.actor_dk_intel WHERE ACTOR_ID IS NOT NULL)")

    where_sql = " OR ".join(relation_clauses) if relation_clauses else "FALSE"

    label_cols = [
        label_select_if_col(udi_cols, "RISK_CLASS_ID", "RISK_CLASS", "RISK_CLASS_ID"),
        label_select_if_col(udi_cols, "APPLICABLE_LEGISLATION_ID", "LEGISLATION", "APPLICABLE_LEGISLATION_ID"),
        label_select_if_col(udi_cols, "DEVICE_STATUS_TYPE_ID", "DEVICE_STATUS_TYPE", "DEVICE_STATUS_TYPE_ID"),
        label_select_if_col(udi_cols, "STATUS_ID", "STATUS", "STATUS_ID"),
        label_select_if_col(udi_cols, "PLACED_ON_THE_MARKET_ID", "PLACED_ON_THE_MARKET", "PLACED_ON_THE_MARKET_ID"),
        label_select_if_col(udi_cols, "SPECIAL_DEVICE_TYPE_ID", "SPECIAL_DEVICE_TYPE", "SPECIAL_DEVICE_TYPE_ID"),
        label_select_if_col(udi_cols, "MULTI_COMPONENT_ID", "MULTI_COMPONENT", "MULTI_COMPONENT_ID"),
    ]

    # Basic Danish summary. Risk-class summary is enriched with Danish labels.
    if "OLD_RISK_CLASS_ID" in get_attached_table_columns(con, "state_db", "udi_change_events"):
        pass

    risk_label_current = (
        ref_value_subquery("u.RISK_CLASS_ID", "RISK_CLASS_ID")
        if "RISK_CLASS_ID" in udi_cols
        else "NULL"
    )

    change_summary_dk = f"""
        CASE
            WHEN u.CHANGE_TYPE = 'UPDATED_RISK_CLASS' THEN
                concat('Risikoklasse ændret til ', COALESCE(CAST({risk_label_current} AS VARCHAR), COALESCE(CAST(u.RISK_CLASS_ID AS VARCHAR), '')))
            WHEN u.CHANGE_TYPE = 'UPDATED' THEN 'Udstyrsoplysninger ændret'
            WHEN u.CHANGE_TYPE = 'NEW' THEN NULL
            WHEN u.CHANGE_TYPE = 'UNCHANGED' THEN NULL
            WHEN u.CHANGE_TYPE = 'KEY_MISSING' THEN NULL
            ELSE NULL
        END AS CHANGE_SUMMARY_DK
    """

    con.execute(
        f"""
        CREATE OR REPLACE TABLE state_db.udi_dk_intel AS
        SELECT
            u.*,
            {", ".join(label_cols)},
            {change_summary_dk}
        FROM state_db.udi u
        WHERE {where_sql}
        """
    )

    count = con.execute("SELECT COUNT(*) FROM state_db.udi_dk_intel").fetchone()[0]
    print(f"OK DK INTEL: udi_dk_intel rows: {count:,}", flush=True)


def build_actor_dk_intel(con: duckdb.DuckDBPyConnection) -> None:
    print("DK INTEL: building actor_dk_intel...", flush=True)

    if not attached_table_exists(con, "state_db", "actors"):
        print("WARNING DK INTEL: state_db.actors missing. Skipping actor_dk_intel.", flush=True)
        return

    actor_cols = get_attached_table_columns(con, "state_db", "actors")

    if "ACTOR_ID" not in actor_cols:
        print("WARNING DK INTEL: ACTOR_ID missing in actors. Skipping actor_dk_intel.", flush=True)
        return

    country_col = first_existing_column(actor_cols, ["ACT_COUNTRY_ISO2_CODE", "COUNTRY_ISO2_CODE", "COUNTRY"])

    country_clause = f"OR a.{q(country_col)} = 'DK'" if country_col else ""

    # Previous DK intelligence metrics are captured once at the start of DK build.
    # Do NOT use actor_dk_intel created earlier in the same run as "previous".
    if table_exists(con, "previous_actor_dk_intel_snapshot"):
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE previous_actor_dk_intel AS
            SELECT *
            FROM previous_actor_dk_intel_snapshot
            """
        )
        prev_count = con.execute("SELECT COUNT(*) FROM previous_actor_dk_intel").fetchone()[0]
        print(
            f"DK INTEL: previous actor_dk_intel snapshot loaded from pre-build snapshot: {prev_count:,} rows",
            flush=True,
        )
    else:
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE previous_actor_dk_intel AS
            SELECT CAST(NULL AS VARCHAR) AS ACTOR_ID
            WHERE FALSE
            """
        )
        print(
            "DK INTEL: no previous actor_dk_intel snapshot found. Previous metrics will be NULL/0.",
            flush=True,
        )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE state_db.actor_dk_intel AS
        SELECT *
        FROM state_db.actors a
        WHERE a.ACTOR_ID LIKE 'DK-%'
        {country_clause}
        """
    )

    count = con.execute("SELECT COUNT(*) FROM state_db.actor_dk_intel").fetchone()[0]
    print(f"OK DK INTEL: actor_dk_intel base rows: {count:,}", flush=True)

    # Need udi_dk_intel to calculate metrics.
    if not attached_table_exists(con, "state_db", "udi_dk_intel"):
        print("WARNING DK INTEL: udi_dk_intel missing. Actor metrics will be NULL/0.", flush=True)
        return

    udi_cols = get_attached_table_columns(con, "state_db", "udi_dk_intel")
    mf_srn = first_existing_column(udi_cols, MF_SRN_CANDIDATES)
    ar_srn = first_existing_column(udi_cols, AR_SRN_CANDIDATES)

    if not mf_srn and not ar_srn:
        print("WARNING DK INTEL: no MF/AR SRN columns found. Actor metrics will be zero.", flush=True)

    union_parts = []
    if mf_srn:
        union_parts.append(
            f"""
            SELECT
                {q(mf_srn)} AS ACTOR_ID,
                UUID,
                RISK_CLASS,
                LEGISLATION,
                RISK_CLASS_ID,
                APPLICABLE_LEGISLATION_ID
            FROM state_db.udi_dk_intel
            WHERE {q(mf_srn)} IS NOT NULL
            """
        )
    if ar_srn:
        union_parts.append(
            f"""
            SELECT
                {q(ar_srn)} AS ACTOR_ID,
                UUID,
                RISK_CLASS,
                LEGISLATION,
                RISK_CLASS_ID,
                APPLICABLE_LEGISLATION_ID
            FROM state_db.udi_dk_intel
            WHERE {q(ar_srn)} IS NOT NULL
            """
        )

    if union_parts:
        actor_device_sql = "\nUNION ALL\n".join(union_parts)
    else:
        actor_device_sql = """
            SELECT
                CAST(NULL AS VARCHAR) AS ACTOR_ID,
                CAST(NULL AS VARCHAR) AS UUID,
                CAST(NULL AS VARCHAR) AS RISK_CLASS,
                CAST(NULL AS VARCHAR) AS LEGISLATION,
                CAST(NULL AS VARCHAR) AS RISK_CLASS_ID,
                CAST(NULL AS VARCHAR) AS APPLICABLE_LEGISLATION_ID
            WHERE FALSE
        """

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE actor_device_current AS
        SELECT DISTINCT *
        FROM (
            {actor_device_sql}
        )
        WHERE ACTOR_ID IS NOT NULL
        """
    )

    # Previous device relation base from previous raw UDI snapshot if it exists.
    # Created during UDI CDC before state overwrite.
    if table_exists(con, "previous_udi_for_intel"):
        prev_cols = get_table_columns(con, "previous_udi_for_intel")
        prev_mf = first_existing_column(prev_cols, MF_SRN_CANDIDATES)
        prev_ar = first_existing_column(prev_cols, AR_SRN_CANDIDATES)
        prev_uuid = first_existing_column(prev_cols, ["UUID"])

        prev_union = []
        if prev_mf and prev_uuid:
            prev_union.append(f"SELECT {q(prev_mf)} AS ACTOR_ID, {q(prev_uuid)} AS UUID FROM previous_udi_for_intel WHERE {q(prev_mf)} IS NOT NULL")
        if prev_ar and prev_uuid:
            prev_union.append(f"SELECT {q(prev_ar)} AS ACTOR_ID, {q(prev_uuid)} AS UUID FROM previous_udi_for_intel WHERE {q(prev_ar)} IS NOT NULL")

        if prev_union:
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE actor_device_previous AS
                SELECT DISTINCT *
                FROM (
                    {" UNION ALL ".join(prev_union)}
                )
                WHERE ACTOR_ID IS NOT NULL
                """
            )
        else:
            con.execute("CREATE OR REPLACE TEMP TABLE actor_device_previous AS SELECT CAST(NULL AS VARCHAR) AS ACTOR_ID, CAST(NULL AS VARCHAR) AS UUID WHERE FALSE")
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE actor_device_previous AS SELECT CAST(NULL AS VARCHAR) AS ACTOR_ID, CAST(NULL AS VARCHAR) AS UUID WHERE FALSE")

    # Counts
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE actor_count_current AS
        SELECT ACTOR_ID, COUNT(DISTINCT UUID) AS UDI_DEVICE_COUNT
        FROM actor_device_current
        GROUP BY ACTOR_ID
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE actor_count_previous AS
        SELECT ACTOR_ID, COUNT(DISTINCT UUID) AS UDI_DEVICE_COUNT_PREVIOUS
        FROM actor_device_previous
        GROUP BY ACTOR_ID
        """
    )

    # Dominant risk class
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE actor_dominant_risk_current AS
        SELECT *
        FROM (
            SELECT
                ACTOR_ID,
                RISK_CLASS AS DOMINANT_RISK_CLASS,
                COUNT(DISTINCT UUID) AS DOMINANT_RISK_CLASS_DEVICE_COUNT,
                ROW_NUMBER() OVER (
                    PARTITION BY ACTOR_ID
                    ORDER BY COUNT(DISTINCT UUID) DESC, RISK_CLASS
                ) AS rn
            FROM actor_device_current
            WHERE RISK_CLASS IS NOT NULL
            GROUP BY ACTOR_ID, RISK_CLASS
        )
        WHERE rn = 1
        """
    )

    # Dominant legislation
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE actor_dominant_legislation_current AS
        SELECT *
        FROM (
            SELECT
                ACTOR_ID,
                LEGISLATION AS DOMINANT_LEGISLATION,
                COUNT(DISTINCT UUID) AS DOMINANT_LEGISLATION_DEVICE_COUNT,
                ROW_NUMBER() OVER (
                    PARTITION BY ACTOR_ID
                    ORDER BY COUNT(DISTINCT UUID) DESC, LEGISLATION
                ) AS rn
            FROM actor_device_current
            WHERE LEGISLATION IS NOT NULL
            GROUP BY ACTOR_ID, LEGISLATION
        )
        WHERE rn = 1
        """
    )

    # Highest MDR/MDD risk
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE actor_highest_mdr_mdd_current AS
        SELECT *
        FROM (
            SELECT
                ACTOR_ID,
                RISK_CLASS AS HIGHEST_MDR_MDD_RISK_CLASS,
                {risk_rank_expr("RISK_CLASS_ID")} AS HIGHEST_MDR_MDD_RISK_CLASS_RANK,
                COUNT(DISTINCT UUID) AS HIGHEST_MDR_MDD_RISK_CLASS_DEVICE_COUNT,
                ROW_NUMBER() OVER (
                    PARTITION BY ACTOR_ID
                    ORDER BY {risk_rank_expr("RISK_CLASS_ID")} DESC NULLS LAST,
                             COUNT(DISTINCT UUID) DESC,
                             RISK_CLASS
                ) AS rn
            FROM actor_device_current
            WHERE {id_norm_expr("RISK_CLASS_ID")} IN ('-10', '-205', '-204', '-203', '-154')
            GROUP BY ACTOR_ID, RISK_CLASS, RISK_CLASS_ID
        )
        WHERE rn = 1
        """
    )

    # Highest IVDR/IVDD risk
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE actor_highest_ivdr_ivdd_current AS
        SELECT *
        FROM (
            SELECT
                ACTOR_ID,
                RISK_CLASS AS HIGHEST_IVDR_IVDD_RISK_CLASS,
                {risk_rank_expr("RISK_CLASS_ID")} AS HIGHEST_IVDR_IVDD_RISK_CLASS_RANK,
                COUNT(DISTINCT UUID) AS HIGHEST_IVDR_IVDD_RISK_CLASS_DEVICE_COUNT,
                ROW_NUMBER() OVER (
                    PARTITION BY ACTOR_ID
                    ORDER BY {risk_rank_expr("RISK_CLASS_ID")} DESC NULLS LAST,
                             COUNT(DISTINCT UUID) DESC,
                             RISK_CLASS
                ) AS rn
            FROM actor_device_current
            WHERE {id_norm_expr("RISK_CLASS_ID")} IN ('-202', '-201', '-155', '-200', '-156', '-219', '-199', '-157')
            GROUP BY ACTOR_ID, RISK_CLASS, RISK_CLASS_ID
        )
        WHERE rn = 1
        """
    )

    # Latest and oldest legislation profiles by framework
    # IDs:
    # MDR -197, MDD -53, AIMDD -54, IVDR -198, IVDD -55
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE actor_legislation_profiles AS
        SELECT *
        FROM (
            SELECT
                ACTOR_ID,
                LEGISLATION,
                APPLICABLE_LEGISLATION_ID,
                COUNT(DISTINCT UUID) AS DEVICE_COUNT,
                {legislation_rank_expr("APPLICABLE_LEGISLATION_ID")} AS LEGISLATION_RANK
            FROM actor_device_current
            WHERE LEGISLATION IS NOT NULL
            GROUP BY ACTOR_ID, LEGISLATION, APPLICABLE_LEGISLATION_ID
        )
        """
    )

    def create_leg_profile(name: str, id_list: list[str], direction: str, prefix: str) -> None:
        ids = ", ".join(f"'{x}'" for x in id_list)
        order_dir = "DESC" if direction == "latest" else "ASC"
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {q(name)} AS
            SELECT *
            FROM (
                SELECT
                    ACTOR_ID,
                    LEGISLATION AS {q(prefix + "_LEGISLATION")},
                    DEVICE_COUNT AS {q(prefix + "_LEGISLATION_DEVICE_COUNT")},
                    ROW_NUMBER() OVER (
                        PARTITION BY ACTOR_ID
                        ORDER BY LEGISLATION_RANK {order_dir} NULLS LAST,
                                 DEVICE_COUNT DESC,
                                 LEGISLATION
                    ) AS rn
                FROM actor_legislation_profiles
                WHERE {id_norm_expr("APPLICABLE_LEGISLATION_ID")} IN ({ids})
            )
            WHERE rn = 1
            """
        )

    create_leg_profile("latest_mdr_mdd_current", ["-197", "-53", "-54"], "latest", "LATEST_MDR_MDD")
    create_leg_profile("oldest_mdr_mdd_current", ["-197", "-53", "-54"], "oldest", "OLDEST_MDR_MDD")
    create_leg_profile("latest_ivdr_ivdd_current", ["-198", "-55"], "latest", "LATEST_IVDR_IVDD")
    create_leg_profile("oldest_ivdr_ivdd_current", ["-198", "-55"], "oldest", "OLDEST_IVDR_IVDD")

    # Migration scores
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE actor_migration_current AS
        SELECT
            ACTOR_ID,
            SUM(CASE WHEN {id_norm_expr("APPLICABLE_LEGISLATION_ID")} = '-197' THEN 1 ELSE 0 END) AS MDR_MDD_DEVICE_COUNT,
            SUM(CASE WHEN {id_norm_expr("APPLICABLE_LEGISLATION_ID")} IN ('-53', '-54') THEN 1 ELSE 0 END) AS MDD_AIMDD_DEVICE_COUNT,
            CASE
                WHEN SUM(CASE WHEN {id_norm_expr("APPLICABLE_LEGISLATION_ID")} IN ('-197', '-53', '-54') THEN 1 ELSE 0 END) = 0
                THEN NULL
                ELSE ROUND(
                    100.0 * SUM(CASE WHEN {id_norm_expr("APPLICABLE_LEGISLATION_ID")} = '-197' THEN 1 ELSE 0 END)
                    / SUM(CASE WHEN {id_norm_expr("APPLICABLE_LEGISLATION_ID")} IN ('-197', '-53', '-54') THEN 1 ELSE 0 END),
                    2
                )
            END AS MDR_MIGRATION_SCORE,
            SUM(CASE WHEN {id_norm_expr("APPLICABLE_LEGISLATION_ID")} = '-198' THEN 1 ELSE 0 END) AS IVDR_DEVICE_COUNT,
            SUM(CASE WHEN {id_norm_expr("APPLICABLE_LEGISLATION_ID")} = '-55' THEN 1 ELSE 0 END) AS IVDD_DEVICE_COUNT,
            CASE
                WHEN SUM(CASE WHEN {id_norm_expr("APPLICABLE_LEGISLATION_ID")} IN ('-198', '-55') THEN 1 ELSE 0 END) = 0
                THEN NULL
                ELSE ROUND(
                    100.0 * SUM(CASE WHEN {id_norm_expr("APPLICABLE_LEGISLATION_ID")} = '-198' THEN 1 ELSE 0 END)
                    / SUM(CASE WHEN {id_norm_expr("APPLICABLE_LEGISLATION_ID")} IN ('-198', '-55') THEN 1 ELSE 0 END),
                    2
                )
            END AS IVDR_MIGRATION_SCORE
        FROM actor_device_current
        GROUP BY ACTOR_ID
        """
    )


    # Build migration-safe previous actor intelligence metrics.
    # Older/first-run actor_dk_intel may not contain the new intelligence columns yet.
    # This temp table guarantees every previous metric column exists before joins.
    prev_cols = get_table_columns(con, "previous_actor_dk_intel")

    def prev_metric_expr(col_name: str, cast_type: str = "DOUBLE") -> str:
        if col_name in prev_cols:
            return f"CAST({q(col_name)} AS {cast_type}) AS {q(col_name)}"
        return f"CAST(NULL AS {cast_type}) AS {q(col_name)}"

    previous_metric_columns = ACTOR_DK_INTEL_PREVIOUS_METRIC_COLUMNS

    previous_metric_select = [
        "CAST(ACTOR_ID AS VARCHAR) AS ACTOR_ID"
    ] + [
        prev_metric_expr(col_name, cast_type)
        for col_name, cast_type in previous_metric_columns
    ]

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE previous_actor_dk_intel_metrics AS
        SELECT {", ".join(previous_metric_select)}
        FROM previous_actor_dk_intel
        """
    )

    # Enrich actor table by recreating it with metrics.
    con.execute(
        """
        CREATE OR REPLACE TABLE state_db.actor_dk_intel AS
        SELECT
            a.*,

            COALESCE(cc.UDI_DEVICE_COUNT, 0) AS UDI_DEVICE_COUNT,
            COALESCE(pc.UDI_DEVICE_COUNT_PREVIOUS, 0) AS UDI_DEVICE_COUNT_PREVIOUS,
            COALESCE(cc.UDI_DEVICE_COUNT, 0) - COALESCE(pc.UDI_DEVICE_COUNT_PREVIOUS, 0) AS UDI_DEVICE_COUNT_CHANGE,
            COALESCE(prev.UDI_DEVICE_COUNT_FIRST_SEEN, COALESCE(cc.UDI_DEVICE_COUNT, 0)) AS UDI_DEVICE_COUNT_FIRST_SEEN,
            COALESCE(cc.UDI_DEVICE_COUNT, 0) - COALESCE(prev.UDI_DEVICE_COUNT_FIRST_SEEN, COALESCE(cc.UDI_DEVICE_COUNT, 0)) AS UDI_DEVICE_COUNT_LIFETIME_CHANGE,

            dr.DOMINANT_RISK_CLASS,
            dr.DOMINANT_RISK_CLASS_DEVICE_COUNT,
            prev.DOMINANT_RISK_CLASS_DEVICE_COUNT AS DOMINANT_RISK_CLASS_DEVICE_COUNT_PREVIOUS,
            COALESCE(dr.DOMINANT_RISK_CLASS_DEVICE_COUNT, 0) - COALESCE(prev.DOMINANT_RISK_CLASS_DEVICE_COUNT, 0) AS DOMINANT_RISK_CLASS_DEVICE_COUNT_CHANGE,

            hm.HIGHEST_MDR_MDD_RISK_CLASS,
            hm.HIGHEST_MDR_MDD_RISK_CLASS_RANK,
            hm.HIGHEST_MDR_MDD_RISK_CLASS_DEVICE_COUNT,
            prev.HIGHEST_MDR_MDD_RISK_CLASS_DEVICE_COUNT AS HIGHEST_MDR_MDD_RISK_CLASS_DEVICE_COUNT_PREVIOUS,
            COALESCE(hm.HIGHEST_MDR_MDD_RISK_CLASS_DEVICE_COUNT, 0) - COALESCE(prev.HIGHEST_MDR_MDD_RISK_CLASS_DEVICE_COUNT, 0) AS HIGHEST_MDR_MDD_RISK_CLASS_DEVICE_COUNT_CHANGE,

            hi.HIGHEST_IVDR_IVDD_RISK_CLASS,
            hi.HIGHEST_IVDR_IVDD_RISK_CLASS_RANK,
            hi.HIGHEST_IVDR_IVDD_RISK_CLASS_DEVICE_COUNT,
            prev.HIGHEST_IVDR_IVDD_RISK_CLASS_DEVICE_COUNT AS HIGHEST_IVDR_IVDD_RISK_CLASS_DEVICE_COUNT_PREVIOUS,
            COALESCE(hi.HIGHEST_IVDR_IVDD_RISK_CLASS_DEVICE_COUNT, 0) - COALESCE(prev.HIGHEST_IVDR_IVDD_RISK_CLASS_DEVICE_COUNT, 0) AS HIGHEST_IVDR_IVDD_RISK_CLASS_DEVICE_COUNT_CHANGE,

            dl.DOMINANT_LEGISLATION,
            prev.DOMINANT_LEGISLATION AS DOMINANT_LEGISLATION_PREVIOUS,
            CASE
                WHEN prev.ACTOR_ID IS NULL THEN FALSE
                WHEN dl.DOMINANT_LEGISLATION IS DISTINCT FROM prev.DOMINANT_LEGISLATION THEN TRUE
                ELSE FALSE
            END AS DOMINANT_LEGISLATION_CHANGED,
            dl.DOMINANT_LEGISLATION_DEVICE_COUNT,
            prev.DOMINANT_LEGISLATION_DEVICE_COUNT AS DOMINANT_LEGISLATION_DEVICE_COUNT_PREVIOUS,
            COALESCE(dl.DOMINANT_LEGISLATION_DEVICE_COUNT, 0) - COALESCE(prev.DOMINANT_LEGISLATION_DEVICE_COUNT, 0) AS DOMINANT_LEGISLATION_DEVICE_COUNT_CHANGE,

            lmm.LATEST_MDR_MDD_LEGISLATION,
            lmm.LATEST_MDR_MDD_LEGISLATION_DEVICE_COUNT,
            prev.LATEST_MDR_MDD_LEGISLATION_DEVICE_COUNT AS LATEST_MDR_MDD_LEGISLATION_DEVICE_COUNT_PREVIOUS,
            COALESCE(lmm.LATEST_MDR_MDD_LEGISLATION_DEVICE_COUNT, 0) - COALESCE(prev.LATEST_MDR_MDD_LEGISLATION_DEVICE_COUNT, 0) AS LATEST_MDR_MDD_LEGISLATION_DEVICE_COUNT_CHANGE,

            liv.LATEST_IVDR_IVDD_LEGISLATION,
            liv.LATEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT,
            prev.LATEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT AS LATEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT_PREVIOUS,
            COALESCE(liv.LATEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT, 0) - COALESCE(prev.LATEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT, 0) AS LATEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT_CHANGE,

            omm.OLDEST_MDR_MDD_LEGISLATION,
            omm.OLDEST_MDR_MDD_LEGISLATION_DEVICE_COUNT,
            prev.OLDEST_MDR_MDD_LEGISLATION_DEVICE_COUNT AS OLDEST_MDR_MDD_LEGISLATION_DEVICE_COUNT_PREVIOUS,
            COALESCE(omm.OLDEST_MDR_MDD_LEGISLATION_DEVICE_COUNT, 0) - COALESCE(prev.OLDEST_MDR_MDD_LEGISLATION_DEVICE_COUNT, 0) AS OLDEST_MDR_MDD_LEGISLATION_DEVICE_COUNT_CHANGE,

            oiv.OLDEST_IVDR_IVDD_LEGISLATION,
            oiv.OLDEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT,
            prev.OLDEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT AS OLDEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT_PREVIOUS,
            COALESCE(oiv.OLDEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT, 0) - COALESCE(prev.OLDEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT, 0) AS OLDEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT_CHANGE,

            mig.MDR_MDD_DEVICE_COUNT,
            mig.MDD_AIMDD_DEVICE_COUNT,
            mig.MDR_MIGRATION_SCORE,
            prev.MDR_MIGRATION_SCORE AS MDR_MIGRATION_SCORE_PREVIOUS,
            mig.MDR_MIGRATION_SCORE - prev.MDR_MIGRATION_SCORE AS MDR_MIGRATION_SCORE_CHANGE,

            mig.IVDR_DEVICE_COUNT,
            mig.IVDD_DEVICE_COUNT,
            mig.IVDR_MIGRATION_SCORE,
            prev.IVDR_MIGRATION_SCORE AS IVDR_MIGRATION_SCORE_PREVIOUS,
            mig.IVDR_MIGRATION_SCORE - prev.IVDR_MIGRATION_SCORE AS IVDR_MIGRATION_SCORE_CHANGE

        FROM state_db.actor_dk_intel a
        LEFT JOIN actor_count_current cc ON a.ACTOR_ID = cc.ACTOR_ID
        LEFT JOIN actor_count_previous pc ON a.ACTOR_ID = pc.ACTOR_ID
        LEFT JOIN previous_actor_dk_intel_metrics prev ON a.ACTOR_ID = prev.ACTOR_ID
        LEFT JOIN actor_dominant_risk_current dr ON a.ACTOR_ID = dr.ACTOR_ID
        LEFT JOIN actor_highest_mdr_mdd_current hm ON a.ACTOR_ID = hm.ACTOR_ID
        LEFT JOIN actor_highest_ivdr_ivdd_current hi ON a.ACTOR_ID = hi.ACTOR_ID
        LEFT JOIN actor_dominant_legislation_current dl ON a.ACTOR_ID = dl.ACTOR_ID
        LEFT JOIN latest_mdr_mdd_current lmm ON a.ACTOR_ID = lmm.ACTOR_ID
        LEFT JOIN latest_ivdr_ivdd_current liv ON a.ACTOR_ID = liv.ACTOR_ID
        LEFT JOIN oldest_mdr_mdd_current omm ON a.ACTOR_ID = omm.ACTOR_ID
        LEFT JOIN oldest_ivdr_ivdd_current oiv ON a.ACTOR_ID = oiv.ACTOR_ID
        LEFT JOIN actor_migration_current mig ON a.ACTOR_ID = mig.ACTOR_ID
        """
    )

    final_count = con.execute("SELECT COUNT(*) FROM state_db.actor_dk_intel").fetchone()[0]
    print(f"OK DK INTEL: actor_dk_intel enriched rows: {final_count:,}", flush=True)


def build_udi_dk_intel_change_events(con: duckdb.DuckDBPyConnection) -> None:
    print("DK INTEL: building udi_dk_intel_change_events...", flush=True)

    if not attached_table_exists(con, "state_db", "udi_change_events"):
        print("WARNING DK INTEL: udi_change_events missing. Skipping udi_dk_intel_change_events.", flush=True)
        return

    if not attached_table_exists(con, "state_db", "actor_dk_intel"):
        print("WARNING DK INTEL: actor_dk_intel missing. Skipping udi_dk_intel_change_events.", flush=True)
        return

    event_cols = get_attached_table_columns(con, "state_db", "udi_change_events")

    mf_srn = first_existing_column(event_cols, MF_SRN_CANDIDATES)
    ar_srn = first_existing_column(event_cols, AR_SRN_CANDIDATES)

    relation_clauses = []
    if mf_srn:
        relation_clauses.append(f"e.{q(mf_srn)} LIKE 'DK-%'")
        relation_clauses.append(f"e.{q(mf_srn)} IN (SELECT ACTOR_ID FROM state_db.actor_dk_intel WHERE ACTOR_ID IS NOT NULL)")
    if ar_srn:
        relation_clauses.append(f"e.{q(ar_srn)} LIKE 'DK-%'")
        relation_clauses.append(f"e.{q(ar_srn)} IN (SELECT ACTOR_ID FROM state_db.actor_dk_intel WHERE ACTOR_ID IS NOT NULL)")

    where_sql = " OR ".join(relation_clauses) if relation_clauses else "FALSE"

    uuid_col = "UUID" if "UUID" in event_cols else None

    select_context = [
        "e.EVENT_DATE",
        "e.EVENT_DATETIME_UTC",
        "e.PREVIOUS_EXTRACT_DATE",
        "e.PREVIOUS_EXTRACT_DATETIME_UTC",
        "e.CURRENT_EXTRACT_DATE",
        "e.CURRENT_EXTRACT_DATETIME_UTC",
        "e.CHANGE_TYPE",
        "e.CHANGE_SEVERITY",
        "e.CHANGED_COLUMNS",
        "e.CHANGED_COLUMNS_COUNT",
        "e.CHANGE_SUMMARY",
        """
        CASE
            WHEN e.CHANGE_TYPE = 'UPDATED_RISK_CLASS' THEN
                concat(
                    'Risikoklasse ændret fra ',
                    COALESCE(CAST((SELECT r.VALUE FROM state_db.reference r WHERE regexp_replace(trim(cast(r.ID AS VARCHAR)), '\\\\.0$', '') = regexp_replace(trim(cast(e.OLD_RISK_CLASS_ID AS VARCHAR)), '\\\\.0$', '') AND r.CODE = 'RISK_CLASS_ID' AND r.LANGUAGE = 'da' LIMIT 1) AS VARCHAR), COALESCE(e.OLD_RISK_CLASS_ID, '')),
                    ' til ',
                    COALESCE(CAST((SELECT r.VALUE FROM state_db.reference r WHERE regexp_replace(trim(cast(r.ID AS VARCHAR)), '\\\\.0$', '') = regexp_replace(trim(cast(e.NEW_RISK_CLASS_ID AS VARCHAR)), '\\\\.0$', '') AND r.CODE = 'RISK_CLASS_ID' AND r.LANGUAGE = 'da' LIMIT 1) AS VARCHAR), COALESCE(e.NEW_RISK_CLASS_ID, ''))
                )
            WHEN e.CHANGE_TYPE = 'UPDATED' THEN 'Udstyrsoplysninger ændret'
            ELSE NULL
        END AS CHANGE_SUMMARY_DK
        """,
        "e.BUSINESS_KEY",
        "e.ENTITY_VARIANT_KEY",
    ]

    for out_col, candidates in [
        ("UUID", ["UUID"]),
        ("PRIMARY_DI", PRIMARY_DI_CANDIDATES),
        ("BASIC_UDI_DI", BASIC_UDI_CANDIDATES),
        ("DEVICE_NAME", DEVICE_NAME_CANDIDATES),
        ("MF_SRN", MF_SRN_CANDIDATES),
        ("MF_NAME", MF_NAME_CANDIDATES),
        ("AR_SRN", AR_SRN_CANDIDATES),
        ("AR_NAME", AR_NAME_CANDIDATES),
    ]:
        select_context.append(col_or_null(event_cols, candidates, "e", out_col))

    select_context.extend(
        [
            "e.ROW_HASH",
            "e.OLD_ROW_HASH",
        ]
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE state_db.udi_dk_intel_change_events AS
        SELECT
            {", ".join(select_context)}
        FROM state_db.udi_change_events e
        WHERE ({where_sql})
          AND e.CHANGE_TYPE <> 'UNCHANGED'
        """
    )

    count = con.execute("SELECT COUNT(*) FROM state_db.udi_dk_intel_change_events").fetchone()[0]
    print(f"OK DK INTEL: udi_dk_intel_change_events rows: {count:,}", flush=True)


def capture_previous_actor_dk_intel_snapshot(con: duckdb.DuckDBPyConnection) -> None:
    """Capture previous actor_dk_intel once before DK tables are rebuilt.

    This prevents same-run temporary/base actor_dk_intel from being treated as a
    previous release. If no previous DB/table exists, an empty snapshot with all
    required previous metric columns is created.

    For NEW actors or actors with NEW UDI relations, previous metric values are
    naturally NULL after the LEFT JOIN. Count deltas use 0 as previous count,
    while previous descriptive fields remain NULL.
    """
    select_parts = []

    if attached_table_exists(con, "state_db", "actor_dk_intel"):
        cols = get_attached_table_columns(con, "state_db", "actor_dk_intel")

        if "ACTOR_ID" in cols:
            select_parts.append(f"CAST(s.{q('ACTOR_ID')} AS VARCHAR) AS ACTOR_ID")
        else:
            select_parts.append("CAST(NULL AS VARCHAR) AS ACTOR_ID")

        for col_name, cast_type in ACTOR_DK_INTEL_PREVIOUS_METRIC_COLUMNS:
            if col_name in cols:
                select_parts.append(f"CAST(s.{q(col_name)} AS {cast_type}) AS {q(col_name)}")
            else:
                select_parts.append(f"CAST(NULL AS {cast_type}) AS {q(col_name)}")

        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE previous_actor_dk_intel_snapshot AS
            SELECT {", ".join(select_parts)}
            FROM state_db.actor_dk_intel s
            """
        )
        cnt = con.execute("SELECT COUNT(*) FROM previous_actor_dk_intel_snapshot").fetchone()[0]
        print(
            f"DK INTEL: captured previous actor_dk_intel snapshot before rebuild: {cnt:,} rows",
            flush=True,
        )
    else:
        select_parts = ["CAST(NULL AS VARCHAR) AS ACTOR_ID"]
        for col_name, cast_type in ACTOR_DK_INTEL_PREVIOUS_METRIC_COLUMNS:
            select_parts.append(f"CAST(NULL AS {cast_type}) AS {q(col_name)}")

        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE previous_actor_dk_intel_snapshot AS
            SELECT {", ".join(select_parts)}
            WHERE FALSE
            """
        )
        print(
            "DK INTEL: no previous actor_dk_intel in state DB. Created empty previous snapshot.",
            flush=True,
        )




def fix_actor_dk_intel_migration_and_legislation_metrics(con: duckdb.DuckDBPyConnection) -> None:
    """Robustly populate actor-level migration and legislation-span metrics.

    The DK intelligence tables live in the persistent attached state database.
    This function therefore updates state_db.actor_dk_intel directly.

    Canonical count definitions:
    - MDR_MDD_DEVICE_COUNT = MDR + MDD + AIMDD
    - MDR_DEVICE_COUNT = MDR only
    - MDD_AIMDD_DEVICE_COUNT = MDD + AIMDD
    - IVDR_IVDD_DEVICE_COUNT = IVDR + IVDD
    - IVDR_DEVICE_COUNT = IVDR only
    - IVDD_DEVICE_COUNT = IVDD only

    Migration score definitions:
    - MDR_MIGRATION_SCORE = MDR_DEVICE_COUNT / MDR_MDD_DEVICE_COUNT
    - IVDR_MIGRATION_SCORE = IVDR_DEVICE_COUNT / IVDR_IVDD_DEVICE_COUNT
    """
    print("DK INTEL: fixing migration and legislation-span metrics...", flush=True)

    if not attached_table_exists(con, "state_db", "actor_dk_intel"):
        print("WARNING DK INTEL: state_db.actor_dk_intel missing. Migration/legislation fix skipped.", flush=True)
        return

    if not attached_table_exists(con, "state_db", "udi_dk_intel"):
        print("WARNING DK INTEL: state_db.udi_dk_intel missing. Migration/legislation fix skipped.", flush=True)
        return

    actor_table = "state_db.actor_dk_intel"
    udi_table = "state_db.udi_dk_intel"

    actor_cols = set(get_table_columns(con, "actor_dk_intel", schema_name="state_db"))

    columns_to_add = {
        "MDR_DEVICE_COUNT": "BIGINT",
        "IVDR_IVDD_DEVICE_COUNT": "BIGINT",
        "NEWEST_MD_LEGISLATION": "VARCHAR",
        "OLDEST_MD_LEGISLATION": "VARCHAR",
        "NEWEST_IVD_LEGISLATION": "VARCHAR",
        "OLDEST_IVD_LEGISLATION": "VARCHAR",
        "NEWEST_MD_LEGISLATION_DEVICE_COUNT": "BIGINT",
        "OLDEST_MD_LEGISLATION_DEVICE_COUNT": "BIGINT",
        "NEWEST_IVD_LEGISLATION_DEVICE_COUNT": "BIGINT",
        "OLDEST_IVD_LEGISLATION_DEVICE_COUNT": "BIGINT",
    }

    for col, dtype in columns_to_add.items():
        if col not in actor_cols:
            con.execute(f"ALTER TABLE {actor_table} ADD COLUMN {q(col)} {dtype}")
            print(f"OK DK INTEL: added column {col}", flush=True)

    actor_cols = set(get_table_columns(con, "actor_dk_intel", schema_name="state_db"))

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE actor_dk_metric_fix AS
        WITH actor_device_legislation AS (
            SELECT
                MF_SRN AS ACTOR_ID,
                UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) AS LEG
            FROM {udi_table}
            WHERE MF_SRN IS NOT NULL
              AND TRIM(CAST(MF_SRN AS VARCHAR)) <> ''

            UNION ALL

            SELECT
                AR_SRN AS ACTOR_ID,
                UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) AS LEG
            FROM {udi_table}
            WHERE AR_SRN IS NOT NULL
              AND TRIM(CAST(AR_SRN AS VARCHAR)) <> ''
              AND UPPER(TRIM(CAST(AR_SRN AS VARCHAR))) <> 'NONE'
        ),
        counts AS (
            SELECT
                ACTOR_ID,
                SUM(CASE WHEN LEG LIKE 'MDR%' THEN 1 ELSE 0 END) AS MDR_DEVICE_COUNT_FIX,
                SUM(CASE WHEN LEG LIKE 'MDD%' OR LEG LIKE 'AIMDD%' THEN 1 ELSE 0 END) AS MDD_AIMDD_DEVICE_COUNT_FIX,
                SUM(CASE WHEN LEG LIKE 'IVDR%' THEN 1 ELSE 0 END) AS IVDR_DEVICE_COUNT_FIX,
                SUM(CASE WHEN LEG LIKE 'IVDD%' THEN 1 ELSE 0 END) AS IVDD_DEVICE_COUNT_FIX,
                SUM(CASE WHEN LEG LIKE 'AIMDD%' THEN 1 ELSE 0 END) AS AIMDD_DEVICE_COUNT_FIX,
                SUM(CASE WHEN LEG LIKE 'MDD%' THEN 1 ELSE 0 END) AS MDD_ONLY_DEVICE_COUNT_FIX
            FROM actor_device_legislation
            GROUP BY ACTOR_ID
        )
        SELECT
            ACTOR_ID,

            COALESCE(MDR_DEVICE_COUNT_FIX, 0) + COALESCE(MDD_AIMDD_DEVICE_COUNT_FIX, 0) AS MDR_MDD_DEVICE_COUNT_FIX,
            MDR_DEVICE_COUNT_FIX,
            MDD_AIMDD_DEVICE_COUNT_FIX,

            COALESCE(IVDR_DEVICE_COUNT_FIX, 0) + COALESCE(IVDD_DEVICE_COUNT_FIX, 0) AS IVDR_IVDD_DEVICE_COUNT_FIX,
            IVDR_DEVICE_COUNT_FIX,
            IVDD_DEVICE_COUNT_FIX,

            CASE
                WHEN MDR_DEVICE_COUNT_FIX > 0 THEN 'MDR'
                WHEN MDD_ONLY_DEVICE_COUNT_FIX > 0 THEN 'MDD'
                WHEN AIMDD_DEVICE_COUNT_FIX > 0 THEN 'AIMDD'
                ELSE NULL
            END AS NEWEST_MD_LEGISLATION_FIX,

            CASE
                WHEN AIMDD_DEVICE_COUNT_FIX > 0 THEN 'AIMDD'
                WHEN MDD_ONLY_DEVICE_COUNT_FIX > 0 THEN 'MDD'
                WHEN MDR_DEVICE_COUNT_FIX > 0 THEN 'MDR'
                ELSE NULL
            END AS OLDEST_MD_LEGISLATION_FIX,

            CASE
                WHEN MDR_DEVICE_COUNT_FIX > 0 THEN MDR_DEVICE_COUNT_FIX
                WHEN MDD_ONLY_DEVICE_COUNT_FIX > 0 THEN MDD_ONLY_DEVICE_COUNT_FIX
                WHEN AIMDD_DEVICE_COUNT_FIX > 0 THEN AIMDD_DEVICE_COUNT_FIX
                ELSE NULL
            END AS NEWEST_MD_LEGISLATION_DEVICE_COUNT_FIX,

            CASE
                WHEN AIMDD_DEVICE_COUNT_FIX > 0 THEN AIMDD_DEVICE_COUNT_FIX
                WHEN MDD_ONLY_DEVICE_COUNT_FIX > 0 THEN MDD_ONLY_DEVICE_COUNT_FIX
                WHEN MDR_DEVICE_COUNT_FIX > 0 THEN MDR_DEVICE_COUNT_FIX
                ELSE NULL
            END AS OLDEST_MD_LEGISLATION_DEVICE_COUNT_FIX,

            CASE
                WHEN IVDR_DEVICE_COUNT_FIX > 0 THEN 'IVDR'
                WHEN IVDD_DEVICE_COUNT_FIX > 0 THEN 'IVDD'
                ELSE NULL
            END AS NEWEST_IVD_LEGISLATION_FIX,

            CASE
                WHEN IVDD_DEVICE_COUNT_FIX > 0 THEN 'IVDD'
                WHEN IVDR_DEVICE_COUNT_FIX > 0 THEN 'IVDR'
                ELSE NULL
            END AS OLDEST_IVD_LEGISLATION_FIX,

            CASE
                WHEN IVDR_DEVICE_COUNT_FIX > 0 THEN IVDR_DEVICE_COUNT_FIX
                WHEN IVDD_DEVICE_COUNT_FIX > 0 THEN IVDD_DEVICE_COUNT_FIX
                ELSE NULL
            END AS NEWEST_IVD_LEGISLATION_DEVICE_COUNT_FIX,

            CASE
                WHEN IVDD_DEVICE_COUNT_FIX > 0 THEN IVDD_DEVICE_COUNT_FIX
                WHEN IVDR_DEVICE_COUNT_FIX > 0 THEN IVDR_DEVICE_COUNT_FIX
                ELSE NULL
            END AS OLDEST_IVD_LEGISLATION_DEVICE_COUNT_FIX,

            CASE
                WHEN COALESCE(MDR_DEVICE_COUNT_FIX, 0) + COALESCE(MDD_AIMDD_DEVICE_COUNT_FIX, 0) = 0 THEN NULL
                ELSE CAST(COALESCE(MDR_DEVICE_COUNT_FIX, 0) AS DOUBLE)
                     / CAST((COALESCE(MDR_DEVICE_COUNT_FIX, 0) + COALESCE(MDD_AIMDD_DEVICE_COUNT_FIX, 0)) AS DOUBLE)
            END AS MDR_MIGRATION_SCORE_FIX,

            CASE
                WHEN COALESCE(IVDR_DEVICE_COUNT_FIX, 0) + COALESCE(IVDD_DEVICE_COUNT_FIX, 0) = 0 THEN NULL
                ELSE CAST(COALESCE(IVDR_DEVICE_COUNT_FIX, 0) AS DOUBLE)
                     / CAST((COALESCE(IVDR_DEVICE_COUNT_FIX, 0) + COALESCE(IVDD_DEVICE_COUNT_FIX, 0)) AS DOUBLE)
            END AS IVDR_MIGRATION_SCORE_FIX
        FROM counts
        """
    )

    assignments = []

    def maybe_set(col: str, expr: str) -> None:
        if col in actor_cols:
            assignments.append(f"{q(col)} = {expr}")

    maybe_set("MDR_MDD_DEVICE_COUNT", "COALESCE(f.MDR_MDD_DEVICE_COUNT_FIX, 0)")
    maybe_set("MDR_DEVICE_COUNT", "COALESCE(f.MDR_DEVICE_COUNT_FIX, 0)")
    maybe_set("MDD_AIMDD_DEVICE_COUNT", "COALESCE(f.MDD_AIMDD_DEVICE_COUNT_FIX, 0)")

    maybe_set("IVDR_IVDD_DEVICE_COUNT", "COALESCE(f.IVDR_IVDD_DEVICE_COUNT_FIX, 0)")
    maybe_set("IVDR_DEVICE_COUNT", "COALESCE(f.IVDR_DEVICE_COUNT_FIX, 0)")
    maybe_set("IVDD_DEVICE_COUNT", "COALESCE(f.IVDD_DEVICE_COUNT_FIX, 0)")

    maybe_set("MDR_MIGRATION_SCORE", "f.MDR_MIGRATION_SCORE_FIX")
    maybe_set("IVDR_MIGRATION_SCORE", "f.IVDR_MIGRATION_SCORE_FIX")

    maybe_set("NEWEST_MD_LEGISLATION", "f.NEWEST_MD_LEGISLATION_FIX")
    maybe_set("OLDEST_MD_LEGISLATION", "f.OLDEST_MD_LEGISLATION_FIX")
    maybe_set("NEWEST_IVD_LEGISLATION", "f.NEWEST_IVD_LEGISLATION_FIX")
    maybe_set("OLDEST_IVD_LEGISLATION", "f.OLDEST_IVD_LEGISLATION_FIX")

    maybe_set("NEWEST_MD_LEGISLATION_DEVICE_COUNT", "f.NEWEST_MD_LEGISLATION_DEVICE_COUNT_FIX")
    maybe_set("OLDEST_MD_LEGISLATION_DEVICE_COUNT", "f.OLDEST_MD_LEGISLATION_DEVICE_COUNT_FIX")
    maybe_set("NEWEST_IVD_LEGISLATION_DEVICE_COUNT", "f.NEWEST_IVD_LEGISLATION_DEVICE_COUNT_FIX")
    maybe_set("OLDEST_IVD_LEGISLATION_DEVICE_COUNT", "f.OLDEST_IVD_LEGISLATION_DEVICE_COUNT_FIX")

    # Backward-compatible aliases.
    maybe_set("LATEST_MDR_MDD_LEGISLATION", "f.NEWEST_MD_LEGISLATION_FIX")
    maybe_set("OLDEST_MDR_MDD_LEGISLATION", "f.OLDEST_MD_LEGISLATION_FIX")
    maybe_set("LATEST_IVDR_IVDD_LEGISLATION", "f.NEWEST_IVD_LEGISLATION_FIX")
    maybe_set("OLDEST_IVDR_IVDD_LEGISLATION", "f.OLDEST_IVD_LEGISLATION_FIX")
    maybe_set("LATEST_MDR_MDD_LEGISLATION_DEVICE_COUNT", "f.NEWEST_MD_LEGISLATION_DEVICE_COUNT_FIX")
    maybe_set("OLDEST_MDR_MDD_LEGISLATION_DEVICE_COUNT", "f.OLDEST_MD_LEGISLATION_DEVICE_COUNT_FIX")
    maybe_set("LATEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT", "f.NEWEST_IVD_LEGISLATION_DEVICE_COUNT_FIX")
    maybe_set("OLDEST_IVDR_IVDD_LEGISLATION_DEVICE_COUNT", "f.OLDEST_IVD_LEGISLATION_DEVICE_COUNT_FIX")

    if assignments:
        con.execute(
            f"""
            UPDATE {actor_table} AS a
            SET {", ".join(assignments)}
            FROM actor_dk_metric_fix f
            WHERE a.ACTOR_ID = f.ACTOR_ID
            """
        )
    else:
        print("WARNING DK INTEL: no matching state_db.actor_dk_intel columns found for migration/legislation fix.", flush=True)

    validation_cols = [
        "MDR_MDD_DEVICE_COUNT",
        "MDR_DEVICE_COUNT",
        "MDD_AIMDD_DEVICE_COUNT",
        "IVDR_IVDD_DEVICE_COUNT",
        "IVDR_DEVICE_COUNT",
        "IVDD_DEVICE_COUNT",
        "MDR_MIGRATION_SCORE",
        "IVDR_MIGRATION_SCORE",
        "NEWEST_MD_LEGISLATION",
        "OLDEST_MD_LEGISLATION",
        "NEWEST_IVD_LEGISLATION",
        "OLDEST_IVD_LEGISLATION",
        "LATEST_MDR_MDD_LEGISLATION",
        "OLDEST_MDR_MDD_LEGISLATION",
        "LATEST_IVDR_IVDD_LEGISLATION",
        "OLDEST_IVDR_IVDD_LEGISLATION",
    ]

    actor_cols = set(get_table_columns(con, "actor_dk_intel", schema_name="state_db"))
    for col in validation_cols:
        if col in actor_cols:
            non_null = con.execute(f"SELECT COUNT(*) FROM {actor_table} WHERE {q(col)} IS NOT NULL").fetchone()[0]
            if col.endswith("_COUNT"):
                non_zero = con.execute(f"SELECT COUNT(*) FROM {actor_table} WHERE COALESCE({q(col)}, 0) <> 0").fetchone()[0]
                print(f"OK DK INTEL validation {col}: non-null={non_null:,}; non-zero={non_zero:,}", flush=True)
            else:
                print(f"OK DK INTEL validation {col}: non-null={non_null:,}", flush=True)

    print("OK DK INTEL: migration and legislation-span metrics fixed.", flush=True)



def build_dk_intelligence(con: duckdb.DuckDBPyConnection) -> None:
    print("=" * 80, flush=True)
    print("BUILDING DANISH INTELLIGENCE LAYER", flush=True)
    print("=" * 80, flush=True)

    capture_previous_actor_dk_intel_snapshot(con)

    build_actor_dk_intel(con)
    build_udi_dk_intel(con)
    # Rebuild actor intel once udi_dk_intel exists so metrics can be calculated.
    build_actor_dk_intel(con)
    build_udi_dk_intel_change_events(con)

    fix_actor_dk_intel_migration_and_legislation_metrics(con)

    print("OK Danish intelligence layer complete.", flush=True)


# =============================================================================
# EXPORT / RELEASE NOTES
# =============================================================================

def copy_table_to_export_db(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    export_db_schema: str = "export_db",
) -> None:
    if not attached_table_exists(con, "state_db", table_name):
        print(f"WARNING Export DB copy skipped. Missing table: {table_name}", flush=True)
        return

    started = time.perf_counter()
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {q(export_db_schema)}.{q(table_name)} AS
        SELECT *
        FROM state_db.{q(table_name)}
        """
    )
    elapsed = round(time.perf_counter() - started, 1)
    print(f"OK Copied {table_name} to {EXPORT_DB} in {elapsed}s", flush=True)


def export_csv_if_exists(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> None:
    if not attached_table_exists(con, "state_db", table_name):
        print(f"WARNING CSV export skipped. Missing table: {table_name}", flush=True)
        return

    started = time.perf_counter()
    con.execute(
        f"""
        COPY state_db.{q(table_name)}
        TO '{table_name}.csv'
        (HEADER, DELIMITER ',')
        """
    )
    elapsed = round(time.perf_counter() - started, 1)
    print(f"OK Exported {table_name}.csv in {elapsed}s", flush=True)


def export_dk_insight_assets(con: duckdb.DuckDBPyConnection, stats_dict: dict | None = None) -> None:
    """Create lightweight DK insight release assets.

    Assets:
    - actor_dk_intel_summary.csv
    - top10_dk_manufacturers.csv
    - top10_dk_authorised_representatives.csv
    - eudamed_insights_summary.json
    - eudamed_dk_exports.zip
    """
    print("=" * 80, flush=True)
    print("EXPORTING DK INSIGHT ASSETS", flush=True)
    print("=" * 80, flush=True)

    if not attached_table_exists(con, "state_db", "actor_dk_intel"):
        print("WARNING DK insight export skipped: missing actor_dk_intel", flush=True)
        return
    if not attached_table_exists(con, "state_db", "udi_dk_intel"):
        print("WARNING DK insight export skipped: missing udi_dk_intel", flush=True)
        return

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE dk_legislation_counts_by_actor AS
        SELECT
            COALESCE(MF_SRN, AR_SRN) AS ACTOR_SRN,
            SUM(CASE WHEN UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) LIKE 'MDR%' THEN 1 ELSE 0 END) AS MDR_COUNT,
            SUM(CASE WHEN UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) LIKE 'MDD%' THEN 1 ELSE 0 END) AS MDD_COUNT,
            SUM(CASE WHEN UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) LIKE 'IVDR%' THEN 1 ELSE 0 END) AS IVDR_COUNT,
            SUM(CASE WHEN UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) LIKE 'IVDD%' THEN 1 ELSE 0 END) AS IVDD_COUNT
        FROM state_db.udi_dk_intel
        GROUP BY COALESCE(MF_SRN, AR_SRN)
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE actor_dk_intel_summary AS
        SELECT
            COALESCE(a.NAME, a.ACTOR_ID) AS ACTOR_NAME,
            a.ACTOR_ID AS ACTOR_SRN,
            a.ACTOR_TYPE,
            a.ACT_COUNTRY_ISO2_CODE,
            a.UDI_DEVICE_COUNT,
            COALESCE(l.MDR_COUNT, 0) AS MDR_COUNT,
            COALESCE(l.MDD_COUNT, 0) AS MDD_COUNT,
            COALESCE(l.IVDR_COUNT, 0) AS IVDR_COUNT,
            COALESCE(l.IVDD_COUNT, 0) AS IVDD_COUNT,
            a.DOMINANT_RISK_CLASS,
            a.DOMINANT_LEGISLATION,
            a.HIGHEST_MDR_MDD_RISK_CLASS,
            a.HIGHEST_MDR_MDD_RISK_CLASS_RANK,
            a.HIGHEST_IVDR_IVDD_RISK_CLASS,
            a.HIGHEST_IVDR_IVDD_RISK_CLASS_RANK,
            a.MDR_MIGRATION_SCORE,
            a.IVDR_MIGRATION_SCORE
        FROM state_db.actor_dk_intel a
        LEFT JOIN dk_legislation_counts_by_actor l
            ON a.ACTOR_ID = l.ACTOR_SRN
        ORDER BY a.UDI_DEVICE_COUNT DESC NULLS LAST, ACTOR_NAME
        """
    )

    con.execute(
        """
        COPY actor_dk_intel_summary
        TO 'actor_dk_intel_summary.csv'
        (HEADER, DELIMITER ',')
        """
    )
    print("OK Exported actor_dk_intel_summary.csv", flush=True)

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE top10_dk_manufacturers AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, MF_NAME, MF_SRN) AS RANK,
            MF_NAME,
            MF_SRN,
            COUNT(*) AS DEVICE_COUNT,
            SUM(CASE WHEN UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) LIKE 'MDR%' THEN 1 ELSE 0 END) AS MDR_COUNT,
            SUM(CASE WHEN UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) LIKE 'MDD%' THEN 1 ELSE 0 END) AS MDD_COUNT,
            SUM(CASE WHEN UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) LIKE 'IVDR%' THEN 1 ELSE 0 END) AS IVDR_COUNT,
            SUM(CASE WHEN UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) LIKE 'IVDD%' THEN 1 ELSE 0 END) AS IVDD_COUNT,
            MODE(RISK_CLASS) AS DOMINANT_RISK_CLASS,
            MODE(LEGISLATION) AS DOMINANT_LEGISLATION
        FROM state_db.udi_dk_intel
        WHERE MF_SRN IS NOT NULL AND TRIM(CAST(MF_SRN AS VARCHAR)) <> ''
        GROUP BY MF_NAME, MF_SRN
        QUALIFY RANK <= 10
        ORDER BY RANK
        """
    )

    con.execute(
        """
        COPY top10_dk_manufacturers
        TO 'top10_dk_manufacturers.csv'
        (HEADER, DELIMITER ',')
        """
    )
    print("OK Exported top10_dk_manufacturers.csv", flush=True)

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE top10_dk_authorised_representatives AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, AR_NAME, AR_SRN) AS RANK,
            AR_NAME,
            AR_SRN,
            COUNT(*) AS DEVICE_COUNT,
            SUM(CASE WHEN UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) LIKE 'MDR%' THEN 1 ELSE 0 END) AS MDR_COUNT,
            SUM(CASE WHEN UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) LIKE 'MDD%' THEN 1 ELSE 0 END) AS MDD_COUNT,
            SUM(CASE WHEN UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) LIKE 'IVDR%' THEN 1 ELSE 0 END) AS IVDR_COUNT,
            SUM(CASE WHEN UPPER(TRIM(CAST(LEGISLATION AS VARCHAR))) LIKE 'IVDD%' THEN 1 ELSE 0 END) AS IVDD_COUNT,
            MODE(RISK_CLASS) AS DOMINANT_RISK_CLASS,
            MODE(LEGISLATION) AS DOMINANT_LEGISLATION
        FROM state_db.udi_dk_intel
        WHERE AR_SRN IS NOT NULL
          AND TRIM(CAST(AR_SRN AS VARCHAR)) <> ''
          AND UPPER(TRIM(CAST(AR_SRN AS VARCHAR))) <> 'NONE'
          AND TRIM(CAST(AR_SRN AS VARCHAR)) LIKE 'DK-AR-%'
        GROUP BY AR_NAME, AR_SRN
        QUALIFY RANK <= 10
        ORDER BY RANK
        """
    )

    con.execute(
        """
        COPY top10_dk_authorised_representatives
        TO 'top10_dk_authorised_representatives.csv'
        (HEADER, DELIMITER ',')
        """
    )
    print("OK Exported top10_dk_authorised_representatives.csv", flush=True)

    # Build JSON summary from small tables only.
    def rows_as_dicts(table_name: str) -> list[dict]:
        rel = con.execute(f"SELECT * FROM {q(table_name)}").fetchdf()
        return rel.where(rel.notna(), None).to_dict(orient="records")

    legislation_distribution = con.execute(
        """
        SELECT LEGISLATION, COUNT(*) AS DEVICE_COUNT
        FROM state_db.udi_dk_intel
        GROUP BY LEGISLATION
        ORDER BY DEVICE_COUNT DESC
        """
    ).fetchdf().where(lambda df: df.notna(), None).to_dict(orient="records")

    risk_distribution = con.execute(
        """
        SELECT RISK_CLASS, COUNT(*) AS DEVICE_COUNT
        FROM state_db.udi_dk_intel
        GROUP BY RISK_CLASS
        ORDER BY DEVICE_COUNT DESC
        """
    ).fetchdf().where(lambda df: df.notna(), None).to_dict(orient="records")

    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "extract_date": RUN_EXTRACT_DATE if "RUN_EXTRACT_DATE" in globals() else None,
        "run_time": RUN_TIME if "RUN_TIME" in globals() else None,
        "counts": {
            "reference": table_count(con, "reference"),
            "actors": table_count(con, "actors"),
            "udi": table_count(con, "udi"),
            "actor_dk_intel": table_count(con, "actor_dk_intel"),
            "udi_dk_intel": table_count(con, "udi_dk_intel"),
            "udi_dk_intel_change_events": table_count(con, "udi_dk_intel_change_events"),
        },
        "legislation_distribution": legislation_distribution,
        "risk_distribution": risk_distribution,
        "top10_dk_manufacturers": rows_as_dicts("top10_dk_manufacturers"),
        "top10_dk_authorised_representatives": rows_as_dicts("top10_dk_authorised_representatives"),
        "cdc": stats_dict or {},
    }

    with open("eudamed_insights_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print("OK Exported eudamed_insights_summary.json", flush=True)

    zip_files = [
        "actor_dk_intel.csv",
        "udi_dk_intel.csv",
        "udi_dk_intel_change_events.csv",
        "actor_dk_intel_summary.csv",
        "top10_dk_manufacturers.csv",
        "top10_dk_authorised_representatives.csv",
        "eudamed_insights_summary.json",
        "run_stats.json",
    ]

    with zipfile.ZipFile("eudamed_dk_exports.zip", "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_name in zip_files:
            if os.path.exists(file_name):
                zf.write(file_name)
                print(f"OK Added to eudamed_dk_exports.zip: {file_name}", flush=True)
            else:
                print(f"WARNING ZIP skipped missing file: {file_name}", flush=True)

    print("OK Created eudamed_dk_exports.zip", flush=True)


def export_duckdb_csv_parquet(con: duckdb.DuckDBPyConnection) -> None:
    """Create final DuckDB plus only Danish intelligence CSV exports.

    This intentionally does NOT export global CSV/parquet files anymore.
    The full canonical EU layer and all event tables are still inside eudamed.duckdb.
    Standalone files are limited to Danish intelligence CSVs to reduce release size/time.
    """
    if os.path.exists(EXPORT_DB):
        os.remove(EXPORT_DB)

    con.execute(f"ATTACH '{EXPORT_DB}' AS export_db")

    duckdb_tables = [
        "reference",
        "actors",
        "udi",
        "reference_change_events",
        "actors_change_events",
        "udi_change_events",
        "actor_dk_intel",
        "udi_dk_intel",
        "udi_dk_intel_change_events",
    ]

    for table_name in duckdb_tables:
        try:
            copy_table_to_export_db(con, table_name)
        except Exception as e:
            print(f"WARNING Could not copy {table_name} to export DB: {e}", flush=True)

    con.execute("DETACH export_db")
    con.execute("CHECKPOINT")
    con.execute("VACUUM")

    print(f"OK Created {EXPORT_DB}", flush=True)

    dk_csv_tables = [
        "actor_dk_intel",
        "udi_dk_intel",
        "udi_dk_intel_change_events",
    ]

    for table_name in dk_csv_tables:
        try:
            export_csv_if_exists(con, table_name)
        except Exception as e:
            print(f"WARNING Could not export {table_name}.csv: {e}", flush=True)


def table_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    try:
        if not attached_table_exists(con, "state_db", table_name):
            return 0
        return con.execute(f"SELECT COUNT(*) FROM state_db.{q(table_name)}").fetchone()[0]
    except Exception:
        return 0


def log_resource_usage(label: str) -> None:
    """Log best-effort process memory and disk usage.

    ru_maxrss is max resident set size, not current memory, but it is useful on
    GitHub Actions to see if the job is approaching memory pressure.
    """
    try:
        import resource

        usage_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        print(f"RESOURCE {label}: max RSS {usage_mb:,.1f} MB", flush=True)
    except Exception as e:
        print(f"RESOURCE {label}: memory usage unavailable: {e}", flush=True)

    try:
        total, used, free = shutil.disk_usage(".")
        print(
            f"RESOURCE {label}: disk used {used / (1024**3):,.2f} GB | "
            f"free {free / (1024**3):,.2f} GB",
            flush=True,
        )
    except Exception as e:
        print(f"RESOURCE {label}: disk usage unavailable: {e}", flush=True)


def checkpoint_and_gc(
    con: duckdb.DuckDBPyConnection,
    label: str,
    vacuum: bool = False,
) -> None:
    """Flush DuckDB state and trigger Python GC around heavy phases."""
    print(f"MAINTENANCE {label}: checkpoint starting...", flush=True)
    try:
        con.execute("CHECKPOINT")
        print(f"MAINTENANCE {label}: checkpoint OK", flush=True)
    except Exception as e:
        print(f"WARNING MAINTENANCE {label}: checkpoint failed: {e}", flush=True)

    if vacuum:
        print(f"MAINTENANCE {label}: vacuum starting...", flush=True)
        try:
            con.execute("VACUUM")
            print(f"MAINTENANCE {label}: vacuum OK", flush=True)
        except Exception as e:
            print(f"WARNING MAINTENANCE {label}: vacuum failed: {e}", flush=True)

    collected = gc.collect()
    print(f"MAINTENANCE {label}: Python GC collected {collected} objects", flush=True)
    log_resource_usage(label)


def drop_tables_if_exist(
    con: duckdb.DuckDBPyConnection,
    table_names: list[str],
    label: str,
) -> None:
    """Drop transient DuckDB tables/views and log each cleanup.

    This is intentionally defensive; missing tables are ignored.
    """
    print(f"MAINTENANCE {label}: dropping transient tables/views...", flush=True)
    for table_name in table_names:
        try:
            con.execute(f"DROP TABLE IF EXISTS {q(table_name)}")
            con.execute(f"DROP VIEW IF EXISTS {q(table_name)}")
            print(f"MAINTENANCE {label}: dropped if existed: {table_name}", flush=True)
        except Exception as e:
            print(f"WARNING MAINTENANCE {label}: could not drop {table_name}: {e}", flush=True)


def cleanup_after_cdc(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    vacuum: bool = False,
) -> None:
    """Release temp CDC intermediates after a table has been persisted to state_db."""
    transient = [
        f"{table_name}_current_hashed_all",
        f"{table_name}_current_hashed",
        f"{table_name}_previous_compare",
        f"{table_name}_previous_compare_aug",
        f"{table_name}_compared_pre",
        f"{table_name}_compared",
    ]
    drop_tables_if_exist(con, transient, f"after CDC {table_name}")
    checkpoint_and_gc(con, f"after CDC {table_name}", vacuum=vacuum)


def cleanup_before_dk_intelligence(con: duckdb.DuckDBPyConnection) -> None:
    """Reduce memory/disk pressure before derived DK intelligence calculations."""
    print("=" * 80, flush=True)
    print("MAINTENANCE BEFORE DK INTELLIGENCE", flush=True)
    print("=" * 80, flush=True)

    transient = [
        "reference_current_hashed_all",
        "reference_current_hashed",
        "reference_previous_compare",
        "reference_previous_compare_aug",
        "reference_compared_pre",
        "reference_compared",
        "actors_current_hashed_all",
        "actors_current_hashed",
        "actors_previous_compare",
        "actors_previous_compare_aug",
        "actors_compared_pre",
        "actors_compared",
        "udi_current_hashed_all",
        "udi_current_hashed",
        "udi_previous_compare",
        "udi_previous_compare_aug",
        "udi_compared_pre",
        "udi_compared",
    ]
    drop_tables_if_exist(con, transient, "before DK intelligence")
    checkpoint_and_gc(con, "before DK intelligence", vacuum=False)


def cleanup_before_export(con: duckdb.DuckDBPyConnection) -> None:
    """Drop DK intelligence working tables before release export."""
    print("=" * 80, flush=True)
    print("MAINTENANCE BEFORE EXPORT", flush=True)
    print("=" * 80, flush=True)

    transient = [
        "actor_device_current",
        "actor_device_previous",
        "actor_count_current",
        "actor_count_previous",
        "actor_dominant_risk_current",
        "actor_dominant_legislation_current",
        "actor_highest_mdr_mdd_current",
        "actor_highest_ivdr_ivdd_current",
        "actor_legislation_profiles",
        "latest_mdr_mdd_current",
        "oldest_mdr_mdd_current",
        "latest_ivdr_ivdd_current",
        "oldest_ivdr_ivdd_current",
        "actor_migration_current",
        "previous_actor_dk_intel",
        "previous_actor_dk_intel_metrics",
        "previous_actor_dk_intel_snapshot",
    ]
    drop_tables_if_exist(con, transient, "before export")
    checkpoint_and_gc(con, "before export", vacuum=False)


def cleanup_change_events_if_enabled(con: duckdb.DuckDBPyConnection) -> None:
    """Optional controlled cleanup for schema/hash migration noise.

    This function deletes only rows matching:
    - configured event tables
    - configured event dates
    - configured CHANGE_TYPE values

    It does nothing unless ENABLE_CHANGE_EVENT_CLEANUP is True.
    """
    if not ENABLE_CHANGE_EVENT_CLEANUP:
        print("CHANGE EVENT CLEANUP: disabled.", flush=True)
        return

    if not CHANGE_EVENT_CLEANUP_DATES:
        print("CHANGE EVENT CLEANUP: enabled but no dates configured. Nothing deleted.", flush=True)
        return

    if not CHANGE_EVENT_CLEANUP_TYPES:
        print("CHANGE EVENT CLEANUP: enabled but no change types configured. Nothing deleted.", flush=True)
        return

    print("=" * 80, flush=True)
    print("CHANGE EVENT CLEANUP ENABLED", flush=True)
    print(f"Cleanup dates: {CHANGE_EVENT_CLEANUP_DATES}", flush=True)
    print(f"Cleanup types: {CHANGE_EVENT_CLEANUP_TYPES}", flush=True)
    print("=" * 80, flush=True)

    date_placeholders = ", ".join(["?"] * len(CHANGE_EVENT_CLEANUP_DATES))
    type_placeholders = ", ".join(["?"] * len(CHANGE_EVENT_CLEANUP_TYPES))
    params = CHANGE_EVENT_CLEANUP_DATES + CHANGE_EVENT_CLEANUP_TYPES

    for table_name in CHANGE_EVENT_CLEANUP_TABLES:
        if not attached_table_exists(con, "state_db", table_name):
            print(f"CHANGE EVENT CLEANUP: skipped missing table {table_name}", flush=True)
            continue

        cols = get_attached_table_columns(con, "state_db", table_name)
        if "EVENT_DATE" in cols:
            date_col = "EVENT_DATE"
        elif "EXTRACT_DATE" in cols:
            date_col = "EXTRACT_DATE"
        else:
            print(f"CHANGE EVENT CLEANUP: skipped {table_name}, no EVENT_DATE/EXTRACT_DATE", flush=True)
            continue

        before = con.execute(f"SELECT COUNT(*) FROM state_db.{q(table_name)}").fetchone()[0]

        con.execute(
            f"""
            DELETE FROM state_db.{q(table_name)}
            WHERE {q(date_col)} IN ({date_placeholders})
              AND CHANGE_TYPE IN ({type_placeholders})
            """,
            params,
        )

        after = con.execute(f"SELECT COUNT(*) FROM state_db.{q(table_name)}").fetchone()[0]
        print(
            f"OK CHANGE EVENT CLEANUP {table_name}: removed {before - after:,} rows",
            flush=True,
        )

    checkpoint_and_gc(con, "after change event cleanup", vacuum=False)


def load_previous_stats() -> dict[str, Any]:
    if os.path.exists(RUN_STATS_FILE):
        with open(RUN_STATS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_run_stats(stats: dict[str, Any]) -> None:
    with open(RUN_STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)


def generate_release_notes(
    con: duckdb.DuckDBPyConnection,
    stats_dict: dict[str, Any],
    run_time: str,
    mode: str,
    today: str,
) -> str:
    notes = f"""# EUDAMED Data Release

**Run Date & Time:** {run_time}  
**Mode:** `{mode}`  
**Pipeline Version:** `{PIPELINE_VERSION}`

## Canonical EU Layer

| Table | Rows |
|---|---:|
| `reference` | {table_count(con, "reference"):,} |
| `actors` | {table_count(con, "actors"):,} |
| `udi` | {table_count(con, "udi"):,} |

## Canonical CDC Event Tables

| Table | Rows |
|---|---:|
| `reference_change_events` | {table_count(con, "reference_change_events"):,} |
| `actors_change_events` | {table_count(con, "actors_change_events"):,} |
| `udi_change_events` | {table_count(con, "udi_change_events"):,} |

## Danish Intelligence Layer

| Table | Rows |
|---|---:|
| `actor_dk_intel` | {table_count(con, "actor_dk_intel"):,} |
| `udi_dk_intel` | {table_count(con, "udi_dk_intel"):,} |
| `udi_dk_intel_change_events` | {table_count(con, "udi_dk_intel_change_events"):,} |

## CDC Summary This Run

"""

    for table_name in TABLES:
        stats = stats_dict.get(table_name, {})
        notes += f"""### {table_name}

- **Total Rows:** {stats.get("total", 0):,}
- **NEW:** {stats.get("new", 0):,}
- **UPDATED:** {stats.get("updated", 0):,}
- **UPDATED_PRRC:** {stats.get("updated_prrc", 0):,}
- **UPDATED_RISK_CLASS:** {stats.get("updated_risk_class", 0):,}
- **UNCHANGED:** {stats.get("unchanged", 0):,}
- **KEY_MISSING:** {stats.get("key_missing", 0):,}
- **Duplicate Business Key Groups:** {stats.get("duplicate_business_keys", 0):,}
- **Changed Columns Rows:** {stats.get("changed_columns_rows", 0):,}
- **Events Appended:** {stats.get("events_appended", 0):,}
- **Status:** {stats.get("status", "UNKNOWN")}

"""

    notes += """## Files Included

### DuckDB

- `eudamed.duckdb`

The DuckDB file contains:
- canonical EU tables
- global CDC event tables
- Danish intelligence tables

### Standalone CSV

Only Danish intelligence CSV files are exported:

- `actor_dk_intel.csv`
- `udi_dk_intel.csv`
- `udi_dk_intel_change_events.csv`
- `actor_dk_intel_summary.csv`
- `top10_dk_manufacturers.csv`
- `top10_dk_authorised_representatives.csv`
- `eudamed_insights_summary.json`
- `eudamed_dk_exports.zip`

No global CSV or Parquet files are exported in this lightweight release mode.

## CDC Columns

All main canonical tables use:

```text
EXTRACT_DATE
EXTRACT_DATETIME_UTC
FIRST_SEEN_DATE
FIRST_SEEN_DATETIME_UTC
LAST_SEEN_DATE
LAST_SEEN_DATETIME_UTC
CHANGE_TYPE
CHANGED_COLUMNS
CHANGED_COLUMNS_COUNT
CHANGE_SUMMARY
BUSINESS_KEY
ENTITY_VARIANT_KEY
ROW_HASH
```

## Danish Intelligence

The Danish intelligence layer is derived from the complete EU snapshot and includes:
- Danish actors
- Devices related to Danish manufacturers or authorised representatives
- Danish labels from reference using `ID + CODE + LANGUAGE='da'`
- Actor-level device counts and migration scores
- DK-specific UDI change intelligence
"""

    stats_out = {
        "run_time": run_time,
        "extract_date": today,
        "dataset": "eudamed",
        "pipeline_version": PIPELINE_VERSION,
        "database": EXPORT_DB,
        "state_database": STATE_DB,
        "tables": stats_dict,
        "counts": {
            "reference": table_count(con, "reference"),
            "actors": table_count(con, "actors"),
            "udi": table_count(con, "udi"),
            "actor_dk_intel": table_count(con, "actor_dk_intel"),
            "udi_dk_intel": table_count(con, "udi_dk_intel"),
            "udi_dk_intel_change_events": table_count(con, "udi_dk_intel_change_events"),
        },
    }

    save_run_stats(stats_out)
    return notes


def generate_nextlink_release_notes(run_time: str, mode: str) -> str:
    if not os.path.exists(NEXTLINK_DB):
        return f"""# EUDAMED NextLink Release

**Run Date & Time:** {run_time}  
**Mode:** `{mode}`

No nextLink database was created.
"""

    con = duckdb.connect(NEXTLINK_DB)

    total_rows = con.execute("SELECT COUNT(*) FROM nextlinks").fetchone()[0]
    new_rows = con.execute("SELECT COUNT(*) FROM nextlinks WHERE CHANGE_TYPE = 'NEW'").fetchone()[0]
    unchanged_rows = con.execute("SELECT COUNT(*) FROM nextlinks WHERE CHANGE_TYPE = 'UNCHANGED'").fetchone()[0]
    failed_rows = con.execute(
        """
        SELECT COUNT(*)
        FROM nextlinks
        WHERE ERROR_MESSAGE IS NOT NULL
           OR STATUS_CODE >= 400
        """
    ).fetchone()[0]

    endpoint_rows = con.execute(
        """
        SELECT
            ENDPOINT,
            COUNT(*) AS pages,
            SUM(ROWS_COUNT) AS rows_seen
        FROM nextlinks
        WHERE EXTRACT_DATE = ?
        GROUP BY ENDPOINT
        ORDER BY ENDPOINT
        """,
        [now_date()],
    ).fetchall()

    con.close()

    endpoint_summary = ""
    for endpoint, pages, rows_seen in endpoint_rows:
        endpoint_summary += f"- `{endpoint}`: {pages:,} pages | {rows_seen or 0:,} rows seen\n"

    return f"""# EUDAMED NextLink Release

**Run Date & Time:** {run_time}  
**Mode:** `{mode}`

## Summary

- **Total nextLink rows in DB:** {total_rows:,}
- **New page hashes:** {new_rows:,}
- **Unchanged page hashes:** {unchanged_rows:,}
- **Failed/error rows:** {failed_rows:,}

## Current Run by Endpoint

{endpoint_summary if endpoint_summary else "- No endpoint rows found for current run."}
"""


# =============================================================================
# MAIN
# =============================================================================

def skipped_stats(status: str) -> dict[str, Any]:
    return {
        "total": 0,
        "new": 0,
        "updated": 0,
        "updated_prrc": 0,
        "updated_risk_class": 0,
        "unchanged": 0,
        "key_missing": 0,
        "duplicate_business_keys": 0,
        "changed_columns_rows": 0,
        "events_appended": 0,
        "status": status,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=[
            "partitioned",
            "unfiltered",
            "partitioned_nextlink",
            "unfiltered_nextlink",
        ],
        default="partitioned",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_dt_utc = utc_now_dt()
    today = utc_date_string(run_dt_utc)
    run_time = utc_datetime_string(run_dt_utc)
    track_nextlinks = should_track_nextlinks(args.mode)

    if os.path.exists(TEMP_DB):
        os.remove(TEMP_DB)

    ensure_state_db_exists()
    config = load_config()

    stats_dict: dict[str, Any] = {}

    processor_threads: list[Thread] = []
    writer_thread: Thread | None = None

    try:
        print("=" * 80, flush=True)
        print("STARTING EUDAMED PIPELINE", flush=True)
        print(f"Mode: {args.mode}", flush=True)
        print(f"Track nextLinks: {track_nextlinks}", flush=True)
        print(f"Persistent state DB: {STATE_DB}", flush=True)
        print(f"Current run workspace DB: {TEMP_DB}", flush=True)
        print(f"New release output DB: {EXPORT_DB}", flush=True)
        print(f"Fetch workers: {MAX_WORKERS}", flush=True)
        print(f"Process workers: {PROCESS_WORKERS}", flush=True)
        print(f"Batch size: {BATCH_SIZE}", flush=True)
        print("=" * 80, flush=True)
        log_resource_usage("pipeline start")

        processor_threads, writer_thread = start_streaming_pipeline()

        print("\n=== FETCHING REFERENCE DATA ===", flush=True)
        reference_count = fetch_reference(track_nextlinks)
        wait_for_pipeline_idle("reference")

        con = duckdb.connect(TEMP_DB)
        con.execute(f"ATTACH '{STATE_DB}' AS state_db")
        if reference_count > 0:
            normalize_reference_id_columns_in_table(con, "reference")
            stats_dict["reference"] = compute_hashes_and_tracking(con, "reference", today, run_time)
            cleanup_after_cdc(con, "reference", vacuum=False)
        else:
            stats_dict["reference"] = skipped_stats("FAILED_OR_EMPTY")
        con.execute("DETACH state_db")
        con.close()

        print("\n=== FETCHING ACTORS DATA ===", flush=True)
        if args.mode in {"unfiltered", "unfiltered_nextlink"}:
            actor_count = fetch_actors_unfiltered(track_nextlinks)
        else:
            actor_count = fetch_actors_partitioned(config, track_nextlinks)

        wait_for_pipeline_idle("actors")

        con = duckdb.connect(TEMP_DB)
        con.execute(f"ATTACH '{STATE_DB}' AS state_db")
        if actor_count > 0:
            normalize_reference_id_columns_in_table(con, "actors")
            stats_dict["actors"] = compute_hashes_and_tracking(con, "actors", today, run_time)
            cleanup_after_cdc(con, "actors", vacuum=False)
        else:
            stats_dict["actors"] = skipped_stats("FAILED_OR_EMPTY")
        con.execute("DETACH state_db")
        con.close()

        print("\n=== FETCHING UDI DATA ===", flush=True)
        if args.mode in {"unfiltered", "unfiltered_nextlink"}:
            udi_count = fetch_udi_unfiltered(track_nextlinks)
        else:
            udi_count = fetch_udi_partitioned(config, track_nextlinks)

        wait_for_pipeline_idle("udi")

        con = duckdb.connect(TEMP_DB)
        con.execute(f"ATTACH '{STATE_DB}' AS state_db")

        # Preserve previous UDI snapshot before overwriting state_db.udi.
        if attached_table_exists(con, "state_db", "udi"):
            con.execute(
                """
                CREATE OR REPLACE TEMP TABLE previous_udi_for_intel AS
                SELECT *
                FROM state_db.udi
                """
            )
            print("OK Preserved previous UDI snapshot for DK actor previous counts.", flush=True)

        if udi_count > 0:
            normalize_reference_id_columns_in_table(con, "udi")
            stats_dict["udi"] = compute_hashes_and_tracking(con, "udi", today, run_time)
            cleanup_after_cdc(con, "udi", vacuum=False)
        else:
            stats_dict["udi"] = skipped_stats("FAILED_OR_EMPTY")

        cleanup_before_dk_intelligence(con)

        print("\n=== BUILDING DK INTELLIGENCE TABLES ===", flush=True)
        build_dk_intelligence(con)

        cleanup_change_events_if_enabled(con)
        cleanup_before_export(con)

        print("\n=== EXPORTING DUCKDB AND DK CSV FILES ===", flush=True)
        export_duckdb_csv_parquet(con)

        print("\n=== EXPORTING DK INSIGHT ASSETS ===", flush=True)
        export_dk_insight_assets(con, stats_dict)

        print("\n=== GENERATING RELEASE NOTES ===", flush=True)
        release_notes = generate_release_notes(con, stats_dict, run_time, args.mode, today)
        with open(RELEASE_NOTES_FILE, "w", encoding="utf-8") as f:
            f.write(release_notes)

        con.execute("DETACH state_db")
        con.close()

        print("\n=== STOPPING STREAMING PIPELINE ===", flush=True)
        stop_streaming_pipeline(processor_threads, writer_thread)
        processor_threads = []
        writer_thread = None

        if track_nextlinks:
            print("\n=== WRITING NEXTLINK DATABASE ===", flush=True)
            write_nextlink_events_to_db()

            nextlink_release_notes = generate_nextlink_release_notes(run_time, args.mode)
            with open(NEXTLINK_RELEASE_NOTES_FILE, "w", encoding="utf-8") as f:
                f.write(nextlink_release_notes)

        print(f"OK Saved {RELEASE_NOTES_FILE}", flush=True)
        if track_nextlinks:
            print(f"OK Saved {NEXTLINK_RELEASE_NOTES_FILE}", flush=True)

        print("\n=== EUDAMED PIPELINE COMPLETE ===", flush=True)
        for table_name in TABLES:
            stats = stats_dict.get(table_name, {})
            print(
                f"{table_name}: "
                f"{stats.get('total', 0):,} total | "
                f"{stats.get('new', 0):,} NEW | "
                f"{stats.get('updated', 0):,} UPDATED | "
                f"{stats.get('updated_prrc', 0):,} UPDATED_PRRC | "
                f"{stats.get('updated_risk_class', 0):,} UPDATED_RISK_CLASS | "
                f"{stats.get('unchanged', 0):,} UNCHANGED | "
                f"{stats.get('key_missing', 0):,} KEY_MISSING | "
                f"{stats.get('duplicate_business_keys', 0):,} duplicate key groups | "
                f"{stats.get('changed_columns_rows', 0):,} changed-column rows | "
                f"{stats.get('events_appended', 0):,} events appended | "
                f"{stats.get('status', 'UNKNOWN')}",
                flush=True,
            )

    finally:
        if writer_thread is not None and processor_threads:
            try:
                stop_streaming_pipeline(processor_threads, writer_thread)
            except Exception as e:
                print(f"WARNING Could not cleanly stop streaming pipeline: {e}", flush=True)


if __name__ == "__main__":
    main()