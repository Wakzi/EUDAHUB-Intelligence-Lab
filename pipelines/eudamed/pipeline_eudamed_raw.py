from __future__ import annotations

"""
EUDAHUB Intelligence - EUDAMED RAW pipeline

Purpose
-------
This pipeline is intentionally boring and conservative:

1. Fetch EUDAMED source endpoints.
2. Store only source tables in DuckDB:
   - reference
   - actors
   - udi
3. Write sidecar metadata JSON.
4. Write release notes.
5. Optionally store API trace / nextLink observations when *_trace modes are used.

Important EUDAHUB principle
---------------------------
RAW is the source truth for the exact run and timestamp.
Therefore this pipeline does NOT add CDC columns, DK subset, intelligence, labels,
metrics, joins, mappings, or any EUDAHUB interpretation.

The only unavoidable technical transformation is that nested JSON values are stored
as compact JSON text and scalar values are stored as text/NULL in DuckDB columns so
schema evolution can be handled reliably across batches and API changes.
Column names are kept exactly as exposed by the API.
"""

import argparse
import hashlib
import json
import os
import shutil
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
# CONFIG
# =============================================================================

BASE_URL = "https://api.datalake.sante.service.ec.europa.eu/eudamed"

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_FILE = SCRIPT_DIR / "config" / "eudamed_filters.json"

PIPELINE_NAME = "eudamed_raw"
PIPELINE_VERSION = "2.0.0-raw"

RAW_LATEST_DB = "eudamed_raw_latest.duckdb"
TRACE_LATEST_DB = "eudamed_trace_latest.duckdb"

RAW_LATEST_METADATA = "eudamed_raw_latest.metadata.json"
TRACE_LATEST_METADATA = "eudamed_trace_latest.metadata.json"

RAW_RELEASE_NOTES = "RELEASE_NOTES_EUDAMED_RAW.md"
TRACE_RELEASE_NOTES = "RELEASE_NOTES_EUDAMED_TRACE.md"

PREVIOUS_METADATA_DEFAULT = "previous_raw_metadata.json"

TABLES = ["reference", "actors", "udi"]

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

HEADERS = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
}

PROCESS_QUEUE: Queue = Queue(maxsize=PROCESS_QUEUE_MAXSIZE)
WRITE_QUEUE: Queue = Queue(maxsize=WRITE_QUEUE_MAXSIZE)
STOP = object()

TRACE_EVENTS: list[dict[str, Any]] = []
TRACE_LOCK = threading.Lock()


# =============================================================================
# TIME / SMALL HELPERS
# =============================================================================

def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def utc_date_string(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def utc_timestamp_string(dt: datetime) -> str:
    # ISO-8601 UTC timestamp, good for JSON and release notes.
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def timestamp_compact(dt: datetime) -> str:
    # Good for filenames/tags, sortable and compact.
    return dt.strftime("%Y%m%d_%H%M%S")


def q(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def md5_text(value: str | None) -> str | None:
    if value is None:
        return None
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {q(table_name)} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def table_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    if not table_exists(con, table_name):
        return 0
    return int(con.execute(f"SELECT COUNT(*) FROM {q(table_name)}").fetchone()[0])


def get_table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    if not table_exists(con, table_name):
        return []
    return [row[0] for row in con.execute(f"DESCRIBE {q(table_name)}").fetchall()]


def log_resource_usage(label: str) -> None:
    try:
        import resource

        usage_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        print(f"RESOURCE {label}: max RSS {usage_mb:,.1f} MB", flush=True)
    except Exception as e:
        print(f"RESOURCE {label}: memory unavailable: {e}", flush=True)

    try:
        total, used, free = shutil.disk_usage(".")
        print(
            f"RESOURCE {label}: disk used {used / (1024**3):,.2f} GB | "
            f"free {free / (1024**3):,.2f} GB",
            flush=True,
        )
    except Exception as e:
        print(f"RESOURCE {label}: disk unavailable: {e}", flush=True)


def checkpoint(con: duckdb.DuckDBPyConnection, label: str) -> None:
    try:
        con.execute("CHECKPOINT")
        print(f"OK CHECKPOINT: {label}", flush=True)
    except Exception as e:
        print(f"WARNING CHECKPOINT failed for {label}: {e}", flush=True)


# =============================================================================
# MODE HANDLING
# =============================================================================

def parse_mode(mode: str) -> tuple[str, bool]:
    """Return (fetch_strategy, trace_enabled)."""
    if mode == "partitioned":
        return "partitioned", False
    if mode == "partitioned_trace":
        return "partitioned", True
    if mode == "full":
        return "full", False
    if mode == "full_trace":
        return "full", True
    raise ValueError(f"Unsupported mode: {mode}")


# =============================================================================
# CONFIG / HTTP
# =============================================================================

def load_config(config_file: Path) -> dict[str, Any]:
    with open(config_file, "r", encoding="utf-8") as f:
        return json.load(f)


def request_json(
    url: str,
    params: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, int | None, int | None, str | None, str | None]:
    """GET JSON with retries. Returns payload plus request diagnostics."""
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
# TRACE COLLECTION
# =============================================================================

def collect_trace_event(
    enabled: bool,
    endpoint: str,
    label: str,
    page: int,
    rows_count: int,
    status_code: int | None,
    request_duration_ms: int | None,
    request_url: str | None,
    next_link: str | None,
    fetched_at_utc: str,
    error_message: str | None = None,
) -> None:
    if not enabled:
        return

    page_hash_source = "|".join(
        [str(endpoint), str(label), str(page), str(request_url), str(next_link)]
    )

    event = {
        "FETCHED_AT_UTC": fetched_at_utc,
        "ENDPOINT": endpoint,
        "PARTITION_LABEL": label,
        "PAGE_NUMBER": page,
        "ROWS_RETURNED": rows_count,
        "STATUS_CODE": status_code,
        "REQUEST_DURATION_MS": request_duration_ms,
        "REQUEST_URL": request_url,
        "NEXT_LINK": next_link,
        "PAGE_HASH": md5_text(page_hash_source),
        "REQUEST_HASH": md5_text(request_url),
        "NEXT_LINK_HASH": md5_text(next_link),
        "ERROR_MESSAGE": error_message,
    }

    with TRACE_LOCK:
        TRACE_EVENTS.append(event)


def write_trace_db(trace_db_path: str) -> dict[str, Any]:
    if os.path.exists(trace_db_path):
        os.remove(trace_db_path)

    con = duckdb.connect(trace_db_path)

    if TRACE_EVENTS:
        df = pd.DataFrame(TRACE_EVENTS)
        con.register("trace_pages_view", df)
        con.execute(
            """
            CREATE TABLE trace_pages AS
            SELECT *
            FROM trace_pages_view
            """
        )
        con.unregister("trace_pages_view")
    else:
        con.execute(
            """
            CREATE TABLE trace_pages (
                FETCHED_AT_UTC VARCHAR,
                ENDPOINT VARCHAR,
                PARTITION_LABEL VARCHAR,
                PAGE_NUMBER INTEGER,
                ROWS_RETURNED INTEGER,
                STATUS_CODE INTEGER,
                REQUEST_DURATION_MS INTEGER,
                REQUEST_URL VARCHAR,
                NEXT_LINK VARCHAR,
                PAGE_HASH VARCHAR,
                REQUEST_HASH VARCHAR,
                NEXT_LINK_HASH VARCHAR,
                ERROR_MESSAGE VARCHAR
            )
            """
        )

    con.execute(
        """
        CREATE TABLE trace_partitions AS
        SELECT
            ENDPOINT,
            PARTITION_LABEL,
            COUNT(*) AS PAGES,
            SUM(ROWS_RETURNED) AS ROWS_RETURNED,
            MIN(FETCHED_AT_UTC) AS FIRST_PAGE_AT_UTC,
            MAX(FETCHED_AT_UTC) AS LAST_PAGE_AT_UTC,
            SUM(CASE WHEN ERROR_MESSAGE IS NOT NULL THEN 1 ELSE 0 END) AS ERROR_PAGES
        FROM trace_pages
        GROUP BY ENDPOINT, PARTITION_LABEL
        ORDER BY ENDPOINT, PARTITION_LABEL
        """
    )

    stats = {
        "trace_pages": table_count(con, "trace_pages"),
        "trace_partitions": table_count(con, "trace_partitions"),
        "trace_error_pages": int(
            con.execute(
                "SELECT COUNT(*) FROM trace_pages WHERE ERROR_MESSAGE IS NOT NULL"
            ).fetchone()[0]
        ),
    }

    con.execute("CHECKPOINT")
    con.close()

    print(f"OK wrote trace DB: {trace_db_path} ({stats})", flush=True)
    return stats


# =============================================================================
# RAW VALUE NORMALIZATION FOR DUCKDB STORAGE
# =============================================================================

def raw_cell_to_storage_value(value: Any) -> str | None:
    """Store API values reliably without filtering/renaming source fields.

    DuckDB tables need stable column types across batches. To avoid type conflicts
    when the API changes or returns mixed values, values are stored as VARCHAR.
    - None stays NULL.
    - dict/list are compact JSON text.
    - scalar values are converted to text.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def normalize_rows_for_duckdb(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        normalized_rows.append(
            {str(key): raw_cell_to_storage_value(value) for key, value in row.items()}
        )

    return pd.DataFrame(normalized_rows, dtype="object")


# =============================================================================
# STREAMING WRITER
# =============================================================================

def append_df_to_raw_db(
    table_name: str,
    df: pd.DataFrame,
    con_raw: duckdb.DuckDBPyConnection,
) -> int:
    if df.empty:
        return 0

    # Source column names are kept as-is. New source columns are added when seen.
    if table_exists(con_raw, table_name):
        existing_cols = [
            row[1]
            for row in con_raw.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        ]

        for col in df.columns:
            if col not in existing_cols:
                con_raw.execute(f"ALTER TABLE {q(table_name)} ADD COLUMN {q(col)} VARCHAR")
                existing_cols.append(col)
                print(f"INFO raw schema expanded: {table_name}.{col}", flush=True)

        for col in existing_cols:
            if col not in df.columns:
                df[col] = None

        df = df[existing_cols]

    view_name = f"{table_name}_append_view"
    con_raw.register(view_name, df)

    if not table_exists(con_raw, table_name):
        col_defs = ", ".join(f"{q(col)} VARCHAR" for col in df.columns)
        con_raw.execute(f"CREATE TABLE {q(table_name)} ({col_defs})")
        con_raw.execute(f"INSERT INTO {q(table_name)} SELECT * FROM {q(view_name)}")
    else:
        con_raw.execute(f"INSERT INTO {q(table_name)} SELECT * FROM {q(view_name)}")

    con_raw.unregister(view_name)
    return len(df)


def enqueue_rows_for_processing(table_name: str, rows: list[dict[str, Any]], label: str) -> int:
    if not rows:
        return 0
    PROCESS_QUEUE.put({"table_name": table_name, "rows": rows, "label": label})
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

            df = normalize_rows_for_duckdb(item["rows"])
            if not df.empty:
                WRITE_QUEUE.put(
                    {
                        "table_name": item["table_name"],
                        "df": df,
                        "label": item.get("label", item["table_name"]),
                    }
                )
        except Exception as e:
            print(f"ERROR PROCESSOR-{worker_id} failed: {e}", flush=True)
            raise
        finally:
            PROCESS_QUEUE.task_done()


def writer_worker(raw_db_path: str, expected_processor_stops: int) -> None:
    print("WRITER started", flush=True)

    con_raw = duckdb.connect(raw_db_path)
    con_raw.execute("SET preserve_insertion_order=false")

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
                        f"WRITER received stop signal {stopped_processors}/{expected_processor_stops}",
                        flush=True,
                    )
                    if stopped_processors >= expected_processor_stops:
                        print(
                            f"WRITER stopping. Batches={write_batches:,}; "
                            f"rows={rows_written_total:,}; by_table={rows_by_table}",
                            flush=True,
                        )
                        return
                    continue

                table_name = item["table_name"]
                df = item["df"]
                label = item.get("label", table_name)

                inserted = append_df_to_raw_db(table_name, df, con_raw)

                write_batches += 1
                rows_written_total += inserted
                rows_by_table[table_name] = rows_by_table.get(table_name, 0) + inserted

                if write_batches == 1 or write_batches % LOG_WRITES_EVERY == 0:
                    print(
                        f"WRITER batch {write_batches:,}: {label} -> {inserted:,} rows | "
                        f"total written={rows_written_total:,}",
                        flush=True,
                    )
            except Exception as e:
                print(f"ERROR WRITER failed: {e}", flush=True)
                raise
            finally:
                WRITE_QUEUE.task_done()
    finally:
        checkpoint(con_raw, "writer final")
        con_raw.close()


def start_streaming_pipeline(raw_db_path: str) -> tuple[list[Thread], Thread]:
    processor_threads = [
        Thread(target=processor_worker, args=(i + 1,), daemon=True)
        for i in range(PROCESS_WORKERS)
    ]
    writer_thread = Thread(
        target=writer_worker,
        args=(raw_db_path, PROCESS_WORKERS),
        daemon=True,
    )

    for thread in processor_threads:
        thread.start()
    writer_thread.start()
    return processor_threads, writer_thread


def wait_for_pipeline_idle(stage_name: str) -> None:
    print(f"WAIT pipeline drain: {stage_name}", flush=True)
    PROCESS_QUEUE.join()
    WRITE_QUEUE.join()
    print(f"OK pipeline drained: {stage_name}", flush=True)


def stop_streaming_pipeline(processor_threads: list[Thread], writer_thread: Thread) -> None:
    print("Stopping streaming pipeline...", flush=True)
    for _ in range(PROCESS_WORKERS):
        PROCESS_QUEUE.put(STOP)
    PROCESS_QUEUE.join()
    WRITE_QUEUE.join()
    for thread in processor_threads:
        thread.join()
    writer_thread.join()
    print("OK streaming pipeline stopped", flush=True)


# =============================================================================
# FETCH REFERENCE / ACTORS / UDI
# =============================================================================

def stream_pages_to_processing_queue(
    endpoint: str,
    params: dict[str, Any],
    label: str,
    table_name: str,
    trace_enabled: bool,
    fetched_at_utc: str,
) -> int:
    total_rows = 0
    buffer: list[dict[str, Any]] = []

    url = f"{BASE_URL}/{endpoint}"
    page = 1
    next_params = params

    while url:
        data, status_code, duration_ms, request_url, error_message = request_json(url, next_params)

        if data is None:
            collect_trace_event(
                enabled=trace_enabled,
                endpoint=endpoint,
                label=label,
                page=page,
                rows_count=0,
                status_code=status_code,
                request_duration_ms=duration_ms,
                request_url=request_url,
                next_link=None,
                fetched_at_utc=fetched_at_utc,
                error_message=error_message,
            )
            print(f"WARNING stopped pagination for {label}. queued={total_rows:,}", flush=True)
            break

        rows = data.get("value", [])
        next_link = data.get("nextLink")

        collect_trace_event(
            enabled=trace_enabled,
            endpoint=endpoint,
            label=label,
            page=page,
            rows_count=len(rows),
            status_code=status_code,
            request_duration_ms=duration_ms,
            request_url=request_url,
            next_link=next_link,
            fetched_at_utc=fetched_at_utc,
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
            print(f"{label}: page {page:,}, rows seen={total_rows + len(buffer):,}", flush=True)

        url = next_link
        next_params = None
        page += 1

    if buffer:
        queued = enqueue_rows_for_processing(table_name, buffer, label)
        total_rows += queued
        print(f"QUEUED {label}: final batch. total={total_rows:,}", flush=True)

    return total_rows


def fetch_reference(trace_enabled: bool, fetched_at_utc: str) -> int:
    total_rows = 0

    for language in ["da", "en"]:
        params = {"LANGUAGE": language, "format": "json", "api-version": "v1.0"}
        label = f"reference_{language}"
        total_rows += stream_pages_to_processing_queue(
            endpoint="reference",
            params=params,
            label=label,
            table_name="reference",
            trace_enabled=trace_enabled,
            fetched_at_utc=fetched_at_utc,
        )

    print(f"OK queued {total_rows:,} reference rows", flush=True)
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
    trace_enabled: bool,
    fetched_at_utc: str,
) -> tuple[str, int, str | None]:
    try:
        row_count = stream_pages_to_processing_queue(
            endpoint=endpoint,
            params=params,
            label=label,
            table_name=table_name,
            trace_enabled=trace_enabled,
            fetched_at_utc=fetched_at_utc,
        )
        return label, row_count, None
    except Exception as e:
        return label, 0, str(e)


def fetch_actors_partitioned(config: dict[str, Any], trace_enabled: bool, fetched_at_utc: str) -> int:
    actors_config = config["actors_parameters"]
    if not actors_config.get("active", True):
        print("WARNING actors_parameters inactive. actors skipped.", flush=True)
        return 0

    request_parameter = actors_config["request_parameter"]
    values = active_parameter_values(actors_config)

    total_rows = 0
    completed = 0

    print("=" * 80, flush=True)
    print("FETCHING ACTORS PARTITIONED", flush=True)
    print(f"Parameter: {request_parameter}; partitions={len(values)}; workers={MAX_WORKERS}", flush=True)
    print("=" * 80, flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for value in values:
            params = {request_parameter: value, "format": "json", "api-version": "v1.0"}
            label = f"actors_{request_parameter}_{value}"
            futures[
                executor.submit(
                    fetch_partition_stream_safe,
                    "actors",
                    params,
                    label,
                    "actors",
                    trace_enabled,
                    fetched_at_utc,
                )
            ] = label

        for future in as_completed(futures):
            label, row_count, error = future.result()
            completed += 1
            if error:
                print(f"WARNING [{completed}/{len(futures)}] failed {label}: {error}", flush=True)
                continue
            total_rows += row_count
            print(f"OK [{completed}/{len(futures)}] {label}: {row_count:,}; total={total_rows:,}", flush=True)

    return total_rows


def fetch_actors_full(trace_enabled: bool, fetched_at_utc: str) -> int:
    return stream_pages_to_processing_queue(
        endpoint="actors",
        params={"format": "json", "api-version": "v1.0"},
        label="actors_full",
        table_name="actors",
        trace_enabled=trace_enabled,
        fetched_at_utc=fetched_at_utc,
    )


def build_udi_partitions(config: dict[str, Any]) -> list[dict[str, Any]]:
    partitions: list[dict[str, Any]] = []
    covered_parameter_values = set()

    for rule in config.get("udi_combination_rules", []):
        if not rule.get("active", True):
            continue
        rule_name = rule.get("name", "combination")
        for pair in rule.get("pairs", []):
            pair_params = {key: value for key, value in pair.items() if key != "label" and value is not None}
            if not pair_params:
                continue
            partitions.append(pair_params)
            for key, value in pair_params.items():
                covered_parameter_values.add((key, value))
            if pair.get("label"):
                print(f"Configured UDI combination partition: {rule_name}/{pair['label']}", flush=True)

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


def fetch_udi_partitioned(config: dict[str, Any], trace_enabled: bool, fetched_at_utc: str) -> int:
    partitions = build_udi_partitions(config)
    if not partitions:
        print("WARNING no active UDI partitions. udi skipped.", flush=True)
        return 0

    total_rows = 0
    completed = 0

    print("=" * 80, flush=True)
    print("FETCHING UDI PARTITIONED", flush=True)
    print(f"partitions={len(partitions)}; workers={MAX_WORKERS}; batch_size={BATCH_SIZE}", flush=True)
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
                    trace_enabled,
                    fetched_at_utc,
                )
            ] = label

        for future in as_completed(futures):
            label, row_count, error = future.result()
            completed += 1
            if error:
                print(f"WARNING [{completed}/{len(futures)}] failed {label}: {error}", flush=True)
                continue
            total_rows += row_count
            print(f"OK [{completed}/{len(futures)}] {label}: {row_count:,}; total={total_rows:,}", flush=True)

    return total_rows


def fetch_udi_full(trace_enabled: bool, fetched_at_utc: str) -> int:
    return stream_pages_to_processing_queue(
        endpoint="udi",
        params={"format": "json", "api-version": "v1.0"},
        label="udi_full",
        table_name="udi",
        trace_enabled=trace_enabled,
        fetched_at_utc=fetched_at_utc,
    )


# =============================================================================
# METADATA / RELEASE NOTES
# =============================================================================

def schema_for_db(db_path: str) -> dict[str, Any]:
    con = duckdb.connect(db_path, read_only=True)
    try:
        tables: dict[str, Any] = {}
        for table in TABLES:
            columns = get_table_columns(con, table)
            tables[table] = {
                "columns": columns,
                "column_count": len(columns),
                "row_count": table_count(con, table),
            }
        return {"tables": tables}
    finally:
        con.close()


def load_previous_metadata(path: str | Path) -> dict[str, Any] | None:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"WARNING could not read previous metadata {p}: {e}", flush=True)
        return None


def detect_schema_changes(
    previous_metadata: dict[str, Any] | None,
    current_schema: dict[str, Any],
) -> dict[str, Any]:
    changes: dict[str, Any] = {}
    previous_tables = ((previous_metadata or {}).get("schema") or {}).get("tables") or {}
    current_tables = current_schema.get("tables") or {}

    for table in TABLES:
        prev_cols = set((previous_tables.get(table) or {}).get("columns") or [])
        curr_cols = set((current_tables.get(table) or {}).get("columns") or [])
        added = sorted(curr_cols - prev_cols)
        removed = sorted(prev_cols - curr_cols)
        changes[table] = {
            "added_columns": added,
            "removed_columns": removed,
            "added_column_count": len(added),
            "removed_column_count": len(removed),
        }

    return changes


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"OK wrote JSON: {path}", flush=True)


def format_duration(seconds: int | float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def generate_raw_release_notes(metadata: dict[str, Any]) -> str:
    schema_changes = metadata.get("schema_changes", {})
    schema_lines = []
    for table in TABLES:
        entry = schema_changes.get(table, {})
        added = entry.get("added_columns", [])
        removed = entry.get("removed_columns", [])
        if added:
            schema_lines.append(f"- `{table}` added columns: " + ", ".join(f"`{c}`" for c in added))
        if removed:
            schema_lines.append(f"- `{table}` removed columns: " + ", ".join(f"`{c}`" for c in removed))
    if not schema_lines:
        schema_lines.append("- No schema changes detected compared with previous raw metadata.")

    counts = metadata.get("counts", {})
    notes = f"""# EUDAMED Raw Snapshot

This release contains a raw EUDAMED snapshot. The DuckDB database contains only source tables and no EUDAHUB CDC, DK subset, enrichment, intelligence, or derived fields.

## Run

- **Pipeline:** `{metadata.get('pipeline_name')}`
- **Pipeline version:** `{metadata.get('pipeline_version')}`
- **Mode:** `{metadata.get('mode')}`
- **Trace enabled:** `{metadata.get('trace_enabled')}`
- **Extract date:** `{metadata.get('extract_date')}`
- **Extracted at UTC:** `{metadata.get('extracted_at_utc')}`
- **Duration:** {format_duration(metadata.get('duration_seconds', 0))}

## Row counts

| Table | Rows |
|---|---:|
| `reference` | {counts.get('reference', 0):,} |
| `actors` | {counts.get('actors', 0):,} |
| `udi` | {counts.get('udi', 0):,} |
| **Total** | {counts.get('total', 0):,} |

## Schema notes

{chr(10).join(schema_lines)}

## Assets

- `{metadata.get('raw_asset_name')}`
- `{metadata.get('metadata_asset_name')}`

## EUDAHUB raw principle

RAW is 1:1 with the source fields exposed by EUDAMED for this run and timestamp. Raw does not add CDC columns, labels, mappings, DK subset, or intelligence.
"""
    return notes


def generate_trace_release_notes(metadata: dict[str, Any]) -> str:
    trace = metadata.get("trace", {})
    notes = f"""# EUDAMED Trace Research Snapshot

This release is for EUDAHUB API research only. It is not an official EUDAMED data product.

Trace is used to study pagination and nextLink behaviour, including whether nextLink observations can later support faster or incremental fetch strategies.

## Run

- **Pipeline:** `{metadata.get('pipeline_name')}`
- **Pipeline version:** `{metadata.get('pipeline_version')}`
- **Mode:** `{metadata.get('mode')}`
- **Extracted at UTC:** `{metadata.get('extracted_at_utc')}`

## Trace counts

| Item | Count |
|---|---:|
| Trace pages | {trace.get('trace_pages', 0):,} |
| Trace partitions | {trace.get('trace_partitions', 0):,} |
| Error pages | {trace.get('trace_error_pages', 0):,} |

## Assets

- `{metadata.get('trace_asset_name')}`
- `{metadata.get('trace_metadata_asset_name')}`
"""
    return notes


# =============================================================================
# CLI / MAIN
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="EUDAHUB EUDAMED raw pipeline")
    parser.add_argument(
        "--mode",
        choices=["partitioned", "partitioned_trace", "full", "full_trace"],
        default="partitioned",
        help="Fetch strategy. Mode changes fetch strategy/trace only, never the raw data model.",
    )
    parser.add_argument(
        "--config-file",
        default=str(DEFAULT_CONFIG_FILE),
        help="Path to eudamed_filters.json.",
    )
    parser.add_argument(
        "--previous-metadata",
        default=PREVIOUS_METADATA_DEFAULT,
        help="Optional previous raw metadata JSON used only for schema change notes.",
    )
    parser.add_argument(
        "--timestamp",
        default=None,
        help="Optional UTC timestamp compact format YYYYMMDD_HHMMSS. Mostly for reproducible tests.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    strategy, trace_enabled = parse_mode(args.mode)

    run_started = utc_now_dt()
    extract_date = utc_date_string(run_started)
    extracted_at_utc = utc_timestamp_string(run_started)
    ts = args.timestamp or timestamp_compact(run_started)

    raw_dated_db = f"eudamed_raw_{ts}.duckdb"
    raw_dated_metadata = f"eudamed_raw_{ts}.metadata.json"
    trace_dated_db = f"eudamed_trace_{ts}.duckdb"
    trace_dated_metadata = f"eudamed_trace_{ts}.metadata.json"

    for path in [RAW_LATEST_DB, raw_dated_db, TRACE_LATEST_DB, trace_dated_db]:
        if os.path.exists(path):
            os.remove(path)

    print("=" * 80, flush=True)
    print("STARTING EUDAMED RAW PIPELINE", flush=True)
    print(f"Mode: {args.mode}", flush=True)
    print(f"Fetch strategy: {strategy}", flush=True)
    print(f"Trace enabled: {trace_enabled}", flush=True)
    print(f"Extracted at UTC: {extracted_at_utc}", flush=True)
    print(f"Raw latest DB: {RAW_LATEST_DB}", flush=True)
    print(f"Raw dated DB: {raw_dated_db}", flush=True)
    print(f"Config file: {args.config_file}", flush=True)
    print("=" * 80, flush=True)
    log_resource_usage("pipeline start")

    config = load_config(Path(args.config_file))

    processor_threads: list[Thread] = []
    writer_thread: Thread | None = None

    row_counts = {"reference": 0, "actors": 0, "udi": 0}

    try:
        processor_threads, writer_thread = start_streaming_pipeline(RAW_LATEST_DB)

        print("\n=== FETCHING REFERENCE DATA ===", flush=True)
        row_counts["reference"] = fetch_reference(trace_enabled, extracted_at_utc)
        wait_for_pipeline_idle("reference")

        print("\n=== FETCHING ACTORS DATA ===", flush=True)
        if strategy == "full":
            row_counts["actors"] = fetch_actors_full(trace_enabled, extracted_at_utc)
        else:
            row_counts["actors"] = fetch_actors_partitioned(config, trace_enabled, extracted_at_utc)
        wait_for_pipeline_idle("actors")

        print("\n=== FETCHING UDI DATA ===", flush=True)
        if strategy == "full":
            row_counts["udi"] = fetch_udi_full(trace_enabled, extracted_at_utc)
        else:
            row_counts["udi"] = fetch_udi_partitioned(config, trace_enabled, extracted_at_utc)
        wait_for_pipeline_idle("udi")

        print("\n=== STOPPING STREAMING PIPELINE ===", flush=True)
        stop_streaming_pipeline(processor_threads, writer_thread)
        processor_threads = []
        writer_thread = None

        # Validate final DB counts from DuckDB, not only queued counts.
        con = duckdb.connect(RAW_LATEST_DB)
        actual_counts = {table: table_count(con, table) for table in TABLES}
        checkpoint(con, "raw latest final")
        con.close()

        shutil.copyfile(RAW_LATEST_DB, raw_dated_db)
        print(f"OK copied {RAW_LATEST_DB} -> {raw_dated_db}", flush=True)

        current_schema = schema_for_db(RAW_LATEST_DB)
        previous_metadata = load_previous_metadata(args.previous_metadata)
        schema_changes = detect_schema_changes(previous_metadata, current_schema)

        duration_seconds = int((utc_now_dt() - run_started).total_seconds())
        total_rows = sum(actual_counts.values())

        metadata: dict[str, Any] = {
            "pipeline_name": PIPELINE_NAME,
            "pipeline_version": PIPELINE_VERSION,
            "dataset": "eudamed",
            "layer": "raw",
            "mode": args.mode,
            "fetch_strategy": strategy,
            "trace_enabled": trace_enabled,
            "extract_date": extract_date,
            "extracted_at_utc": extracted_at_utc,
            "duration_seconds": duration_seconds,
            "counts": {
                "reference": actual_counts.get("reference", 0),
                "actors": actual_counts.get("actors", 0),
                "udi": actual_counts.get("udi", 0),
                "total": total_rows,
            },
            "queued_counts": row_counts,
            "schema": current_schema,
            "schema_changes": schema_changes,
            "raw_asset_name": raw_dated_db,
            "metadata_asset_name": raw_dated_metadata,
            "latest_raw_asset_name": RAW_LATEST_DB,
            "latest_metadata_asset_name": RAW_LATEST_METADATA,
            "github": {
                "repository": os.environ.get("GITHUB_REPOSITORY"),
                "run_id": os.environ.get("GITHUB_RUN_ID"),
                "run_number": os.environ.get("GITHUB_RUN_NUMBER"),
                "sha": os.environ.get("GITHUB_SHA"),
            },
            "principles": {
                "raw": "Raw contains source tables only. No CDC, DK subset, enrichment, intelligence, labels, mappings, or derived fields are added.",
                "mode": "Mode changes fetch strategy and optional trace collection only; it does not change the raw data model.",
            },
        }

        if trace_enabled:
            trace_stats = write_trace_db(TRACE_LATEST_DB)
            shutil.copyfile(TRACE_LATEST_DB, trace_dated_db)
            print(f"OK copied {TRACE_LATEST_DB} -> {trace_dated_db}", flush=True)
            metadata["trace"] = trace_stats
            metadata["trace_asset_name"] = trace_dated_db
            metadata["trace_metadata_asset_name"] = trace_dated_metadata
            metadata["latest_trace_asset_name"] = TRACE_LATEST_DB
            metadata["latest_trace_metadata_asset_name"] = TRACE_LATEST_METADATA

        write_json(RAW_LATEST_METADATA, metadata)
        write_json(raw_dated_metadata, metadata)

        with open(RAW_RELEASE_NOTES, "w", encoding="utf-8") as f:
            f.write(generate_raw_release_notes(metadata))
        print(f"OK wrote {RAW_RELEASE_NOTES}", flush=True)

        if trace_enabled:
            trace_metadata = {
                **metadata,
                "layer": "trace",
                "dataset": "eudamed_trace",
                "purpose": "API pagination and nextLink research. Not an official EUDAHUB data product.",
            }
            write_json(TRACE_LATEST_METADATA, trace_metadata)
            write_json(trace_dated_metadata, trace_metadata)
            with open(TRACE_RELEASE_NOTES, "w", encoding="utf-8") as f:
                f.write(generate_trace_release_notes(trace_metadata))
            print(f"OK wrote {TRACE_RELEASE_NOTES}", flush=True)

        print("\n=== EUDAMED RAW PIPELINE COMPLETE ===", flush=True)
        for table, count in actual_counts.items():
            print(f"{table}: {count:,} rows", flush=True)
        if trace_enabled:
            print(f"trace_pages: {metadata.get('trace', {}).get('trace_pages', 0):,}", flush=True)

    finally:
        if writer_thread is not None and processor_threads:
            try:
                stop_streaming_pipeline(processor_threads, writer_thread)
            except Exception as e:
                print(f"WARNING could not cleanly stop streaming pipeline: {e}", flush=True)


if __name__ == "__main__":
    main()
