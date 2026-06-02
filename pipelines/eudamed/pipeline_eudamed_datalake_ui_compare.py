from __future__ import annotations

"""
EUDAHUB Intelligence - EUDAMED Datalake vs UI API comparison

Purpose
-------
This pipeline compares two existing EUDAMED snapshots:

1. EUDAMED Datalake raw DuckDB
   - usually from EUDAHUB-Intelligence-Data release: eudamed-raw-latest
   - expected table: udi

2. EUDAMED UI Lab DuckDB
   - usually from EUDAHUB-Intelligence release: eudamed-ui-lab-latest
   - table names may change over time, so this script auto-detects a best table

The output is deliberately insight-only:
- CSV files with inventories, column overlap and key overlap
- Markdown release notes

It does NOT create a new DuckDB release, does NOT alter source DBs, and does NOT
try to build EUDAHUB intelligence. It is a diagnostic tool to answer:

    Is Datalake sufficient, or does UI API expose important extra data?

Design rules
------------
- Read-only comparison.
- No raw/source mutation.
- Robust table/key auto-detection, with CLI overrides.
- CSV-first outputs so results are easy to inspect in GitHub release assets.
"""

import argparse
import csv
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

PIPELINE_NAME = "eudamed_datalake_ui_compare"
PIPELINE_VERSION = "1.0.0"

OUT_DIR_DEFAULT = "dist/eudamed_datalake_ui_compare"
RELEASE_NOTES_NAME = "RELEASE_NOTES_EUDAMED_DATALAKE_UI_COMPARE.md"
SUMMARY_JSON_NAME = "eudamed_datalake_ui_compare_summary.json"

# Candidate keys are tried in order. The script chooses the first key that exists
# in both compared tables. UUID is expected to be the best UDI row key.
KEY_CANDIDATES = [
    "UUID",
    "uuid",
    "BASIC_UDI_DI",
    "BASICUDI",
    "BASIC_UDI",
    "BASICUDI_DI",
    "PRIMARY_DI",
    "DI",
    "ID",
    "id",
]

# Avoid picking pure metadata tables as the main UI comparison table.
UI_TABLE_NEGATIVE_PATTERNS = [
    "trace",
    "stat",
    "stats",
    "meta",
    "metadata",
    "nextlink",
    "log",
]

UI_TABLE_POSITIVE_PATTERNS = [
    "udi",
    "device",
    "devices",
    "basic",
    "detail",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso_utc(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def date_utc(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def q(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_") or "table"


def ensure_out_dir(path: str | Path) -> Path:
    out = Path(path)
    out.mkdir(parents=True, exist_ok=True)
    return out


def attach_db(con: duckdb.DuckDBPyConnection, db_path: str, alias: str) -> None:
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Missing DuckDB file: {db_path}")
    con.execute(f"ATTACH {repr(str(db_path))} AS {q(alias)} (READ_ONLY)")


def list_tables(con: duckdb.DuckDBPyConnection, schema: str) -> pd.DataFrame:
    return con.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        [schema],
    ).df()


def table_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    return [
        row[0]
        for row in con.execute(f"DESCRIBE {q(schema)}.{q(table)}").fetchall()
    ]


def table_count(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> int:
    try:
        return int(con.execute(f"SELECT COUNT(*) FROM {q(schema)}.{q(table)}").fetchone()[0])
    except Exception:
        return 0


def distinct_key_count(con: duckdb.DuckDBPyConnection, schema: str, table: str, key: str) -> int:
    return int(
        con.execute(
            f"""
            SELECT COUNT(DISTINCT CAST({q(key)} AS VARCHAR))
            FROM {q(schema)}.{q(table)}
            WHERE {q(key)} IS NOT NULL
              AND TRIM(CAST({q(key)} AS VARCHAR)) <> ''
              AND LOWER(TRIM(CAST({q(key)} AS VARCHAR))) NOT IN ('none', 'nan', 'nat')
            """
        ).fetchone()[0]
    )


def score_ui_table(table_name: str, row_count: int, columns: list[str]) -> tuple[int, int]:
    lower = table_name.lower()
    score = 0

    if any(p in lower for p in UI_TABLE_NEGATIVE_PATTERNS):
        score -= 1000
    if any(p in lower for p in UI_TABLE_POSITIVE_PATTERNS):
        score += 100

    upper_cols = {c.upper() for c in columns}
    for key in ["UUID", "BASIC_UDI_DI", "PRIMARY_DI", "ID"]:
        if key in upper_cols:
            score += 50

    # Use row_count as secondary ordering, but do not let it dominate table name/key fit.
    return score, row_count


def choose_table(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    requested: str,
    preferred: str | None = None,
    ui_mode: bool = False,
) -> str:
    tables_df = list_tables(con, schema)
    tables = tables_df["table_name"].tolist()

    if requested and requested != "auto":
        if requested not in tables:
            raise ValueError(f"Requested table {requested!r} not found in {schema}. Available: {tables}")
        return requested

    if preferred and preferred in tables:
        return preferred

    if not tables:
        raise ValueError(f"No tables found in schema {schema}")

    scored = []
    for table in tables:
        cols = table_columns(con, schema, table)
        cnt = table_count(con, schema, table)
        if ui_mode:
            score = score_ui_table(table, cnt, cols)
        else:
            score = (100 if table.lower() == "udi" else 0, cnt)
        scored.append((score, table, cnt, cols))

    scored.sort(reverse=True, key=lambda x: x[0])
    return scored[0][1]


def choose_common_key(dl_cols: list[str], ui_cols: list[str], requested: str) -> str:
    if requested and requested != "auto":
        if requested not in dl_cols:
            raise ValueError(f"Requested key {requested!r} not found in Datalake table")
        if requested not in ui_cols:
            raise ValueError(f"Requested key {requested!r} not found in UI table")
        return requested

    dl_lookup = {c.upper(): c for c in dl_cols}
    ui_lookup = {c.upper(): c for c in ui_cols}
    for candidate in KEY_CANDIDATES:
        found_dl = dl_lookup.get(candidate.upper())
        found_ui = ui_lookup.get(candidate.upper())
        if found_dl and found_ui:
            # Return Datalake spelling. SQL queries use separate key names per side.
            return found_dl

    raise ValueError(
        "Could not auto-detect common key. "
        f"Datalake candidates: {dl_cols[:20]} | UI candidates: {ui_cols[:20]}"
    )


def find_corresponding_col(cols: list[str], wanted: str) -> str | None:
    lookup = {c.upper(): c for c in cols}
    return lookup.get(wanted.upper())


def write_df_csv(df: pd.DataFrame, out_path: Path) -> None:
    df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"OK wrote CSV: {out_path}", flush=True)


def build_table_inventory(con: duckdb.DuckDBPyConnection, schema: str, source_name: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    tables = list_tables(con, schema)["table_name"].tolist()
    for table in tables:
        cols = table_columns(con, schema, table)
        rows.append(
            {
                "source": source_name,
                "schema": schema,
                "table_name": table,
                "row_count": table_count(con, schema, table),
                "column_count": len(cols),
                "columns_preview": ", ".join(cols[:20]),
            }
        )
    return pd.DataFrame(rows)


def build_column_comparison(dl_cols: list[str], ui_cols: list[str]) -> pd.DataFrame:
    dl_upper = {c.upper(): c for c in dl_cols}
    ui_upper = {c.upper(): c for c in ui_cols}
    all_upper = sorted(set(dl_upper) | set(ui_upper))

    rows = []
    for col_upper in all_upper:
        rows.append(
            {
                "column_upper": col_upper,
                "datalake_column": dl_upper.get(col_upper),
                "ui_column": ui_upper.get(col_upper),
                "in_datalake": col_upper in dl_upper,
                "in_ui": col_upper in ui_upper,
                "status": (
                    "both"
                    if col_upper in dl_upper and col_upper in ui_upper
                    else "datalake_only"
                    if col_upper in dl_upper
                    else "ui_only"
                ),
            }
        )
    return pd.DataFrame(rows)


def create_key_views(
    con: duckdb.DuckDBPyConnection,
    dl_table: str,
    ui_table: str,
    dl_key: str,
    ui_key: str,
) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE dl_keys AS
        SELECT DISTINCT CAST({q(dl_key)} AS VARCHAR) AS key_value
        FROM dl.{q(dl_table)}
        WHERE {q(dl_key)} IS NOT NULL
          AND TRIM(CAST({q(dl_key)} AS VARCHAR)) <> ''
          AND LOWER(TRIM(CAST({q(dl_key)} AS VARCHAR))) NOT IN ('none', 'nan', 'nat')
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ui_keys AS
        SELECT DISTINCT CAST({q(ui_key)} AS VARCHAR) AS key_value
        FROM ui.{q(ui_table)}
        WHERE {q(ui_key)} IS NOT NULL
          AND TRIM(CAST({q(ui_key)} AS VARCHAR)) <> ''
          AND LOWER(TRIM(CAST({q(ui_key)} AS VARCHAR))) NOT IN ('none', 'nan', 'nat')
        """
    )


def build_key_overlap(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    counts = con.execute(
        """
        WITH overlap AS (
            SELECT
                (SELECT COUNT(*) FROM dl_keys) AS datalake_distinct_keys,
                (SELECT COUNT(*) FROM ui_keys) AS ui_distinct_keys,
                (SELECT COUNT(*) FROM dl_keys d INNER JOIN ui_keys u USING (key_value)) AS common_keys,
                (SELECT COUNT(*) FROM dl_keys d ANTI JOIN ui_keys u USING (key_value)) AS datalake_only_keys,
                (SELECT COUNT(*) FROM ui_keys u ANTI JOIN dl_keys d USING (key_value)) AS ui_only_keys
        )
        SELECT * FROM overlap
        """
    ).df()
    counts["common_pct_of_datalake"] = (
        counts["common_keys"] / counts["datalake_distinct_keys"].replace(0, pd.NA) * 100
    ).round(4)
    counts["common_pct_of_ui"] = (
        counts["common_keys"] / counts["ui_distinct_keys"].replace(0, pd.NA) * 100
    ).round(4)
    return counts


def export_key_samples(
    con: duckdb.DuckDBPyConnection,
    out_dir: Path,
    dl_table: str,
    ui_table: str,
    dl_key: str,
    ui_key: str,
    sample_limit: int,
) -> dict[str, str]:
    outputs = {}

    sample_queries = {
        "datalake_only_keys_sample.csv": f"""
            SELECT d.key_value
            FROM dl_keys d
            ANTI JOIN ui_keys u USING (key_value)
            ORDER BY d.key_value
            LIMIT {int(sample_limit)}
        """,
        "ui_only_keys_sample.csv": f"""
            SELECT u.key_value
            FROM ui_keys u
            ANTI JOIN dl_keys d USING (key_value)
            ORDER BY u.key_value
            LIMIT {int(sample_limit)}
        """,
        "common_keys_sample.csv": f"""
            SELECT d.key_value
            FROM dl_keys d
            INNER JOIN ui_keys u USING (key_value)
            ORDER BY d.key_value
            LIMIT {int(sample_limit)}
        """,
    }

    for filename, sql in sample_queries.items():
        path = out_dir / filename
        con.execute(f"COPY ({sql}) TO {repr(str(path))} (HEADER, DELIMITER ',')")
        outputs[filename] = str(path)
        print(f"OK wrote sample: {path}", flush=True)

    # Include row-level examples with all columns can become huge/wide, so only include
    # a small key + selected source preview for quick investigation.
    for side, filename, source_schema, table, key_col, anti_schema in [
        ("datalake", "datalake_only_rows_sample.csv", "dl", dl_table, dl_key, "ui_keys"),
        ("ui", "ui_only_rows_sample.csv", "ui", ui_table, ui_key, "dl_keys"),
    ]:
        key_table = "dl_keys" if side == "datalake" else "ui_keys"
        anti_join = "ui_keys" if side == "datalake" else "dl_keys"
        path = out_dir / filename
        con.execute(
            f"""
            COPY (
                SELECT s.*
                FROM {key_table} k
                ANTI JOIN {anti_join} a USING (key_value)
                INNER JOIN {q(source_schema)}.{q(table)} s
                    ON CAST(s.{q(key_col)} AS VARCHAR) = k.key_value
                LIMIT {int(min(sample_limit, 1000))}
            ) TO {repr(str(path))} (HEADER, DELIMITER ',')
            """
        )
        outputs[filename] = str(path)
        print(f"OK wrote row sample: {path}", flush=True)

    return outputs


def export_shared_column_presence(
    con: duckdb.DuckDBPyConnection,
    out_dir: Path,
    dl_table: str,
    ui_table: str,
    dl_cols: list[str],
    ui_cols: list[str],
) -> pd.DataFrame:
    """For shared columns, compare non-null/non-empty presence counts.

    This does not compare row values one-by-one. It answers whether a column exists
    in both sources but is more populated in one source than the other.
    """
    ui_lookup = {c.upper(): c for c in ui_cols}
    shared = [(dl_col, ui_lookup[dl_col.upper()]) for dl_col in dl_cols if dl_col.upper() in ui_lookup]

    rows = []
    for dl_col, ui_col in shared:
        dl_non_empty = int(
            con.execute(
                f"""
                SELECT COUNT(*)
                FROM dl.{q(dl_table)}
                WHERE {q(dl_col)} IS NOT NULL
                  AND TRIM(CAST({q(dl_col)} AS VARCHAR)) <> ''
                  AND LOWER(TRIM(CAST({q(dl_col)} AS VARCHAR))) NOT IN ('none', 'nan', 'nat')
                """
            ).fetchone()[0]
        )
        ui_non_empty = int(
            con.execute(
                f"""
                SELECT COUNT(*)
                FROM ui.{q(ui_table)}
                WHERE {q(ui_col)} IS NOT NULL
                  AND TRIM(CAST({q(ui_col)} AS VARCHAR)) <> ''
                  AND LOWER(TRIM(CAST({q(ui_col)} AS VARCHAR))) NOT IN ('none', 'nan', 'nat')
                """
            ).fetchone()[0]
        )
        rows.append(
            {
                "column_upper": dl_col.upper(),
                "datalake_column": dl_col,
                "ui_column": ui_col,
                "datalake_non_empty_rows": dl_non_empty,
                "ui_non_empty_rows": ui_non_empty,
                "ui_minus_datalake_non_empty": ui_non_empty - dl_non_empty,
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["ui_minus_datalake_non_empty", "column_upper"], ascending=[False, True])
    write_df_csv(df, out_dir / "shared_column_presence_comparison.csv")
    return df


def generate_release_notes(summary: dict[str, Any]) -> str:
    counts = summary["counts"]
    key_overlap = summary["key_overlap"]
    columns = summary["columns"]

    return f"""# EUDAMED Datalake vs UI API Comparison

This release contains insight-only comparison outputs. It does not contain raw source DuckDB databases.

## Purpose

Compare the latest EUDAMED Datalake raw snapshot with the latest EUDAMED UI Lab crawl to assess whether Datalake is sufficient, or whether the UI API exposes important extra records or columns.

## Run

- **Pipeline:** `{summary['pipeline_name']}`
- **Pipeline version:** `{summary['pipeline_version']}`
- **Run at UTC:** `{summary['run_at_utc']}`
- **Datalake DB:** `{summary['inputs']['datalake_db']}`
- **UI DB:** `{summary['inputs']['ui_db']}`
- **Datalake table:** `{summary['tables']['datalake_table']}`
- **UI table:** `{summary['tables']['ui_table']}`
- **Comparison key:** `{summary['keys']['datalake_key']}` / `{summary['keys']['ui_key']}`

## Row counts

| Source | Table | Rows | Distinct comparison keys |
|---|---|---:|---:|
| Datalake | `{summary['tables']['datalake_table']}` | {counts['datalake_rows']:,} | {counts['datalake_distinct_keys']:,} |
| UI API | `{summary['tables']['ui_table']}` | {counts['ui_rows']:,} | {counts['ui_distinct_keys']:,} |

## Key overlap

| Metric | Count |
|---|---:|
| Common keys | {key_overlap['common_keys']:,} |
| Datalake-only keys | {key_overlap['datalake_only_keys']:,} |
| UI-only keys | {key_overlap['ui_only_keys']:,} |
| Common as % of Datalake | {key_overlap['common_pct_of_datalake']}% |
| Common as % of UI | {key_overlap['common_pct_of_ui']}% |

## Column comparison

| Metric | Count |
|---|---:|
| Datalake columns | {columns['datalake_column_count']:,} |
| UI columns | {columns['ui_column_count']:,} |
| Shared columns | {columns['shared_column_count']:,} |
| Datalake-only columns | {columns['datalake_only_column_count']:,} |
| UI-only columns | {columns['ui_only_column_count']:,} |

## Assets

- `comparison_summary.csv`
- `table_inventory.csv`
- `column_comparison.csv`
- `key_overlap_summary.csv`
- `shared_column_presence_comparison.csv`
- `datalake_only_keys_sample.csv`
- `ui_only_keys_sample.csv`
- `common_keys_sample.csv`
- `datalake_only_rows_sample.csv`
- `ui_only_rows_sample.csv`
- `{SUMMARY_JSON_NAME}`

## Interpretation guide

- `ui_only_keys_sample.csv` shows devices/records found in UI but not in Datalake.
- `datalake_only_keys_sample.csv` shows devices/records found in Datalake but not in UI.
- `column_comparison.csv` shows which fields exist only in one source.
- `shared_column_presence_comparison.csv` shows shared fields where one source is more populated than the other.

This comparison is diagnostic. It should not be treated as an official source layer.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare EUDAMED Datalake raw DB with UI Lab DB")
    parser.add_argument("--datalake-db", required=True, help="Path to eudamed_raw_latest.duckdb")
    parser.add_argument("--ui-db", required=True, help="Path to eudamed_ui_lab.duckdb")
    parser.add_argument("--datalake-table", default="udi", help="Datalake table name or auto")
    parser.add_argument("--ui-table", default="auto", help="UI table name or auto")
    parser.add_argument("--key", default="auto", help="Comparison key, usually UUID, or auto")
    parser.add_argument("--out-dir", default=OUT_DIR_DEFAULT, help="Output directory")
    parser.add_argument("--sample-limit", type=int, default=5000, help="Max key samples per side")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    run_dt = utc_now()
    out_dir = ensure_out_dir(args.out_dir)

    print("=" * 80, flush=True)
    print("STARTING EUDAMED DATALAKE VS UI COMPARISON", flush=True)
    print(f"Pipeline version: {PIPELINE_VERSION}", flush=True)
    print(f"Datalake DB: {args.datalake_db}", flush=True)
    print(f"UI DB: {args.ui_db}", flush=True)
    print(f"Out dir: {out_dir}", flush=True)
    print("=" * 80, flush=True)

    con = duckdb.connect()
    attach_db(con, args.datalake_db, "dl")
    attach_db(con, args.ui_db, "ui")

    dl_table = choose_table(con, "dl", args.datalake_table, preferred="udi", ui_mode=False)
    ui_table = choose_table(con, "ui", args.ui_table, preferred=None, ui_mode=True)

    dl_cols = table_columns(con, "dl", dl_table)
    ui_cols = table_columns(con, "ui", ui_table)

    dl_key = choose_common_key(dl_cols, ui_cols, args.key)
    ui_key = find_corresponding_col(ui_cols, dl_key) or dl_key

    print(f"Selected Datalake table: {dl_table}", flush=True)
    print(f"Selected UI table: {ui_table}", flush=True)
    print(f"Selected key: Datalake.{dl_key} vs UI.{ui_key}", flush=True)

    dl_rows = table_count(con, "dl", dl_table)
    ui_rows = table_count(con, "ui", ui_table)
    dl_distinct = distinct_key_count(con, "dl", dl_table, dl_key)
    ui_distinct = distinct_key_count(con, "ui", ui_table, ui_key)

    create_key_views(con, dl_table, ui_table, dl_key, ui_key)
    key_overlap_df = build_key_overlap(con)
    key_overlap = key_overlap_df.iloc[0].to_dict()

    inventory = pd.concat(
        [
            build_table_inventory(con, "dl", "datalake"),
            build_table_inventory(con, "ui", "ui"),
        ],
        ignore_index=True,
    )
    write_df_csv(inventory, out_dir / "table_inventory.csv")

    column_comparison = build_column_comparison(dl_cols, ui_cols)
    write_df_csv(column_comparison, out_dir / "column_comparison.csv")

    write_df_csv(key_overlap_df, out_dir / "key_overlap_summary.csv")

    shared_presence = export_shared_column_presence(con, out_dir, dl_table, ui_table, dl_cols, ui_cols)
    export_key_samples(con, out_dir, dl_table, ui_table, dl_key, ui_key, args.sample_limit)

    status_counts = column_comparison["status"].value_counts().to_dict()
    duration_seconds = int(time.perf_counter() - started)

    summary = {
        "pipeline_name": PIPELINE_NAME,
        "pipeline_version": PIPELINE_VERSION,
        "run_date": date_utc(run_dt),
        "run_at_utc": iso_utc(run_dt),
        "duration_seconds": duration_seconds,
        "inputs": {
            "datalake_db": args.datalake_db,
            "ui_db": args.ui_db,
        },
        "tables": {
            "datalake_table": dl_table,
            "ui_table": ui_table,
        },
        "keys": {
            "datalake_key": dl_key,
            "ui_key": ui_key,
        },
        "counts": {
            "datalake_rows": dl_rows,
            "ui_rows": ui_rows,
            "datalake_distinct_keys": dl_distinct,
            "ui_distinct_keys": ui_distinct,
        },
        "key_overlap": {
            "common_keys": int(key_overlap.get("common_keys", 0)),
            "datalake_only_keys": int(key_overlap.get("datalake_only_keys", 0)),
            "ui_only_keys": int(key_overlap.get("ui_only_keys", 0)),
            "common_pct_of_datalake": float(key_overlap.get("common_pct_of_datalake", 0) or 0),
            "common_pct_of_ui": float(key_overlap.get("common_pct_of_ui", 0) or 0),
        },
        "columns": {
            "datalake_column_count": len(dl_cols),
            "ui_column_count": len(ui_cols),
            "shared_column_count": int(status_counts.get("both", 0)),
            "datalake_only_column_count": int(status_counts.get("datalake_only", 0)),
            "ui_only_column_count": int(status_counts.get("ui_only", 0)),
        },
        "outputs": sorted(p.name for p in out_dir.glob("*.csv")),
    }

    comparison_summary = pd.DataFrame(
        [
            {"metric": "datalake_table", "value": dl_table},
            {"metric": "ui_table", "value": ui_table},
            {"metric": "datalake_rows", "value": dl_rows},
            {"metric": "ui_rows", "value": ui_rows},
            {"metric": "datalake_distinct_keys", "value": dl_distinct},
            {"metric": "ui_distinct_keys", "value": ui_distinct},
            {"metric": "common_keys", "value": summary["key_overlap"]["common_keys"]},
            {"metric": "datalake_only_keys", "value": summary["key_overlap"]["datalake_only_keys"]},
            {"metric": "ui_only_keys", "value": summary["key_overlap"]["ui_only_keys"]},
            {"metric": "datalake_column_count", "value": len(dl_cols)},
            {"metric": "ui_column_count", "value": len(ui_cols)},
            {"metric": "shared_column_count", "value": summary["columns"]["shared_column_count"]},
            {"metric": "datalake_only_column_count", "value": summary["columns"]["datalake_only_column_count"]},
            {"metric": "ui_only_column_count", "value": summary["columns"]["ui_only_column_count"]},
            {"metric": "duration_seconds", "value": duration_seconds},
        ]
    )
    write_df_csv(comparison_summary, out_dir / "comparison_summary.csv")

    with open(out_dir / SUMMARY_JSON_NAME, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"OK wrote JSON: {out_dir / SUMMARY_JSON_NAME}", flush=True)

    notes = generate_release_notes(summary)
    with open(out_dir / RELEASE_NOTES_NAME, "w", encoding="utf-8") as f:
        f.write(notes)
    print(f"OK wrote release notes: {out_dir / RELEASE_NOTES_NAME}", flush=True)

    con.close()

    print("=" * 80, flush=True)
    print("EUDAMED DATALAKE VS UI COMPARISON COMPLETE", flush=True)
    print(json.dumps(summary["key_overlap"], indent=2), flush=True)
    print("=" * 80, flush=True)


if __name__ == "__main__":
    main()
