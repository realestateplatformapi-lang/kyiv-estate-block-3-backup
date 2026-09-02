#!/usr/bin/env python3
"""Maintain a bounded-memory PostgreSQL index of the canonical Active Sheets.

Google Sheets remain the source of truth.  This process reads small ranges and
stores text/formulas/URLs only; it never downloads photographs and never edits
the spreadsheets.  Rows missing after a *complete* worksheet scan are marked,
not deleted, in the local index.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

import gspread
import psycopg2
import psycopg2.extras
from google.oauth2.service_account import Credentials


SHEETS = {
    "apartments": "1RY4BiRospnPYLFoW2LLJleDgi08yomwhtUlKKvSpkr8",
    "houses": "1BeIvPPeem-CWYgl2pS1pf1_CxMllcxXCaIstB5IonFY",
    "commercial": "15eFtcBjMYRAHLgDFP0u6Bo57ORVy8RWZ954Hp6bDDtw",
}
OPERATIONS = {"Оренда": "rent", "Продаж": "sale"}
DEFAULT_LOCK = Path("/var/lib/kyiv-estate/locks/production-heavy.lock")
URL_RE = re.compile(r"https?://[^\s\"')]+", re.IGNORECASE)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS google_sheets_catalog (
    catalog TEXT NOT NULL,
    operation TEXT NOT NULL,
    sheet_id TEXT NOT NULL,
    sheet_tab TEXT NOT NULL,
    sheet_row INTEGER NOT NULL,
    listing_id TEXT NOT NULL,
    external_id TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT '',
    source_url TEXT NOT NULL DEFAULT '',
    photo_url TEXT NOT NULL DEFAULT '',
    telegraph_ua TEXT NOT NULL DEFAULT '',
    telegraph_en TEXT NOT NULL DEFAULT '',
    comments TEXT NOT NULL DEFAULT '',
    payload JSONB NOT NULL,
    row_hash CHAR(64) NOT NULL,
    sheet_status TEXT NOT NULL DEFAULT 'active',
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_run UUID NOT NULL,
    PRIMARY KEY (catalog, operation, listing_id)
);
CREATE INDEX IF NOT EXISTS google_sheets_catalog_external_idx
    ON google_sheets_catalog (catalog, source, external_id);
CREATE INDEX IF NOT EXISTS google_sheets_catalog_status_idx
    ON google_sheets_catalog (sheet_status, catalog, operation);
CREATE INDEX IF NOT EXISTS google_sheets_catalog_url_idx
    ON google_sheets_catalog (source_url) WHERE source_url <> '';

CREATE TABLE IF NOT EXISTS google_sheets_catalog_runs (
    run_id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    rows_seen INTEGER NOT NULL DEFAULT 0,
    rows_changed INTEGER NOT NULL DEFAULT 0,
    error TEXT NOT NULL DEFAULT ''
);
"""

UPSERT_SQL = """
INSERT INTO google_sheets_catalog (
    catalog, operation, sheet_id, sheet_tab, sheet_row, listing_id,
    external_id, source, source_url, photo_url, telegraph_ua, telegraph_en,
    comments, payload, row_hash, sheet_status, last_seen_at, last_seen_run
) VALUES %s
ON CONFLICT (catalog, operation, listing_id) DO UPDATE SET
    sheet_id = EXCLUDED.sheet_id,
    sheet_tab = EXCLUDED.sheet_tab,
    sheet_row = EXCLUDED.sheet_row,
    external_id = EXCLUDED.external_id,
    source = EXCLUDED.source,
    source_url = EXCLUDED.source_url,
    photo_url = EXCLUDED.photo_url,
    telegraph_ua = EXCLUDED.telegraph_ua,
    telegraph_en = EXCLUDED.telegraph_en,
    comments = EXCLUDED.comments,
    payload = CASE
        WHEN google_sheets_catalog.row_hash <> EXCLUDED.row_hash THEN EXCLUDED.payload
        ELSE google_sheets_catalog.payload
    END,
    row_hash = EXCLUDED.row_hash,
    sheet_status = 'active',
    last_seen_at = EXCLUDED.last_seen_at,
    last_seen_run = EXCLUDED.last_seen_run
RETURNING TRUE
"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def log(event: str, **fields: object) -> None:
    print(json.dumps({"at": utc_now().isoformat(), "event": event, **fields}, ensure_ascii=False), flush=True)


def retry(call, *, attempts: int = 6):
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return call()
        except Exception as error:  # gspread wraps several Google/http errors
            last_error = error
            if attempt + 1 == attempts:
                raise
            delay = min(32.0, 2.0**attempt) + random.uniform(0.0, 0.5)
            log("sheets_retry", attempt=attempt + 1, delay=round(delay, 2), error=type(error).__name__)
            time.sleep(delay)
    raise last_error or RuntimeError("retry failed")


def column_name(index: int) -> str:
    if index < 1:
        raise ValueError("column index starts at 1")
    result = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def clean(value: object) -> str:
    return "" if value is None else str(value).strip()


def first(mapping: dict[str, str], *names: str) -> str:
    for name in names:
        value = clean(mapping.get(name, ""))
        if value:
            return value
    return ""


def direct_url(value: str) -> str:
    match = URL_RE.search(value or "")
    return match.group(0) if match else ""


def normalize_row(headers: list[str], values: list[object]) -> dict[str, str]:
    padded = [clean(value) for value in values[: len(headers)]]
    padded.extend([""] * (len(headers) - len(padded)))
    return dict(zip(headers, padded))


def row_tuple(
    catalog: str,
    operation: str,
    sheet_id: str,
    tab: str,
    sheet_row: int,
    row: dict[str, str],
    run_id: str,
    seen_at: datetime,
) -> tuple:
    listing_id = first(row, "ID")
    payload_text = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
    photo_cell = first(row, "Фото")
    return (
        catalog,
        operation,
        sheet_id,
        tab,
        sheet_row,
        listing_id,
        first(row, "Ext ID"),
        first(row, "Source", "Джерело"),
        direct_url(first(row, "URL")),
        direct_url(photo_cell),
        first(row, "Telegraph UA"),
        first(row, "Telegraph EN"),
        first(row, "Коментарі", "Коментарі"),
        psycopg2.extras.Json(row, dumps=lambda value: json.dumps(value, ensure_ascii=False)),
        digest,
        "active",
        seen_at,
        run_id,
    )


def worksheet_batches(worksheet, headers: list[str], batch_size: int) -> Iterator[tuple[int, list[list[str]]]]:
    last_column = column_name(len(headers))
    for first_row in range(2, worksheet.row_count + 1, batch_size):
        last_row = min(worksheet.row_count, first_row + batch_size - 1)
        values = retry(
            lambda first_row=first_row, last_row=last_row: worksheet.get(
                f"A{first_row}:{last_column}{last_row}",
                value_render_option="FORMULA",
                major_dimension="ROWS",
            )
        )
        yield first_row, list(values)


def connect_database():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DBNAME", "real_estate"),
        user=os.getenv("PG_USER", "admin"),
        password=os.getenv("PG_PASSWORD", ""),
        connect_timeout=10,
        application_name="google_sheets_catalog_sync",
    )


def sheets_client():
    credentials_path = os.environ["GOOGLE_CREDENTIALS_FILE"]
    credentials = Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    return gspread.authorize(credentials)


@contextmanager
def production_lock(path: Path):
    if os.name != "posix":
        yield True
        return
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o640)
    acquired = False
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError:
            pass
        yield acquired
    finally:
        if acquired:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def selected_scopes(catalog: str | None, operation: str | None) -> Iterable[tuple[str, str, str]]:
    for current_catalog, sheet_id in SHEETS.items():
        if catalog and current_catalog != catalog:
            continue
        for tab, current_operation in OPERATIONS.items():
            if operation and current_operation != operation:
                continue
            yield current_catalog, sheet_id, tab


def sync_scope(
    connection,
    client,
    *,
    catalog: str,
    sheet_id: str,
    tab: str,
    run_id: uuid.UUID,
    batch_size: int,
    delay: float,
    max_rows: int,
    dry_run: bool,
) -> tuple[int, int]:
    book = retry(lambda: client.open_by_key(sheet_id))
    worksheet = retry(lambda: book.worksheet(tab))
    headers = [clean(value) for value in retry(lambda: worksheet.row_values(1))]
    if not headers or headers[0] != "ID" or "Ext ID" not in headers:
        raise RuntimeError(f"{catalog}/{tab}: unexpected header; refusing to sync")

    operation = OPERATIONS[tab]
    seen = changed = 0
    complete = True
    for first_row, values in worksheet_batches(worksheet, headers, batch_size):
        records = []
        for offset, values_row in enumerate(values):
            row = normalize_row(headers, values_row)
            if not first(row, "ID"):
                continue
            records.append(row_tuple(catalog, operation, sheet_id, tab, first_row + offset, row, run_id, utc_now()))
            seen += 1
            if max_rows and seen >= max_rows:
                complete = False
                break

        if records and not dry_run:
            with connection.cursor() as cursor:
                returned = psycopg2.extras.execute_values(
                    cursor,
                    UPSERT_SQL,
                    records,
                    page_size=min(batch_size, 1000),
                    fetch=True,
                )
                changed += sum(bool(item[0]) for item in returned)
            connection.commit()
        elif records:
            changed += len(records)

        log("sheet_batch", catalog=catalog, operation=operation, first_row=first_row, rows=len(records), total=seen)
        if max_rows and seen >= max_rows:
            break
        if delay:
            time.sleep(delay)

    if complete and not dry_run:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE google_sheets_catalog
                   SET sheet_status='missing_from_active_sheet'
                 WHERE catalog=%s AND operation=%s
                   AND sheet_status='active' AND last_seen_run<>%s
                """,
                (catalog, operation, run_id),
            )
        connection.commit()
    log("sheet_complete", catalog=catalog, operation=operation, rows=seen, changed=changed, complete=complete)
    return seen, changed


def peak_rss_mb() -> float | None:
    if os.name != "posix":
        return None
    import resource

    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", choices=sorted(SHEETS))
    parser.add_argument("--operation", choices=sorted(set(OPERATIONS.values())))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("SHEETS_CATALOG_BATCH_SIZE", "1000")))
    parser.add_argument("--delay", type=float, default=float(os.getenv("SHEETS_CATALOG_DELAY", "1.0")))
    parser.add_argument("--max-rows", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--lock-file", type=Path, default=DEFAULT_LOCK)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not 100 <= args.batch_size <= 2000:
        raise SystemExit("--batch-size must be between 100 and 2000")
    run_id = str(uuid.uuid4())
    started = utc_now()

    with production_lock(args.lock_file) as acquired:
        if not acquired:
            log("sync_deferred", reason="production_heavy_lock_busy")
            return 75

        connection = None if args.dry_run else connect_database()
        try:
            if connection:
                with connection.cursor() as cursor:
                    cursor.execute("SET statement_timeout = '120s'")
                    cursor.execute("SET lock_timeout = '5s'")
                    cursor.execute(SCHEMA_SQL)
                    cursor.execute(
                        "INSERT INTO google_sheets_catalog_runs(run_id,started_at,status) VALUES(%s,%s,'running')",
                        (run_id, started),
                    )
                connection.commit()

            client = sheets_client()
            total_seen = total_changed = 0
            for catalog, sheet_id, tab in selected_scopes(args.catalog, args.operation):
                seen, changed = sync_scope(
                    connection,
                    client,
                    catalog=catalog,
                    sheet_id=sheet_id,
                    tab=tab,
                    run_id=run_id,
                    batch_size=args.batch_size,
                    delay=args.delay,
                    max_rows=args.max_rows,
                    dry_run=args.dry_run,
                )
                total_seen += seen
                total_changed += changed

            if connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE google_sheets_catalog_runs
                           SET finished_at=NOW(),status='succeeded',rows_seen=%s,rows_changed=%s
                         WHERE run_id=%s
                        """,
                        (total_seen, total_changed, run_id),
                    )
                connection.commit()
            log("sync_complete", rows=total_seen, changed=total_changed, dry_run=args.dry_run, peak_rss_mb=peak_rss_mb())
            return 0
        except Exception as error:
            if connection:
                connection.rollback()
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE google_sheets_catalog_runs
                           SET finished_at=NOW(),status='failed',error=%s
                         WHERE run_id=%s
                        """,
                        (f"{type(error).__name__}: {error}"[:1000], run_id),
                    )
                connection.commit()
            log("sync_failed", error=type(error).__name__, detail=str(error)[:300], peak_rss_mb=peak_rss_mb())
            return 1
        finally:
            if connection:
                connection.close()


if __name__ == "__main__":
    sys.exit(main())
