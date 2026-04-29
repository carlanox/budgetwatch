"""
BudgetWatch — Postgres-backed Store.

Drop-in replacement for the in-memory `Store` class in api.py. When
BUDGETWATCH_DB_URL is set, api.py imports `PostgresStore` instead.

Schema is intentionally simple: one `line_items` table holding the JSON
representation of every LineItem, plus an `ingest_runs` log. We keep the
JSON-blob approach for the prototype because:
  1. The schema is still evolving (new fields appear regularly)
  2. Reads are dominated by full-table scans for filters anyway
  3. PostgreSQL JSONB is fast enough for 100k+ rows

If the dataset grows past ~1M rows, normalize the columns and add proper
indexes per (province, agency, flagged).
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from models import LineItem, Source, Status, AgencyLevel, Category

log = logging.getLogger("budgetwatch.store_pg")


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS line_items (
    id              TEXT PRIMARY KEY,
    province        TEXT,
    agency_name     TEXT,
    agency_code     TEXT,
    category        TEXT,
    source          TEXT,
    flagged         BOOLEAN DEFAULT FALSE,
    fiscal_year     INTEGER,
    total_amount    NUMERIC,
    payload         JSONB NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_line_items_province  ON line_items(province);
CREATE INDEX IF NOT EXISTS idx_line_items_agency    ON line_items(agency_name);
CREATE INDEX IF NOT EXISTS idx_line_items_flagged   ON line_items(flagged) WHERE flagged = TRUE;
CREATE INDEX IF NOT EXISTS idx_line_items_category  ON line_items(category);
CREATE INDEX IF NOT EXISTS idx_line_items_source    ON line_items(source);

CREATE TABLE IF NOT EXISTS ingest_runs (
    id          SERIAL PRIMARY KEY,
    started_at  TIMESTAMPTZ DEFAULT now(),
    summary     JSONB NOT NULL
);
"""


def _serialize(item: LineItem) -> dict:
    """LineItem → JSON-friendly dict for storage."""
    d = item.to_api()
    # to_api already coerces decimals/datetimes/enums to JSON-friendly forms
    return d


def _deserialize(row: dict) -> LineItem:
    """JSON dict → LineItem reconstruction."""
    p = row["payload"] if "payload" in row else row
    return LineItem(
        id=p["id"],
        source=Source(p["source"]),
        source_record_id=p["source_record_id"],
        source_url=p["source_url"],
        source_label=p["source_label"],
        fiscal_year=p["fiscal_year"],
        agency_level=AgencyLevel(p["agency_level"]),
        agency_name=p["agency_name"],
        agency_code=p["agency_code"],
        province=p["province"],
        program=p["program"],
        activity=p["activity"],
        description=p["description"],
        category=Category(p["category"]),
        unit=p["unit"],
        quantity=Decimal(str(p["quantity"])),
        unit_price=Decimal(str(p["unit_price"])),
        total_amount=Decimal(str(p["total_amount"])),
        status=Status(p["status"]),
        ingested_at=datetime.fromisoformat(p["ingested_at"]),
        raw_payload_uri=p.get("raw_payload_uri", ""),
        marketplace_median=Decimal(str(p["marketplace_median"])) if p.get("marketplace_median") is not None else None,
        marketplace_samples=p.get("marketplace_samples", []),
        confidence=p.get("confidence"),
        flagged=p.get("flagged", False),
        markup_percent=p.get("markup_percent"),
    )


class PostgresStore:
    """Drop-in replacement for the in-memory Store."""

    def __init__(self, dsn: str | None = None):
        self.dsn = dsn or os.environ["BUDGETWATCH_DB_URL"]
        self._init_schema()
        self.last_ingest_runs: list[dict] = []

    def _init_schema(self):
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(_SCHEMA_SQL)
            conn.commit()
        log.info("Postgres schema initialized")

    @contextmanager
    def _conn(self):
        with psycopg.connect(self.dsn) as conn:
            yield conn

    # ----- API the rest of the app uses -----

    def upsert(self, items: list[LineItem]) -> int:
        if not items:
            return 0
        rows = [
            (
                it.id, it.province, it.agency_name, it.agency_code,
                it.category.value, it.source.value, it.flagged,
                it.fiscal_year, float(it.total_amount),
                Jsonb(_serialize(it)),
            )
            for it in items
        ]
        with self._conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO line_items
                  (id, province, agency_name, agency_code, category, source,
                   flagged, fiscal_year, total_amount, payload, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    province     = EXCLUDED.province,
                    agency_name  = EXCLUDED.agency_name,
                    agency_code  = EXCLUDED.agency_code,
                    category     = EXCLUDED.category,
                    source       = EXCLUDED.source,
                    flagged      = EXCLUDED.flagged,
                    fiscal_year  = EXCLUDED.fiscal_year,
                    total_amount = EXCLUDED.total_amount,
                    payload      = EXCLUDED.payload,
                    updated_at   = now()
                """,
                rows,
            )
            conn.commit()
        return len(rows)

    def all(self) -> list[LineItem]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT payload FROM line_items")
            rows = cur.fetchall()
        return [_deserialize({"payload": r[0]}) for r in rows]

    def get(self, item_id: str) -> LineItem | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT payload FROM line_items WHERE id = %s", (item_id,))
            row = cur.fetchone()
        return _deserialize({"payload": row[0]}) if row else None

    @property
    def items(self) -> dict:
        """Compat shim — health endpoint reads len(store.items)."""
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM line_items")
            n = cur.fetchone()[0]
        return {"_count": n} if False else _CountProxy(n)


class _CountProxy:
    """Lets `len(store.items)` work without loading everything."""
    def __init__(self, n): self._n = n
    def __len__(self): return self._n
