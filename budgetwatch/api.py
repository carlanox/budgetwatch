"""
BudgetWatch — Public REST API.

Endpoints (all read-only, no auth required):

  GET /api/v1/health
  GET /api/v1/provinces                     -> [{code, name, total_amount, flagged_count}]
  GET /api/v1/provinces/{code}              -> province summary + agency rollup
  GET /api/v1/provinces/{code}/agencies     -> [{name, total_amount, flagged_count, item_count}]
  GET /api/v1/agencies/{slug}               -> agency summary + program rollup
  GET /api/v1/items                         -> paginated list with filters
       ?province=DKI+Jakarta
       &agency=Dinas+Pendidikan
       &category=GOODS
       &flagged=true
       &q=laptop
       &page=1&size=50
  GET /api/v1/items/{id}                    -> single item with evidence

Admin endpoints (auth-gated, called by the orchestrator):

  POST /admin/ingest/dki-jakarta            -> trigger one-shot pull
  GET  /admin/ingest/runs                   -> recent ingestion runs

Notes:
  * The store here is in-memory for the prototype. In production this is
    Postgres + Meilisearch (see arch §4.3).
  * CORS is enabled for any origin so the Next.js frontend can call it.
  * Decimal is serialized as float — fine for IDR up to 1e15 with no
    precision loss (Indonesian budgets fit comfortably).
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from connectors import run_all_for_dki_jakarta
from matching import Matcher, enrich
from models import LineItem, Source, Category

log = logging.getLogger("budgetwatch.api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

# ----------------------------------------------------------------------
# In-memory store (swap for Postgres in prod)
# ----------------------------------------------------------------------

class Store:
    """Thin facade over a dict — kept tiny so swapping for SQLAlchemy is one PR."""
    def __init__(self):
        self.items: dict[str, LineItem] = {}
        self.last_ingest_runs: list[dict] = []

    def upsert(self, items: list[LineItem]) -> int:
        for it in items:
            self.items[it.id] = it
        return len(items)

    def all(self) -> list[LineItem]:
        return list(self.items.values())

    def get(self, item_id: str) -> LineItem | None:
        return self.items.get(item_id)


STORE: object  # Store | PostgresStore — duck-typed, both expose upsert/all/get/items
if os.getenv("BUDGETWATCH_DB_URL"):
    log.info("Using PostgresStore (BUDGETWATCH_DB_URL set)")
    from store_pg import PostgresStore
    STORE = PostgresStore()
else:
    log.info("Using in-memory Store (no BUDGETWATCH_DB_URL)")
    STORE = Store()
RAW_DIR = Path(os.getenv("BUDGETWATCH_RAW_DIR", "/var/budgetwatch/raw"))
ISB_TOKEN = os.getenv("BUDGETWATCH_ISB_TOKEN")     # set when LKPP MoU is approved
ADMIN_TOKEN = os.getenv("BUDGETWATCH_ADMIN_TOKEN", "dev-only-change-me")


# ----------------------------------------------------------------------
# Lifespan — load fixture on startup so /api works even before first ingest
# ----------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Production: replace with a SELECT from Postgres.
    # Dev: load a fixture so `uvicorn api:app` Just Works.
    log.info("Loading fixture for DKI Jakarta...")
    from fixtures import load_dki_jakarta_fixture
    fixture_items = load_dki_jakarta_fixture()
    STORE.upsert(fixture_items)
    log.info("Loaded %d fixture items.", len(fixture_items))
    yield


# ----------------------------------------------------------------------
# App
# ----------------------------------------------------------------------

app = FastAPI(
    title="BudgetWatch API",
    version="0.1.0",
    description="Public, read-only API exposing Indonesian government budget line items with marketplace flag analysis.",
    lifespan=lifespan,
)

_allowed = os.getenv("BUDGETWATCH_ALLOWED_ORIGIN", "*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[_allowed] if _allowed != "*" else ["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ----------------------------------------------------------------------
# Schemas
# ----------------------------------------------------------------------

class ItemSummary(BaseModel):
    id: str
    description: str
    agency_name: str
    province: str
    category: str
    unit: str
    quantity: float
    unit_price: float
    total_amount: float
    flagged: bool
    markup_percent: Optional[float] = None
    confidence: Optional[float] = None
    source_url: str


class ItemDetail(ItemSummary):
    program: str
    activity: str
    fiscal_year: int
    status: str
    source: str
    source_label: str
    marketplace_median: Optional[float] = None
    marketplace_samples: list = []


class AgencyRollup(BaseModel):
    name: str
    slug: str
    total_amount: float
    item_count: int
    flagged_count: int
    flagged_amount: float


class ProvinceSummary(BaseModel):
    code: str
    name: str
    total_amount: float
    item_count: int
    flagged_count: int
    flagged_amount: float
    estimated_overspend: float
    agency_count: int


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _to_summary(it: LineItem) -> ItemSummary:
    return ItemSummary(
        id=it.id,
        description=it.description,
        agency_name=it.agency_name,
        province=it.province,
        category=it.category.value,
        unit=it.unit,
        quantity=float(it.quantity),
        unit_price=float(it.unit_price),
        total_amount=float(it.total_amount),
        flagged=it.flagged,
        markup_percent=it.markup_percent,
        confidence=it.confidence,
        source_url=it.source_url,
    )


def _to_detail(it: LineItem) -> ItemDetail:
    return ItemDetail(
        **_to_summary(it).model_dump(),
        program=it.program,
        activity=it.activity,
        fiscal_year=it.fiscal_year,
        status=it.status.value,
        source=it.source.value,
        source_label=it.source_label,
        marketplace_median=float(it.marketplace_median) if it.marketplace_median else None,
        marketplace_samples=it.marketplace_samples,
    )


def _slugify(name: str) -> str:
    return "-".join(name.lower().split()).replace("/", "-")


# ----------------------------------------------------------------------
# Public endpoints
# ----------------------------------------------------------------------

@app.get("/api/v1/health")
def health():
    return {"ok": True, "items_loaded": len(STORE.items)}


@app.get("/api/v1/provinces")
def provinces():
    by_prov = defaultdict(list)
    for it in STORE.all():
        if it.source != Source.EKATALOG:    # don't surface catalog rows
            by_prov[it.province].append(it)

    out = []
    for prov, items in sorted(by_prov.items()):
        if not prov:
            continue
        flagged = [i for i in items if i.flagged]
        out.append({
            "name": prov,
            "code": _slugify(prov),
            "item_count": len(items),
            "total_amount": sum(float(i.total_amount) for i in items),
            "flagged_count": len(flagged),
            "flagged_amount": sum(float(i.total_amount) for i in flagged),
        })
    return out


@app.get("/api/v1/provinces/{code}", response_model=ProvinceSummary)
def province_summary(code: str):
    items = [i for i in STORE.all() if _slugify(i.province) == code and i.source != Source.EKATALOG]
    if not items:
        raise HTTPException(404, "Province not found")
    flagged = [i for i in items if i.flagged]
    overspend = sum(
        (float(i.unit_price) - float(i.marketplace_median)) * float(i.quantity)
        for i in flagged if i.marketplace_median
    )
    agencies = {i.agency_name for i in items}
    return ProvinceSummary(
        code=code,
        name=items[0].province,
        total_amount=sum(float(i.total_amount) for i in items),
        item_count=len(items),
        flagged_count=len(flagged),
        flagged_amount=sum(float(i.total_amount) for i in flagged),
        estimated_overspend=overspend,
        agency_count=len(agencies),
    )


@app.get("/api/v1/provinces/{code}/agencies", response_model=list[AgencyRollup])
def province_agencies(code: str):
    items = [i for i in STORE.all() if _slugify(i.province) == code and i.source != Source.EKATALOG]
    if not items:
        raise HTTPException(404, "Province not found")

    by_agency: dict[str, list[LineItem]] = defaultdict(list)
    for it in items:
        by_agency[it.agency_name].append(it)

    rollup = []
    for name, agency_items in by_agency.items():
        flagged = [i for i in agency_items if i.flagged]
        rollup.append(AgencyRollup(
            name=name,
            slug=_slugify(name),
            total_amount=sum(float(i.total_amount) for i in agency_items),
            item_count=len(agency_items),
            flagged_count=len(flagged),
            flagged_amount=sum(float(i.total_amount) for i in flagged),
        ))
    rollup.sort(key=lambda a: a.flagged_count, reverse=True)
    return rollup


@app.get("/api/v1/agencies/{slug}")
def agency_detail(slug: str, province: str | None = None):
    items = [
        i for i in STORE.all()
        if _slugify(i.agency_name) == slug
        and (province is None or _slugify(i.province) == province)
        and i.source != Source.EKATALOG
    ]
    if not items:
        raise HTTPException(404, "Agency not found")

    by_program: dict[str, list[LineItem]] = defaultdict(list)
    for it in items:
        by_program[it.program or "(tanpa program)"].append(it)

    return {
        "name": items[0].agency_name,
        "slug": slug,
        "province": items[0].province,
        "total_amount": sum(float(i.total_amount) for i in items),
        "item_count": len(items),
        "flagged_count": sum(1 for i in items if i.flagged),
        "programs": [
            {
                "name": pname,
                "total_amount": sum(float(i.total_amount) for i in pitems),
                "item_count": len(pitems),
                "flagged_count": sum(1 for i in pitems if i.flagged),
            }
            for pname, pitems in sorted(by_program.items(), key=lambda kv: -sum(float(i.total_amount) for i in kv[1]))
        ],
        "items": [_to_summary(i).model_dump() for i in sorted(items, key=lambda i: -float(i.total_amount))],
    }


@app.get("/api/v1/items")
def list_items(
    province: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    flagged: bool | None = None,
    q: str | None = None,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=500),
):
    items = [i for i in STORE.all() if i.source != Source.EKATALOG]
    if province:
        items = [i for i in items if _slugify(i.province) == _slugify(province)]
    if agency:
        items = [i for i in items if _slugify(i.agency_name) == _slugify(agency)]
    if category:
        items = [i for i in items if i.category.value == category.upper()]
    if flagged is not None:
        items = [i for i in items if i.flagged == flagged]
    if q:
        ql = q.lower()
        items = [
            i for i in items
            if ql in i.description.lower()
            or ql in i.agency_name.lower()
            or ql in i.program.lower()
        ]

    items.sort(key=lambda i: -float(i.total_amount))
    total = len(items)
    start = (page - 1) * size
    page_items = items[start:start + size]
    return {
        "total": total,
        "page": page,
        "size": size,
        "items": [_to_summary(i).model_dump() for i in page_items],
    }


@app.get("/api/v1/items/{item_id}", response_model=ItemDetail)
def item_detail(item_id: str):
    it = STORE.get(item_id)
    if not it:
        raise HTTPException(404, "Item not found")
    return _to_detail(it)


# ----------------------------------------------------------------------
# Admin endpoints
# ----------------------------------------------------------------------

def _require_admin(authorization: str | None):
    if not authorization or authorization != f"Bearer {ADMIN_TOKEN}":
        raise HTTPException(401, "Admin token required")


@app.post("/admin/ingest/dki-jakarta")
async def trigger_dki_ingest(
    year: int = 2026,
    authorization: str | None = Header(default=None),
):
    """Pulls all four sources for DKI Jakarta, runs match+flag, upserts."""
    _require_admin(authorization)

    results = await run_all_for_dki_jakarta(RAW_DIR, ISB_TOKEN, year=year)

    all_items: list[LineItem] = []
    for r in results.values():
        all_items.extend(r.items)

    # Build matcher from EKATALOG items for now; in prod this is Meilisearch.
    from search_backends import InMemorySearchBackend
    ekat_items = [i for i in all_items if i.source == Source.EKATALOG]
    matcher = Matcher(InMemorySearchBackend(ekat_items))

    enriched = enrich(all_items, matcher)
    n = STORE.upsert(enriched)

    summary = {
        "year": year,
        "items_ingested": n,
        "by_source": {s.value: len(r.items) for s, r in results.items()},
        "errors": {s.value: r.errors for s, r in results.items() if r.errors},
    }
    STORE.last_ingest_runs.insert(0, summary)
    STORE.last_ingest_runs = STORE.last_ingest_runs[:20]
    return summary


@app.get("/admin/ingest/runs")
def ingest_runs(authorization: str | None = Header(default=None)):
    _require_admin(authorization)
    return STORE.last_ingest_runs
