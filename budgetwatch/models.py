"""
BudgetWatch — Canonical data model.

Mirrors the schema in §5 of the architecture doc. The point of this module is
that *every* source normalizes into LineItem before anything else (matching,
flagging, indexing) touches it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional


class Source(str, Enum):
    APBN = "APBN"
    APBD = "APBD"
    SIRUP = "SIRUP"
    EKATALOG = "EKATALOG"
    LPSE = "LPSE"


class AgencyLevel(str, Enum):
    NATIONAL = "NATIONAL"
    PROVINSI = "PROVINSI"
    KABUPATEN = "KABUPATEN"
    KOTA = "KOTA"


class Category(str, Enum):
    GOODS = "GOODS"
    SERVICES = "SERVICES"
    PROJECT = "PROJECT"
    PERSONNEL = "PERSONNEL"
    OTHER = "OTHER"


class Status(str, Enum):
    PLANNED = "PLANNED"
    TENDERED = "TENDERED"
    AWARDED = "AWARDED"
    REALIZED = "REALIZED"


# ----------------------------------------------------------------------
# Canonical record
# ----------------------------------------------------------------------

@dataclass
class LineItem:
    id: str                       # opaque platform ID
    source: Source
    source_record_id: str         # ID at the source — for back-traceability
    source_url: str               # deep link back to government page
    source_label: str             # human-readable label of the source

    fiscal_year: int
    agency_level: AgencyLevel
    agency_name: str
    agency_code: str
    province: str

    program: str
    activity: str
    description: str

    category: Category
    unit: str
    quantity: Decimal
    unit_price: Decimal
    total_amount: Decimal
    status: Status

    ingested_at: datetime
    raw_payload_uri: str

    # Filled in later by the matcher/flagger:
    marketplace_median: Optional[Decimal] = None
    marketplace_samples: list = field(default_factory=list)
    confidence: Optional[float] = None
    flagged: bool = False
    markup_percent: Optional[float] = None

    def to_api(self) -> dict:
        """Shape sent to the frontend. Decimal/datetime/Enum -> JSON-friendly."""
        d = asdict(self)
        d["source"] = self.source.value
        d["agency_level"] = self.agency_level.value
        d["category"] = self.category.value
        d["status"] = self.status.value
        d["quantity"] = float(self.quantity)
        d["unit_price"] = float(self.unit_price)
        d["total_amount"] = float(self.total_amount)
        if self.marketplace_median is not None:
            d["marketplace_median"] = float(self.marketplace_median)
        d["ingested_at"] = self.ingested_at.isoformat()
        return d


# ----------------------------------------------------------------------
# Classifier — implements §6.1 of the architecture doc
# ----------------------------------------------------------------------

# Order matters — first match wins.
_PROJECT_PATTERNS = re.compile(
    r"\b(pembangunan|pembebasan lahan|renovasi|rehabilitasi|konstruksi|pengaspalan|"
    r"pembetonan|pemasangan|jembatan|gedung|jalan layang|flyover|underpass|"
    r"normalisasi|drainase|trotoar)\b",
    re.IGNORECASE,
)
_CONSULTANCY_PATTERNS = re.compile(
    r"\b(jasa konsultansi|kajian|studi|perencanaan teknis|masterplan|"
    r"feasibility|supervisi)\b",
    re.IGNORECASE,
)
_SERVICES_PATTERNS = re.compile(
    r"\b(jasa pemeliharaan|sewa|langganan|asuransi|cleaning service|"
    r"keamanan|catering|jasa kebersihan)\b",
    re.IGNORECASE,
)

_GOODS_UNITS = {"unit", "buah", "set", "rim", "box", "liter", "kg", "pcs", "pack", "lembar"}
_PROJECT_PAKET_THRESHOLD_IDR = 200_000_000   # IDR 200 jt
_GOODS_QTY_CEILING = 10_000


def classify_line_item(description: str, unit: str, total_amount: float) -> Category:
    """
    Implements §6.1 classification rules in order.

    Returns one of:
      - PROJECT   (construction / renovation / consultancy paket > 200 jt)
      - PERSONNEL (orang / orang-bulan)
      - GOODS     (countable units, qty <= 10k)
      - SERVICES  (recognized service keywords)
      - OTHER     (manual review)
    """
    desc = (description or "").strip()
    u = (unit or "").lower().strip()

    # 1. Big paket -> PROJECT (catches construction items even without keywords)
    if u == "paket" and total_amount > _PROJECT_PAKET_THRESHOLD_IDR:
        return Category.PROJECT

    # 2/3. Keyword match -> PROJECT
    if _PROJECT_PATTERNS.search(desc) or _CONSULTANCY_PATTERNS.search(desc):
        return Category.PROJECT

    # 4. Services keywords (sewa / pemeliharaan / etc.)
    if _SERVICES_PATTERNS.search(desc):
        return Category.SERVICES

    # 5. Personnel
    if u in {"orang", "orang/bulan", "ob", "om"}:
        return Category.PERSONNEL

    # 6. Goods units
    if u in _GOODS_UNITS:
        return Category.GOODS

    return Category.OTHER
