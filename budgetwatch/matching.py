"""
BudgetWatch — Marketplace matching + the 20% flag.

Implements §7 of the architecture doc.

Pipeline per GOODS line item:
  1. Build a normalized product key (brand + model + key specs).
  2. Search e-Katalog first — verified gov reference price gets bonus weight.
  3. Hybrid search (BM25 + multilingual embedding cosine) across marketplace
     index. Top 10 candidates.
  4. Spec filter: regex extract specs from description, compare to candidate.
  5. If <3 valid candidates, mark INSUFFICIENT_DATA and skip flagging.
  6. Compute median over remaining candidates -> reference price.
  7. If unit_price > 1.20 * reference_price -> raise PRICE_MARKUP flag.
"""

from __future__ import annotations

import logging
import re
import statistics
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Sequence

from models import LineItem, Category, Source

log = logging.getLogger("budgetwatch.matching")

DEFAULT_THRESHOLD_PCT = 20.0
MIN_SAMPLES_FOR_FLAG = 3
LOW_CONFIDENCE_FLOOR = 0.5


# ----------------------------------------------------------------------
# Marketplace samples — what the scrapers/APIs feed in
# ----------------------------------------------------------------------

@dataclass
class MarketplaceSample:
    vendor: str           # TOKOPEDIA | SHOPEE | BUKALAPAK | BLIBLI | EKATALOG
    title: str
    price: Decimal
    url: str              # public listing URL — surfaced as evidence
    captured_at: str      # ISO timestamp
    spec_match_score: float = 1.0    # 0..1, set by spec filter

    @property
    def is_ekatalog(self) -> bool:
        return self.vendor.upper() == "EKATALOG"


# ----------------------------------------------------------------------
# Province cost-of-living adjustment (§7.2)
# ----------------------------------------------------------------------
# Sourced from BPS regional CPI relative to Jakarta. These are illustrative
# defaults; in production they're refreshed monthly from BPS.
PROVINCE_THRESHOLD_OVERRIDES: dict[str, float] = {
    "Papua": 35.0,           # logistics premium
    "Papua Barat": 35.0,
    "Maluku": 28.0,
    "Maluku Utara": 30.0,
    "Nusa Tenggara Timur": 25.0,
    # Default 20% applies to everywhere else, including Jakarta.
}


def threshold_for_province(province: str) -> float:
    return PROVINCE_THRESHOLD_OVERRIDES.get(province, DEFAULT_THRESHOLD_PCT)


# ----------------------------------------------------------------------
# Spec extraction — pulls structured specs out of free-text Indonesian
# ----------------------------------------------------------------------

_RAM_RE = re.compile(r"(\d+)\s?GB\s?RAM", re.IGNORECASE)
_SSD_RE = re.compile(r"(\d+)\s?(GB|TB)\s?SSD", re.IGNORECASE)
_RES_RE = re.compile(r"(\d+)\s?MP", re.IGNORECASE)
_SIZE_RE = re.compile(r"(\d+)\s?(L|liter|inch|in)\b", re.IGNORECASE)
_BRAND_RE = re.compile(
    r"\b(Lenovo|HP|Dell|Asus|Acer|Apple|Samsung|Canon|Epson|Brother|"
    r"Omron|Hikvision|Dahua|Logitech|Microsoft|Xiaomi|Huawei|Toshiba)\b",
    re.IGNORECASE,
)


def extract_specs(text: str) -> dict[str, str]:
    """Best-effort extraction. The matcher is lenient — missing specs don't
    disqualify, they just lower the spec_match_score."""
    specs: dict[str, str] = {}
    if m := _BRAND_RE.search(text):
        specs["brand"] = m.group(1).lower()
    if m := _RAM_RE.search(text):
        specs["ram_gb"] = m.group(1)
    if m := _SSD_RE.search(text):
        specs["storage"] = m.group(0).upper().replace(" ", "")
    if m := _RES_RE.search(text):
        specs["resolution_mp"] = m.group(1)
    if m := _SIZE_RE.search(text):
        specs["size"] = m.group(0).lower().replace(" ", "")
    return specs


def spec_overlap(specs_a: dict[str, str], specs_b: dict[str, str]) -> float:
    """Jaccard-style overlap — 1.0 if all extracted specs match, 0.0 if none."""
    if not specs_a or not specs_b:
        return 0.5  # neutral when we can't tell
    keys = set(specs_a.keys()) & set(specs_b.keys())
    if not keys:
        return 0.5
    matches = sum(1 for k in keys if specs_a[k] == specs_b[k])
    return matches / len(keys)


# ----------------------------------------------------------------------
# Matcher — finds candidate marketplace listings for a LineItem
# ----------------------------------------------------------------------

class Matcher:
    """
    In production this calls into Meilisearch (BM25 keyword) + pgvector
    (cosine similarity on multilingual-e5-base embeddings). The interface
    here is small enough that the actual search backend can be swapped.
    """

    def __init__(self, search_backend):
        self.search = search_backend

    def candidates_for(self, item: LineItem, k: int = 10) -> list[MarketplaceSample]:
        """Return up to k marketplace samples ranked by combined score."""
        if item.category != Category.GOODS:
            return []

        item_specs = extract_specs(item.description)
        raw_candidates = self.search.hybrid(item.description, k=k)

        # Spec filter — score each candidate by spec overlap.
        out = []
        for c in raw_candidates:
            cand_specs = extract_specs(c.title)
            score = spec_overlap(item_specs, cand_specs)
            c.spec_match_score = score
            out.append(c)
        # Drop the obvious mismatches — score below 0.4 means brand or
        # primary spec didn't match.
        return [c for c in out if c.spec_match_score >= 0.4]


# ----------------------------------------------------------------------
# Flagger — decides whether a flag should be raised
# ----------------------------------------------------------------------

@dataclass
class FlagDecision:
    flagged: bool
    reason: str
    markup_percent: float | None
    market_median: Decimal | None
    market_samples_used: list[MarketplaceSample]
    confidence: float


def confidence_score(
    samples: Sequence[MarketplaceSample],
    has_ekatalog_match: bool,
) -> float:
    """
    Confidence is a 0-1 score combining:
      - sample count (more samples = better)
      - mean spec match score
      - inverse of price variance (low variance = trustworthy reference)
      - bonus if e-Katalog match is present
    """
    if not samples:
        return 0.0
    n = len(samples)
    sample_factor = min(n / 5.0, 1.0)            # 5+ samples = full credit

    mean_spec = sum(s.spec_match_score for s in samples) / n

    prices = [float(s.price) for s in samples]
    if len(prices) >= 2 and statistics.mean(prices) > 0:
        cv = statistics.pstdev(prices) / statistics.mean(prices)
        variance_factor = max(0.0, 1.0 - min(cv, 1.0))
    else:
        variance_factor = 0.5

    base = 0.4 * sample_factor + 0.3 * mean_spec + 0.2 * variance_factor
    if has_ekatalog_match:
        base += 0.1
    return round(min(base, 1.0), 2)


def decide_flag(
    item: LineItem,
    samples: list[MarketplaceSample],
    threshold_pct: float | None = None,
) -> FlagDecision:
    """
    Apply the 20% rule (or province-adjusted threshold).
    """
    threshold = threshold_pct if threshold_pct is not None else threshold_for_province(item.province)

    if item.category == Category.PROJECT:
        return FlagDecision(False, "Project — not auto-comparable", None, None, [], 0.0)

    if item.category != Category.GOODS:
        return FlagDecision(False, f"Category {item.category.value} not flagged in v1", None, None, [], 0.0)

    if len(samples) < MIN_SAMPLES_FOR_FLAG:
        return FlagDecision(
            False,
            f"Insufficient samples ({len(samples)}/{MIN_SAMPLES_FOR_FLAG}) — not flagged",
            None, None, samples, 0.0,
        )

    prices = sorted(float(s.price) for s in samples)
    median = Decimal(str(statistics.median(prices)))
    has_ekat = any(s.is_ekatalog for s in samples)
    conf = confidence_score(samples, has_ekat)

    if conf < LOW_CONFIDENCE_FLOOR:
        return FlagDecision(
            False,
            f"Low confidence ({conf:.2f}) — surfaced in Low Confidence tab",
            None, median, samples, conf,
        )

    markup_pct = (float(item.unit_price) - float(median)) / float(median) * 100.0

    if markup_pct > threshold:
        return FlagDecision(
            True,
            f"Markup {markup_pct:.1f}% exceeds {threshold}% threshold",
            markup_pct, median, samples, conf,
        )
    return FlagDecision(False, f"Markup {markup_pct:.1f}% within {threshold}% threshold", markup_pct, median, samples, conf)


# ----------------------------------------------------------------------
# Apply matcher + flagger to a stream of items
# ----------------------------------------------------------------------

def enrich(items: Iterable[LineItem], matcher: Matcher) -> list[LineItem]:
    """For each item, find marketplace samples and attach flag decision."""
    enriched: list[LineItem] = []
    for item in items:
        samples = matcher.candidates_for(item)
        decision = decide_flag(item, samples)

        item.marketplace_median = decision.market_median
        item.marketplace_samples = [
            {
                "vendor": s.vendor,
                "title": s.title,
                "price": float(s.price),
                "url": s.url,
                "captured_at": s.captured_at,
            }
            for s in decision.market_samples_used
        ]
        item.confidence = decision.confidence
        item.flagged = decision.flagged
        item.markup_percent = decision.markup_percent
        enriched.append(item)
    return enriched
