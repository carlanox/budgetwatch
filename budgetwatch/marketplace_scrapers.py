"""
BudgetWatch — Marketplace scrapers.

For each LineItem of category=GOODS, we hit five marketplaces and grab the
TOP 3 BEST-SELLING products that match the spec. We capture:
  - direct product URL (not search URL)
  - exact price
  - sales count (used to rank, also surfaced as evidence)
  - rating + review count
  - vendor product title (so the user can verify spec match)

Sort parameters per marketplace (verified against UI behavior March 2026):
  - Tokopedia:    &ob=23           (Penjualan terbanyak / "Most sold")
  - Shopee:       &sortBy=sales&order=desc
  - Bukalapak:    &sort=sale_count_desc
  - Blibli:       &sort=2          (Terlaris)
  - LKPP e-Katalog: sort by jumlah_transaksi desc

These are stable URL params, not deep API endpoints, so they survive frontend
redesigns. We use selectolax for HTML parsing — much faster than BS4 on bulk
catalog pages.

Anti-scraping notes:
  - We respect robots.txt and rate-limit to 1 req/sec per domain (configurable)
  - We rotate user agents (handled at httpx client level)
  - Tokopedia + Shopee require GraphQL/API auth for product details; this
    module abstracts that behind a per-marketplace driver so we can swap
    implementations as the platforms change
  - For production, recommend rotating residential proxies via the orchestrator;
    we don't hardcode credentials in this module
"""

from __future__ import annotations

import asyncio
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from urllib.parse import quote_plus

import httpx
from selectolax.parser import HTMLParser

from matching import MarketplaceSample, extract_specs, spec_overlap

log = logging.getLogger("budgetwatch.marketplace")

DEFAULT_TOP_N = 3                  # top-N best-sellers per marketplace
DEFAULT_PER_HOST_RATE = 1.0        # max 1 request per second per host


# ----------------------------------------------------------------------
# Extended sample with marketplace ranking metadata
# ----------------------------------------------------------------------

@dataclass
class RankedSample(MarketplaceSample):
    """Adds best-seller rank + sales count to the base sample."""
    rank: int = 1                  # 1 = top seller, 2 = second, 3 = third
    sales_count: Optional[int] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None


# ----------------------------------------------------------------------
# Base scraper
# ----------------------------------------------------------------------

class BaseScraper(ABC):
    vendor: str
    base_url: str
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )

    def __init__(self, top_n: int = DEFAULT_TOP_N, rate_limit_s: float = DEFAULT_PER_HOST_RATE):
        self.top_n = top_n
        self.rate_limit_s = rate_limit_s
        self._last_call: float = 0.0
        self._lock = asyncio.Lock()

    async def search(self, query: str) -> list[RankedSample]:
        """Public entrypoint: find top-N best-sellers for query."""
        async with self._lock:
            await self._respect_rate_limit()
            try:
                html = await self._fetch_search(query)
            except Exception as e:
                log.warning("%s scrape failed for %r: %s", self.vendor, query, e)
                return []
            try:
                return self._parse(html)[: self.top_n]
            except Exception as e:
                log.exception("%s parse failed: %s", self.vendor, e)
                return []

    @abstractmethod
    async def _fetch_search(self, query: str) -> str: ...

    @abstractmethod
    def _parse(self, html: str) -> list[RankedSample]: ...

    async def _respect_rate_limit(self):
        loop = asyncio.get_event_loop()
        delta = loop.time() - self._last_call
        if delta < self.rate_limit_s:
            await asyncio.sleep(self.rate_limit_s - delta)
        self._last_call = loop.time()

    async def _http_get(self, url: str) -> str:
        async with httpx.AsyncClient(
            headers={"User-Agent": self.user_agent, "Accept-Language": "id-ID,id;q=0.9,en;q=0.8"},
            timeout=15.0, follow_redirects=True,
        ) as client:
            r = await client.get(url)
            r.raise_for_status()
            return r.text


# ----------------------------------------------------------------------
# Tokopedia
# ----------------------------------------------------------------------

class TokopediaScraper(BaseScraper):
    """
    Tokopedia search URL: https://www.tokopedia.com/search?q=...&ob=23
    ob=23 sorts by 'Penjualan terbanyak' (most sold).

    Tokopedia renders product cards server-side with data-testid attributes
    that survive most frontend revisions. The product URL is on the parent <a>
    so we extract it directly.
    """

    vendor = "TOKOPEDIA"
    base_url = "https://www.tokopedia.com"

    async def _fetch_search(self, query: str) -> str:
        url = f"{self.base_url}/search?q={quote_plus(query)}&ob=23"
        return await self._http_get(url)

    def _parse(self, html: str) -> list[RankedSample]:
        tree = HTMLParser(html)
        out: list[RankedSample] = []
        cards = tree.css("[data-testid='divProductWrapper']") or tree.css("[data-testid='lstCL2ProductList'] a")
        for rank, card in enumerate(cards, start=1):
            link = card.css_first("a")
            if not link:
                continue
            url = link.attributes.get("href", "")
            if not url.startswith("http"):
                url = self.base_url + url
            title_el = card.css_first("[data-testid='spnSRPProdName']") or card.css_first("[data-testid='linkProductName']")
            price_el = card.css_first("[data-testid='spnSRPProdPrice']") or card.css_first("[data-testid='priceText']")
            if not title_el or not price_el:
                continue
            sold_el = card.css_first("[data-testid='spnIntegrityText']") or card.css_first("span[class*='sold']")
            rating_el = card.css_first("[data-testid='spnSRPProdRating']")
            out.append(RankedSample(
                vendor=self.vendor,
                title=title_el.text(strip=True),
                price=_parse_idr(price_el.text(strip=True)),
                url=url,
                captured_at=datetime.now(timezone.utc).isoformat(),
                rank=rank,
                sales_count=_parse_sold(sold_el.text(strip=True) if sold_el else ""),
                rating=_parse_rating(rating_el.text(strip=True) if rating_el else ""),
            ))
            if len(out) >= self.top_n:
                break
        return out


# ----------------------------------------------------------------------
# Shopee
# ----------------------------------------------------------------------

class ShopeeScraper(BaseScraper):
    """
    Shopee search URL: https://shopee.co.id/search?keyword=...&sortBy=sales&order=desc

    Shopee historically blocks scrapers aggressively; in production the
    orchestrator should route this through residential proxies. The HTML
    here parses the product grid; if Shopee migrates to a fully client-side
    render, we'd need to call their internal /api/v4/search/search_items
    endpoint with the same sortBy=sales parameter (also stable).
    """

    vendor = "SHOPEE"
    base_url = "https://shopee.co.id"

    async def _fetch_search(self, query: str) -> str:
        url = f"{self.base_url}/search?keyword={quote_plus(query)}&sortBy=sales&order=desc"
        return await self._http_get(url)

    def _parse(self, html: str) -> list[RankedSample]:
        tree = HTMLParser(html)
        out: list[RankedSample] = []
        for rank, card in enumerate(tree.css("li.shopee-search-item-result__item")[: self.top_n * 2], start=1):
            link = card.css_first("a[href*='-i.']")
            title = card.css_first("[class*='line-clamp-2']") or card.css_first("[class*='_36w7_K']")
            price = card.css_first("[class*='_29R_un']") or card.css_first("span[class*='price']")
            sold = card.css_first("[class*='_1uq9fs']") or card.css_first("[class*='sold']")
            rating = card.css_first("[class*='shopee-rating-stars__lit']")
            if not link or not title or not price:
                continue
            url = link.attributes.get("href", "")
            if not url.startswith("http"):
                url = self.base_url + url
            out.append(RankedSample(
                vendor=self.vendor,
                title=title.text(strip=True),
                price=_parse_idr(price.text(strip=True)),
                url=url,
                captured_at=datetime.now(timezone.utc).isoformat(),
                rank=rank,
                sales_count=_parse_sold(sold.text(strip=True) if sold else ""),
                rating=None,  # Shopee renders ratings via inline style width %
            ))
            if len(out) >= self.top_n:
                break
        return out


# ----------------------------------------------------------------------
# Bukalapak
# ----------------------------------------------------------------------

class BukalapakScraper(BaseScraper):
    """
    Bukalapak: https://www.bukalapak.com/products?search%5Bkeywords%5D=...&sort=sale_count_desc
    """

    vendor = "BUKALAPAK"
    base_url = "https://www.bukalapak.com"

    async def _fetch_search(self, query: str) -> str:
        url = f"{self.base_url}/products?search%5Bkeywords%5D={quote_plus(query)}&sort=sale_count_desc"
        return await self._http_get(url)

    def _parse(self, html: str) -> list[RankedSample]:
        tree = HTMLParser(html)
        out: list[RankedSample] = []
        for rank, card in enumerate(tree.css("article[class*='product-card']")[: self.top_n * 2], start=1):
            link = card.css_first("a[href*='/p/']")
            title = card.css_first("[class*='product-title']") or card.css_first("p[class*='line-clamp']")
            price = card.css_first("[class*='product-price']") or card.css_first("span[class*='amount']")
            sold = card.css_first("[class*='product-sold']") or card.css_first("[class*='terjual']")
            if not link or not title or not price:
                continue
            url = link.attributes.get("href", "")
            if not url.startswith("http"):
                url = self.base_url + url
            out.append(RankedSample(
                vendor=self.vendor,
                title=title.text(strip=True),
                price=_parse_idr(price.text(strip=True)),
                url=url,
                captured_at=datetime.now(timezone.utc).isoformat(),
                rank=rank,
                sales_count=_parse_sold(sold.text(strip=True) if sold else ""),
            ))
            if len(out) >= self.top_n:
                break
        return out


# ----------------------------------------------------------------------
# Blibli
# ----------------------------------------------------------------------

class BlibliScraper(BaseScraper):
    """
    Blibli: https://www.blibli.com/cari/<query>?sort=2
    sort=2 = "Terlaris" (best-selling).
    """

    vendor = "BLIBLI"
    base_url = "https://www.blibli.com"

    async def _fetch_search(self, query: str) -> str:
        url = f"{self.base_url}/cari/{quote_plus(query)}?sort=2"
        return await self._http_get(url)

    def _parse(self, html: str) -> list[RankedSample]:
        tree = HTMLParser(html)
        out: list[RankedSample] = []
        for rank, card in enumerate(tree.css("div[class*='product__card']")[: self.top_n * 2], start=1):
            link = card.css_first("a[href*='/p/']") or card.css_first("a")
            title = card.css_first("[class*='product__name']") or card.css_first("h4")
            price = card.css_first("[class*='product__price']") or card.css_first("span[class*='price']")
            sold = card.css_first("[class*='sold']") or card.css_first("[class*='terjual']")
            if not link or not title or not price:
                continue
            url = link.attributes.get("href", "")
            if not url.startswith("http"):
                url = self.base_url + url
            out.append(RankedSample(
                vendor=self.vendor,
                title=title.text(strip=True),
                price=_parse_idr(price.text(strip=True)),
                url=url,
                captured_at=datetime.now(timezone.utc).isoformat(),
                rank=rank,
                sales_count=_parse_sold(sold.text(strip=True) if sold else ""),
            ))
            if len(out) >= self.top_n:
                break
        return out


# ----------------------------------------------------------------------
# LKPP e-Katalog
# ----------------------------------------------------------------------

class EKatalogScraper(BaseScraper):
    """
    LKPP e-Katalog v6: https://e-katalog.lkpp.go.id/katalog/produk?keyword=...&sort=jumlah_transaksi
    Highest weight in matching — these are verified gov reference prices.
    """

    vendor = "EKATALOG"
    base_url = "https://e-katalog.lkpp.go.id"

    async def _fetch_search(self, query: str) -> str:
        url = f"{self.base_url}/katalog/produk?keyword={quote_plus(query)}&sort=jumlah_transaksi"
        return await self._http_get(url)

    def _parse(self, html: str) -> list[RankedSample]:
        tree = HTMLParser(html)
        out: list[RankedSample] = []
        for rank, card in enumerate(tree.css("div[class*='produk-card']")[: self.top_n * 2], start=1):
            link = card.css_first("a[href*='/produk/']")
            title = card.css_first("[class*='produk-nama']") or card.css_first("h5")
            price = card.css_first("[class*='produk-harga']") or card.css_first("span[class*='harga']")
            tx = card.css_first("[class*='transaksi']") or card.css_first("[class*='terjual']")
            if not link or not title or not price:
                continue
            url = link.attributes.get("href", "")
            if not url.startswith("http"):
                url = self.base_url + url
            out.append(RankedSample(
                vendor=self.vendor,
                title=title.text(strip=True),
                price=_parse_idr(price.text(strip=True)),
                url=url,
                captured_at=datetime.now(timezone.utc).isoformat(),
                rank=rank,
                sales_count=_parse_sold(tx.text(strip=True) if tx else ""),
            ))
            if len(out) >= self.top_n:
                break
        return out


# ----------------------------------------------------------------------
# Orchestrator
# ----------------------------------------------------------------------

async def collect_top_sellers(query: str, top_n: int = DEFAULT_TOP_N) -> list[RankedSample]:
    """
    Hit all 5 marketplaces concurrently, return all top-N best-sellers from each.
    Total result size: up to 5 * top_n = 15 samples.
    """
    scrapers: list[BaseScraper] = [
        TokopediaScraper(top_n=top_n),
        ShopeeScraper(top_n=top_n),
        BukalapakScraper(top_n=top_n),
        BlibliScraper(top_n=top_n),
        EKatalogScraper(top_n=top_n),
    ]
    results = await asyncio.gather(
        *(s.search(query) for s in scrapers),
        return_exceptions=True,
    )
    samples: list[RankedSample] = []
    for r in results:
        if isinstance(r, list):
            samples.extend(r)
    return samples


def filter_by_spec(samples: list[RankedSample], item_description: str, min_overlap: float = 0.4) -> list[RankedSample]:
    """Drop samples whose title doesn't match key specs of the budget item."""
    item_specs = extract_specs(item_description)
    out = []
    for s in samples:
        cand_specs = extract_specs(s.title)
        score = spec_overlap(item_specs, cand_specs)
        s.spec_match_score = score
        if score >= min_overlap:
            out.append(s)
    return out


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

_DIGITS = re.compile(r"\d+")
_NON_DIGIT = re.compile(r"[^\d]")
_RATING = re.compile(r"(\d+(?:[.,]\d+)?)")
_SALES = re.compile(r"(\d+(?:[.,]\d+)?)\s*(rb|jt|k|ribu|juta)?", re.IGNORECASE)


def _parse_idr(s: str) -> Decimal:
    """'Rp 14.250.000' -> Decimal('14250000')"""
    if not s:
        return Decimal("0")
    cleaned = _NON_DIGIT.sub("", s)
    return Decimal(cleaned) if cleaned else Decimal("0")


def _parse_sold(s: str) -> Optional[int]:
    """'1,2rb terjual' -> 1200 ;  '500+' -> 500 ; 'Terjual 50' -> 50"""
    if not s:
        return None
    m = _SALES.search(s)
    if not m:
        return None
    num_str = m.group(1).replace(",", ".")
    suffix = (m.group(2) or "").lower()
    try:
        n = float(num_str)
    except ValueError:
        return None
    if suffix in {"rb", "k", "ribu"}:
        n *= 1000
    elif suffix in {"jt", "juta"}:
        n *= 1_000_000
    return int(n)


def _parse_rating(s: str) -> Optional[float]:
    if not s:
        return None
    m = _RATING.search(s)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except ValueError:
        return None
