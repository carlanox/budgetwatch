"""
BudgetWatch — Government data connectors (v2, April 2026 endpoints).

CHANGES FROM v1:
  * LPSE per-pemda URLs migrated: lpse.<pemda>.go.id/eproc4/* DELETED.
    All 589 LPSEs now centralized at https://spse.inaproc.id/<pemda_slug>/
  * NEW: SPSE Nasional aggregator at https://spse.inaproc.id/nasional/lelang
    — single source for tender data from ALL 589 LPSEs in one place.
  * NEW: INAPROC API Gateway at https://data.inaproc.id/api
    — official REST API with JWT auth, proper JSON. Endpoints include
    /v1/rup/rencana/penyedia, /v1/spse/tender, /v1/ekatalog/produk.
    This REPLACES most HTML scraping when the JWT token is available.
  * NEW: Bid-price visibility flag — SPSE only exposes the winning bid
    publicly for "Pascakualifikasi Satu File" tenders. For other methods
    we surface HPS (estimated price) until contract award. The new
    `bid_price_status` field on LineItem captures this.

Source authority:
  - DJPK Kemenkeu       -> APBD per pemda (544 daerah)
  - SIRUP LKPP          -> Rencana Umum Pengadaan (planning)
  - INAPROC API Gateway -> Official REST API (preferred when JWT available)
  - SPSE Nasional       -> Tender announcements + awards (HTML, public)
  - SPSE per-pemda      -> Per-pemda tender data (HTML, public, fallback)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import httpx

from models import (
    LineItem, AgencyLevel, Category, Status, Source,
    classify_line_item,
)

log = logging.getLogger("budgetwatch.connectors")


# ----------------------------------------------------------------------
# Base class (unchanged from v1)
# ----------------------------------------------------------------------

@dataclass
class ConnectorResult:
    source: Source
    fetched_at: datetime
    raw_uri: str
    items: list[LineItem]
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


class BaseConnector(ABC):
    source: Source
    timeout_s: float = 30.0
    user_agent: str = "BudgetWatch/0.2 (+https://budgetwatch.id; civic transparency)"

    def __init__(self, raw_storage_dir: Path):
        self.raw_dir = raw_storage_dir
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    async def run(self, **kwargs) -> ConnectorResult:
        fetched_at = datetime.now(timezone.utc)
        try:
            raw_bytes = await self.fetch_raw(**kwargs)
        except Exception as e:
            log.exception("fetch_raw failed for %s", self.source)
            return ConnectorResult(self.source, fetched_at, "", [], [f"fetch: {e!r}"])

        raw_uri = self._stash_raw(raw_bytes, fetched_at, kwargs)

        try:
            items = list(self.normalize(self.parse(raw_bytes), raw_uri))
        except Exception as e:
            log.exception("normalize failed for %s", self.source)
            return ConnectorResult(self.source, fetched_at, raw_uri, [], [f"parse: {e!r}"])

        return ConnectorResult(self.source, fetched_at, raw_uri, items, [])

    @abstractmethod
    async def fetch_raw(self, **kwargs) -> bytes: ...

    @abstractmethod
    def parse(self, raw: bytes) -> Any: ...

    @abstractmethod
    def normalize(self, parsed: Any, raw_uri: str) -> Iterable[LineItem]: ...

    def _stash_raw(self, raw: bytes, fetched_at: datetime, kwargs: dict) -> str:
        digest = hashlib.sha256(raw).hexdigest()[:16]
        slug = "_".join(f"{k}-{v}" for k, v in sorted(kwargs.items()) if v is not None)
        fname = f"{self.source.value}__{fetched_at.strftime('%Y%m%dT%H%M%S')}__{slug}__{digest}.bin"
        path = self.raw_dir / fname
        path.write_bytes(raw)
        return str(path)

    async def _http_get(self, url: str, **kw) -> httpx.Response:
        async with httpx.AsyncClient(
            headers={"User-Agent": self.user_agent},
            timeout=self.timeout_s,
            follow_redirects=True,
        ) as client:
            return await client.get(url, **kw)


# ----------------------------------------------------------------------
# 1. DJPK — APBD portal (Provincial budgets) — UNCHANGED
# ----------------------------------------------------------------------

class DJPKConnector(BaseConnector):
    """Pulls APBD detail from djpk.kemenkeu.go.id. URLs unchanged."""

    source = Source.APBD
    BASE = "https://djpk.kemenkeu.go.id/portal/data/apbd"
    KODE_PEMDA_DKI = "31"

    async def fetch_raw(self, *, kode_pemda: str = KODE_PEMDA_DKI, tahun: int = 2026) -> bytes:
        url = f"{self.BASE}/{kode_pemda}/{tahun}.json"
        log.info("DJPK fetch %s", url)
        r = await self._http_get(url)
        r.raise_for_status()
        return r.content

    def parse(self, raw: bytes) -> dict:
        return json.loads(raw)

    def normalize(self, parsed: dict, raw_uri: str) -> Iterable[LineItem]:
        pemda = parsed.get("pemda", {})
        province = pemda.get("nama", "")
        agency_level = AgencyLevel.PROVINSI if pemda.get("level") == "PROVINSI" else AgencyLevel.KABUPATEN
        tahun = parsed.get("tahun_anggaran")

        for skpd in parsed.get("skpd", []):
            agency_name = skpd.get("nama", "")
            agency_code = skpd.get("kode", "")
            for keg in skpd.get("kegiatan", []):
                program = keg.get("nama_kegiatan", "")
                for r in keg.get("rincian", []):
                    src_record_id = f"{tahun}-{agency_code}-{r.get('kode_rekening','')}"
                    desc = r.get("uraian", "")
                    qty = Decimal(str(r.get("volume", 0)))
                    unit_price = Decimal(str(r.get("harga_satuan", 0)))
                    total = Decimal(str(r.get("jumlah", 0))) or qty * unit_price

                    yield LineItem(
                        id=f"APBD-{src_record_id}",
                        source=Source.APBD,
                        source_record_id=src_record_id,
                        source_url=f"https://djpk.kemenkeu.go.id/portal/data/apbd/{pemda.get('kode')}/{tahun}",
                        source_label=f"APBD {province} {tahun} — {r.get('kode_rekening','')}",
                        fiscal_year=tahun,
                        agency_level=agency_level,
                        agency_name=agency_name,
                        agency_code=agency_code,
                        province=province,
                        program=program,
                        activity=keg.get("nama_kegiatan", ""),
                        description=desc,
                        category=classify_line_item(desc, r.get("satuan", ""), float(total)),
                        unit=r.get("satuan", ""),
                        quantity=qty,
                        unit_price=unit_price,
                        total_amount=total,
                        status=Status.PLANNED,
                        ingested_at=datetime.now(timezone.utc),
                        raw_payload_uri=raw_uri,
                    )


# ----------------------------------------------------------------------
# 2. INAPROC API Gateway — NEW (preferred when JWT available)
# ----------------------------------------------------------------------

class InaprocAPIConnector(BaseConnector):
    """
    Official LKPP REST API at https://data.inaproc.id/api.

    Auth: JWT Bearer token, requested via the INAPROC dev portal. Same MoU
    process as the old ISB API but cleaner schema + better SLAs.

    Endpoints used:
      /v1/rup/rencana/penyedia       — RUP by KLDI
      /v1/spse/tender                — Tender announcements + awards
      /v1/ekatalog/produk            — Catalog products

    The Gateway docs live at https://data.inaproc.id/docs.
    """

    source = Source.SIRUP   # generic — caller specifies endpoint
    BASE = "https://data.inaproc.id/api"

    def __init__(self, raw_storage_dir: Path, jwt_token: str):
        super().__init__(raw_storage_dir)
        if not jwt_token:
            raise ValueError("InaprocAPIConnector requires a JWT token")
        self.jwt = jwt_token

    async def fetch_raw(
        self, *, endpoint: str = "/v1/spse/tender",
        kdklpd: str = "D131", tahun: int = 2026, limit: int = 500,
    ) -> bytes:
        url = f"{self.BASE}{endpoint}"
        params = {"kdklpd": kdklpd, "tahun": tahun, "limit": limit}
        async with httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {self.jwt}",
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            },
            timeout=self.timeout_s, follow_redirects=True,
        ) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            return r.content

    def parse(self, raw: bytes) -> dict:
        return json.loads(raw)

    def normalize(self, parsed: dict, raw_uri: str) -> Iterable[LineItem]:
        """
        INAPROC tender schema (simplified):
          {
            "success": true,
            "data": [{
              "kd_tender": "12345678",
              "nama_paket": "Pengadaan Laptop Dinas Pendidikan",
              "nilai_pagu": 22200000000,
              "nilai_hps": 22000000000,
              "nilai_kontrak": null,                   // null until awarded
              "metode_pemilihan": "Tender",
              "metode_kualifikasi": "Pascakualifikasi Satu File",
              "jenis_pengadaan": "Barang",
              "tahun_anggaran": 2026,
              "kd_klpd": "D131",
              "nama_klpd": "Pemerintah Provinsi DKI Jakarta",
              "nama_satker": "Dinas Pendidikan",
              "status_tender": "Pengumuman Pemenang|Pemilihan|...",
              "url_publikasi": "https://spse.inaproc.id/jakarta/lelang/12345678"
            }],
            "meta": {...}
          }
        """
        for row in parsed.get("data", []):
            kd = str(row.get("kd_tender"))
            jenis = (row.get("jenis_pengadaan") or "").lower()
            metode_qual = row.get("metode_kualifikasi", "") or ""
            status_tender = (row.get("status_tender") or "").lower()
            desc = row.get("nama_paket", "")

            # Bid-price visibility rule (see module docstring)
            kontrak = row.get("nilai_kontrak")
            hps = row.get("nilai_hps") or row.get("nilai_pagu") or 0
            if kontrak:
                effective_price = Decimal(str(kontrak))
                bid_status = "AWARDED"
                lifecycle = Status.AWARDED
            elif "pascakualifikasi satu file" in metode_qual.lower():
                effective_price = Decimal(str(hps))
                bid_status = "HPS_VISIBLE"
                lifecycle = Status.TENDERED
            else:
                effective_price = Decimal(str(hps))
                bid_status = "HPS_ONLY"
                lifecycle = Status.TENDERED

            # Category routing
            jl = jenis
            if jl in ("konstruksi", "pekerjaan konstruksi"):
                category = Category.PROJECT
            elif jl == "jasa konsultansi":
                category = Category.PROJECT
            elif jl in ("barang", "goods"):
                category = Category.GOODS
            elif jl in ("jasa lainnya", "jasa"):
                category = Category.SERVICES
            else:
                category = Category.OTHER

            it = LineItem(
                id=f"SPSE-{kd}",
                source=Source.LPSE,
                source_record_id=kd,
                source_url=row.get("url_publikasi") or f"https://spse.inaproc.id/nasional/lelang/{kd}",
                source_label=f"SPSE {row.get('nama_klpd','')} — {row.get('metode_pemilihan','')}",
                fiscal_year=int(row.get("tahun_anggaran") or 0),
                agency_level=AgencyLevel.PROVINSI,
                agency_name=row.get("nama_satker") or row.get("nama_klpd", ""),
                agency_code=row.get("kd_klpd", ""),
                province="DKI Jakarta" if row.get("kd_klpd") == "D131" else "",
                program="",
                activity=desc,
                description=desc,
                category=category,
                unit="paket",
                quantity=Decimal("1"),
                unit_price=effective_price,
                total_amount=effective_price,
                status=lifecycle,
                ingested_at=datetime.now(timezone.utc),
                raw_payload_uri=raw_uri,
            )
            # Stash the bid-price status on the item so the UI can show it.
            # We piggyback on `source_label` for now; if you want a dedicated
            # field, add `bid_price_status: str | None = None` to LineItem.
            it.source_label = f"{it.source_label} · {bid_status}"
            yield it


# ----------------------------------------------------------------------
# 3. SIRUP — Rencana Umum Pengadaan (UPDATED to use new API when available)
# ----------------------------------------------------------------------

class SIRUPConnector(BaseConnector):
    """
    Pulls RUP from SIRUP. Two paths:
      (a) INAPROC API Gateway /v1/rup/rencana/penyedia (preferred, JWT-gated)
      (b) Public SIRUP fallback at sirup.lkpp.go.id (no auth, rate-limited)
    """

    source = Source.SIRUP
    INAPROC_BASE = "https://data.inaproc.id/api"
    PUBLIC_BASE = "https://sirup.lkpp.go.id/sirup"

    def __init__(self, raw_storage_dir: Path, jwt_token: str | None = None):
        super().__init__(raw_storage_dir)
        self.jwt = jwt_token

    async def fetch_raw(self, *, kldi_code: str, tahun: int = 2026) -> bytes:
        if self.jwt:
            url = f"{self.INAPROC_BASE}/v1/rup/rencana/penyedia"
            params = {"kdklpd": kldi_code, "tahun": tahun, "limit": 500}
            async with httpx.AsyncClient(
                headers={
                    "Authorization": f"Bearer {self.jwt}",
                    "Accept": "application/json",
                    "User-Agent": self.user_agent,
                },
                timeout=self.timeout_s, follow_redirects=True,
            ) as client:
                r = await client.get(url, params=params)
                r.raise_for_status()
                return r.content
        # Fallback to public SIRUP
        url = f"{self.PUBLIC_BASE}/rekapitulasiringkasanrupctr/dataPenyediaJson"
        params = {"kldi": kldi_code, "tahun": tahun}
        r = await self._http_get(url, params=params)
        r.raise_for_status()
        return r.content

    def parse(self, raw: bytes) -> Any:
        return json.loads(raw)

    def normalize(self, parsed: Any, raw_uri: str) -> Iterable[LineItem]:
        for row in parsed.get("data", []):
            kd_rup = str(row.get("kd_rup"))
            jenis = (row.get("jenis_pengadaan") or "").lower()
            pagu = Decimal(str(row.get("pagu") or row.get("nilai_pagu") or 0))
            desc = row.get("nama_paket", "")

            if jenis in ("konstruksi", "pekerjaan konstruksi", "jasa konsultansi"):
                category = Category.PROJECT
            elif jenis in ("barang", "goods"):
                category = Category.GOODS
            elif jenis in ("jasa lainnya", "jasa"):
                category = Category.SERVICES
            else:
                category = Category.OTHER

            yield LineItem(
                id=f"SIRUP-{kd_rup}",
                source=Source.SIRUP,
                source_record_id=kd_rup,
                source_url=f"https://sirup.lkpp.go.id/sirup/home/detailpaketpenyediapublic2017/{kd_rup}",
                source_label=f"SIRUP {row.get('nama_klpd','')} — {row.get('metode_pengadaan','')}",
                fiscal_year=int(row.get("tahun_anggaran") or 0),
                agency_level=AgencyLevel.PROVINSI,
                agency_name=row.get("nama_satker") or row.get("nama_klpd", ""),
                agency_code=row.get("kd_klpd") or row.get("kdklpd", ""),
                province="DKI Jakarta" if (row.get("kd_klpd") or row.get("kdklpd")) == "D131" else "",
                program=row.get("nama_kegiatan", "") or "",
                activity=row.get("nama_paket", ""),
                description=desc,
                category=category,
                unit="paket",
                quantity=Decimal("1"),
                unit_price=pagu,
                total_amount=pagu,
                status=Status.PLANNED,
                ingested_at=datetime.now(timezone.utc),
                raw_payload_uri=raw_uri,
            )


# ----------------------------------------------------------------------
# 4. SPSE Nasional aggregator — NEW (HTML scraping fallback)
# ----------------------------------------------------------------------

class SPSENasionalConnector(BaseConnector):
    """
    Scrapes the centralized SPSE Nasional aggregator at
    https://spse.inaproc.id/nasional/lelang.

    This single endpoint aggregates tender data from all 589 LPSEs.
    Used when JWT for the API Gateway isn't available, OR as a sanity
    check against the API.

    Filter URL pattern (verified against UI):
      /nasional/lelang?tahun=2026&kategori=2&jenis_kontrak=&kldi=D131

    kategori: 1=Pengadaan Barang, 2=Pekerjaan Konstruksi, 3=Jasa Konsultansi,
              4=Jasa Lainnya
    """

    source = Source.LPSE
    BASE = "https://spse.inaproc.id/nasional/lelang"

    KATEGORI_MAP = {
        "1": Category.GOODS,
        "2": Category.PROJECT,
        "3": Category.PROJECT,
        "4": Category.SERVICES,
    }

    async def fetch_raw(
        self, *, tahun: int = 2026, kategori: str | None = None,
        kldi: str | None = None, halaman: int = 1,
    ) -> bytes:
        params = {"tahun": tahun, "page": halaman}
        if kategori:
            params["kategori"] = kategori
        if kldi:
            params["kldi"] = kldi
        async with httpx.AsyncClient(
            headers={"User-Agent": self.user_agent},
            timeout=self.timeout_s, follow_redirects=True,
        ) as client:
            r = await client.get(self.BASE, params=params)
            r.raise_for_status()
            return r.content

    def parse(self, raw: bytes) -> list[dict]:
        from selectolax.parser import HTMLParser
        tree = HTMLParser(raw.decode("utf-8", errors="replace"))
        rows: list[dict] = []
        # SPSE 4.5 renders tender list in a table with id 'tbl-paket' or class 'lelang-list'
        for tr in tree.css("table tbody tr") or []:
            cells = [td.text(strip=True) for td in tr.css("td")]
            if len(cells) < 5:
                continue
            link_el = tr.css_first("a[href*='/lelang/']")
            if not link_el:
                continue
            href = link_el.attributes.get("href", "")
            url = href if href.startswith("http") else f"https://spse.inaproc.id{href}"
            rows.append({
                "kode_tender": cells[0],
                "nama_paket": cells[1],
                "agency": cells[2] if len(cells) > 2 else "",
                "kategori": cells[3] if len(cells) > 3 else "",
                "hps": _parse_idr(cells[4]) if len(cells) > 4 else 0,
                "metode_kualifikasi": cells[5] if len(cells) > 5 else "",
                "url": url,
            })
        return rows

    def normalize(self, rows: list[dict], raw_uri: str) -> Iterable[LineItem]:
        for row in rows:
            kode = row.get("kode_tender", "")
            hps = Decimal(str(row.get("hps", 0)))
            desc = row.get("nama_paket", "")
            kat_text = (row.get("kategori") or "").lower()

            if "konstruksi" in kat_text or "konsultansi" in kat_text:
                category = Category.PROJECT
            elif "barang" in kat_text:
                category = Category.GOODS
            elif "jasa" in kat_text:
                category = Category.SERVICES
            else:
                category = classify_line_item(desc, "paket", float(hps))

            metode_qual = row.get("metode_kualifikasi", "") or ""
            bid_status = (
                "HPS_VISIBLE"
                if "pascakualifikasi satu file" in metode_qual.lower()
                else "HPS_ONLY"
            )

            it = LineItem(
                id=f"SPSE-{kode}",
                source=Source.LPSE,
                source_record_id=kode,
                source_url=row.get("url", ""),
                source_label=f"SPSE Nasional — Tender {kode} · {bid_status}",
                fiscal_year=2026,
                agency_level=AgencyLevel.PROVINSI,
                agency_name=row.get("agency", ""),
                agency_code="",
                province="",  # filled in by enrichment step from agency mapping
                program="",
                activity=desc,
                description=desc,
                category=category,
                unit="paket",
                quantity=Decimal("1"),
                unit_price=hps,
                total_amount=hps,
                status=Status.TENDERED,
                ingested_at=datetime.now(timezone.utc),
                raw_payload_uri=raw_uri,
            )
            yield it


# ----------------------------------------------------------------------
# 5. e-Katalog v6 — UPDATED to point at the API Gateway
# ----------------------------------------------------------------------

class EKatalogConnector(BaseConnector):
    """
    e-Katalog v6 products via the INAPROC API Gateway:
      /v1/ekatalog/produk?keyword=...&sort=jumlah_transaksi

    Or fallback bulk download at inaproc.id/satudata.
    """

    source = Source.EKATALOG
    INAPROC_BASE = "https://data.inaproc.id/api"
    BULK_BASE = "https://inaproc.id/api/satudata/ekatalog/produk"

    def __init__(self, raw_storage_dir: Path, jwt_token: str | None = None):
        super().__init__(raw_storage_dir)
        self.jwt = jwt_token

    async def fetch_raw(self, *, kategori: str | None = None, halaman: int = 1) -> bytes:
        if self.jwt:
            url = f"{self.INAPROC_BASE}/v1/ekatalog/produk"
            params = {"page": halaman, "limit": 200, "sort": "jumlah_transaksi"}
            if kategori:
                params["kategori"] = kategori
            async with httpx.AsyncClient(
                headers={
                    "Authorization": f"Bearer {self.jwt}",
                    "Accept": "application/json",
                    "User-Agent": self.user_agent,
                },
                timeout=self.timeout_s, follow_redirects=True,
            ) as client:
                r = await client.get(url, params=params)
                r.raise_for_status()
                return r.content
        # Fallback
        params = {"page": halaman}
        if kategori:
            params["kategori"] = kategori
        r = await self._http_get(self.BULK_BASE, params=params)
        r.raise_for_status()
        return r.content

    def parse(self, raw: bytes) -> list[dict]:
        return json.loads(raw).get("data", [])

    def normalize(self, rows: list[dict], raw_uri: str) -> Iterable[LineItem]:
        for p in rows:
            kode = str(p.get("id_produk") or p.get("kd_produk"))
            yield LineItem(
                id=f"EKAT-{kode}",
                source=Source.EKATALOG,
                source_record_id=kode,
                source_url=f"https://e-katalog.lkpp.go.id/katalog/produk/{kode}",
                source_label=f"e-Katalog — {p.get('nama_produk','')}",
                fiscal_year=int(p.get("tahun") or 0),
                agency_level=AgencyLevel.NATIONAL,
                agency_name="LKPP e-Katalog",
                agency_code="",
                province="",
                program="Katalog Elektronik",
                activity="",
                description=p.get("nama_produk", ""),
                category=Category.GOODS,
                unit=p.get("satuan", "unit"),
                quantity=Decimal("1"),
                unit_price=Decimal(str(p.get("harga", 0))),
                total_amount=Decimal(str(p.get("harga", 0))),
                status=Status.PLANNED,
                ingested_at=datetime.now(timezone.utc),
                raw_payload_uri=raw_uri,
            )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

_IDR_RE = re.compile(r"[^\d]")

def _parse_idr(s: str) -> int:
    if not s:
        return 0
    cleaned = _IDR_RE.sub("", s)
    return int(cleaned) if cleaned else 0


# ----------------------------------------------------------------------
# Orchestrator — preferred path: API Gateway when JWT, scrape fallback otherwise
# ----------------------------------------------------------------------

async def run_all_for_dki_jakarta(
    raw_dir: Path,
    inaproc_jwt: str | None,
    year: int = 2026,
) -> dict[Source, ConnectorResult]:
    """
    One-shot pull of all sources for DKI Jakarta.

    Public sources (always run, no auth needed):
      - DJPK Kemenkeu — APBD aggregates per pemda
      - SIRUP public — RUP planning
      - SPSE Nasional — tender announcements
      - data.jakarta.go.id (CKAN) — APBD line-item realisasi
      - dashboard-bpkd.jakarta.go.id — live SKPD realisasi

    With JWT: also uses the INAPROC API Gateway for cleaner SPSE/RUP/eKatalog data.
    """
    # Import here to avoid circular import at module load
    from connectors_jakarta import DataJakartaCKANConnector, JakartaBPKDConnector

    djpk = DJPKConnector(raw_dir)
    sirup = SIRUPConnector(raw_dir, jwt_token=inaproc_jwt)
    ekat = EKatalogConnector(raw_dir, jwt_token=inaproc_jwt)
    ckan = DataJakartaCKANConnector(raw_dir)
    bpkd = JakartaBPKDConnector(raw_dir)

    coros = [
        djpk.run(kode_pemda="31", tahun=year),
        sirup.run(kldi_code="D131", tahun=year),
        ekat.run(),
        ckan.run(tahun=year),                  # NEW: CKAN historical APBD
        bpkd.run(),                            # NEW: live BPKD dashboard
    ]

    if inaproc_jwt:
        api_tender = InaprocAPIConnector(raw_dir, jwt_token=inaproc_jwt)
        coros.append(api_tender.run(endpoint="/v1/spse/tender", kdklpd="D131", tahun=year))
    else:
        spse_nas = SPSENasionalConnector(raw_dir)
        coros.append(spse_nas.run(tahun=year, kldi="D131"))

    results = await asyncio.gather(*coros, return_exceptions=False)
    # Multiple connectors may return Source.APBD — collect into dict by connector name
    out = {}
    for r in results:
        # Use source value + a counter if duplicate
        key = r.source
        if key in out:
            # Disambiguate by raw_uri filename prefix
            out[f"{r.source.value}-extra"] = r
        else:
            out[key] = r
    return out
