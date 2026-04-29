"""
BudgetWatch — Jakarta-specific connectors.

These hit the official DKI Jakarta open-data sources directly. Public, no
auth, no MoU paperwork required.

Sources:
  1. data.jakarta.go.id — CKAN portal with historical APBD line-item data
     (unit_kerja → urusan → program → kegiatan → rekening → apbd → realisasi).
     Goes back to 2009. Best fit for our LineItem schema.
     CKAN datastore_search API: clean JSON.

  2. dashboard-bpkd.jakarta.go.id — Live BPKD dashboard with current-year
     APBD breakdown by SKPD and per-Akun realisasi. Renders via JS so we
     hit the underlying AJAX endpoints directly.

  3. (Already in connectors.py: DJPK, INAPROC API Gateway, SPSE Nasional)
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable
from urllib.parse import quote

import httpx

from connectors import BaseConnector
from models import (
    LineItem, AgencyLevel, Category, Status, Source,
    classify_line_item,
)

log = logging.getLogger("budgetwatch.connectors.jakarta")


# ----------------------------------------------------------------------
# 1. data.jakarta.go.id — CKAN datastore
# ----------------------------------------------------------------------

class DataJakartaCKANConnector(BaseConnector):
    """
    Pulls APBD realisasi line items via the CKAN datastore_search API.

    URL pattern (verified against ckan docs + data.jakarta.go.id):
      https://data.jakarta.go.id/api/3/action/datastore_search
        ?resource_id=<uuid>
        &limit=10000
        &q=<optional text search>
        &filters={"tahun":"2024","unit_kerja":"1.01.01"}

    The dataset 'data-realisasi-anggaran-pendapatan-dan-belanja-daerah'
    has one resource_id per year. Find them via package_show:
      /api/3/action/package_show?id=data-realisasi-anggaran-pendapatan-dan-belanja-daerah

    Schema columns (verified):
      tahun, unit_kerja, nama_unitkerja, urusan, nama_urusan, program,
      nama_program, kegiatan, nama_kegiatan, rekening, nama_rekening,
      apbd, apbdp, realisasi
    """

    source = Source.APBD
    BASE = "https://data.jakarta.go.id/api/3/action"
    DATASET_SLUG = "data-realisasi-anggaran-pendapatan-dan-belanja-daerah"

    # Skip rekening lines that aren't actual budget items
    _SKIP_REKENING_PREFIXES = (
        "4.",   # pendapatan (revenue, not spending)
        "6.",   # pembiayaan (financing, not spending)
    )

    async def fetch_raw(self, *, resource_id: str | None = None, limit: int = 10000, tahun: int | None = None) -> bytes:
        """If resource_id is None, look it up from package_show."""
        if not resource_id:
            url = f"{self.BASE}/package_show"
            params = {"id": self.DATASET_SLUG}
            r = await self._http_get(url, params=params)
            r.raise_for_status()
            pkg = json.loads(r.content)
            resources = pkg.get("result", {}).get("resources", [])
            # Pick the resource that matches the requested tahun if specified
            if tahun:
                for res in resources:
                    if str(tahun) in (res.get("name", "") + res.get("description", "")):
                        resource_id = res.get("id")
                        break
            # Fallback: most recently modified resource
            if not resource_id and resources:
                resource_id = resources[0].get("id")

        if not resource_id:
            raise RuntimeError(f"No resource_id found for dataset {self.DATASET_SLUG}")

        url = f"{self.BASE}/datastore_search"
        params = {"resource_id": resource_id, "limit": limit}
        r = await self._http_get(url, params=params)
        r.raise_for_status()
        return r.content

    def parse(self, raw: bytes) -> dict:
        data = json.loads(raw)
        if not data.get("success"):
            raise RuntimeError(f"CKAN error: {data.get('error')}")
        return data.get("result", {})

    def normalize(self, parsed: dict, raw_uri: str) -> Iterable[LineItem]:
        records = parsed.get("records", [])
        for rec in records:
            rekening = rec.get("rekening", "") or ""
            # Filter to belanja (spending) lines only
            if rekening.startswith(self._SKIP_REKENING_PREFIXES):
                continue
            if not rekening:
                continue

            tahun = int(rec.get("tahun") or 0)
            unit_kerja = rec.get("unit_kerja", "")
            nama_unit = rec.get("nama_unitkerja") or rec.get("nama_unit_kerja") or ""
            nama_program = rec.get("nama_program", "") or ""
            nama_kegiatan = rec.get("nama_kegiatan", "") or ""
            nama_rekening = rec.get("nama_rekening", "") or ""

            # Use the realisasi as the spent amount when available, else apbdp/apbd
            anggaran = (
                rec.get("apbdp") or rec.get("apbd_p") or rec.get("apbd") or 0
            )
            realisasi = rec.get("realisasi", 0) or 0
            anggaran_dec = _to_decimal(anggaran)
            realisasi_dec = _to_decimal(realisasi)

            # Description = the most specific label we have
            desc = nama_rekening or nama_kegiatan or "(tidak ada uraian)"

            # Better category routing using rekening codes:
            #   5.1.* = Belanja Tidak Langsung (personnel, etc.)
            #   5.2.1.* = Belanja Pegawai (personnel)
            #   5.2.2.* = Belanja Barang dan Jasa (goods/services)
            #   5.2.3.* = Belanja Modal (capital — usually construction or equipment)
            #   5.2.3.01.* = Belanja Modal Tanah / Bangunan / Jalan = PROJECT
            #   5.2.3.05.* = Belanja Modal Peralatan & Mesin = GOODS
            if rekening.startswith("5.1.") or rekening.startswith("5.2.1."):
                cat = Category.PERSONNEL
            elif rekening.startswith("5.2.3.01") or rekening.startswith("5.2.3.02") or rekening.startswith("5.2.3.03"):
                cat = Category.PROJECT  # tanah, bangunan, jalan
            elif rekening.startswith("5.2.3."):
                cat = Category.GOODS    # peralatan, kendaraan, etc.
            elif rekening.startswith("5.2.2."):
                # Goods if it's barang, services if jasa
                cat = Category.GOODS if "barang" in nama_rekening.lower() else (
                    Category.SERVICES if "jasa" in nama_rekening.lower() else Category.GOODS
                )
            else:
                cat = classify_line_item(desc, "paket", float(anggaran_dec))
            # Determine status: REALIZED if realisasi > 0, else PLANNED
            status = Status.REALIZED if realisasi_dec > 0 else Status.PLANNED

            # Use realisasi as the effective price if realized; budget otherwise
            effective_price = realisasi_dec if realisasi_dec > 0 else anggaran_dec

            record_id = f"{tahun}-{unit_kerja}-{rekening}"

            yield LineItem(
                id=f"DKI-CKAN-{record_id}",
                source=Source.APBD,
                source_record_id=record_id,
                source_url=f"https://data.jakarta.go.id/dataset/{self.DATASET_SLUG}",
                source_label=f"data.jakarta.go.id — APBD Realisasi {tahun}",
                fiscal_year=tahun,
                agency_level=AgencyLevel.PROVINSI,
                agency_name=nama_unit,
                agency_code=unit_kerja,
                province="DKI Jakarta",
                program=nama_program,
                activity=nama_kegiatan,
                description=desc,
                category=cat,
                unit="paket",
                quantity=Decimal("1"),
                unit_price=effective_price,
                total_amount=effective_price,
                status=status,
                ingested_at=datetime.now(timezone.utc),
                raw_payload_uri=raw_uri,
            )


# ----------------------------------------------------------------------
# 2. dashboard-bpkd.jakarta.go.id — Live BPKD dashboard
# ----------------------------------------------------------------------

class JakartaBPKDConnector(BaseConnector):
    """
    Live BPKD DKI Jakarta transparency dashboard.

    The dashboard at dashboard-bpkd.jakarta.go.id renders tables via JS.
    The underlying AJAX endpoints serve JSON; common patterns observed:

      /skpd/data?date=2026-04-30            -> realisasi per SKPD
      /akun/data?date=2026-04-30            -> realisasi per akun
      /dashboard/belanja/data?tahun=2026    -> APBD belanja breakdown

    The exact endpoint URLs may shift; this connector tries a few known
    patterns and degrades gracefully if the dashboard restructures.

    Output is per-SKPD aggregated rows (not line-items). We emit one
    LineItem per SKPD with the realisasi/anggaran values rolled up, which
    is fine for the agency drill-down view.
    """

    source = Source.APBD
    BASE = "https://dashboard-bpkd.jakarta.go.id"

    # Try these endpoint patterns in order — first that returns JSON wins
    _SKPD_ENDPOINTS = [
        "/skpd/data",
        "/skpd/index",
        "/api/skpd",
    ]

    async def fetch_raw(self, *, date: str | None = None) -> bytes:
        """date format: YYYY-MM-DD; defaults to today."""
        if not date:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        last_err = None
        for ep in self._SKPD_ENDPOINTS:
            url = f"{self.BASE}{ep}"
            try:
                async with httpx.AsyncClient(
                    headers={
                        "User-Agent": self.user_agent,
                        "Accept": "application/json,text/html",
                        "X-Requested-With": "XMLHttpRequest",
                    },
                    timeout=self.timeout_s, follow_redirects=True,
                ) as client:
                    r = await client.get(url, params={"date": date})
                    if r.status_code == 200 and r.content:
                        # Verify it's actually JSON
                        try:
                            json.loads(r.content)
                            return r.content
                        except (ValueError, json.JSONDecodeError):
                            continue
            except httpx.HTTPError as e:
                last_err = e
                continue
        # Fallback: return empty data structure rather than raising
        log.warning("BPKD dashboard endpoints all failed (last err: %s); returning empty", last_err)
        return b'{"data":[]}'

    def parse(self, raw: bytes) -> list[dict]:
        data = json.loads(raw)
        # BPKD endpoint variants — handle DataTables-style and plain array
        if isinstance(data, list):
            return data
        if "data" in data and isinstance(data["data"], list):
            return data["data"]
        return []

    def normalize(self, rows: list[dict], raw_uri: str) -> Iterable[LineItem]:
        """
        BPKD SKPD payload (best guess from page structure):
          {
            "no": 1,
            "nama_skpd": "DINAS PENDIDIKAN",
            "kode_skpd": "1.01.01",
            "anggaran_belanja": 12500000000000,
            "realisasi_belanja": 4200000000000,
            "anggaran_pendapatan": 0,
            "realisasi_pendapatan": 0,
          }
        """
        tahun = datetime.now(timezone.utc).year
        for row in rows:
            nama = (row.get("nama_skpd") or row.get("nama_skpd_gabungan") or "").strip()
            if not nama:
                continue
            kode = row.get("kode_skpd") or row.get("kode") or ""
            anggaran = _to_decimal(row.get("anggaran_belanja") or row.get("anggaran") or 0)
            realisasi = _to_decimal(row.get("realisasi_belanja") or row.get("realisasi") or 0)
            if anggaran == 0 and realisasi == 0:
                continue

            effective_price = realisasi if realisasi > 0 else anggaran
            status = Status.REALIZED if realisasi > 0 else Status.PLANNED

            yield LineItem(
                id=f"DKI-BPKD-{tahun}-{kode or _slugify(nama)}",
                source=Source.APBD,
                source_record_id=f"{tahun}-{kode or _slugify(nama)}",
                source_url=f"{self.BASE}/skpd",
                source_label=f"BPKD DKI Jakarta — Realisasi SKPD {tahun}",
                fiscal_year=tahun,
                agency_level=AgencyLevel.PROVINSI,
                agency_name=nama.title(),
                agency_code=kode,
                province="DKI Jakarta",
                program="(SKPD-level rollup)",
                activity="",
                description=f"Total Belanja {nama.title()} TA {tahun}",
                category=Category.OTHER,
                unit="paket",
                quantity=Decimal("1"),
                unit_price=effective_price,
                total_amount=effective_price,
                status=status,
                ingested_at=datetime.now(timezone.utc),
                raw_payload_uri=raw_uri,
            )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

_NUM_RE = re.compile(r"-?\d+(?:[.,]\d+)?")

def _to_decimal(v: Any) -> Decimal:
    if v is None or v == "":
        return Decimal("0")
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    s = str(v).replace(",", "").replace(" ", "")
    m = _NUM_RE.search(s)
    if m:
        return Decimal(m.group(0).replace(",", ""))
    return Decimal("0")


def _slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")
