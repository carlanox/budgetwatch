"""
BudgetWatch — Dev fixtures (v2 — top-3 best-sellers per marketplace).

Each GOODS line item now carries up to 15 marketplace samples
(5 marketplaces × top 3 best-sellers each), with:
  - direct product URLs (not search URLs)
  - exact sales count
  - rating + review count where available
  - rank: 1 = top-seller, 2 = second, 3 = third

These URLs are realistic synthetic examples — in production they come
from the marketplace_scrapers module hitting the real sites with
sales-sorted queries.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from models import LineItem, Source, AgencyLevel, Category, Status, classify_line_item


def _now():
    return datetime.now(timezone.utc)


def _samples(specs):
    """
    Build the marketplace_samples list from a compact spec.

    specs = [
      {
        "vendor": "TOKOPEDIA",
        "items": [
          {"title": "...", "price": 13990000, "url": "https://...", "sold": 1200, "rating": 4.9},
          {"title": "...", "price": 14100000, "url": "https://...", "sold": 850, "rating": 4.8},
          {"title": "...", "price": 14250000, "url": "https://...", "sold": 420, "rating": 4.9},
        ]
      },
      ...
    ]
    """
    out = []
    for vendor_block in specs:
        v = vendor_block["vendor"]
        for rank, item in enumerate(vendor_block["items"], start=1):
            out.append({
                "vendor": v,
                "title": item["title"],
                "price": item["price"],
                "url": item["url"],
                "rank": rank,
                "sales_count": item.get("sold"),
                "rating": item.get("rating"),
                "review_count": item.get("reviews"),
                "captured_at": _now().isoformat(),
            })
    return out


def _mk(*, seq, agency, program, description, unit, qty, unit_price,
        marketplace_median=None, samples=None, confidence=None,
        project=False, status=Status.PLANNED):
    total = Decimal(str(qty * unit_price))
    category = Category.PROJECT if project else classify_line_item(description, unit, float(total))
    src_url = ("https://lpse.jakarta.go.id/eproc4/lelang" if project
               else "https://djpk.kemenkeu.go.id/portal/data/apbd/31/2026")
    src_label = ("LPSE DKI Jakarta — Tender Konstruksi" if project
                 else "APBD DKI Jakarta 2026 — Belanja Modal")
    item_id = f"JKT-2026-{seq:03d}"

    item = LineItem(
        id=item_id, source=Source.LPSE if project else Source.APBD,
        source_record_id=item_id, source_url=src_url, source_label=src_label,
        fiscal_year=2026, agency_level=AgencyLevel.PROVINSI,
        agency_name=agency, agency_code="31", province="DKI Jakarta",
        program=program, activity=description, description=description,
        category=category, unit=unit,
        quantity=Decimal(str(qty)), unit_price=Decimal(str(unit_price)),
        total_amount=total, status=status,
        ingested_at=_now(), raw_payload_uri="(fixture)",
    )

    if marketplace_median is not None:
        item.marketplace_median = Decimal(str(marketplace_median))
        markup = (unit_price - marketplace_median) / marketplace_median * 100
        item.markup_percent = round(markup, 1)
        item.flagged = markup > 20.0
        item.confidence = confidence or 0.85
        item.marketplace_samples = samples or []
    return item


def load_dki_jakarta_fixture() -> list[LineItem]:
    items: list[LineItem] = []

    # ====================================================================
    # 1. Laptop Lenovo ThinkPad E14 — Dinas Pendidikan
    # ====================================================================
    items.append(_mk(
        seq=1, agency="Dinas Pendidikan DKI Jakarta",
        program="Penyediaan Sarana Pendidikan",
        description="Laptop Lenovo ThinkPad E14 Gen 5, Intel i5, 16GB RAM, 512GB SSD",
        unit="unit", qty=1200, unit_price=18_500_000,
        marketplace_median=14_200_000, confidence=0.94,
        samples=_samples([
            {"vendor": "TOKOPEDIA", "items": [
                {"title": "Lenovo ThinkPad E14 Gen 5 i5-1335U 16GB 512GB SSD Resmi", "price": 13_990_000, "url": "https://www.tokopedia.com/lenovostore/lenovo-thinkpad-e14-gen-5-i5-1335u-16gb-512gb-ssd-resmi-1-tahun", "sold": 1247, "rating": 4.9},
                {"title": "ThinkPad E14 G5 Core i5 13th Gen 16GB DDR4 512GB NVMe", "price": 14_100_000, "url": "https://www.tokopedia.com/notebookcenter/thinkpad-e14-g5-core-i5-13th-gen-16gb-ddr4-512gb-nvme", "sold": 856, "rating": 4.8},
                {"title": "Laptop Lenovo ThinkPad E14 Gen5 i5/16/512 Win11 Pro", "price": 14_250_000, "url": "https://www.tokopedia.com/promojakarta/laptop-lenovo-thinkpad-e14-gen5-i5-16-512-win11-pro", "sold": 421, "rating": 4.9},
            ]},
            {"vendor": "SHOPEE", "items": [
                {"title": "Lenovo ThinkPad E14 Gen 5 i5 16GB 512GB Original Resmi", "price": 14_050_000, "url": "https://shopee.co.id/Lenovo-ThinkPad-E14-Gen-5-i5-16GB-512GB-Original-Resmi-i.234567890.876543210", "sold": 980, "rating": 4.9},
                {"title": "ThinkPad E14 Gen5 Core i5-1335U 16GB RAM 512GB SSD", "price": 14_250_000, "url": "https://shopee.co.id/ThinkPad-E14-Gen5-Core-i5-1335U-16GB-RAM-512GB-SSD-i.234567891.876543211", "sold": 654, "rating": 4.8},
                {"title": "Laptop Lenovo TP E14 G5 i5 16/512 Win11 Garansi Resmi", "price": 14_400_000, "url": "https://shopee.co.id/Laptop-Lenovo-TP-E14-G5-i5-16-512-Win11-Garansi-Resmi-i.234567892.876543212", "sold": 312, "rating": 4.9},
            ]},
            {"vendor": "BUKALAPAK", "items": [
                {"title": "Lenovo ThinkPad E14 Gen 5 i5-1335U 16GB 512GB SSD Bergaransi", "price": 14_100_000, "url": "https://www.bukalapak.com/p/komputer/laptop/lenovo-thinkpad-e14-gen-5-i5-1335u-16gb-512gb-ssd-bergaransi", "sold": 387, "rating": 4.8},
                {"title": "ThinkPad E14 G5 Core i5 16GB 512GB NVMe Resmi Lenovo Indonesia", "price": 14_300_000, "url": "https://www.bukalapak.com/p/komputer/laptop/thinkpad-e14-g5-core-i5-16gb-512gb-nvme-resmi-lenovo-indonesia", "sold": 245, "rating": 4.9},
                {"title": "Lenovo TP E14 Gen 5 i5/16GB/512GB SSD Win 11 Pro Original", "price": 14_500_000, "url": "https://www.bukalapak.com/p/komputer/laptop/lenovo-tp-e14-gen-5-i5-16gb-512gb-ssd-win-11-pro-original", "sold": 178, "rating": 4.7},
            ]},
            {"vendor": "BLIBLI", "items": [
                {"title": "Lenovo ThinkPad E14 Gen 5 i5-1335U 16GB 512GB Win 11 Pro Black", "price": 14_400_000, "url": "https://www.blibli.com/p/lenovo-thinkpad-e14-gen-5-i5-1335u-16gb-512gb-win-11-pro-black/ps--PRA-12345-12345", "sold": 287, "rating": 4.9},
                {"title": "ThinkPad E14 G5 Intel Core i5 13th 16GB DDR4 512GB SSD NVMe", "price": 14_550_000, "url": "https://www.blibli.com/p/thinkpad-e14-g5-intel-core-i5-13th-16gb-ddr4-512gb-ssd-nvme/ps--PRA-12345-12346", "sold": 156, "rating": 4.8},
                {"title": "Laptop Lenovo ThinkPad E14 Gen 5 i5/16GB/512GB Garansi Resmi", "price": 14_750_000, "url": "https://www.blibli.com/p/laptop-lenovo-thinkpad-e14-gen-5-i5-16gb-512gb-garansi-resmi/ps--PRA-12345-12347", "sold": 92, "rating": 4.9},
            ]},
            {"vendor": "EKATALOG", "items": [
                {"title": "ThinkPad E14 Gen 5 i5-1335U 16GB 512GB — Katalog Elektronik LKPP", "price": 14_100_000, "url": "https://e-katalog.lkpp.go.id/katalog/produk/lenovo-thinkpad-e14-gen-5-i5-1335u-16gb-512gb", "sold": 2450, "rating": None},
                {"title": "Lenovo ThinkPad E14 G5 Core i5 16GB 512GB SSD — e-Katalog", "price": 14_250_000, "url": "https://e-katalog.lkpp.go.id/katalog/produk/lenovo-thinkpad-e14-g5-core-i5-16gb-512gb-ssd", "sold": 1680, "rating": None},
                {"title": "ThinkPad E14 Gen5 Intel i5 16GB DDR4 512GB NVMe — e-Katalog v6", "price": 14_400_000, "url": "https://e-katalog.lkpp.go.id/katalog/produk/thinkpad-e14-gen5-intel-i5-16gb-ddr4-512gb-nvme", "sold": 980, "rating": None},
            ]},
        ]),
    ))

    # ====================================================================
    # 2. Tensimeter Omron HEM-7156 — Dinas Kesehatan
    # ====================================================================
    items.append(_mk(
        seq=2, agency="Dinas Kesehatan DKI Jakarta",
        program="Penyediaan Alat Kesehatan Puskesmas",
        description="Tensimeter Digital Omron HEM-7156",
        unit="unit", qty=320, unit_price=1_450_000,
        marketplace_median=925_000, confidence=0.97,
        status=Status.TENDERED,
        samples=_samples([
            {"vendor": "TOKOPEDIA", "items": [
                {"title": "Omron HEM-7156 Tensimeter Digital Original Garansi Resmi", "price": 899_000, "url": "https://www.tokopedia.com/alkesresmi/omron-hem-7156-tensimeter-digital-original-garansi-resmi", "sold": 4520, "rating": 4.9},
                {"title": "Tensimeter Digital Omron HEM 7156 Upper Arm BP Monitor", "price": 925_000, "url": "https://www.tokopedia.com/medicalstore/tensimeter-digital-omron-hem-7156-upper-arm-bp-monitor", "sold": 2890, "rating": 4.9},
                {"title": "Omron HEM-7156 Alat Tensi Darah Digital Lengan Atas", "price": 949_000, "url": "https://www.tokopedia.com/alatkesehatan/omron-hem-7156-alat-tensi-darah-digital-lengan-atas", "sold": 1670, "rating": 4.8},
            ]},
            {"vendor": "SHOPEE", "items": [
                {"title": "Omron HEM 7156 Tensimeter Digital Garansi Resmi 5 Tahun", "price": 925_000, "url": "https://shopee.co.id/Omron-HEM-7156-Tensimeter-Digital-Garansi-Resmi-5-Tahun-i.345678901.987654321", "sold": 5230, "rating": 4.9},
                {"title": "Tensi Digital Omron HEM-7156 Original Indonesia", "price": 949_000, "url": "https://shopee.co.id/Tensi-Digital-Omron-HEM-7156-Original-Indonesia-i.345678902.987654322", "sold": 3120, "rating": 4.9},
                {"title": "Omron HEM7156 BP Monitor Digital Upper Arm Resmi", "price": 975_000, "url": "https://shopee.co.id/Omron-HEM7156-BP-Monitor-Digital-Upper-Arm-Resmi-i.345678903.987654323", "sold": 1840, "rating": 4.8},
            ]},
            {"vendor": "BUKALAPAK", "items": [
                {"title": "Tensimeter Omron HEM-7156 Digital Original Garansi", "price": 950_000, "url": "https://www.bukalapak.com/p/kesehatan/alat-kesehatan/tensimeter-omron-hem-7156-digital-original-garansi", "sold": 1240, "rating": 4.8},
                {"title": "Omron HEM 7156 Alat Tensi Darah Digital", "price": 985_000, "url": "https://www.bukalapak.com/p/kesehatan/alat-kesehatan/omron-hem-7156-alat-tensi-darah-digital", "sold": 690, "rating": 4.7},
                {"title": "Omron Digital BP Monitor HEM-7156 Resmi Indonesia", "price": 999_000, "url": "https://www.bukalapak.com/p/kesehatan/alat-kesehatan/omron-digital-bp-monitor-hem-7156-resmi-indonesia", "sold": 412, "rating": 4.8},
            ]},
            {"vendor": "BLIBLI", "items": [
                {"title": "Omron HEM-7156 Automatic Blood Pressure Monitor Digital", "price": 939_000, "url": "https://www.blibli.com/p/omron-hem-7156-automatic-blood-pressure-monitor-digital/ps--HEA-23456-78901", "sold": 890, "rating": 4.9},
                {"title": "Tensimeter Digital Omron HEM 7156 Resmi Indonesia 5Y Warranty", "price": 965_000, "url": "https://www.blibli.com/p/tensimeter-digital-omron-hem-7156-resmi-indonesia-5y-warranty/ps--HEA-23456-78902", "sold": 524, "rating": 4.9},
                {"title": "Omron HEM-7156 BP Monitor Original Garansi Resmi", "price": 989_000, "url": "https://www.blibli.com/p/omron-hem-7156-bp-monitor-original-garansi-resmi/ps--HEA-23456-78903", "sold": 287, "rating": 4.8},
            ]},
            {"vendor": "EKATALOG", "items": [
                {"title": "Omron HEM-7156 Tensimeter Digital — e-Katalog LKPP", "price": 920_000, "url": "https://e-katalog.lkpp.go.id/katalog/produk/omron-hem-7156-tensimeter-digital", "sold": 3450},
                {"title": "Tensimeter Omron HEM 7156 Upper Arm — e-Katalog v6", "price": 945_000, "url": "https://e-katalog.lkpp.go.id/katalog/produk/tensimeter-omron-hem-7156-upper-arm", "sold": 2180},
                {"title": "Omron HEM-7156 Digital BP Monitor — e-Katalog", "price": 975_000, "url": "https://e-katalog.lkpp.go.id/katalog/produk/omron-hem-7156-digital-bp-monitor", "sold": 1290},
            ]},
        ]),
    ))

    # ====================================================================
    # Items 3-18: keep the original prices/markups, but update samples to
    # also be top-3 per marketplace with direct URLs and sales data.
    # For brevity we use a helper to generate richer samples programmatically.
    # ====================================================================

    def _generate_samples(base_query, base_price, slug):
        """Build top-3 samples per marketplace at small price variations."""
        # Variation: rank-1 = best price, rank-2 = base, rank-3 = +3%
        # but rank reflects sales not price (top sales often cheap & viable).
        prices = [int(base_price * 0.97), base_price, int(base_price * 1.03)]
        sales = {
            "TOKOPEDIA": [3200, 1850, 920],
            "SHOPEE":    [4100, 2240, 1180],
            "BUKALAPAK": [890, 530, 280],
            "BLIBLI":    [620, 410, 215],
            "EKATALOG":  [2480, 1670, 980],
        }
        urls = {
            "TOKOPEDIA": "https://www.tokopedia.com/{seller}/{slug}-{rank}",
            "SHOPEE":    "https://shopee.co.id/{slug}-{rank}-i.{a}.{b}",
            "BUKALAPAK": "https://www.bukalapak.com/p/{slug}-{rank}",
            "BLIBLI":    "https://www.blibli.com/p/{slug}-{rank}/ps--{slug}-{rank}",
            "EKATALOG":  "https://e-katalog.lkpp.go.id/katalog/produk/{slug}-{rank}",
        }
        sellers = {1: "alkesresmi", 2: "promojakarta", 3: "officialstore"}
        result = []
        for v in ["TOKOPEDIA", "SHOPEE", "BUKALAPAK", "BLIBLI", "EKATALOG"]:
            block = {"vendor": v, "items": []}
            for r in range(3):
                u = urls[v].format(
                    seller=sellers.get(r+1, "store"),
                    slug=slug, rank=r+1,
                    a=f"{300+r*10}", b=f"{900+r*100}{slug.replace('-','')[:6]}",
                )
                block["items"].append({
                    "title": f"{base_query} — Pilihan #{r+1} {v.title()}",
                    "price": prices[r],
                    "url": u,
                    "sold": sales[v][r],
                    "rating": [4.9, 4.8, 4.8][r] if v != "EKATALOG" else None,
                })
            result.append(block)
        return _samples(result)

    items.append(_mk(
        seq=3, agency="Dinas Lingkungan Hidup DKI Jakarta",
        program="Pengelolaan Sampah Perkotaan",
        description="Tempat sampah plastik 240L roda, HDPE",
        unit="unit", qty=850, unit_price=1_250_000,
        marketplace_median=1_180_000, confidence=0.84,
        samples=_generate_samples("Tempat Sampah Roda 240L HDPE", 1_180_000, "tempat-sampah-240l-hdpe"),
    ))
    items.append(_mk(
        seq=4, agency="Dinas Bina Marga DKI Jakarta",
        program="Pembangunan dan Peningkatan Jalan",
        description="Pembangunan flyover Kawasan Kelapa Gading — Pulomas, panjang 1.2 km",
        unit="paket", qty=1, unit_price=285_000_000_000,
        project=True, status=Status.TENDERED,
    ))
    items.append(_mk(
        seq=5, agency="Sekretariat Daerah DKI Jakarta",
        program="Pengadaan Perlengkapan Kantor",
        description="Kursi kerja ergonomis dengan sandaran mesh",
        unit="unit", qty=450, unit_price=4_200_000,
        marketplace_median=1_850_000, confidence=0.78,
        samples=_generate_samples("Kursi Kantor Ergonomis Mesh High Back", 1_850_000, "kursi-ergonomis-mesh"),
    ))
    items.append(_mk(
        seq=6, agency="Dinas Perhubungan DKI Jakarta",
        program="Penyediaan Sarana Perhubungan",
        description="CCTV outdoor IP camera 4MP dengan housing weatherproof",
        unit="unit", qty=600, unit_price=3_800_000,
        marketplace_median=2_400_000, confidence=0.88,
        samples=_generate_samples("CCTV IP Camera Outdoor 4MP Hikvision", 2_400_000, "cctv-ip-4mp-outdoor"),
    ))
    items.append(_mk(
        seq=7, agency="Dinas Pendidikan DKI Jakarta",
        program="Penyediaan Buku Pelajaran",
        description="Buku Bahasa Indonesia SMP Kelas 7 (Kurikulum Merdeka)",
        unit="buah", qty=45000, unit_price=92_000,
        marketplace_median=88_000, confidence=0.81,
        samples=_generate_samples("Buku Bahasa Indonesia SMP Kelas 7 Kurikulum Merdeka", 88_000, "buku-bi-smp-7-merdeka"),
    ))
    items.append(_mk(
        seq=8, agency="Dinas Sumber Daya Air DKI Jakarta",
        program="Pengendalian Banjir",
        description="Normalisasi Kali Ciliwung segmen Kampung Melayu — Bukit Duri",
        unit="paket", qty=1, unit_price=87_500_000_000,
        project=True,
    ))
    items.append(_mk(
        seq=9, agency="Sekretariat Daerah DKI Jakarta",
        program="Pengadaan Perlengkapan Kantor",
        description="Printer multifungsi Canon imageRUNNER 2630i, A3, mono laser",
        unit="unit", qty=75, unit_price=38_500_000,
        marketplace_median=31_200_000, confidence=0.91,
        samples=_generate_samples("Canon imageRUNNER 2630i A3 Mono Multifunction", 31_200_000, "canon-ir-2630i-a3"),
    ))
    items.append(_mk(
        seq=10, agency="Dinas Pemuda dan Olahraga DKI Jakarta",
        program="Pemeliharaan Fasilitas Olahraga",
        description="Renovasi GOR Cempaka Putih — pengerjaan lantai dan pencahayaan",
        unit="paket", qty=1, unit_price=12_400_000_000,
        project=True,
    ))
    items.append(_mk(
        seq=11, agency="Dinas Pendidikan DKI Jakarta",
        program="Penyediaan Sarana Pendidikan",
        description="Proyektor portable Epson EB-X06 3LCD 3600 lumens",
        unit="unit", qty=240, unit_price=8_900_000,
        marketplace_median=7_200_000, confidence=0.88,
        samples=_generate_samples("Epson EB-X06 Projector 3600 Lumens 3LCD", 7_200_000, "epson-eb-x06"),
    ))
    items.append(_mk(
        seq=12, agency="Dinas Pendidikan DKI Jakarta",
        program="Pemeliharaan Bangunan Sekolah",
        description="Renovasi atap SDN Cipinang Besar Selatan 03",
        unit="paket", qty=1, unit_price=1_850_000_000,
        project=True, status=Status.TENDERED,
    ))
    items.append(_mk(
        seq=13, agency="Dinas Kesehatan DKI Jakarta",
        program="Penyediaan Alat Kesehatan Puskesmas",
        description="Stethoscope Littmann Classic III, dewasa",
        unit="unit", qty=180, unit_price=2_400_000,
        marketplace_median=2_350_000, confidence=0.93,
        samples=_generate_samples("Littmann Classic III Stethoscope Dewasa", 2_350_000, "littmann-classic-iii"),
    ))
    items.append(_mk(
        seq=14, agency="Dinas Kesehatan DKI Jakarta",
        program="Pengadaan Obat dan Vaksin",
        description="Paracetamol 500mg tablet (kemasan strip @10 tablet)",
        unit="box", qty=8500, unit_price=18_500,
        marketplace_median=14_500, confidence=0.74,
        samples=_generate_samples("Paracetamol 500mg Strip 10 Tablet Generik", 14_500, "paracetamol-500mg-strip"),
    ))
    items.append(_mk(
        seq=15, agency="Sekretariat Daerah DKI Jakarta",
        program="Pengadaan Perlengkapan Kantor",
        description="Kertas HVS A4 80gsm, 500 lembar",
        unit="rim", qty=3200, unit_price=58_000,
        marketplace_median=52_000, confidence=0.95,
        samples=_generate_samples("Kertas HVS A4 80gsm 500 Lembar Sinar Dunia", 52_000, "hvs-a4-80gsm"),
    ))
    items.append(_mk(
        seq=16, agency="Dinas Lingkungan Hidup DKI Jakarta",
        program="Pengelolaan Sampah Perkotaan",
        description="Truk pengangkut sampah dump 6 m3, Mitsubishi Canter",
        unit="unit", qty=12, unit_price=485_000_000,
        marketplace_median=420_000_000, confidence=0.82,
        samples=_generate_samples("Mitsubishi Canter Dump Truck 6m3 Sampah", 420_000_000, "mitsubishi-canter-dump-6m3"),
    ))
    items.append(_mk(
        seq=17, agency="Dinas Perhubungan DKI Jakarta",
        program="Pemeliharaan Sarana Perhubungan",
        description="Rambu lalu lintas reflektif aluminium 60x60 cm",
        unit="unit", qty=2200, unit_price=520_000,
        marketplace_median=465_000, confidence=0.79,
        samples=_generate_samples("Rambu Lalu Lintas Reflektif Aluminium 60x60", 465_000, "rambu-aluminium-60x60"),
    ))
    items.append(_mk(
        seq=18, agency="Dinas Sosial DKI Jakarta",
        program="Bantuan Sosial",
        description="Beras premium 5kg untuk Bansos",
        unit="kg", qty=580_000, unit_price=18_500,
        marketplace_median=14_800, confidence=0.83,
        samples=_generate_samples("Beras Premium 5kg Pandan Wangi", 14_800, "beras-premium-5kg"),
    ))

    return items
