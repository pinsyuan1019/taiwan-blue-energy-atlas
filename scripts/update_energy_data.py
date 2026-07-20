#!/usr/bin/env python3
"""Download, normalize, validate, and publish Taiwan official energy data.

The browser reads the compact JSON snapshot.  The Parquet file retains a
normalized long-form history for reproducible analysis and future retrieval.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests


SOURCES = {
    "generation_monthly": {
        "title": "經濟部能源署－發電量月資料",
        "dataset_url": "https://data.gov.tw/dataset/112650",
        "download_url": "https://www.moeaea.gov.tw/ECW/populace/opendata/wHandOpenData_File.ashx?set_id=171",
        "filename": "generation-monthly.csv",
        "frequency": "每月",
    },
    "renewable_annual": {
        "title": "經濟部能源署－再生能源發電量年資料",
        "dataset_url": "https://data.gov.tw/en/datasets/163720",
        "download_url": "https://www.moeaea.gov.tw/ECW/populace/opendata/wHandOpenData_File.ashx?set_id=248",
        "filename": "renewable-annual.csv",
        "frequency": "每年",
    },
    "taipower_wind_solar": {
        "title": "台灣電力公司－自建風力與太陽光電發電量",
        "dataset_url": "https://data.gov.tw/en/datasets/17140",
        "download_url": "https://service.taipower.com.tw/data/opendata/apply/file/d693001/001.csv",
        "filename": "taipower-wind-solar.csv",
        "frequency": "每月",
    },
    "township_sales_2025": {
        "title": "台灣電力公司－114 年鄉鎮市（郵遞區）別用電統計",
        "dataset_url": "https://data.gov.tw/dataset/14135",
        "download_url": "https://service.taipower.com.tw/data/opendata/apply/file/d007025/dist_kwh_114.csv",
        "filename": "township-sales-2025.csv",
        "frequency": "每年",
    },
}

COUNTY_ORDER = [
    "臺北市", "基隆市", "新北市", "連江縣", "宜蘭縣", "新竹市", "新竹縣", "桃園市",
    "苗栗縣", "臺中市", "彰化縣", "南投縣", "嘉義市", "嘉義縣", "雲林縣", "臺南市",
    "高雄市", "澎湖縣", "金門縣", "屏東縣", "臺東縣", "花蓮縣",
]

COUNTY_POSTAL_RANGES = [
    (100, 116, "臺北市"), (200, 206, "基隆市"), (207, 208, "新北市"),
    (209, 212, "連江縣"), (220, 253, "新北市"), (260, 272, "宜蘭縣"),
    (300, 300, "新竹市"), (302, 315, "新竹縣"), (320, 338, "桃園市"),
    (350, 369, "苗栗縣"), (400, 439, "臺中市"), (500, 530, "彰化縣"),
    (540, 558, "南投縣"), (600, 600, "嘉義市"), (602, 625, "嘉義縣"),
    (630, 655, "雲林縣"), (700, 745, "臺南市"), (800, 852, "高雄市"),
    (880, 885, "澎湖縣"), (890, 896, "金門縣"), (900, 947, "屏東縣"),
    (950, 966, "臺東縣"), (970, 983, "花蓮縣"),
]

MONTHLY_METRICS = {
    "total": "全國發電量_總計(數值)",
    "pumped_hydro": "全國發電量_抽蓄水力(數值)",
    "thermal": "全國發電量_火力_合計(數值)",
    "coal": "全國發電量_火力_燃煤(數值)",
    "oil": "全國發電量_火力_燃油(數值)",
    "gas": "全國發電量_火力_燃氣(數值)",
    "nuclear": "全國發電量_核能(數值)",
    "renewable_total": "全國發電量_再生能源_合計(數值)",
    "conventional_hydro": "全國發電量_再生能源_慣常水力(數值)",
    "geothermal": "全國發電量_再生能源_地熱(數值)",
    "solar": "全國發電量_再生能源_太陽光電(數值)",
    "wind_total": "全國發電量_再生能源_風力(數值)",
    "biomass": "全國發電量_再生能源_生質能(數值)",
    "waste": "全國發電量_再生能源_廢棄物(數值)",
}

ANNUAL_METRICS = {
    "renewable_total": "再生能源發電量合計(統計數值)",
    "conventional_hydro": "慣常水力(統計數值)",
    "geothermal": "地熱(統計數值)",
    "solar": "太陽光電(統計數值)",
    "wind_total": "風力_小計(統計數值)",
    "wind_onshore": "風力_陸域(統計數值)",
    "wind_offshore": "風力_離岸(統計數值)",
    "biomass": "生質能_小計(統計數值)",
    "waste": "廢棄物(統計數值)",
}


def download(url: str, attempts: int = 3) -> bytes:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.get(
                url,
                timeout=60,
                headers={"User-Agent": "TaiwanBlueEnergyAtlas/1.0 (+GitHub Pages data update)"},
            )
            response.raise_for_status()
            payload = response.content
            if len(payload) < 100:
                raise ValueError(f"download was unexpectedly small: {len(payload)} bytes")
            return payload
        except Exception as exc:  # pragma: no cover - network retry path
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(2**attempt)
    raise RuntimeError(f"failed to download {url}: {last_error}")


def load_payloads(input_dir: Path | None) -> dict[str, bytes]:
    payloads: dict[str, bytes] = {}
    for source_id, source in SOURCES.items():
        if input_dir:
            payloads[source_id] = (input_dir / source["filename"]).read_bytes()
        else:
            payloads[source_id] = download(source["download_url"])
    return payloads


def parse_csv(payload: bytes) -> list[dict[str, str]]:
    text = payload.decode("utf-8-sig").replace("\x00", "")
    return list(csv.DictReader(io.StringIO(text)))


def number(row: dict[str, str], column: str) -> float:
    raw = str(row.get(column, "") or "").replace(",", "").strip()
    if raw in {"", "-", "--"}:
        return 0.0
    match = re.fullmatch(r"([+-]?\d+(?:\.\d+)?)(?:\s+\d+)?", raw)
    if not match:
        raise ValueError(f"unrecognized numeric value in {column}: {raw!r}")
    return float(match.group(1))


def require_columns(rows: list[dict[str, str]], columns: list[str], source_id: str) -> None:
    if not rows:
        raise ValueError(f"{source_id}: no records")
    missing = [column for column in columns if column not in rows[0]]
    if missing:
        raise ValueError(f"{source_id}: missing columns {missing}")


def normalize_monthly(rows: list[dict[str, str]]) -> tuple[list[dict], list[dict]]:
    require_columns(rows, ["日期(年/月)", *MONTHLY_METRICS.values()], "generation_monthly")
    summaries: list[dict] = []
    normalized: list[dict] = []
    for row in rows:
        raw_period = str(row["日期(年/月)"]).strip()
        if len(raw_period) != 6 or not raw_period.isdigit():
            continue
        period = f"{raw_period[:4]}-{raw_period[4:]}"
        metrics = {metric: number(row, column) for metric, column in MONTHLY_METRICS.items()}
        summaries.append({"period": period, "unit": "GWh", **metrics})
        for metric, value in metrics.items():
            normalized.append(
                {
                    "dataset_id": "generation_monthly",
                    "period": period,
                    "frequency": "month",
                    "scope": "Taiwan",
                    "category": "generation",
                    "metric": metric,
                    "value": value,
                    "unit": "GWh",
                    "value_gwh": value,
                    "source_url": SOURCES["generation_monthly"]["dataset_url"],
                }
            )
    summaries.sort(key=lambda item: item["period"])
    return summaries, normalized


def normalize_annual(rows: list[dict[str, str]]) -> tuple[list[dict], list[dict]]:
    require_columns(rows, ["西元年", *ANNUAL_METRICS.values()], "renewable_annual")
    summaries: list[dict] = []
    normalized: list[dict] = []
    for row in rows:
        year = str(row["西元年"]).strip()
        if len(year) != 4 or not year.isdigit():
            continue
        metrics = {metric: number(row, column) for metric, column in ANNUAL_METRICS.items()}
        summaries.append({"year": int(year), "unit": "GWh", **metrics})
        for metric, value in metrics.items():
            normalized.append(
                {
                    "dataset_id": "renewable_annual",
                    "period": year,
                    "frequency": "year",
                    "scope": "Taiwan",
                    "category": "renewable_generation",
                    "metric": metric,
                    "value": value,
                    "unit": "GWh",
                    "value_gwh": value,
                    "source_url": SOURCES["renewable_annual"]["dataset_url"],
                }
            )
    summaries.sort(key=lambda item: item["year"])
    return summaries, normalized


def normalize_taipower(rows: list[dict[str, str]]) -> tuple[dict, list[dict]]:
    columns = [
        "年度/Year",
        "月份/Month",
        "能源別/Energy Type",
        "發電站名稱/Station Name",
        "發電量(度)/Power Generation(kWh)",
    ]
    require_columns(rows, columns, "taipower_wind_solar")
    normalized: list[dict] = []
    latest_period = ""
    latest_rows: list[dict] = []
    for row in rows:
        year = str(row[columns[0]]).strip()
        month = str(row[columns[1]]).strip().zfill(2)
        if len(year) != 4 or not year.isdigit() or not month.isdigit():
            continue
        period = f"{year}-{month}"
        value_kwh = number(row, columns[4])
        item = {
            "dataset_id": "taipower_wind_solar",
            "period": period,
            "frequency": "month",
            "scope": str(row[columns[3]]).strip(),
            "category": str(row[columns[2]]).strip(),
            "metric": "station_generation",
            "value": value_kwh,
            "unit": "kWh",
            "value_gwh": value_kwh / 1_000_000,
            "source_url": SOURCES["taipower_wind_solar"]["dataset_url"],
        }
        normalized.append(item)
        if period > latest_period:
            latest_period = period
            latest_rows = [item]
        elif period == latest_period:
            latest_rows.append(item)
    totals: dict[str, float] = {}
    for row in latest_rows:
        totals[row["category"]] = totals.get(row["category"], 0.0) + row["value_gwh"]
    return {
        "period": latest_period,
        "unit": "GWh",
        "station_count": len(latest_rows),
        "generation_by_energy_type": {key: round(value, 6) for key, value in sorted(totals.items())},
    }, normalized


def county_for_postal(postal_code: str) -> str | None:
    if not postal_code.isdigit():
        return None
    code = int(postal_code)
    for start, end, county in COUNTY_POSTAL_RANGES:
        if start <= code <= end:
            return county
    return None


def normalize_county_sales(rows: list[dict[str, str]]) -> tuple[list[dict], list[dict], dict]:
    columns = ["年度", "月份", "郵遞區號", "行政區", "項目", "售電度數(度)"]
    require_columns(rows, columns, "township_sales_2025")
    county_district_kwh: dict[str, dict[str, float]] = {county: {} for county in COUNTY_ORDER}
    county_months: dict[str, set[str]] = {county: set() for county in COUNTY_ORDER}
    selected_rows = 0
    unmapped_nonzero: list[str] = []
    for row in rows:
        postal_code = str(row["郵遞區號"]).strip().zfill(3)
        if str(row["項目"]).strip() != "26總計（含臨時用電）":
            continue
        year = str(row["年度"]).strip()
        month = str(row["月份"]).strip().zfill(2)
        if year != "114" or not month.isdigit():
            continue
        value_kwh = number(row, "售電度數(度)")
        county = county_for_postal(postal_code)
        if not county:
            if value_kwh:
                unmapped_nonzero.append(f"{postal_code} {row['行政區']} {value_kwh}")
            continue
        district = str(row["行政區"]).strip()
        district_key = f"{postal_code}:{district}"
        county_district_kwh[county][district_key] = county_district_kwh[county].get(district_key, 0.0) + value_kwh
        county_months[county].add(month)
        selected_rows += 1

    if unmapped_nonzero:
        raise ValueError(f"township_sales_2025: unmapped non-zero postal rows: {unmapped_nonzero[:5]}")

    county_records: list[dict] = []
    normalized: list[dict] = []
    for county in COUNTY_ORDER:
        districts = [
            {"postal_code": key.split(":", 1)[0], "name": key.split(":", 1)[1], "sales_gwh": round(value / 1_000_000, 6)}
            for key, value in sorted(county_district_kwh[county].items())
        ]
        total_gwh = round(sum(item["sales_gwh"] for item in districts), 6)
        record = {
            "year": 2025,
            "scope": county,
            "unit": "GWh",
            "total": total_gwh,
            "district_count": len(districts),
            "districts": districts,
            "months": len(county_months[county]),
            "definition": "各行政區每月『總計（含臨時用電）』售電度數加總",
        }
        county_records.append(record)
        normalized.append({
            "dataset_id": "township_sales_2025",
            "period": "2025",
            "frequency": "year",
            "scope": county,
            "category": "electricity_sales",
            "metric": "sales_total",
            "value": total_gwh,
            "unit": "GWh",
            "value_gwh": total_gwh,
            "source_url": SOURCES["township_sales_2025"]["dataset_url"],
        })
    diagnostics = {
        "selected_rows": selected_rows,
        "district_count": sum(item["district_count"] for item in county_records),
        "county_count": len(county_records),
        "total_gwh": round(sum(item["total"] for item in county_records), 6),
    }
    return county_records, normalized, diagnostics


def build_annual_generation(monthly: list[dict]) -> list[dict]:
    by_year: dict[int, list[dict]] = {}
    for row in monthly:
        year = int(row["period"][:4])
        by_year.setdefault(year, []).append(row)
    return [
        {
            "year": year,
            "unit": "GWh",
            "total": round(sum(float(row["total"]) for row in rows), 6),
            "months": 12,
        }
        for year, rows in sorted(by_year.items())
        if len(rows) == 12
    ]


def month_age(period: str) -> int:
    year, month = (int(part) for part in period.split("-"))
    now = datetime.now(timezone.utc)
    return (now.year - year) * 12 + now.month - month


def validate_source_data(monthly: list[dict], annual: list[dict], station: dict, county_sales: list[dict], county_diagnostics: dict) -> list[dict]:
    checks: list[dict] = []

    def check(name: str, condition: bool, detail: str) -> None:
        checks.append({"name": name, "status": "passed" if condition else "failed", "detail": detail})
        if not condition:
            raise ValueError(f"quality check failed: {name}: {detail}")

    latest_month = monthly[-1]
    latest_year = annual[-1]
    generation_sum = latest_month["pumped_hydro"] + latest_month["thermal"] + latest_month["nuclear"] + latest_month["renewable_total"]
    renewable_sum = sum(latest_month[key] for key in ("conventional_hydro", "geothermal", "solar", "wind_total", "biomass", "waste"))
    annual_sum = sum(latest_year[key] for key in ("conventional_hydro", "geothermal", "solar", "wind_total", "biomass", "waste"))
    check("monthly_record_count", len(monthly) >= 180, f"{len(monthly)} monthly rows")
    check("annual_record_count", len(annual) >= 18, f"{len(annual)} annual rows")
    check("monthly_freshness", month_age(latest_month["period"]) <= 6, f"latest {latest_month['period']}")
    check("monthly_total_reconciles", abs(latest_month["total"] - generation_sum) < 0.05, f"difference {latest_month['total'] - generation_sum:.6f} GWh")
    check("monthly_renewables_reconcile", abs(latest_month["renewable_total"] - renewable_sum) < 0.05, f"difference {latest_month['renewable_total'] - renewable_sum:.6f} GWh")
    check("annual_renewables_reconcile", abs(latest_year["renewable_total"] - annual_sum) < 0.05, f"difference {latest_year['renewable_total'] - annual_sum:.6f} GWh")
    check("offshore_within_wind_total", 0 <= latest_year["wind_offshore"] <= latest_year["wind_total"], f"offshore {latest_year['wind_offshore']:.3f} / total {latest_year['wind_total']:.3f} GWh")
    check("taipower_latest_period", bool(station["period"]), f"latest {station['period']}")
    check("county_count", len(county_sales) == 22, f"{len(county_sales)} counties and cities")
    check("county_full_year", all(item["months"] == 12 for item in county_sales), "all county records contain 12 months")
    check("county_district_count", county_diagnostics["district_count"] == 368, f"{county_diagnostics['district_count']} districts")
    check("county_sales_positive", all(item["total"] > 0 for item in county_sales), f"national county sales sum {county_diagnostics['total_gwh']:.6f} GWh")
    check("county_selected_rows", county_diagnostics["selected_rows"] == 4_416, f"{county_diagnostics['selected_rows']} monthly district rows")
    return checks


def sql_value(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def build_d1_seed(monthly: list[dict], annual: list[dict], annual_generation: list[dict], county_sales: list[dict]) -> tuple[str, int]:
    records: list[tuple] = []

    def add(record_id: str, period: str, year: int, month: int | None, area: str, area_level: str,
            metric: str, energy_type: str, value: float, source_id: str, notes: str) -> None:
        source = SOURCES[source_id]
        records.append((record_id, period, year, month, area, area_level, metric, energy_type,
                        round(float(value), 6), "GWh", source_id, source["title"], source["dataset_url"], notes))

    for row in annual_generation:
        add(f"national-generation-{row['year']}", str(row["year"]), row["year"], None, "全臺", "national",
            "generation_total", "all", row["total"], "generation_monthly", "12 個月全國發電量加總")
    for row in county_sales:
        add(f"county-sales-{row['year']}-{COUNTY_ORDER.index(row['scope']) + 1:02}", str(row["year"]), row["year"], None,
            row["scope"], "county", "electricity_sales", "all", row["total"], "township_sales_2025", row["definition"])
    for row in annual:
        for energy_type in ANNUAL_METRICS:
            add(f"national-renewable-{row['year']}-{energy_type}", str(row["year"]), row["year"], None, "全臺", "national",
                "generation", energy_type, row[energy_type], "renewable_annual", "再生能源年度發電量")
    for row in monthly[-36:]:
        year, month = (int(part) for part in row["period"].split("-"))
        for energy_type in MONTHLY_METRICS:
            add(f"national-monthly-{row['period']}-{energy_type}", row["period"], year, month, "全臺", "national",
                "generation", energy_type, row[energy_type], "generation_monthly", "全國月發電量")

    columns = "record_id,period,year,month,area,area_level,metric,energy_type,value,unit,source_id,source_title,source_url,notes"
    statements = ["DELETE FROM energy_records;"]
    statements.extend(f"INSERT INTO energy_records ({columns}) VALUES ({','.join(sql_value(value) for value in record)});" for record in records)
    return "\n".join(statements) + "\n", len(records)


def write_outputs(output_dir: Path, payloads: dict[str, bytes]) -> None:
    parsed = {source_id: parse_csv(payload) for source_id, payload in payloads.items()}
    monthly, monthly_normalized = normalize_monthly(parsed["generation_monthly"])
    annual, annual_normalized = normalize_annual(parsed["renewable_annual"])
    station, station_normalized = normalize_taipower(parsed["taipower_wind_solar"])
    county_sales, county_sales_normalized, county_diagnostics = normalize_county_sales(parsed["township_sales_2025"])
    annual_generation = build_annual_generation(monthly)
    checks = validate_source_data(monthly, annual, station, county_sales, county_diagnostics)
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    source_manifest = []
    latest_records = {
        "generation_monthly": monthly[-1]["period"],
        "renewable_annual": str(annual[-1]["year"]),
        "taipower_wind_solar": station["period"],
        "township_sales_2025": str(county_sales[0]["year"]),
    }
    for source_id, source in SOURCES.items():
        source_manifest.append(
            {
                "id": source_id,
                "title": source["title"],
                "dataset_url": source["dataset_url"],
                "download_url": source["download_url"],
                "frequency": source["frequency"],
                "license": "政府資料開放授權條款－第1版",
                "sha256": hashlib.sha256(payloads[source_id]).hexdigest(),
                "bytes": len(payloads[source_id]),
                "rows": len(parsed[source_id]),
                "latest_record": latest_records[source_id],
            }
        )

    snapshot = {
        "schema_version": 1,
        "generated_at": generated_at,
        "display_note": "數據由官方 CSV 自動整理；空間圖示仍為議題導覽，不代表工程邊界。",
        "latest_month": monthly[-1],
        "latest_annual_renewable": annual[-1],
        "taipower_latest": station,
        "annual_generation": annual_generation,
        "city_electricity_sales": county_sales,
        "monthly_generation": monthly[-36:],
        "annual_renewable": annual,
        "sources": source_manifest,
        "quality": {"status": "passed", "checks": checks},
    }

    normalized_rows = monthly_normalized + annual_normalized + station_normalized + county_sales_normalized
    dataframe = pd.DataFrame(normalized_rows)
    d1_seed, d1_record_count = build_d1_seed(monthly, annual, annual_generation, county_sales)
    output_dir.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(output_dir / "official_energy_timeseries.parquet", index=False, compression="zstd")
    (output_dir / "official_energy_snapshot.json").write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output_dir / "source_manifest.json").write_text(json.dumps({"generated_at": generated_at, "sources": source_manifest}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output_dir / "data_quality_report.json").write_text(json.dumps({"generated_at": generated_at, "status": "passed", "checks": checks, "parquet_rows": len(dataframe), "d1_records": d1_record_count}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output_dir / "d1_energy_seed.sql").write_text(d1_seed, encoding="utf-8")
    print(
        f"Wrote {len(dataframe):,} normalized rows; latest month {monthly[-1]['period']}; "
        f"latest renewable year {annual[-1]['year']}; {len(county_sales)} county records; {d1_record_count} D1 records."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, help="Use previously downloaded CSV files instead of the network.")
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    write_outputs(args.output_dir, load_payloads(args.input_dir))


if __name__ == "__main__":
    main()
