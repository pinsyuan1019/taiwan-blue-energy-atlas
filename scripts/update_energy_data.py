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

KEELUNG_POSTAL_CODES = {str(code) for code in range(200, 207)}

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


def normalize_keelung_sales(rows: list[dict[str, str]]) -> tuple[dict, list[dict]]:
    columns = ["年度", "月份", "郵遞區號", "行政區", "項目", "售電度數(度)"]
    require_columns(rows, columns, "township_sales_2025")
    district_kwh: dict[str, float] = {}
    months: set[str] = set()
    selected_rows = 0
    for row in rows:
        postal_code = str(row["郵遞區號"]).strip().zfill(3)
        if postal_code not in KEELUNG_POSTAL_CODES or str(row["項目"]).strip() != "26總計（含臨時用電）":
            continue
        year = str(row["年度"]).strip()
        month = str(row["月份"]).strip().zfill(2)
        if year != "114" or not month.isdigit():
            continue
        district = str(row["行政區"]).strip()
        district_kwh[district] = district_kwh.get(district, 0.0) + number(row, "售電度數(度)")
        months.add(month)
        selected_rows += 1

    districts = [
        {"name": name, "sales_gwh": round(value / 1_000_000, 6)}
        for name, value in sorted(district_kwh.items())
    ]
    total_gwh = round(sum(item["sales_gwh"] for item in districts), 6)
    normalized = [
        {
            "dataset_id": "township_sales_2025",
            "period": "2025",
            "frequency": "year",
            "scope": f"基隆市{item['name']}",
            "category": "electricity_sales",
            "metric": "sales_total",
            "value": item["sales_gwh"],
            "unit": "GWh",
            "value_gwh": item["sales_gwh"],
            "source_url": SOURCES["township_sales_2025"]["dataset_url"],
        }
        for item in districts
    ]
    return {
        "year": 2025,
        "scope": "基隆市",
        "unit": "GWh",
        "total": total_gwh,
        "districts": districts,
        "months": len(months),
        "selected_rows": selected_rows,
        "definition": "七個行政區每月『總計（含臨時用電）』售電度數加總",
    }, normalized


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


def validate_source_data(monthly: list[dict], annual: list[dict], station: dict, city_sales: dict) -> list[dict]:
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
    check("keelung_full_year", city_sales["months"] == 12, f"{city_sales['months']} months")
    check("keelung_district_count", len(city_sales["districts"]) == 7, f"{len(city_sales['districts'])} districts")
    check("keelung_sales_positive", city_sales["total"] > 0, f"{city_sales['total']:.6f} GWh")
    check("keelung_selected_rows", city_sales["selected_rows"] == 84, f"{city_sales['selected_rows']} monthly district rows")
    return checks


def write_outputs(output_dir: Path, payloads: dict[str, bytes]) -> None:
    parsed = {source_id: parse_csv(payload) for source_id, payload in payloads.items()}
    monthly, monthly_normalized = normalize_monthly(parsed["generation_monthly"])
    annual, annual_normalized = normalize_annual(parsed["renewable_annual"])
    station, station_normalized = normalize_taipower(parsed["taipower_wind_solar"])
    city_sales, city_sales_normalized = normalize_keelung_sales(parsed["township_sales_2025"])
    annual_generation = build_annual_generation(monthly)
    checks = validate_source_data(monthly, annual, station, city_sales)
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    source_manifest = []
    latest_records = {
        "generation_monthly": monthly[-1]["period"],
        "renewable_annual": str(annual[-1]["year"]),
        "taipower_wind_solar": station["period"],
        "township_sales_2025": str(city_sales["year"]),
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
        "city_electricity_sales": [city_sales],
        "monthly_generation": monthly[-36:],
        "annual_renewable": annual,
        "sources": source_manifest,
        "quality": {"status": "passed", "checks": checks},
    }

    normalized_rows = monthly_normalized + annual_normalized + station_normalized + city_sales_normalized
    dataframe = pd.DataFrame(normalized_rows)
    output_dir.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(output_dir / "official_energy_timeseries.parquet", index=False, compression="zstd")
    (output_dir / "official_energy_snapshot.json").write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output_dir / "source_manifest.json").write_text(json.dumps({"generated_at": generated_at, "sources": source_manifest}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output_dir / "data_quality_report.json").write_text(json.dumps({"generated_at": generated_at, "status": "passed", "checks": checks, "parquet_rows": len(dataframe)}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        f"Wrote {len(dataframe):,} normalized rows; latest month {monthly[-1]['period']}; "
        f"latest renewable year {annual[-1]['year']}; Keelung {city_sales['year']} sales {city_sales['total']:.3f} GWh."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, help="Use previously downloaded CSV files instead of the network.")
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    write_outputs(args.output_dir, load_payloads(args.input_dir))


if __name__ == "__main__":
    main()
