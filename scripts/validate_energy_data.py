#!/usr/bin/env python3
"""Fail fast when generated JSON/Parquet energy artifacts are inconsistent."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args()

    snapshot = json.loads((args.data_dir / "official_energy_snapshot.json").read_text(encoding="utf-8"))
    report = json.loads((args.data_dir / "data_quality_report.json").read_text(encoding="utf-8"))
    frame = pd.read_parquet(args.data_dir / "official_energy_timeseries.parquet")

    required_columns = {"dataset_id", "period", "frequency", "scope", "category", "metric", "value", "unit", "value_gwh", "source_url"}
    missing = required_columns - set(frame.columns)
    if missing:
        raise SystemExit(f"Parquet columns missing: {sorted(missing)}")
    if snapshot.get("schema_version") != 1:
        raise SystemExit("Unsupported snapshot schema")
    if snapshot.get("quality", {}).get("status") != "passed" or report.get("status") != "passed":
        raise SystemExit("Data quality report did not pass")
    if len(snapshot.get("sources", [])) != 4:
        raise SystemExit("Expected four official data sources")
    if len(frame) < 3_000:
        raise SystemExit(f"Parquet row count is unexpectedly small: {len(frame)}")
    if frame["value_gwh"].isna().any() or (frame["value_gwh"] < 0).any():
        raise SystemExit("Parquet contains missing or negative normalized values")

    latest_year = str(snapshot["latest_annual_renewable"]["year"])
    parquet_offshore = frame.loc[
        (frame["dataset_id"] == "renewable_annual")
        & (frame["period"] == latest_year)
        & (frame["metric"] == "wind_offshore"),
        "value_gwh",
    ]
    if len(parquet_offshore) != 1:
        raise SystemExit("Latest offshore wind value is missing from Parquet")
    json_offshore = float(snapshot["latest_annual_renewable"]["wind_offshore"])
    if abs(float(parquet_offshore.iloc[0]) - json_offshore) > 1e-9:
        raise SystemExit("JSON and Parquet offshore wind values differ")

    annual_generation = {int(row["year"]): row for row in snapshot.get("annual_generation", [])}
    if 2025 not in annual_generation or annual_generation[2025].get("months") != 12:
        raise SystemExit("Complete 2025 national generation total is missing")
    city_rows = snapshot.get("city_electricity_sales", [])
    if len(city_rows) != 1 or city_rows[0].get("scope") != "基隆市" or city_rows[0].get("months") != 12:
        raise SystemExit("Complete Keelung city electricity sales record is missing")
    parquet_keelung = frame.loc[
        (frame["dataset_id"] == "township_sales_2025")
        & (frame["period"] == "2025")
        & (frame["metric"] == "sales_total"),
        "value_gwh",
    ]
    if len(parquet_keelung) != 7:
        raise SystemExit("Expected seven Keelung district sales rows in Parquet")
    if abs(float(parquet_keelung.sum()) - float(city_rows[0]["total"])) > 1e-6:
        raise SystemExit("JSON and Parquet Keelung electricity sales differ")

    print(
        f"Validated {len(frame):,} Parquet rows, latest month {snapshot['latest_month']['period']}, "
        f"{latest_year} offshore wind {json_offshore:,.3f} GWh, and "
        f"2025 Keelung sales {float(city_rows[0]['total']):,.3f} GWh."
    )


if __name__ == "__main__":
    main()
