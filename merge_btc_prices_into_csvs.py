from __future__ import annotations

import argparse
import bisect
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="将每个 15 分钟市场对应的 BTC 价格文件回填合并到原始订单簿 CSV 中。"
    )
    parser.add_argument(
        "--input-dir",
        default="data/btc_15m_orderbook_csv_month",
        help="包含按日期分组的订单簿 CSV 目录，以及其 prices/ 子目录。",
    )
    parser.add_argument(
        "--output-dir",
        default="data/btc_15m_orderbook_csv_month_enriched",
        help="写入带 BTC 价格列的增强版 CSV 输出目录。",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="如果增强版 CSV 已存在则覆盖。",
    )
    return parser.parse_args()


def load_price_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows.sort(key=lambda row: int(row["timestamp"]))
    return rows


def align_price(
    snapshot_ts: int,
    price_timestamps: list[int],
    price_rows: list[dict[str, str]],
) -> dict[str, str]:
    if not price_rows:
        return {
            "btc_price": "",
            "btc_price_timestamp": "",
            "btc_price_timestamp_utc": "",
            "btc_price_match": "missing",
        }

    index = bisect.bisect_right(price_timestamps, snapshot_ts) - 1
    match_type = "prev_or_exact"
    if index < 0:
        index = 0
        match_type = "next_fallback"
    price_row = price_rows[index]
    return {
        "btc_price": price_row["value"],
        "btc_price_timestamp": price_row["timestamp"],
        "btc_price_timestamp_utc": price_row["timestamp_utc"],
        "btc_price_match": match_type,
    }


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for date_dir in sorted(input_dir.iterdir()):
        if not date_dir.is_dir() or date_dir.name == "prices":
            continue
        prices_dir = input_dir / "prices" / date_dir.name
        if not prices_dir.exists():
            print(f"[skip-date] no prices folder for {date_dir.name}")
            continue

        for csv_path in sorted(date_dir.glob("*.csv")):
            market_slug = csv_path.stem.split("__")[0]
            price_csv = prices_dir / f"{market_slug}__btc_price.csv"
            if not price_csv.exists():
                print(f"[skip] missing price file for {market_slug}")
                continue

            out_dir = output_dir / date_dir.name
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / csv_path.name
            if out_path.exists() and not args.overwrite:
                print(f"[skip] existing {out_path}")
                continue

            price_rows = load_price_rows(price_csv)
            price_timestamps = [int(row["timestamp"]) for row in price_rows]

            with csv_path.open("r", encoding="utf-8", newline="") as source:
                reader = csv.DictReader(source)
                fieldnames = list(reader.fieldnames or []) + [
                    "btc_price",
                    "btc_price_timestamp",
                    "btc_price_timestamp_utc",
                    "btc_price_match",
                ]
                with out_path.open("w", encoding="utf-8", newline="") as target:
                    writer = csv.DictWriter(target, fieldnames=fieldnames)
                    writer.writeheader()
                    row_count = 0
                    for row in reader:
                        info = align_price(int(row["timestamp_ms"]), price_timestamps, price_rows)
                        row.update(info)
                        writer.writerow(row)
                        row_count += 1

            print(f"[saved] {out_path} rows={row_count}")


if __name__ == "__main__":
    main()
