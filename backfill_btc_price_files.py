from __future__ import annotations

import argparse
import bisect
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from dome_api_sdk import DomeClient

from fetch_btc_15m_orderbooks import read_api_key, retry_call


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill per-slot BTC price JSON/CSV files for existing enriched market CSVs."
        )
    )
    parser.add_argument(
        "--input-dir",
        default="data/btc_15m_enriched_csv_month",
        help="Directory containing dated market CSV folders.",
    )
    parser.add_argument(
        "--key-file",
        default="key.txt",
        help="Path to key file containing raw API key or `key = ...`.",
    )
    parser.add_argument(
        "--price-page-limit",
        type=int,
        default=100,
        help="Page size for Chainlink price requests. Dome max is 100.",
    )
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=5,
        help="How many times to retry transient API failures.",
    )
    parser.add_argument(
        "--retry-base-sleep",
        type=float,
        default=1.5,
        help="Base sleep in seconds for exponential backoff retries.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing price files.",
    )
    return parser.parse_args()


def fetch_chainlink_prices_for_range(
    client: DomeClient,
    *,
    start_ms: int,
    end_ms: int,
    limit: int,
    retry_attempts: int,
    retry_base_sleep: float,
) -> list[dict[str, object]]:
    prices: list[dict[str, object]] = []
    pagination_key: str | None = None

    while True:
        params = {
            "currency": "btc/usd",
            "start_time": start_ms,
            "end_time": end_ms,
            "limit": limit,
        }
        if pagination_key:
            params["pagination_key"] = pagination_key

        response = retry_call(
            lambda: client.crypto_prices.chainlink.get_chainlink_prices(params),
            attempts=retry_attempts,
            base_sleep=retry_base_sleep,
        )

        for item in response.prices:
            prices.append(
                {
                    "symbol": item.symbol,
                    "value": item.value,
                    "timestamp": item.timestamp,
                    "timestamp_utc": datetime.fromtimestamp(
                        item.timestamp / 1000, timezone.utc
                    ).isoformat(),
                }
            )

        if not response.pagination_key:
            break
        pagination_key = response.pagination_key

    prices.sort(key=lambda row: row["timestamp"])
    return prices


def slice_prices_for_slot(
    prices: list[dict[str, object]],
    timestamps: list[int],
    *,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, object]]:
    left = bisect.bisect_left(timestamps, start_ms)
    right = bisect.bisect_right(timestamps, end_ms)
    return prices[left:right]


def save_price_files(
    *,
    base_dir: Path,
    slot_date: str,
    market_slug: str,
    prices: list[dict[str, object]],
) -> tuple[Path, Path]:
    prices_dir = base_dir / "prices" / slot_date
    prices_dir.mkdir(parents=True, exist_ok=True)
    json_path = prices_dir / f"{market_slug}__btc_price.json"
    csv_path = prices_dir / f"{market_slug}__btc_price.csv"

    json_path.write_text(json.dumps(prices, ensure_ascii=False, indent=2), encoding="utf-8")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["symbol", "value", "timestamp", "timestamp_utc"]
        )
        writer.writeheader()
        writer.writerows(prices)

    return json_path, csv_path


def discover_slots(input_dir: Path) -> dict[str, list[tuple[str, int, int]]]:
    grouped: dict[str, list[tuple[str, int, int]]] = defaultdict(list)
    for date_dir in sorted(input_dir.iterdir()):
        if not date_dir.is_dir():
            continue
        if date_dir.name == "prices":
            continue
        seen = set()
        for csv_path in sorted(date_dir.glob("*.csv")):
            market_slug = csv_path.stem.split("__")[0]
            if market_slug in seen:
                continue
            seen.add(market_slug)
            ts = int(market_slug.rsplit("-", 1)[1])
            start_ms = ts * 1000
            end_ms = start_ms + 15 * 60 * 1000
            grouped[date_dir.name].append((market_slug, start_ms, end_ms))
    return grouped


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    prices_root = input_dir / "prices"
    prices_root.mkdir(parents=True, exist_ok=True)
    api_key = read_api_key(Path(args.key_file))
    client = DomeClient({"api_key": api_key})

    grouped_slots = discover_slots(input_dir)
    print(f"[config] found {sum(len(v) for v in grouped_slots.values())} slots across {len(grouped_slots)} dates")
    print(f"[config] prices_root={prices_root}")

    total_slots = sum(len(v) for v in grouped_slots.values())
    slot_index = 0

    for slot_date in sorted(grouped_slots):
        slots = grouped_slots[slot_date]
        if not slots:
            continue
        day_dir = prices_root / slot_date
        day_dir.mkdir(parents=True, exist_ok=True)
        print(f"[prices] processing {slot_date} ({len(slots)} slots) -> {day_dir}")

        for market_slug, start_ms, end_ms in slots:
            slot_index += 1
            json_path = input_dir / "prices" / slot_date / f"{market_slug}__btc_price.json"
            csv_path = input_dir / "prices" / slot_date / f"{market_slug}__btc_price.csv"
            if not args.overwrite and json_path.exists() and csv_path.exists():
                print(f"[skip] existing price files for {market_slug}")
                continue
            print(f"[slot] {slot_index}/{total_slots} {market_slug} prices")
            prices = fetch_chainlink_prices_for_range(
                client,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=args.price_page_limit,
                retry_attempts=args.retry_attempts,
                retry_base_sleep=args.retry_base_sleep,
            )
            saved_json, saved_csv = save_price_files(
                base_dir=input_dir,
                slot_date=slot_date,
                market_slug=market_slug,
                prices=prices,
            )
            print(f"[saved] {market_slug} prices={len(prices)} -> {saved_csv}")


if __name__ == "__main__":
    main()
