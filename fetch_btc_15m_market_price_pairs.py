from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from dome_api_sdk import DomeClient

from fetch_btc_15m_orderbooks import build_slots, read_api_key, resolve_markets, retry_call


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch BTC 15-minute Polymarket market metadata and the exactly matching "
            "Chainlink BTC/USD price window for each slot."
        )
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Trailing days of completed 15-minute slots to fetch. Default: 30",
    )
    parser.add_argument(
        "--max-slots",
        type=int,
        default=None,
        help="Optional cap on the most recent N slots, useful for validation runs.",
    )
    parser.add_argument(
        "--currency",
        default="btc/usd",
        help="Chainlink currency pair. Default: btc/usd",
    )
    parser.add_argument(
        "--key-file",
        default="key.txt",
        help="Path to key file containing raw API key or `key = ...`.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/btc_15m_market_price_pairs",
        help="Directory for aligned output files.",
    )
    parser.add_argument(
        "--price-limit",
        type=int,
        default=100,
        help="Page size for Chainlink price requests. Dome max is 100.",
    )
    parser.add_argument(
        "--market-batch-size",
        type=int,
        default=100,
        help="How many market slugs to resolve in each request.",
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
        "--save-price-files",
        action="store_true",
        help="Write one JSON and one CSV file for each slot's Chainlink price window.",
    )
    return parser.parse_args()


def fetch_chainlink_prices_for_slot(
    client: DomeClient,
    *,
    currency: str,
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
            "currency": currency,
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


def save_slot_price_files(
    *,
    output_dir: Path,
    market_slug: str,
    slot_date: str,
    prices: list[dict[str, object]],
) -> tuple[str, str]:
    slot_dir = output_dir / "prices" / slot_date
    slot_dir.mkdir(parents=True, exist_ok=True)
    json_path = slot_dir / f"{market_slug}__chainlink.json"
    csv_path = slot_dir / f"{market_slug}__chainlink.csv"

    json_path.write_text(json.dumps(prices, ensure_ascii=False, indent=2), encoding="utf-8")

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["symbol", "value", "timestamp", "timestamp_utc"]
        )
        writer.writeheader()
        writer.writerows(prices)

    return str(json_path.resolve()), str(csv_path.resolve())


def append_jsonl(path: Path, record: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    args = parse_args()
    api_key = read_api_key(Path(args.key_file))
    client = DomeClient({"api_key": api_key})

    slots = build_slots(args.days, args.max_slots)
    if not slots:
        raise ValueError("No slots generated.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_jsonl = output_dir / "manifest.jsonl"
    manifest_csv = output_dir / "manifest.csv"

    print(
        f"[config] slots={len(slots)} range={slots[0].start.isoformat()} -> {slots[-1].end.isoformat()}"
    )
    print(f"[config] output_dir={output_dir.resolve()}")

    markets_by_slug = resolve_markets(
        client,
        slots,
        batch_size=args.market_batch_size,
        retry_attempts=args.retry_attempts,
        retry_base_sleep=args.retry_base_sleep,
    )

    csv_rows: list[dict[str, object]] = []
    for index, slot in enumerate(slots, start=1):
        market = markets_by_slug.get(slot.slug)
        slot_start_ms = int(slot.start.timestamp() * 1000)
        slot_end_ms = int(slot.end.timestamp() * 1000)

        print(f"[slot] {index}/{len(slots)} {slot.slug}")
        prices = fetch_chainlink_prices_for_slot(
            client,
            currency=args.currency,
            start_ms=slot_start_ms,
            end_ms=slot_end_ms,
            limit=args.price_limit,
            retry_attempts=args.retry_attempts,
            retry_base_sleep=args.retry_base_sleep,
        )

        json_path = ""
        csv_path = ""
        if args.save_price_files:
            json_path, csv_path = save_slot_price_files(
                output_dir=output_dir,
                market_slug=slot.slug,
                slot_date=slot.start.strftime("%Y-%m-%d"),
                prices=prices,
            )

        first_price = prices[0]["value"] if prices else None
        last_price = prices[-1]["value"] if prices else None
        first_ts = prices[0]["timestamp_utc"] if prices else None
        last_ts = prices[-1]["timestamp_utc"] if prices else None

        record = {
            "market_slug": slot.slug,
            "slot_start_utc": slot.start.isoformat(),
            "slot_end_utc": slot.end.isoformat(),
            "market_found": market is not None,
            "market_title": market.title if market else "",
            "market_status": market.status if market else "",
            "up_token_id": market.side_a.id if market else "",
            "down_token_id": market.side_b.id if market else "",
            "price_count": len(prices),
            "first_price": first_price,
            "last_price": last_price,
            "first_price_ts_utc": first_ts,
            "last_price_ts_utc": last_ts,
            "price_json_path": json_path,
            "price_csv_path": csv_path,
        }
        append_jsonl(manifest_jsonl, record)
        csv_rows.append(record)
        print(
            f"[saved] market_found={record['market_found']} price_count={record['price_count']} "
            f"first={record['first_price']} last={record['last_price']}"
        )

    with manifest_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "market_slug",
                "slot_start_utc",
                "slot_end_utc",
                "market_found",
                "market_title",
                "market_status",
                "up_token_id",
                "down_token_id",
                "price_count",
                "first_price",
                "last_price",
                "first_price_ts_utc",
                "last_price_ts_utc",
                "price_json_path",
                "price_csv_path",
            ],
        )
        writer.writeheader()
        writer.writerows(csv_rows)

    print(f"[done] manifest_jsonl={manifest_jsonl.resolve()}")
    print(f"[done] manifest_csv={manifest_csv.resolve()}")


if __name__ == "__main__":
    main()
