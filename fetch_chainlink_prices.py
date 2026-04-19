from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from dome_api_sdk import DomeClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Chainlink crypto price history from Dome API and save JSON/CSV."
    )
    parser.add_argument(
        "--currency",
        default="btc/usd",
        help="Chainlink currency pair, e.g. btc/usd",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start time in ISO format, e.g. 2026-04-15T14:15:00+00:00",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End time in ISO format, e.g. 2026-04-15T14:30:00+00:00",
    )
    parser.add_argument(
        "--key-file",
        default="key.txt",
        help="Path to key file containing raw API key or `key = ...`.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/chainlink_prices",
        help="Directory for JSON/CSV output.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Page size. Dome max is 100.",
    )
    return parser.parse_args()


def read_api_key(path: Path) -> str:
    raw = path.read_text(encoding="utf-8").strip()
    if "=" in raw:
        _, raw = raw.split("=", 1)
    api_key = raw.strip().strip('"').strip("'")
    if not api_key:
        raise ValueError(f"No API key found in {path}")
    return api_key


def parse_iso_datetime(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError(f"Timezone is required in datetime: {value}")
    return dt.astimezone(timezone.utc)


def main() -> None:
    args = parse_args()
    start = parse_iso_datetime(args.start)
    end = parse_iso_datetime(args.end)
    if end <= start:
        raise ValueError("--end must be later than --start")

    api_key = read_api_key(Path(args.key_file))
    client = DomeClient({"api_key": api_key})

    all_prices: list[dict[str, object]] = []
    pagination_key: str | None = None
    page = 0

    while True:
        params = {
            "currency": args.currency,
            "start_time": int(start.timestamp() * 1000),
            "end_time": int(end.timestamp() * 1000),
            "limit": args.limit,
        }
        if pagination_key:
            params["pagination_key"] = pagination_key

        response = client.crypto_prices.chainlink.get_chainlink_prices(params)
        page += 1
        print(
            f"[page {page}] returned={len(response.prices)} has_next={response.pagination_key is not None}"
        )

        for item in response.prices:
            all_prices.append(
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

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    safe_currency = args.currency.replace("/", "_")
    start_tag = start.strftime("%Y-%m-%dT%H-%M-%SZ")
    end_tag = end.strftime("%Y-%m-%dT%H-%M-%SZ")
    base_name = f"{safe_currency}_{start_tag}_{end_tag}"

    json_path = output_dir / f"{base_name}.json"
    csv_path = output_dir / f"{base_name}.csv"

    payload = {
        "currency": args.currency,
        "start_utc": start.isoformat(),
        "end_utc": end.isoformat(),
        "count": len(all_prices),
        "prices": all_prices,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["symbol", "value", "timestamp", "timestamp_utc"]
        )
        writer.writeheader()
        writer.writerows(all_prices)

    print(f"[done] saved json: {json_path}")
    print(f"[done] saved csv:  {csv_path}")
    print(f"[done] rows: {len(all_prices)}")


if __name__ == "__main__":
    main()
