from __future__ import annotations

import argparse
import bisect
import csv
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

from dome_api_sdk import DomeClient

from convert_orderbooks_to_csv import build_header, flatten_levels
from fetch_btc_15m_orderbooks import build_slots, read_api_key, resolve_markets, retry_call


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="拉取 BTC 15 分钟 Polymarket 订单簿快照，并在每一行 CSV 中补齐对齐后的 Chainlink BTC/USD 价格。"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="拉取最近多少天已结束的 15 分钟 slot，默认 30。",
    )
    parser.add_argument(
        "--max-slots",
        type=int,
        default=None,
        help="可选，仅处理最近 N 个 slot，适合样本测试。",
    )
    parser.add_argument(
        "--key-file",
        default="key.txt",
        help="密钥文件路径，内容可以是原始 API Key 或 `key = ...`。",
    )
    parser.add_argument(
        "--output-dir",
        default="data/btc_15m_enriched_csv",
        help="增强版 CSV 和 manifest 的输出目录。",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=5,
        help="CSV 中保留多少档最优买卖盘，默认 5。",
    )
    parser.add_argument(
        "--market-batch-size",
        type=int,
        default=100,
        help="每次请求中批量解析多少个 market slug。",
    )
    parser.add_argument(
        "--orderbook-page-limit",
        type=int,
        default=200,
        help="订单簿历史请求的分页大小，Dome 当前最大为 200。",
    )
    parser.add_argument(
        "--price-page-limit",
        type=int,
        default=100,
        help="Chainlink 价格请求的分页大小，Dome 当前最大为 100。",
    )
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=5,
        help="临时 API 失败时的重试次数。",
    )
    parser.add_argument(
        "--retry-base-sleep",
        type=float,
        default=1.5,
        help="指数退避重试的基础等待秒数。",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="如果 CSV 已存在则覆盖，而不是跳过。",
    )
    return parser.parse_args()


def append_jsonl(path: Path, record: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def save_slot_price_files(
    *,
    output_dir: Path,
    slot_date: str,
    market_slug: str,
    prices: list[dict[str, object]],
) -> tuple[str, str]:
    prices_dir = output_dir / "prices" / slot_date
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

    return str(json_path.resolve()), str(csv_path.resolve())


def fetch_chainlink_prices_for_slot(
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
    all_prices: list[dict[str, object]],
    all_price_timestamps: list[int],
    *,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, object]]:
    left = bisect.bisect_left(all_price_timestamps, start_ms)
    right = bisect.bisect_right(all_price_timestamps, end_ms)
    return all_prices[left:right]


def group_slots_by_date(slots):
    grouped = defaultdict(list)
    for slot in slots:
        grouped[slot.start.strftime("%Y-%m-%d")].append(slot)
    return grouped


def fetch_all_snapshots_for_token(
    client: DomeClient,
    *,
    token_id: str,
    start_ms: int,
    end_ms: int,
    limit: int,
    retry_attempts: int,
    retry_base_sleep: float,
) -> list[object]:
    snapshots = []
    pagination_key: str | None = None

    while True:
        params = {
            "token_id": token_id,
            "start_time": start_ms,
            "end_time": end_ms,
            "limit": limit,
        }
        if pagination_key:
            params["pagination_key"] = pagination_key

        response = retry_call(
            lambda: client.polymarket.markets.get_orderbooks(params),
            attempts=retry_attempts,
            base_sleep=retry_base_sleep,
        )
        snapshots.extend(response.snapshots)

        if not response.pagination.has_more or not response.pagination.pagination_key:
            break
        pagination_key = response.pagination.pagination_key

    return snapshots


def align_price(
    snapshot_ts: int,
    price_timestamps: list[int],
    price_rows: list[dict[str, object]],
) -> dict[str, object]:
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


def write_enriched_csv(
    *,
    output_path: Path,
    slot_start_iso: str,
    slot_end_iso: str,
    market,
    side_label: str,
    snapshots: list[object],
    prices: list[dict[str, object]],
    depth: int,
) -> dict[str, object]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    price_timestamps = [int(row["timestamp"]) for row in prices]

    header = build_header(depth) + [
        "btc_price",
        "btc_price_timestamp",
        "btc_price_timestamp_utc",
        "btc_price_match",
    ]

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)

        for snapshot in snapshots:
            snapshot_dict = asdict(snapshot)
            price_info = align_price(snapshot.timestamp, price_timestamps, prices)
            row = [
                market.market_slug,
                market.title,
                market.status,
                side_label,
                snapshot.assetId,
                slot_start_iso,
                slot_end_iso,
                snapshot.timestamp,
                datetime.fromtimestamp(snapshot.timestamp / 1000, timezone.utc).isoformat(),
                snapshot.indexedAt,
                datetime.fromtimestamp(snapshot.indexedAt / 1000, timezone.utc).isoformat(),
                snapshot.hash,
                snapshot.assetId,
                snapshot.market,
                snapshot.minOrderSize,
                snapshot.tickSize,
                snapshot.negRisk,
                *flatten_levels(snapshot_dict.get("bids", []), "bid", depth),
                *flatten_levels(snapshot_dict.get("asks", []), "ask", depth),
                price_info["btc_price"],
                price_info["btc_price_timestamp"],
                price_info["btc_price_timestamp_utc"],
                price_info["btc_price_match"],
            ]
            writer.writerow(row)

    return {
        "snapshot_count": len(snapshots),
        "price_count": len(prices),
        "output_path": str(output_path.resolve()),
    }


def main() -> None:
    args = parse_args()
    api_key = read_api_key(Path(args.key_file))
    client = DomeClient({"api_key": api_key})

    slots = build_slots(args.days, args.max_slots)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "manifest.jsonl"

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

    grouped_slots = group_slots_by_date(slots)
    processed_slots = 0
    total_slots = len(slots)

    for slot_date in sorted(grouped_slots):
        day_slots = grouped_slots[slot_date]
        day_start_ms = int(day_slots[0].start.timestamp() * 1000)
        day_end_ms = int(day_slots[-1].end.timestamp() * 1000)
        print(
            f"[prices] prefetching Chainlink BTC/USD for {slot_date} "
            f"({len(day_slots)} slots)"
        )
        day_prices = fetch_chainlink_prices_for_range(
            client,
            start_ms=day_start_ms,
            end_ms=day_end_ms,
            limit=args.price_page_limit,
            retry_attempts=args.retry_attempts,
            retry_base_sleep=args.retry_base_sleep,
        )
        day_price_timestamps = [int(row["timestamp"]) for row in day_prices]
        print(f"[prices] {slot_date} prefetched {len(day_prices)} price rows")

        for slot in day_slots:
            processed_slots += 1
            market = markets_by_slug.get(slot.slug)
            if market is None:
                print(f"[missing] {slot.slug}")
                append_jsonl(
                    manifest_path,
                    {
                        "market_slug": slot.slug,
                        "slot_start_utc": slot.start.isoformat(),
                        "slot_end_utc": slot.end.isoformat(),
                        "market_found": False,
                    },
                )
                continue

            print(f"[slot] {processed_slots}/{total_slots} {market.market_slug} | {market.title}")
            start_ms = int(slot.start.timestamp() * 1000)
            end_ms = int(slot.end.timestamp() * 1000)
            prices = slice_prices_for_slot(
                day_prices,
                day_price_timestamps,
                start_ms=start_ms,
                end_ms=end_ms,
            )
            price_json_path, price_csv_path = save_slot_price_files(
                output_dir=output_dir,
                slot_date=slot.start.strftime("%Y-%m-%d"),
                market_slug=market.market_slug,
                prices=prices,
            )

            for side_label, token_id in (
                (market.side_a.label, market.side_a.id),
                (market.side_b.label, market.side_b.id),
            ):
                side_suffix = side_label.lower()
                output_path = (
                    output_dir
                    / slot.start.strftime("%Y-%m-%d")
                    / f"{market.market_slug}__{side_suffix}.csv"
                )
                if output_path.exists() and not args.overwrite:
                    print(f"[skip] existing {output_path}")
                    continue

                snapshots = fetch_all_snapshots_for_token(
                    client,
                    token_id=token_id,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    limit=args.orderbook_page_limit,
                    retry_attempts=args.retry_attempts,
                    retry_base_sleep=args.retry_base_sleep,
                )
                stats = write_enriched_csv(
                    output_path=output_path,
                    slot_start_iso=slot.start.isoformat(),
                    slot_end_iso=slot.end.isoformat(),
                    market=market,
                    side_label=side_label,
                    snapshots=snapshots,
                    prices=prices,
                    depth=args.depth,
                )
                append_jsonl(
                    manifest_path,
                    {
                        "market_slug": market.market_slug,
                        "title": market.title,
                        "slot_start_utc": slot.start.isoformat(),
                        "slot_end_utc": slot.end.isoformat(),
                        "market_found": True,
                        "side_label": side_label,
                        "up_token_id": market.side_a.id,
                        "down_token_id": market.side_b.id,
                        "price_json_path": price_json_path,
                        "price_csv_path": price_csv_path,
                        **stats,
                    },
                )
                print(
                    f"[saved] {market.market_slug} {side_label} snapshots={stats['snapshot_count']} prices={stats['price_count']}"
                )

    print(f"[done] manifest={manifest_path.resolve()}")


if __name__ == "__main__":
    main()
