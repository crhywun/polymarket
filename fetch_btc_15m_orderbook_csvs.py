from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from dome_api_sdk import DomeClient

from convert_orderbooks_to_csv import build_header, flatten_levels
from fetch_btc_15m_orderbooks import build_slots, read_api_key, resolve_markets, retry_call


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="拉取 BTC 15 分钟 Polymarket 订单簿快照，并直接保存为普通 CSV。"
    )
    parser.add_argument("--days", type=int, default=30, help="拉取最近多少天已结束的 slot。")
    parser.add_argument("--max-slots", type=int, default=None, help="可选，仅处理最近 N 个 slot。")
    parser.add_argument("--key-file", default="key.txt", help="API Key 文件路径。")
    parser.add_argument(
        "--output-dir",
        default="data/btc_15m_orderbook_csv_month",
        help="按日期分组的 CSV 文件夹和 manifest 输出目录。",
    )
    parser.add_argument("--depth", type=int, default=5, help="保留的最优买卖盘档位数。")
    parser.add_argument("--market-batch-size", type=int, default=100, help="批量解析 market 的请求大小。")
    parser.add_argument("--orderbook-page-limit", type=int, default=200, help="订单簿历史分页大小。")
    parser.add_argument("--retry-attempts", type=int, default=5, help="临时失败时的重试次数。")
    parser.add_argument("--retry-base-sleep", type=float, default=1.5, help="基础重试等待秒数。")
    parser.add_argument("--overwrite", action="store_true", help="如果 CSV 已存在则覆盖。")
    return parser.parse_args()


def append_jsonl(path: Path, record: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


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


def write_plain_csv(
    *,
    output_path: Path,
    slot_start_iso: str,
    slot_end_iso: str,
    market,
    side_label: str,
    snapshots: list[object],
    depth: int,
) -> dict[str, object]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    header = build_header(depth)

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)

        for snapshot in snapshots:
            snapshot_dict = asdict(snapshot)
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
            ]
            writer.writerow(row)

    return {
        "snapshot_count": len(snapshots),
        "output_path": str(output_path.resolve()),
    }


def main() -> None:
    args = parse_args()
    api_key = read_api_key(Path(args.key_file))
    client = DomeClient({"api_key": api_key})

    slots = build_slots(args.days, args.max_slots)
    if not slots:
        raise ValueError("没有生成任何 slot。")

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

    for index, slot in enumerate(slots, start=1):
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

        print(f"[slot] {index}/{len(slots)} {market.market_slug} | {market.title}")
        start_ms = int(slot.start.timestamp() * 1000)
        end_ms = int(slot.end.timestamp() * 1000)

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
            stats = write_plain_csv(
                output_path=output_path,
                slot_start_iso=slot.start.isoformat(),
                slot_end_iso=slot.end.isoformat(),
                market=market,
                side_label=side_label,
                snapshots=snapshots,
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
                    **stats,
                },
            )
            print(f"[saved] {market.market_slug} {side_label} snapshots={stats['snapshot_count']}")

    print(f"[done] manifest={manifest_path.resolve()}")


if __name__ == "__main__":
    main()
