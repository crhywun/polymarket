from __future__ import annotations

import argparse
import csv
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


DEFAULT_DEPTH = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="将 Dome/Polymarket 的订单簿快照 JSONL.GZ 转成扁平 CSV，并保留前 N 档买卖盘。"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="输入的 .jsonl.gz 文件，或包含这类文件的目录。",
    )
    parser.add_argument(
        "--output-dir",
        default="csv_orderbooks",
        help="转换后的 CSV 输出目录。",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=DEFAULT_DEPTH,
        help="CSV 中保留的买卖盘档位数。",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="如果目标 CSV 已存在则覆盖。",
    )
    return parser.parse_args()


def iter_input_files(path: Path) -> Iterable[Path]:
    if path.is_file():
        if path.suffixes[-2:] != [".jsonl", ".gz"]:
            raise ValueError(f"期望输入为 .jsonl.gz 文件，实际得到：{path}")
        yield path
        return

    if not path.is_dir():
        raise ValueError(f"输入路径不存在：{path}")

    for file_path in sorted(path.rglob("*.jsonl.gz")):
        yield file_path


def build_header(depth: int) -> list[str]:
    header = [
        "market_slug",
        "title",
        "status",
        "side_label",
        "token_id",
        "slot_start_utc",
        "slot_end_utc",
        "timestamp_ms",
        "timestamp_utc",
        "indexed_at_ms",
        "indexed_at_utc",
        "hash",
        "asset_id",
        "market",
        "min_order_size",
        "tick_size",
        "neg_risk",
    ]
    for side in ("bid", "ask"):
        for level in range(1, depth + 1):
            header.extend(
                [
                    f"{side}_{level}_price",
                    f"{side}_{level}_size",
                ]
            )
    return header


def millis_to_iso(value: int | None) -> str:
    if value is None:
        return ""
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat()


def select_best_levels(levels: list[dict[str, str]], side: str, depth: int) -> list[dict[str, str]]:
    reverse = side == "bid"
    sorted_levels = sorted(
        levels,
        key=lambda level: float(level.get("price", "")),
        reverse=reverse,
    )
    return sorted_levels[:depth]


def flatten_levels(levels: list[dict[str, str]], side: str, depth: int) -> list[str]:
    values: list[str] = []
    best_levels = select_best_levels(levels, side, depth)
    for level in range(depth):
        if level < len(best_levels):
            values.append(best_levels[level].get("price", ""))
            values.append(best_levels[level].get("size", ""))
        else:
            values.extend(["", ""])
    return values


def convert_file(input_path: Path, output_path: Path, depth: int) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    metadata: dict[str, str] | None = None

    with gzip.open(input_path, "rt", encoding="utf-8") as source, output_path.open(
        "w", encoding="utf-8", newline=""
    ) as target:
        writer = csv.writer(target)
        writer.writerow(build_header(depth))

        for line in source:
            record = json.loads(line)
            record_type = record.get("record_type")
            if record_type == "metadata":
                metadata = record
                continue
            if record_type != "snapshot":
                continue
            if metadata is None:
                raise ValueError(f"文件 {input_path} 中快照记录出现在 metadata 之前")

            timestamp = record.get("timestamp")
            indexed_at = record.get("indexedAt")
            row = [
                metadata.get("market_slug", ""),
                metadata.get("title", ""),
                metadata.get("status", ""),
                metadata.get("side_label", ""),
                metadata.get("token_id", ""),
                metadata.get("slot_start_utc", ""),
                metadata.get("slot_end_utc", ""),
                timestamp or "",
                millis_to_iso(timestamp),
                indexed_at or "",
                millis_to_iso(indexed_at),
                record.get("hash", ""),
                record.get("assetId", ""),
                record.get("market", ""),
                record.get("minOrderSize", ""),
                record.get("tickSize", ""),
                record.get("negRisk", ""),
                *flatten_levels(record.get("bids", []), "bid", depth),
                *flatten_levels(record.get("asks", []), "ask", depth),
            ]
            writer.writerow(row)
            row_count += 1

    return row_count


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    total_files = 0
    total_rows = 0
    for source_path in iter_input_files(input_path):
        relative_name = source_path.name.replace(".jsonl.gz", ".csv")
        if input_path.is_dir():
            relative_parent = source_path.parent.relative_to(input_path)
            output_path = output_dir / relative_parent / relative_name
        else:
            output_path = output_dir / relative_name

        if output_path.exists() and not args.overwrite:
            print(f"[skip] existing CSV {output_path}")
            continue

        row_count = convert_file(source_path, output_path, args.depth)
        total_files += 1
        total_rows += row_count
        print(f"[saved] {output_path} | {row_count} rows")

    print(f"[done] converted {total_files} files | {total_rows} snapshot rows")


if __name__ == "__main__":
    main()
