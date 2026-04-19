from __future__ import annotations

import argparse
import gzip
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Sequence

from dome_api_sdk import DomeClient


WINDOW_MINUTES = 15
DEFAULT_DAYS = 7
DEFAULT_MARKET_BATCH_SIZE = 100
DEFAULT_PAGE_LIMIT = 200
DEFAULT_RETRY_ATTEMPTS = 5
DEFAULT_RETRY_BASE_SLEEP = 1.5
DEFAULT_PAGE_SLEEP = 0.1


@dataclass(frozen=True)
class SlotWindow:
    start: datetime
    end: datetime
    slug: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Polymarket BTC 15-minute Up/Down orderbook snapshots from Dome "
            "for the market's active 15-minute trading window."
        )
    )
    parser.add_argument(
        "--key-file",
        default="key.txt",
        help="Path to a file containing either the raw API key or `key = ...`.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help="How many trailing days of completed 15-minute markets to fetch.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/btc_15m_orderbooks",
        help="Directory where manifests and gzipped JSONL snapshot files are written.",
    )
    parser.add_argument(
        "--market-batch-size",
        type=int,
        default=DEFAULT_MARKET_BATCH_SIZE,
        help="How many market slugs to resolve in each SDK request.",
    )
    parser.add_argument(
        "--page-limit",
        type=int,
        default=DEFAULT_PAGE_LIMIT,
        help="Orderbook page size per request. Dome currently returns up to 200.",
    )
    parser.add_argument(
        "--page-sleep",
        type=float,
        default=DEFAULT_PAGE_SLEEP,
        help="Sleep between paginated orderbook requests for the same token.",
    )
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=DEFAULT_RETRY_ATTEMPTS,
        help="How many times to retry transient SDK/API failures.",
    )
    parser.add_argument(
        "--retry-base-sleep",
        type=float,
        default=DEFAULT_RETRY_BASE_SLEEP,
        help="Base sleep in seconds for exponential backoff retries.",
    )
    parser.add_argument(
        "--max-slots",
        type=int,
        default=None,
        help="Optional cap for the most recent N slots. Useful for test runs.",
    )
    parser.add_argument(
        "--max-pages-per-side",
        type=int,
        default=None,
        help="Optional cap for paginated orderbook pages per market side.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing per-side output files instead of skipping them.",
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


def floor_to_window(dt: datetime, minutes: int = WINDOW_MINUTES) -> datetime:
    floored_minute = (dt.minute // minutes) * minutes
    return dt.replace(minute=floored_minute, second=0, microsecond=0)


def build_slots(days: int, max_slots: int | None) -> list[SlotWindow]:
    end_exclusive = floor_to_window(datetime.now(timezone.utc))
    start_inclusive = end_exclusive - timedelta(days=days)
    slots: list[SlotWindow] = []
    cursor = start_inclusive
    while cursor < end_exclusive:
        slot_end = cursor + timedelta(minutes=WINDOW_MINUTES)
        slots.append(
            SlotWindow(
                start=cursor,
                end=slot_end,
                slug=f"btc-updown-15m-{int(cursor.timestamp())}",
            )
        )
        cursor = slot_end

    if max_slots is not None:
        return slots[-max_slots:]
    return slots


def chunked(items: Sequence[str], size: int) -> Iterator[list[str]]:
    for index in range(0, len(items), size):
        yield list(items[index : index + size])


def retry_call(func, *, attempts: int, base_sleep: float):
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception as exc:  # Dome SDK raises ValueError for API failures.
            last_error = exc
            if attempt == attempts:
                break
            sleep_seconds = base_sleep * (2 ** (attempt - 1))
            print(
                f"[retry] attempt {attempt}/{attempts} failed: {exc}. "
                f"Sleeping {sleep_seconds:.1f}s before retry."
            )
            time.sleep(sleep_seconds)
    assert last_error is not None
    raise last_error


def resolve_markets(
    client: DomeClient,
    slots: Sequence[SlotWindow],
    *,
    batch_size: int,
    retry_attempts: int,
    retry_base_sleep: float,
) -> dict[str, object]:
    slug_to_market: dict[str, object] = {}
    slugs = [slot.slug for slot in slots]
    total_batches = (len(slugs) + batch_size - 1) // batch_size
    for batch_index, slug_batch in enumerate(chunked(slugs, batch_size), start=1):
        print(
            f"[markets] resolving batch {batch_index}/{total_batches} "
            f"({len(slug_batch)} slugs)"
        )
        response = retry_call(
            lambda: client.polymarket.markets.get_markets(
                {
                    "market_slug": slug_batch,
                    "limit": len(slug_batch),
                }
            ),
            attempts=retry_attempts,
            base_sleep=retry_base_sleep,
        )
        for market in response.markets:
            slug_to_market[market.market_slug] = market
    return slug_to_market


def safe_side_name(label: str) -> str:
    return label.strip().lower().replace(" ", "_")


def fetch_side_file(
    client: DomeClient,
    *,
    slot: SlotWindow,
    market,
    side_label: str,
    token_id: str,
    output_path: Path,
    page_limit: int,
    page_sleep: float,
    retry_attempts: int,
    retry_base_sleep: float,
    max_pages_per_side: int | None,
) -> dict[str, object]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pagination_key: str | None = None
    page_count = 0
    snapshot_count = 0
    truncated = False
    first_snapshot_ts: int | None = None
    last_snapshot_ts: int | None = None

    with gzip.open(output_path, "wt", encoding="utf-8") as handle:
        metadata = {
            "record_type": "metadata",
            "market_slug": market.market_slug,
            "title": market.title,
            "status": market.status,
            "side_label": side_label,
            "token_id": token_id,
            "slot_start_utc": slot.start.isoformat(),
            "slot_end_utc": slot.end.isoformat(),
            "slot_start_ms": int(slot.start.timestamp() * 1000),
            "slot_end_ms": int(slot.end.timestamp() * 1000),
            "market_start_time": market.start_time,
            "market_end_time": market.end_time,
        }
        handle.write(json.dumps(metadata, ensure_ascii=False) + "\n")

        while True:
            params = {
                "token_id": token_id,
                "start_time": int(slot.start.timestamp() * 1000),
                "end_time": int(slot.end.timestamp() * 1000),
                "limit": page_limit,
            }
            if pagination_key:
                params["pagination_key"] = pagination_key

            response = retry_call(
                lambda: client.polymarket.markets.get_orderbooks(params),
                attempts=retry_attempts,
                base_sleep=retry_base_sleep,
            )
            page_count += 1
            snapshots = response.snapshots

            for snapshot in snapshots:
                snapshot_count += 1
                if first_snapshot_ts is None:
                    first_snapshot_ts = snapshot.timestamp
                last_snapshot_ts = snapshot.timestamp
                handle.write(
                    json.dumps(
                        {
                            "record_type": "snapshot",
                            "page": page_count,
                            **asdict(snapshot),
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )

            has_more = response.pagination.has_more
            pagination_key = response.pagination.pagination_key
            if not has_more or not pagination_key:
                break
            if max_pages_per_side is not None and page_count >= max_pages_per_side:
                truncated = True
                break
            if page_sleep > 0:
                time.sleep(page_sleep)

    return {
        "snapshot_count": snapshot_count,
        "page_count": page_count,
        "truncated": truncated,
        "first_snapshot_ts": first_snapshot_ts,
        "last_snapshot_ts": last_snapshot_ts,
        "output_path": str(output_path.resolve()),
    }


def append_manifest(path: Path, record: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    args = parse_args()
    key_path = Path(args.key_file)
    api_key = read_api_key(key_path)
    client = DomeClient({"api_key": api_key})

    slots = build_slots(args.days, args.max_slots)
    if not slots:
        raise ValueError("No 15-minute slots were generated.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "manifest.jsonl"

    print(
        f"[config] fetching {len(slots)} completed 15-minute slots "
        f"from {slots[0].start.isoformat()} to {slots[-1].end.isoformat()}"
    )
    print(f"[config] outputs will be written under {output_dir.resolve()}")

    markets_by_slug = resolve_markets(
        client,
        slots,
        batch_size=args.market_batch_size,
        retry_attempts=args.retry_attempts,
        retry_base_sleep=args.retry_base_sleep,
    )

    found_markets = 0
    skipped_sides = 0
    for index, slot in enumerate(slots, start=1):
        market = markets_by_slug.get(slot.slug)
        if market is None:
            print(f"[missing] no market returned for {slot.slug}")
            append_manifest(
                manifest_path,
                {
                    "record_type": "missing_market",
                    "market_slug": slot.slug,
                    "slot_start_utc": slot.start.isoformat(),
                    "slot_end_utc": slot.end.isoformat(),
                },
            )
            continue

        found_markets += 1
        print(
            f"[market] {index}/{len(slots)} {market.market_slug} | "
            f"{market.title}"
        )

        for side_label, token_id in (
            (market.side_a.label, market.side_a.id),
            (market.side_b.label, market.side_b.id),
        ):
            side_suffix = safe_side_name(side_label)
            output_path = (
                output_dir
                / slot.start.strftime("%Y-%m-%d")
                / f"{market.market_slug}__{side_suffix}.jsonl.gz"
            )

            if output_path.exists() and not args.overwrite:
                skipped_sides += 1
                print(f"[skip] existing file {output_path}")
                append_manifest(
                    manifest_path,
                    {
                        "record_type": "skipped_existing",
                        "market_slug": market.market_slug,
                        "title": market.title,
                        "side_label": side_label,
                        "token_id": token_id,
                        "output_path": str(output_path.resolve()),
                    },
                )
                continue

            stats = fetch_side_file(
                client,
                slot=slot,
                market=market,
                side_label=side_label,
                token_id=token_id,
                output_path=output_path,
                page_limit=args.page_limit,
                page_sleep=args.page_sleep,
                retry_attempts=args.retry_attempts,
                retry_base_sleep=args.retry_base_sleep,
                max_pages_per_side=args.max_pages_per_side,
            )
            print(
                f"[saved] {market.market_slug} {side_label}: "
                f"{stats['snapshot_count']} snapshots across {stats['page_count']} pages"
            )
            append_manifest(
                manifest_path,
                {
                    "record_type": "side_fetch",
                    "market_slug": market.market_slug,
                    "title": market.title,
                    "status": market.status,
                    "slot_start_utc": slot.start.isoformat(),
                    "slot_end_utc": slot.end.isoformat(),
                    "side_label": side_label,
                    "token_id": token_id,
                    **stats,
                },
            )

    print(
        f"[done] markets found: {found_markets}/{len(slots)} | "
        f"sides skipped because files already existed: {skipped_sides}"
    )
    print(f"[done] manifest: {manifest_path.resolve()}")


if __name__ == "__main__":
    main()
