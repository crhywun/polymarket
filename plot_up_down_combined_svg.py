from __future__ import annotations

import argparse
import csv
import html
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


SVG_WIDTH = 1600
SVG_HEIGHT = 980
MARGIN_LEFT = 90
MARGIN_RIGHT = 40
MARGIN_TOP = 52
MARGIN_BOTTOM = 72
PANEL_GAP = 48


@dataclass(frozen=True)
class Point:
    x: float
    y: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="将 Up/Down 两份 CSV 的价格序列绘制到同一张 SVG 图中。"
    )
    parser.add_argument("--up", required=True, help="Up 侧 CSV 路径")
    parser.add_argument("--down", required=True, help="Down 侧 CSV 路径")
    parser.add_argument(
        "--output",
        default=None,
        help="可选的 SVG 输出路径，默认写到 Up CSV 同目录。",
    )
    parser.add_argument("--title", default=None, help="可选，自定义图标题。")
    return parser.parse_args()


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def parse_float(value: str) -> float | None:
    value = (value or "").strip()
    if not value:
        return None
    return float(value)


def fmt_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%H:%M:%S")


def scale_points(
    x_values: list[int],
    y_values: list[float],
    *,
    min_x: int,
    max_x: int,
    min_y: float,
    max_y: float,
    x0: float,
    y0: float,
    width: float,
    height: float,
) -> list[Point]:
    if not x_values:
        return []
    if max_x == min_x:
        max_x += 1
    if max_y == min_y:
        max_y += 1
    points: list[Point] = []
    for x_value, y_value in zip(x_values, y_values):
        x = x0 + (x_value - min_x) / (max_x - min_x) * width
        y = y0 + height - (y_value - min_y) / (max_y - min_y) * height
        points.append(Point(x=x, y=y))
    return points


def polyline(points: list[Point], color: str, stroke_width: float, dashed: bool = False) -> str:
    if not points:
        return ""
    raw = " ".join(f"{point.x:.2f},{point.y:.2f}" for point in points)
    dash = ' stroke-dasharray="8 8"' if dashed else ""
    return (
        f'<polyline fill="none" stroke="{color}" stroke-width="{stroke_width}"'
        f'{dash} points="{raw}" />'
    )


def polygon(upper: list[Point], lower: list[Point], fill: str, opacity: float) -> str:
    if not upper or not lower:
        return ""
    raw_points = upper + list(reversed(lower))
    raw = " ".join(f"{point.x:.2f},{point.y:.2f}" for point in raw_points)
    return f'<polygon points="{raw}" fill="{fill}" opacity="{opacity:.2f}" />'


def axis_ticks(min_value: float, max_value: float, count: int) -> list[float]:
    if count <= 1 or max_value == min_value:
        return [min_value]
    step = (max_value - min_value) / (count - 1)
    return [min_value + step * index for index in range(count)]


def build_panel(
    *,
    rows: list[dict[str, str]],
    panel_title: str,
    x0: float,
    y0: float,
    width: float,
    height: float,
    global_min_x: int,
    global_max_x: int,
) -> tuple[list[str], float, float, int]:
    filtered_rows = [
        row
        for row in rows
        if parse_float(row["bid_1_price"]) is not None and parse_float(row["ask_1_price"]) is not None
    ]
    if not filtered_rows:
        raise ValueError(f"{panel_title} 面板没有同时包含 bid_1_price 和 ask_1_price 的有效行。")

    timestamps = [int(row["timestamp_ms"]) for row in filtered_rows]
    bid_1 = [parse_float(row["bid_1_price"]) for row in filtered_rows]
    ask_1 = [parse_float(row["ask_1_price"]) for row in filtered_rows]
    bid_5 = [
        parse_float(row["bid_5_price"]) if parse_float(row["bid_5_price"]) is not None else parse_float(row["bid_1_price"])
        for row in filtered_rows
    ]
    ask_5 = [
        parse_float(row["ask_5_price"]) if parse_float(row["ask_5_price"]) is not None else parse_float(row["ask_1_price"])
        for row in filtered_rows
    ]
    bid_1 = [value for value in bid_1 if value is not None]
    ask_1 = [value for value in ask_1 if value is not None]
    bid_5 = [value for value in bid_5 if value is not None]
    ask_5 = [value for value in ask_5 if value is not None]
    mid = [(bid + ask) / 2 for bid, ask in zip(bid_1, ask_1)]

    min_price = min(bid_5 + bid_1 + ask_1 + ask_5 + mid)
    max_price = max(bid_5 + bid_1 + ask_1 + ask_5 + mid)

    bid1_points = scale_points(
        timestamps,
        bid_1,
        min_x=global_min_x,
        max_x=global_max_x,
        min_y=min_price,
        max_y=max_price,
        x0=x0,
        y0=y0,
        width=width,
        height=height,
    )
    ask1_points = scale_points(
        timestamps,
        ask_1,
        min_x=global_min_x,
        max_x=global_max_x,
        min_y=min_price,
        max_y=max_price,
        x0=x0,
        y0=y0,
        width=width,
        height=height,
    )
    bid5_points = scale_points(
        timestamps,
        bid_5,
        min_x=global_min_x,
        max_x=global_max_x,
        min_y=min_price,
        max_y=max_price,
        x0=x0,
        y0=y0,
        width=width,
        height=height,
    )
    ask5_points = scale_points(
        timestamps,
        ask_5,
        min_x=global_min_x,
        max_x=global_max_x,
        min_y=min_price,
        max_y=max_price,
        x0=x0,
        y0=y0,
        width=width,
        height=height,
    )
    mid_points = scale_points(
        timestamps,
        mid,
        min_x=global_min_x,
        max_x=global_max_x,
        min_y=min_price,
        max_y=max_price,
        x0=x0,
        y0=y0,
        width=width,
        height=height,
    )

    parts = [
        f'<rect x="{x0}" y="{y0}" width="{width}" height="{height}" fill="#fffdfa" stroke="#c6c1b5" />',
        f'<text x="{x0}" y="{y0 - 14}" font-size="18" font-family="Segoe UI, Arial, sans-serif" fill="#1b1b1b">{html.escape(panel_title)}</text>',
    ]

    for value in axis_ticks(min_price, max_price, 6):
        y = y0 + height - (value - min_price) / (max_price - min_price or 1) * height
        parts.append(f'<line x1="{x0}" y1="{y:.2f}" x2="{x0 + width}" y2="{y:.2f}" stroke="#e7e0d0" />')
        parts.append(
            f'<text x="{x0 - 12}" y="{y + 4:.2f}" text-anchor="end" font-size="12" font-family="Consolas, monospace" fill="#6b665b">{value:.4f}</text>'
        )

    parts.append(polygon(ask1_points, ask5_points, "#f3a087", 0.34))
    parts.append(polygon(bid1_points, bid5_points, "#5ba5a0", 0.34))
    parts.append(polyline(ask1_points, "#d94f41", 2.0))
    parts.append(polyline(bid1_points, "#0d7a75", 2.0))
    parts.append(polyline(mid_points, "#8c6f2d", 1.4, dashed=True))

    return parts, min_price, max_price, len(filtered_rows)


def build_svg(up_rows: list[dict[str, str]], down_rows: list[dict[str, str]], title: str) -> str:
    content_width = SVG_WIDTH - MARGIN_LEFT - MARGIN_RIGHT
    panel_height = (SVG_HEIGHT - MARGIN_TOP - MARGIN_BOTTOM - PANEL_GAP) / 2

    all_timestamps = [int(row["timestamp_ms"]) for row in up_rows + down_rows]
    global_min_x = min(all_timestamps)
    global_max_x = max(all_timestamps)

    top_y = MARGIN_TOP + 34
    bottom_y = top_y + panel_height + PANEL_GAP

    up_parts, _, _, up_plotted = build_panel(
        rows=up_rows,
        panel_title="Up",
        x0=MARGIN_LEFT,
        y0=top_y,
        width=content_width,
        height=panel_height,
        global_min_x=global_min_x,
        global_max_x=global_max_x,
    )
    down_parts, _, _, down_plotted = build_panel(
        rows=down_rows,
        panel_title="Down",
        x0=MARGIN_LEFT,
        y0=bottom_y,
        width=content_width,
        height=panel_height,
        global_min_x=global_min_x,
        global_max_x=global_max_x,
    )

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{SVG_WIDTH}" height="{SVG_HEIGHT}" viewBox="0 0 {SVG_WIDTH} {SVG_HEIGHT}">',
        '<rect width="100%" height="100%" fill="#f6f4ef" />',
        f'<text x="{MARGIN_LEFT}" y="28" font-size="24" font-family="Segoe UI, Arial, sans-serif" fill="#1b1b1b">{html.escape(title)}</text>',
        f'<text x="{MARGIN_LEFT}" y="48" font-size="13" font-family="Segoe UI, Arial, sans-serif" fill="#545454">Same time axis for both outcomes. Lines: best bid / best ask / mid. Bands: top-5 bid/ask prices.</text>',
    ]
    svg.extend(up_parts)
    svg.extend(down_parts)

    x_tick_count = 8
    for index in range(x_tick_count):
        ratio = index / max(x_tick_count - 1, 1)
        ts = round(global_min_x + (global_max_x - global_min_x) * ratio)
        x = MARGIN_LEFT + ratio * content_width
        svg.append(f'<line x1="{x:.2f}" y1="{top_y}" x2="{x:.2f}" y2="{bottom_y + panel_height}" stroke="#efe9db" />')
        svg.append(
            f'<text x="{x:.2f}" y="{SVG_HEIGHT - 18}" text-anchor="middle" font-size="12" font-family="Consolas, monospace" fill="#6b665b">{fmt_ts(ts)}</text>'
        )

    legend_x = SVG_WIDTH - MARGIN_RIGHT - 280
    legend_y = 56
    svg.extend(
        [
            f'<rect x="{legend_x}" y="{legend_y}" width="250" height="120" rx="10" fill="#fffdfa" stroke="#c6c1b5" />',
            f'<line x1="{legend_x + 18}" y1="{legend_y + 24}" x2="{legend_x + 62}" y2="{legend_y + 24}" stroke="#0d7a75" stroke-width="2" />',
            f'<text x="{legend_x + 72}" y="{legend_y + 28}" font-size="13" font-family="Segoe UI, Arial, sans-serif" fill="#1b1b1b">best bid</text>',
            f'<line x1="{legend_x + 18}" y1="{legend_y + 46}" x2="{legend_x + 62}" y2="{legend_y + 46}" stroke="#d94f41" stroke-width="2" />',
            f'<text x="{legend_x + 72}" y="{legend_y + 50}" font-size="13" font-family="Segoe UI, Arial, sans-serif" fill="#1b1b1b">best ask</text>',
            f'<line x1="{legend_x + 18}" y1="{legend_y + 68}" x2="{legend_x + 62}" y2="{legend_y + 68}" stroke="#8c6f2d" stroke-width="1.4" stroke-dasharray="8 8" />',
            f'<text x="{legend_x + 72}" y="{legend_y + 72}" font-size="13" font-family="Segoe UI, Arial, sans-serif" fill="#1b1b1b">mid price</text>',
            f'<rect x="{legend_x + 18}" y="{legend_y + 82}" width="44" height="12" fill="#5ba5a0" opacity="0.34" />',
            f'<text x="{legend_x + 72}" y="{legend_y + 93}" font-size="13" font-family="Segoe UI, Arial, sans-serif" fill="#1b1b1b">bid 1-5 band</text>',
            f'<rect x="{legend_x + 18}" y="{legend_y + 100}" width="44" height="12" fill="#f3a087" opacity="0.34" />',
            f'<text x="{legend_x + 72}" y="{legend_y + 111}" font-size="13" font-family="Segoe UI, Arial, sans-serif" fill="#1b1b1b">ask 1-5 band</text>',
        ]
    )

    up_summary = f"Up rows plotted={up_plotted}/{len(up_rows)} range={fmt_ts(int(up_rows[0]['timestamp_ms']))} -> {fmt_ts(int(up_rows[-1]['timestamp_ms']))}"
    down_summary = f"Down rows plotted={down_plotted}/{len(down_rows)} range={fmt_ts(int(down_rows[0]['timestamp_ms']))} -> {fmt_ts(int(down_rows[-1]['timestamp_ms']))}"
    svg.append(
        f'<text x="{MARGIN_LEFT}" y="{SVG_HEIGHT - 44}" font-size="13" font-family="Consolas, monospace" fill="#3b3b3b">{html.escape(up_summary)}</text>'
    )
    svg.append(
        f'<text x="{MARGIN_LEFT}" y="{SVG_HEIGHT - 26}" font-size="13" font-family="Consolas, monospace" fill="#3b3b3b">{html.escape(down_summary)}</text>'
    )
    svg.append("</svg>")
    return "\n".join(svg)


def main() -> None:
    args = parse_args()
    up_path = Path(args.up)
    down_path = Path(args.down)
    up_rows = load_rows(up_path)
    down_rows = load_rows(down_path)
    if not up_rows or not down_rows:
        raise ValueError("Up 或 Down 的 CSV 为空。")

    default_output = up_path.with_name(up_path.stem.replace("__up", "__up_down_combined") + ".svg")
    output_path = Path(args.output) if args.output else default_output
    title = args.title or f"{up_rows[0]['market_slug']} | Up vs Down price changes"
    svg = build_svg(up_rows, down_rows, title)
    output_path.write_text(svg, encoding="utf-8")
    print(f"[saved] {output_path}")


if __name__ == "__main__":
    main()
