from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


UP_COLOR = "#0f766e"
DOWN_COLOR = "#dc5f45"
BTC_COLOR = "#2d3a4a"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="使用增强版 Up/Down 订单簿 CSV，为单个 BTC 15 分钟市场绘制单面板概览图。"
    )
    parser.add_argument("--up", required=True, help="Up 侧增强版 CSV 路径。")
    parser.add_argument("--down", required=True, help="Down 侧增强版 CSV 路径。")
    parser.add_argument(
        "--output",
        default=None,
        help="可选的输出路径，默认在 Up CSV 同目录生成 *_overview.png。",
    )
    return parser.parse_args()


def load_market_frame(path: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    df = pd.read_csv(path)
    df = df[df["bid_1_price"].notna() & df["ask_1_price"].notna()].copy()
    if df.empty:
        raise ValueError(f"{path} 中没有同时包含 bid_1_price 和 ask_1_price 的有效行")

    for column in ("bid_1_price", "ask_1_price", "btc_price"):
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, format="mixed")
    df["mid_price"] = (df["bid_1_price"] + df["ask_1_price"]) / 2

    metadata = {
        "market_slug": str(df["market_slug"].iloc[0]),
        "title": str(df["title"].iloc[0]),
        "slot_start_utc": str(df["slot_start_utc"].iloc[0]),
        "slot_end_utc": str(df["slot_end_utc"].iloc[0]),
    }
    return df, metadata


def load_btc_frame(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "btc_price" not in df.columns or "btc_price_timestamp_utc" not in df.columns:
        raise ValueError(f"{path} 中没有找到 BTC 价格相关字段")
    df = df[df["btc_price"].notna() & df["btc_price_timestamp_utc"].notna()].copy()
    if df.empty:
        raise ValueError(f"{path} 中没有可用的 BTC 价格记录")

    df["btc_price"] = pd.to_numeric(df["btc_price"], errors="coerce")
    df["btc_price_timestamp_utc"] = pd.to_datetime(
        df["btc_price_timestamp_utc"], utc=True, format="mixed"
    )
    df = (
        df.dropna(subset=["btc_price"])
        .drop_duplicates(subset=["btc_price_timestamp_utc"])
        .sort_values("btc_price_timestamp_utc")
    )
    return df[["btc_price_timestamp_utc", "btc_price"]]


def style_axis(ax: plt.Axes) -> None:
    ax.set_facecolor("#fffdf8")
    ax.grid(True, axis="y", color="#ddd4c6", linewidth=0.8)
    ax.grid(False, axis="x")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#b8ab96")
    ax.spines["bottom"].set_color("#b8ab96")
    ax.tick_params(colors="#4f4a40")


def main() -> None:
    args = parse_args()
    up_path = Path(args.up)
    down_path = Path(args.down)
    output_path = (
        Path(args.output)
        if args.output
        else up_path.with_name(up_path.stem.replace("__up", "__overview") + ".png")
    )

    up_df, metadata = load_market_frame(up_path)
    down_df, _ = load_market_frame(down_path)
    btc_df = load_btc_frame(up_path)

    sns.set_theme(style="ticks", context="talk")
    fig, ax_left = plt.subplots(figsize=(16, 6.8), dpi=170)
    fig.patch.set_facecolor("#f6f1e8")
    style_axis(ax_left)

    ax_right = ax_left.twinx()
    ax_right.set_facecolor("none")
    ax_right.spines["top"].set_visible(False)
    ax_right.spines["left"].set_visible(False)
    ax_right.spines["right"].set_color("#b8ab96")
    ax_right.tick_params(colors="#4f4a40")

    ax_left.fill_between(
        up_df["timestamp_utc"],
        up_df["bid_1_price"],
        up_df["ask_1_price"],
        color=UP_COLOR,
        alpha=0.12,
        linewidth=0,
    )
    ax_left.fill_between(
        down_df["timestamp_utc"],
        down_df["bid_1_price"],
        down_df["ask_1_price"],
        color=DOWN_COLOR,
        alpha=0.10,
        linewidth=0,
    )
    ax_left.plot(
        up_df["timestamp_utc"],
        up_df["mid_price"],
        color=UP_COLOR,
        linewidth=2.4,
        label="Up mid",
    )
    ax_left.plot(
        down_df["timestamp_utc"],
        down_df["mid_price"],
        color=DOWN_COLOR,
        linewidth=2.4,
        label="Down mid",
    )
    ax_right.plot(
        btc_df["btc_price_timestamp_utc"],
        btc_df["btc_price"],
        color=BTC_COLOR,
        linewidth=1.8,
        alpha=0.95,
        label="BTC/USD",
    )

    ax_left.set_ylabel("Market probability")
    ax_right.set_ylabel("BTC price (USD)")
    ax_left.set_xlabel("Time (UTC)")
    ax_left.set_ylim(-0.02, 1.02)
    ax_left.set_title(
        f"{metadata['title']}\n{metadata['market_slug']} | {metadata['slot_start_utc']} -> {metadata['slot_end_utc']}",
        fontsize=16,
        loc="left",
        pad=14,
    )

    locator = mdates.AutoDateLocator(minticks=4, maxticks=7)
    formatter = mdates.ConciseDateFormatter(locator)
    ax_left.xaxis.set_major_locator(locator)
    ax_left.xaxis.set_major_formatter(formatter)

    left_handles, left_labels = ax_left.get_legend_handles_labels()
    right_handles, right_labels = ax_right.get_legend_handles_labels()
    ax_left.legend(
        left_handles + right_handles,
        left_labels + right_labels,
        loc="upper left",
        frameon=True,
        facecolor="#fffdf8",
        edgecolor="#d8cfbf",
    )

    fig.tight_layout(rect=[0, 0, 1, 0.98])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"[saved] {output_path}")


if __name__ == "__main__":
    main()
