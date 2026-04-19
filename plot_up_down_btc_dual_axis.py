from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Plot Up/Down market mid prices on the left axis and BTC price on the "
            "right axis from enriched orderbook CSV files."
        )
    )
    parser.add_argument("--up", required=True, help="Path to the Up enriched CSV")
    parser.add_argument("--down", required=True, help="Path to the Down enriched CSV")
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output PNG path. Defaults next to the Up CSV.",
    )
    return parser.parse_args()


def load_market_series(path: Path, label: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df[df["bid_1_price"].notna() & df["ask_1_price"].notna()].copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, format="mixed")
    df["mid_price"] = (df["bid_1_price"].astype(float) + df["ask_1_price"].astype(float)) / 2
    df["series"] = label
    return df[["timestamp_utc", "mid_price", "series"]]


def load_btc_series(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df[df["btc_price"].notna() & df["btc_price_timestamp_utc"].notna()].copy()
    df["btc_price_timestamp_utc"] = pd.to_datetime(
        df["btc_price_timestamp_utc"], utc=True, format="mixed"
    )
    df["btc_price"] = df["btc_price"].astype(float)
    df = df.drop_duplicates(subset=["btc_price_timestamp_utc"]).sort_values("btc_price_timestamp_utc")
    return df[["btc_price_timestamp_utc", "btc_price"]]


def main() -> None:
    args = parse_args()
    up_path = Path(args.up)
    down_path = Path(args.down)
    output_path = (
        Path(args.output)
        if args.output
        else up_path.with_name(up_path.stem.replace("__up", "__up_down_btc_dual_axis") + ".png")
    )

    up_df = load_market_series(up_path, "Up mid")
    down_df = load_market_series(down_path, "Down mid")
    btc_df = load_btc_series(up_path)

    sns.set_theme(style="whitegrid", context="talk")
    fig, ax_left = plt.subplots(figsize=(15, 8), dpi=160)
    ax_right = ax_left.twinx()

    sns.lineplot(
        data=up_df,
        x="timestamp_utc",
        y="mid_price",
        ax=ax_left,
        label="Up mid",
        color="#0d7a75",
        linewidth=2.2,
    )
    sns.lineplot(
        data=down_df,
        x="timestamp_utc",
        y="mid_price",
        ax=ax_left,
        label="Down mid",
        color="#d94f41",
        linewidth=2.2,
    )
    sns.lineplot(
        data=btc_df,
        x="btc_price_timestamp_utc",
        y="btc_price",
        ax=ax_right,
        label="BTC price",
        color="#3f4a61",
        linewidth=2.0,
    )

    title_slug = up_path.stem.split("__")[0]
    ax_left.set_title(f"{title_slug} | Up/Down vs BTC")
    ax_left.set_xlabel("Time (UTC)")
    ax_left.set_ylabel("Market mid price")
    ax_right.set_ylabel("BTC price (USD)")

    ax_left.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    fig.autofmt_xdate()

    left_handles, left_labels = ax_left.get_legend_handles_labels()
    right_handles, right_labels = ax_right.get_legend_handles_labels()
    ax_left.legend(
        left_handles + right_handles,
        left_labels + right_labels,
        loc="upper left",
        frameon=True,
    )
    ax_right.legend_.remove()

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"[saved] {output_path}")


if __name__ == "__main__":
    main()
