# Polymarket BTC 15m Toolkit

Utilities for pulling BTC 15-minute `Up/Down` Polymarket orderbook snapshots from Dome, pairing them with Chainlink `btc/usd` prices, converting snapshots to CSV, and generating quick validation charts.

## What This Repo Covers

- Resolve markets named like `btc-updown-15m-{slot_start_utc_timestamp}`.
- Pull only the market's active 15-minute trading window.
- Save raw orderbook history or flattened top-of-book CSV snapshots.
- Backfill per-slot Chainlink BTC price files.
- Merge BTC price columns back into orderbook CSVs.
- Render market overview charts for quick sanity checks.

## Scripts

- `fetch_btc_15m_orderbooks.py`: raw orderbook history to `jsonl.gz`
- `convert_orderbooks_to_csv.py`: raw `jsonl.gz` to flat CSV with top-N levels
- `fetch_btc_15m_orderbook_csvs.py`: direct orderbook CSV export
- `backfill_btc_price_files.py`: save per-slot BTC price JSON/CSV
- `merge_btc_prices_into_csvs.py`: merge BTC price columns into orderbook CSVs
- `fetch_chainlink_prices.py`: standalone Chainlink price puller
- `plot_market_overview.py`: polished overview plot for a single enriched market
- `plot_up_down_btc_dual_axis.py`: simple dual-axis chart
- `plot_up_down_combined_svg.py`: SVG price comparison chart

## Data Workflow

Recommended sequential flow:

1. Pull orderbook CSVs first.
2. Pull BTC price files second.
3. Merge BTC prices back into the orderbook CSVs.

Example:

```powershell
python fetch_btc_15m_orderbook_csvs.py --days 30 --output-dir data/btc_15m_orderbook_csv_month_seq
python backfill_btc_price_files.py --input-dir data/btc_15m_orderbook_csv_month_seq
python merge_btc_prices_into_csvs.py --input-dir data/btc_15m_orderbook_csv_month_seq
```

## Plotting

Generate the overview chart for one enriched market:

```powershell
python plot_market_overview.py `
  --up data/btc_15m_enriched_csv_month/2026-03-19/btc-updown-15m-1773893700__up.csv `
  --down data/btc_15m_enriched_csv_month/2026-03-19/btc-updown-15m-1773893700__down.csv `
  --output docs/examples/market_overview_1773893700.png
```

The overview chart contains a single panel with:

- `Up` and `Down` top-of-book mid prices
- BTC/USD on a second y-axis
- bid/ask bands for both sides

## Example Charts

### Example 1

`btc-updown-15m-1773893700`

![Market overview 1773893700](docs/examples/market_overview_1773893700.png)

Raw data:

- [btc-updown-15m-1773893700__up.csv](docs/examples/data/btc-updown-15m-1773893700__up.csv)
- [btc-updown-15m-1773893700__down.csv](docs/examples/data/btc-updown-15m-1773893700__down.csv)

### Example 2

`btc-updown-15m-1773947700`

![Market overview 1773947700](docs/examples/market_overview_1773947700.png)

Raw data:

- [btc-updown-15m-1773947700__up.csv](docs/examples/data/btc-updown-15m-1773947700__up.csv)
- [btc-updown-15m-1773947700__down.csv](docs/examples/data/btc-updown-15m-1773947700__down.csv)

### Example 3

`btc-updown-15m-1774040400`

![Market overview 1774040400](docs/examples/market_overview_1774040400.png)

Raw data:

- [btc-updown-15m-1774040400__up.csv](docs/examples/data/btc-updown-15m-1774040400__up.csv)
- [btc-updown-15m-1774040400__down.csv](docs/examples/data/btc-updown-15m-1774040400__down.csv)

## Notes

- `key.txt` and `data/` are intentionally ignored by git.
- Dome orderbook history is event-driven, so large time gaps can be normal.
- The CSV conversion logic sorts bids descending and asks ascending before keeping top-of-book levels.
