# S&P 500 Constituents And Quotes

This repository builds a point-in-time S&P 500 dataset with two goals:

1. Keep the constituent history as accurate as possible with public sources.
2. Keep daily quote CSVs updating automatically with GitHub Actions.

## What Gets Generated

- `data/constituents/membership_spells.csv`
  - One row per membership spell.
  - Includes a stable `member_id`, point-in-time start/end dates, source metadata, and quote coverage status.
- `data/constituents/by_year/YYYY.csv`
  - Year-end constituent snapshots.
  - `1990.csv` answers "what was in the S&P 500 at the end of 1990?"
- `data/quotes/by_year/YYYY.csv`
  - Daily quote rows partitioned by year in a browser-friendly CSV.
  - Includes `date`, `member_id`, `symbol`, `quote_symbol`, `close`, `adj_close`, and `volume`.
- `data/quotes/preview/YYYY.csv`
  - Lightweight previews of the yearly quote datasets.
  - Each file contains the first 200 sorted rows from the matching `data/quotes/by_year/YYYY.csv` file.
- `data/quotes/quote_plan.csv`
  - Shows which membership spells can be downloaded automatically and which need manual help.
- `data/quotes/fetch_failures.csv`
  - Shows quote downloads that were attempted but returned no data.

## Source Stack

- WRDS public S&P 500 constituent change history:
  - `https://wrds-www.wharton.upenn.edu/classroom/sp500-introduction/over-time/`
- Wikipedia current S&P 500 constituents and recent constituent changes:
  - `https://en.wikipedia.org/wiki/List_of_S%26P_500_companies`
- Yahoo Finance via `yfinance` for daily quote downloads

## Accuracy Notes

The constituent history is built to minimize survivorship bias:

- WRDS provides the long-run historical change record.
- Wikipedia current and recent-change tables are used to extend the history through the present.
- A stable `member_id` is assigned per membership spell so reused tickers can be tracked separately.
- The weakest period is pre-1996. With only public sources, some older constituents are still missing from the point-in-time snapshots before 1996, so those files should be treated as best-effort rather than subscription-grade CRSP parity.

Quotes are more limited than constituents because free quote providers are ticker-based:

- Automatically downloaded quotes are reliable for unambiguous symbols.
- Reused symbols, multi-symbol historical ticker strings, and some delisted/bankrupt names are marked in `quote_plan.csv` instead of being assigned a likely-wrong history.
- Manual overrides can be added in `config/quote_overrides.csv` for cases where a better quote identifier is known.

## Browsing On GitHub

Each yearly quote dataset ships as `data/quotes/by_year/YYYY.csv` for the full browser-friendly table view.

If you only need a quick spot check, `data/quotes/preview/YYYY.csv` still contains the first 200 sorted rows for that year.

## Local Usage

Install dependencies:

```bash
python3 -m pip install --break-system-packages -r requirements.txt
```

Run a full rebuild:

```bash
python3 scripts/update_data.py --mode full
```

Run the daily refresh behavior locally:

```bash
python3 scripts/update_data.py --mode daily
```

## GitHub Actions

`.github/workflows/update-data.yml` runs every day at `23:30 UTC`.

The scheduled job:

- rebuilds the constituent files,
- refreshes the current year's quote file,
- commits changes back to the repository when the generated CSVs changed.

Use the workflow dispatch input `mode=full` if you want GitHub Actions to rebuild the entire quote history.
