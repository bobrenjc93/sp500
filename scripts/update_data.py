from __future__ import annotations

import argparse
import csv
import gzip
import io
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
import yfinance as yf


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CONSTITUENTS_DIR = DATA_DIR / "constituents"
CONSTITUENTS_YEAR_DIR = CONSTITUENTS_DIR / "by_year"
QUOTES_DIR = DATA_DIR / "quotes"
QUOTES_YEAR_DIR = QUOTES_DIR / "by_year"
QUOTES_PREVIEW_DIR = QUOTES_DIR / "preview"
CONFIG_DIR = ROOT / "config"

WRDS_URL = "https://wrds-www.wharton.upenn.edu/classroom/sp500-introduction/over-time/"
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
FJA_INTERVALS_URL = "https://raw.githubusercontent.com/fja05680/sp500/master/sp500_ticker_start_end.csv"
FJA_START_DATE = pd.Timestamp("1996-01-02")
INDEX_START_DATE = pd.Timestamp("1957-03-01")
HTTP_HEADERS = {"User-Agent": "sp500-dataset-builder/1.0"}
QUOTE_PREVIEW_ROW_COUNT = 200


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=("daily", "full"),
        default="daily",
        help="daily refreshes constituent files and rebuilds the current-year quote file; "
        "full rebuilds the entire quote history.",
    )
    parser.add_argument(
        "--quote-chunk-size",
        type=int,
        default=40,
        help="Yahoo Finance download chunk size.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return parser.parse_args()


def ensure_dirs() -> None:
    for path in (
        CONSTITUENTS_DIR,
        CONSTITUENTS_YEAR_DIR,
        QUOTES_DIR,
        QUOTES_YEAR_DIR,
        QUOTES_PREVIEW_DIR,
        CONFIG_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def utc_now_naive() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").tz_localize(None)


def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HTTP_HEADERS, timeout=60)
    response.raise_for_status()
    return response.text


def read_html_tables(url: str) -> list[pd.DataFrame]:
    html = fetch_html(url)
    return pd.read_html(io.StringIO(html))


def normalize_company_name(value: object) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def normalize_symbol(value: object) -> str:
    if pd.isna(value):
        return ""
    value = str(value).strip().upper()
    value = re.sub(r"\s+", "", value)
    return value


def normalize_quote_symbol(symbol: str) -> str:
    return symbol.replace(".", "-")


def split_ticker_history(value: object) -> list[str]:
    if pd.isna(value):
        return []
    text = str(value).strip()
    if not text:
        return []
    return [normalize_symbol(part) for part in text.split(",") if normalize_symbol(part)]


def stringify_date(value: pd.Timestamp | pd.NaT) -> str:
    if pd.isna(value):
        return ""
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def flatten_columns(columns: Iterable[object]) -> list[str]:
    flattened: list[str] = []
    for column in columns:
        if isinstance(column, tuple):
            parts = [str(part).strip() for part in column if part and "Unnamed" not in str(part)]
            flattened.append("_".join(parts))
        else:
            flattened.append(str(column).strip())
    return flattened


def load_wrds_intervals() -> pd.DataFrame:
    frames = read_html_tables(WRDS_URL)
    expected_columns = {
        "Added/Removed",
        "PERMNO",
        "Company",
        "Ticker",
        "SP500 Start",
        "SP500 End",
    }
    year_frames = [frame for frame in frames if set(frame.columns) == expected_columns]
    if not year_frames:
        raise RuntimeError("No WRDS yearly tables were found.")

    wrds = pd.concat(year_frames, ignore_index=True)
    wrds = wrds.rename(
        columns={
            "Added/Removed": "change_action",
            "PERMNO": "permno",
            "Company": "company_name",
            "Ticker": "ticker_history",
            "SP500 Start": "start_date",
            "SP500 End": "end_date",
        }
    )
    wrds["company_name"] = wrds["company_name"].map(normalize_company_name)
    wrds["ticker_history"] = wrds["ticker_history"].fillna("").astype(str).str.strip()
    wrds["ticker_history_list"] = wrds["ticker_history"].map(split_ticker_history)
    wrds["symbol"] = wrds["ticker_history_list"].map(lambda values: values[-1] if values else "")
    wrds["ticker_count"] = wrds["ticker_history_list"].map(len)
    wrds["start_date"] = pd.to_datetime(wrds["start_date"])
    wrds["end_date"] = pd.to_datetime(wrds["end_date"])
    wrds["source"] = "wrds"
    wrds["cik"] = ""
    wrds["gics_sector"] = ""
    wrds["gics_sub_industry"] = ""
    wrds["headquarters_location"] = ""
    wrds["founded"] = ""
    wrds["current_security_name"] = ""
    return wrds[
        [
            "permno",
            "company_name",
            "current_security_name",
            "symbol",
            "ticker_history",
            "ticker_count",
            "start_date",
            "end_date",
            "source",
            "cik",
            "gics_sector",
            "gics_sub_industry",
            "headquarters_location",
            "founded",
        ]
    ]


def load_wikipedia_tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    frames = read_html_tables(WIKI_URL)
    if len(frames) < 2:
        raise RuntimeError("Wikipedia tables were not found.")

    current = frames[0].copy()
    current.columns = flatten_columns(current.columns)
    current = current.rename(
        columns={
            "Symbol": "symbol",
            "Security": "current_security_name",
            "GICS Sector": "gics_sector",
            "GICS Sub-Industry": "gics_sub_industry",
            "Headquarters Location": "headquarters_location",
            "Date added": "date_added",
            "CIK": "cik",
            "Founded": "founded",
        }
    )
    current["symbol"] = current["symbol"].map(normalize_symbol)
    current["current_security_name"] = current["current_security_name"].map(normalize_company_name)
    current["date_added"] = pd.to_datetime(current["date_added"])
    current["cik"] = current["cik"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)
    current["founded"] = current["founded"].fillna("").astype(str).str.strip()

    changes = frames[1].copy()
    changes.columns = flatten_columns(changes.columns)
    changes = changes.rename(
        columns={
            "Effective Date_Effective Date": "effective_date",
            "Added_Ticker": "added_symbol",
            "Added_Security": "added_security",
            "Removed_Ticker": "removed_symbol",
            "Removed_Security": "removed_security",
            "Reason_Reason": "reason",
        }
    )
    changes["effective_date"] = pd.to_datetime(changes["effective_date"])
    changes["added_symbol"] = changes["added_symbol"].map(normalize_symbol)
    changes["removed_symbol"] = changes["removed_symbol"].map(normalize_symbol)
    changes["added_security"] = changes["added_security"].map(normalize_company_name)
    changes["removed_security"] = changes["removed_security"].map(normalize_company_name)
    changes["reason"] = changes["reason"].fillna("").astype(str).str.strip()
    return current, changes


def load_fja_intervals() -> pd.DataFrame:
    response = requests.get(FJA_INTERVALS_URL, headers=HTTP_HEADERS, timeout=60)
    response.raise_for_status()
    fja = pd.read_csv(io.StringIO(response.text))
    fja = fja.rename(columns={"ticker": "symbol"})
    fja["symbol"] = fja["symbol"].map(normalize_symbol)
    fja["start_date"] = pd.to_datetime(fja["start_date"])
    fja["end_date"] = pd.to_datetime(fja["end_date"], errors="coerce")
    fja["permno"] = pd.NA
    fja["company_name"] = ""
    fja["current_security_name"] = ""
    fja["ticker_history"] = fja["symbol"]
    fja["ticker_count"] = 1
    fja["source"] = "fja_history"
    fja["cik"] = ""
    fja["gics_sector"] = ""
    fja["gics_sub_industry"] = ""
    fja["headquarters_location"] = ""
    fja["founded"] = ""
    return fja[
        [
            "permno",
            "company_name",
            "current_security_name",
            "symbol",
            "ticker_history",
            "ticker_count",
            "start_date",
            "end_date",
            "source",
            "cik",
            "gics_sector",
            "gics_sub_industry",
            "headquarters_location",
            "founded",
        ]
    ]


def attach_member_ids(intervals: pd.DataFrame) -> pd.DataFrame:
    intervals = intervals.copy()
    member_ids = []
    for row in intervals.itertuples(index=False):
        identifier = f"permno-{int(row.permno)}" if not pd.isna(row.permno) else f"symbol-{row.symbol or 'UNKNOWN'}"
        if pd.isna(row.permno):
            tokenish = re.sub(r"[^A-Z0-9]+", "_", str(row.ticker_history or row.symbol or "UNKNOWN").upper()).strip("_")
            identifier = f"spell-{tokenish or 'UNKNOWN'}"
        member_ids.append(f"{identifier}-{row.start_date.strftime('%Y%m%d')}")
    intervals["member_id"] = member_ids
    return intervals


def pick_open_interval_index(intervals: pd.DataFrame, symbol: str) -> int | None:
    mask = (intervals["symbol"] == symbol) & (intervals["end_date"].isna())
    open_rows = intervals.loc[mask].sort_values("start_date")
    if open_rows.empty:
        return None
    return int(open_rows.index[-1])


def build_membership_intervals() -> pd.DataFrame:
    intervals = load_wrds_intervals()
    current, changes = load_wikipedia_tables()
    fja = load_fja_intervals()

    current_lookup = current.set_index("symbol").to_dict("index")
    removed_lookup = (
        changes.loc[changes["removed_symbol"].ne(""), ["removed_symbol", "removed_security"]]
        .drop_duplicates(subset=["removed_symbol"], keep="last")
        .set_index("removed_symbol")["removed_security"]
        .to_dict()
    )
    added_lookup = (
        changes.loc[changes["added_symbol"].ne(""), ["added_symbol", "added_security"]]
        .drop_duplicates(subset=["added_symbol"], keep="last")
        .set_index("added_symbol")["added_security"]
        .to_dict()
    )

    # Use WRDS for the pre-1996 portion only. The fja dataset is materially
    # more complete from 1996 forward and avoids duplicated open spells.
    intervals = intervals.loc[intervals["start_date"] < FJA_START_DATE].copy()
    crossing_mask = intervals["end_date"] > FJA_START_DATE
    intervals.loc[crossing_mask, "end_date"] = FJA_START_DATE

    # FJA gives a strong 1996+ spell baseline and fills names missing from WRDS'
    # public change tables, especially for recent removals.
    fja["company_name"] = fja["symbol"].map(
        lambda symbol: current_lookup.get(symbol, {}).get("current_security_name")
        or removed_lookup.get(symbol)
        or added_lookup.get(symbol)
        or symbol
    )
    fja["current_security_name"] = fja["symbol"].map(lambda symbol: current_lookup.get(symbol, {}).get("current_security_name", ""))
    fja["cik"] = fja["symbol"].map(lambda symbol: current_lookup.get(symbol, {}).get("cik", ""))
    fja["gics_sector"] = fja["symbol"].map(lambda symbol: current_lookup.get(symbol, {}).get("gics_sector", ""))
    fja["gics_sub_industry"] = fja["symbol"].map(lambda symbol: current_lookup.get(symbol, {}).get("gics_sub_industry", ""))
    fja["headquarters_location"] = fja["symbol"].map(lambda symbol: current_lookup.get(symbol, {}).get("headquarters_location", ""))
    fja["founded"] = fja["symbol"].map(lambda symbol: current_lookup.get(symbol, {}).get("founded", ""))

    intervals = pd.concat([intervals, fja], ignore_index=True)
    intervals = intervals.drop_duplicates().copy()
    intervals = intervals.loc[intervals["end_date"].isna() | (intervals["end_date"] > INDEX_START_DATE)].copy()
    intervals.loc[intervals["start_date"] < INDEX_START_DATE, "start_date"] = INDEX_START_DATE

    fja_cutoff = fja["end_date"].dropna().max()
    recent_changes = changes.loc[changes["effective_date"] > fja_cutoff].sort_values("effective_date")
    for row in recent_changes.itertuples(index=False):
        if row.removed_symbol:
            open_index = pick_open_interval_index(intervals, row.removed_symbol)
            if open_index is not None:
                intervals.loc[open_index, "end_date"] = row.effective_date
            elif not intervals.loc[
                (intervals["symbol"] == row.removed_symbol) & (intervals["end_date"] == row.effective_date)
            ].empty:
                pass
            else:
                logging.warning("Could not find open interval for removed symbol %s on %s", row.removed_symbol, row.effective_date.date())

        if row.added_symbol:
            add_meta = current_lookup.get(row.added_symbol, {})
            already_present = intervals.loc[
                (intervals["symbol"] == row.added_symbol) & (intervals["start_date"] == row.effective_date)
            ]
            if already_present.empty:
                intervals = pd.concat(
                    [
                        intervals,
                        pd.DataFrame(
                            [
                                {
                                    "permno": pd.NA,
                                    "company_name": row.added_security or add_meta.get("current_security_name", ""),
                                    "current_security_name": add_meta.get("current_security_name", row.added_security),
                                    "symbol": row.added_symbol,
                                    "ticker_history": row.added_symbol,
                                    "ticker_count": 1,
                                    "start_date": row.effective_date,
                                    "end_date": pd.NaT,
                                    "source": "wikipedia_recent_changes",
                                    "cik": add_meta.get("cik", ""),
                                    "gics_sector": add_meta.get("gics_sector", ""),
                                    "gics_sub_industry": add_meta.get("gics_sub_industry", ""),
                                    "headquarters_location": add_meta.get("headquarters_location", ""),
                                    "founded": add_meta.get("founded", ""),
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )

    # Bring the current table in as the present-day truth set. This fills in
    # long-running members that never appeared in the WRDS change tables and
    # extends current spells through today.
    for row in current.itertuples(index=False):
        open_index = pick_open_interval_index(intervals, row.symbol)
        if open_index is not None:
            intervals.loc[open_index, "end_date"] = pd.NaT
            intervals.loc[open_index, "current_security_name"] = row.current_security_name
            intervals.loc[open_index, "cik"] = row.cik
            intervals.loc[open_index, "gics_sector"] = row.gics_sector
            intervals.loc[open_index, "gics_sub_industry"] = row.gics_sub_industry
            intervals.loc[open_index, "headquarters_location"] = row.headquarters_location
            intervals.loc[open_index, "founded"] = row.founded
            if not intervals.loc[open_index, "company_name"]:
                intervals.loc[open_index, "company_name"] = row.current_security_name
        else:
            bridge_start_date = max(row.date_added, fja_cutoff + pd.Timedelta(days=1))
            intervals = pd.concat(
                [
                    intervals,
                    pd.DataFrame(
                        [
                            {
                                "permno": pd.NA,
                                "company_name": row.current_security_name,
                                "current_security_name": row.current_security_name,
                                "symbol": row.symbol,
                                "ticker_history": row.symbol,
                                "ticker_count": 1,
                                "start_date": bridge_start_date,
                                "end_date": pd.NaT,
                                "source": "wikipedia_current",
                                "cik": row.cik,
                                "gics_sector": row.gics_sector,
                                "gics_sub_industry": row.gics_sub_industry,
                                "headquarters_location": row.headquarters_location,
                                "founded": row.founded,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

    intervals = intervals.drop_duplicates(
        subset=["permno", "company_name", "symbol", "ticker_history", "start_date", "end_date", "source"]
    ).copy()
    intervals["sort_end_date"] = intervals["end_date"].fillna(pd.Timestamp.max)
    intervals = intervals.sort_values(["start_date", "sort_end_date", "symbol", "company_name"]).drop(columns=["sort_end_date"])
    intervals = attach_member_ids(intervals)
    return intervals


def build_quote_plan(intervals: pd.DataFrame) -> pd.DataFrame:
    overrides_path = CONFIG_DIR / "quote_overrides.csv"
    if overrides_path.exists():
        overrides = pd.read_csv(overrides_path).fillna("")
    else:
        overrides = pd.DataFrame(columns=["member_id", "quote_symbol", "note"])
    override_map = {
        row["member_id"]: (normalize_quote_symbol(normalize_symbol(row["quote_symbol"])), row.get("note", ""))
        for _, row in overrides.iterrows()
        if normalize_symbol(row.get("member_id", "")) or row.get("member_id", "")
    }

    symbol_usage = intervals.groupby("symbol")["member_id"].nunique().to_dict()

    plan_rows = []
    for row in intervals.itertuples(index=False):
        quote_symbol = ""
        quote_status = ""
        note = ""

        if row.member_id in override_map:
            quote_symbol, note = override_map[row.member_id]
            quote_status = "manual_override"
        elif not row.symbol:
            quote_status = "missing_symbol"
            note = "No symbol was available for this membership spell."
        elif row.ticker_count > 1:
            quote_status = "ticker_history_requires_override"
            note = "WRDS reports multiple symbols during the spell; a point-in-time quote symbol is not inferred automatically."
        elif symbol_usage.get(row.symbol, 0) > 1:
            quote_status = "reused_symbol_requires_override"
            note = "The symbol is used by multiple membership spells, so ticker-only quote APIs are ambiguous."
        else:
            quote_symbol = normalize_quote_symbol(row.symbol)
            quote_status = "auto"

        plan_rows.append(
            {
                "member_id": row.member_id,
                "symbol": row.symbol,
                "ticker_history": row.ticker_history,
                "company_name": row.company_name,
                "start_date": stringify_date(row.start_date),
                "end_date": stringify_date(row.end_date),
                "source": row.source,
                "quote_symbol": quote_symbol,
                "quote_status": quote_status,
                "note": note,
            }
        )

    return pd.DataFrame(plan_rows).sort_values(["quote_status", "symbol", "start_date", "member_id"])


def write_membership_outputs(intervals: pd.DataFrame, quote_plan: pd.DataFrame) -> None:
    membership = intervals.copy()
    membership["start_date"] = membership["start_date"].map(stringify_date)
    membership["end_date"] = membership["end_date"].map(stringify_date)
    membership["permno"] = membership["permno"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)

    membership = membership.merge(
        quote_plan[["member_id", "quote_symbol", "quote_status", "note"]],
        on="member_id",
        how="left",
    )
    membership = membership.rename(columns={"note": "quote_note"})
    membership = membership[
        [
            "member_id",
            "permno",
            "symbol",
            "ticker_history",
            "company_name",
            "current_security_name",
            "start_date",
            "end_date",
            "source",
            "cik",
            "gics_sector",
            "gics_sub_industry",
            "headquarters_location",
            "founded",
            "quote_symbol",
            "quote_status",
            "quote_note",
        ]
    ]
    membership.to_csv(CONSTITUENTS_DIR / "membership_spells.csv", index=False)

    start_year = int(intervals["start_date"].dt.year.min())
    end_year = utc_now_naive().year

    for existing_file in CONSTITUENTS_YEAR_DIR.glob("*.csv"):
        existing_file.unlink()

    for year in range(start_year, end_year + 1):
        as_of = pd.Timestamp(year=year, month=12, day=31)
        active = intervals.loc[
            (intervals["start_date"] <= as_of) & (intervals["end_date"].isna() | (intervals["end_date"] > as_of))
        ].copy()
        active["as_of_date"] = as_of.strftime("%Y-%m-%d")
        active["start_date"] = active["start_date"].map(stringify_date)
        active["end_date"] = active["end_date"].map(stringify_date)
        active["permno"] = active["permno"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)
        active = active.merge(quote_plan[["member_id", "quote_status"]], on="member_id", how="left")
        snapshot = active[
            [
                "as_of_date",
                "member_id",
                "permno",
                "symbol",
                "ticker_history",
                "company_name",
                "current_security_name",
                "start_date",
                "end_date",
                "source",
                "quote_status",
            ]
        ].sort_values(["symbol", "company_name", "member_id"])
        snapshot.to_csv(CONSTITUENTS_YEAR_DIR / f"{year}.csv", index=False)


def empty_quote_year_files(target_years: Iterable[int]) -> None:
    for year in target_years:
        for suffix in (".csv", ".csv.gz"):
            path = QUOTES_YEAR_DIR / f"{year}{suffix}"
            if path.exists():
                path.unlink()


def write_quote_rows(year: int, rows: pd.DataFrame) -> None:
    csv_path = QUOTES_YEAR_DIR / f"{year}.csv"
    csv_header = not csv_path.exists()
    rows.to_csv(csv_path, mode="a", index=False, header=csv_header)

    gzip_path = QUOTES_YEAR_DIR / f"{year}.csv.gz"
    gzip_header = not gzip_path.exists()
    with gzip.open(gzip_path, mode="at", newline="") as handle:
        rows.to_csv(handle, index=False, header=gzip_header)


def write_quote_previews(mode: str) -> None:
    if mode == "full":
        for existing_file in QUOTES_PREVIEW_DIR.glob("*.csv"):
            existing_file.unlink()
        target_paths = sorted(QUOTES_YEAR_DIR.glob("*.csv"))
    else:
        current_year = utc_now_naive().year
        current_year_path = QUOTES_YEAR_DIR / f"{current_year}.csv"
        current_preview_path = QUOTES_PREVIEW_DIR / f"{current_year}.csv"
        if current_year_path.exists():
            target_paths = [current_year_path]
        else:
            if current_preview_path.exists():
                current_preview_path.unlink()
            target_paths = []

    for quote_path in target_paths:
        preview = pd.read_csv(quote_path)
        preview = preview.sort_values(["date", "symbol", "member_id"]).head(QUOTE_PREVIEW_ROW_COUNT)
        preview_path = QUOTES_PREVIEW_DIR / quote_path.name
        preview.to_csv(preview_path, index=False)


def chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def extract_symbol_frame(downloaded: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if downloaded.empty:
        return pd.DataFrame()

    if isinstance(downloaded.columns, pd.MultiIndex):
        if downloaded.columns.names[0] == "Ticker":
            if symbol not in downloaded.columns.get_level_values(0):
                return pd.DataFrame()
            frame = downloaded[symbol].copy()
        else:
            tickers = downloaded.columns.get_level_values(1)
            if symbol not in tickers:
                return pd.DataFrame()
            frame = downloaded.xs(symbol, axis=1, level="Ticker").copy()
    else:
        frame = downloaded.copy()

    frame = frame.reset_index()
    columns = {column: str(column).lower().replace(" ", "_") for column in frame.columns}
    frame = frame.rename(columns=columns)
    return frame


def build_quotes(intervals: pd.DataFrame, quote_plan: pd.DataFrame, mode: str, quote_chunk_size: int) -> pd.DataFrame:
    current_year = utc_now_naive().year
    if mode == "full":
        target_years = range(int(intervals["start_date"].dt.year.min()), current_year + 1)
    else:
        target_years = [current_year]

    empty_quote_year_files(target_years)

    fetch_candidates = quote_plan.loc[quote_plan["quote_status"].isin(["auto", "manual_override"])].copy()
    fetch_candidates["start_date"] = pd.to_datetime(fetch_candidates["start_date"])
    fetch_candidates["end_date"] = pd.to_datetime(fetch_candidates["end_date"], errors="coerce")
    if mode == "daily":
        year_start = pd.Timestamp(year=current_year, month=1, day=1)
        fetch_candidates = fetch_candidates.loc[
            (fetch_candidates["start_date"] <= utc_now_naive())
            & (fetch_candidates["end_date"].isna() | (fetch_candidates["end_date"] > year_start))
        ].copy()

    failures: list[dict[str, str]] = []
    candidate_groups = defaultdict(list)
    for row in fetch_candidates.to_dict("records"):
        candidate_groups[row["quote_symbol"]].append(row)

    quote_symbols = sorted(candidate_groups)
    logging.info("Attempting quote downloads for %s quote symbols", len(quote_symbols))

    for chunk in chunked(quote_symbols, quote_chunk_size):
        chunk_rows = [row for symbol in chunk for row in candidate_groups[symbol]]
        chunk_start = min(row["start_date"] for row in chunk_rows)
        chunk_end_values = [
            (row["end_date"] if not pd.isna(row["end_date"]) else utc_now_naive() + pd.Timedelta(days=1))
            for row in chunk_rows
        ]
        chunk_end = max(chunk_end_values)
        logging.info(
            "Downloading %s symbols from %s to %s",
            len(chunk),
            chunk_start.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d"),
        )
        downloaded = yf.download(
            tickers=chunk,
            start=chunk_start.strftime("%Y-%m-%d"),
            end=chunk_end.strftime("%Y-%m-%d"),
            progress=False,
            actions=False,
            auto_adjust=False,
            threads=True,
            group_by="ticker",
        )

        for quote_symbol in chunk:
            symbol_frame = extract_symbol_frame(downloaded, quote_symbol)
            if symbol_frame.empty:
                for row in candidate_groups[quote_symbol]:
                    failures.append(
                        {
                            "member_id": row["member_id"],
                            "symbol": row["symbol"],
                            "quote_symbol": quote_symbol,
                            "start_date": stringify_date(row["start_date"]),
                            "end_date": stringify_date(row["end_date"]),
                            "reason": "No rows returned by yfinance.",
                        }
                    )
                continue

            keep_columns = ["date", "close", "adj_close", "volume"]
            for column in keep_columns:
                if column not in symbol_frame.columns:
                    symbol_frame[column] = pd.NA
            symbol_frame = symbol_frame[keep_columns].copy()
            symbol_frame["date"] = pd.to_datetime(symbol_frame["date"])
            symbol_frame = symbol_frame.dropna(subset=["date"])
            symbol_frame = symbol_frame.loc[symbol_frame[["close", "adj_close", "volume"]].notna().any(axis=1)]

            for row in candidate_groups[quote_symbol]:
                interval_start = row["start_date"]
                interval_end = row["end_date"]
                filtered = symbol_frame.loc[symbol_frame["date"] >= interval_start].copy()
                if not pd.isna(interval_end):
                    filtered = filtered.loc[filtered["date"] < interval_end]
                if mode == "daily":
                    filtered = filtered.loc[filtered["date"].dt.year == current_year]

                if filtered.empty:
                    failures.append(
                        {
                            "member_id": row["member_id"],
                            "symbol": row["symbol"],
                            "quote_symbol": quote_symbol,
                            "start_date": stringify_date(interval_start),
                            "end_date": stringify_date(interval_end),
                            "reason": "The downloaded series had no rows inside the membership spell.",
                        }
                    )
                    continue

                filtered["date"] = filtered["date"].dt.strftime("%Y-%m-%d")
                filtered["member_id"] = row["member_id"]
                filtered["symbol"] = row["symbol"]
                filtered["quote_symbol"] = quote_symbol
                filtered["company_name"] = row["company_name"]
                filtered = filtered[["date", "member_id", "symbol", "quote_symbol", "company_name", "close", "adj_close", "volume"]]
                for year, year_rows in filtered.groupby(pd.to_datetime(filtered["date"]).dt.year):
                    write_quote_rows(int(year), year_rows)

    failure_frame = pd.DataFrame(failures).sort_values(["symbol", "start_date", "member_id"]) if failures else pd.DataFrame(
        columns=["member_id", "symbol", "quote_symbol", "start_date", "end_date", "reason"]
    )
    failure_frame.to_csv(QUOTES_DIR / "fetch_failures.csv", index=False)
    return failure_frame


def write_quote_plan(quote_plan: pd.DataFrame) -> None:
    quote_plan.to_csv(QUOTES_DIR / "quote_plan.csv", index=False)


def validate_outputs(intervals: pd.DataFrame) -> None:
    open_count = int(intervals["end_date"].isna().sum())
    current_table_count = len(load_wikipedia_tables()[0])
    if open_count != current_table_count:
        raise RuntimeError(
            f"Current constituent mismatch: built {open_count} open spells, but Wikipedia current table has {current_table_count} rows."
        )

    current_year_snapshot = pd.read_csv(CONSTITUENTS_YEAR_DIR / f"{utc_now_naive().year}.csv")
    if current_year_snapshot.empty:
        raise RuntimeError("The current-year constituent snapshot is empty.")


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")
    ensure_dirs()

    intervals = build_membership_intervals()
    quote_plan = build_quote_plan(intervals)
    write_quote_plan(quote_plan)
    write_membership_outputs(intervals, quote_plan)
    failures = build_quotes(intervals, quote_plan, mode=args.mode, quote_chunk_size=args.quote_chunk_size)
    write_quote_previews(mode=args.mode)
    validate_outputs(intervals)

    logging.info("Wrote %s membership spells", len(intervals))
    logging.info("Quote plan rows: %s", len(quote_plan))
    logging.info("Quote fetch failures: %s", len(failures))


if __name__ == "__main__":
    main()
