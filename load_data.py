"""
Load the downloaded files into pandas.

    from load_data import load
    markets, books, history = load("data")
"""
import glob
import gzip
import json
import os

import pandas as pd


def load(root: str = "data"):
    def read(pattern):
        files = sorted(glob.glob(os.path.join(root, "data-*", pattern)))
        if not files:
            return pd.DataFrame()
        return pd.concat((pd.read_csv(f) for f in files), ignore_index=True)

    markets = read("markets_*.csv.gz")
    if not markets.empty:
        # one row per bracket: the latest snapshot of its metadata
        markets = markets.sort_values("snapshot_ts").drop_duplicates("market_id", keep="last")
    books = read("books_*.csv.gz")
    if not books.empty:
        books["time"] = pd.to_datetime(books["ts"], unit="s", utc=True)
        books["mid"] = (books["best_bid"] + books["best_ask"]) / 2
        books["spread"] = books["best_ask"] - books["best_bid"]
    history = read("history_*.csv.gz")
    if not history.empty:
        history = history.drop_duplicates(["market_id", "ts"])
        history["time"] = pd.to_datetime(history["ts"], unit="s", utc=True)
    return markets, books, history


def ladders(path: str):
    """Full order books of one hourly file: list of {ts, market_id, bids, asks}."""
    with gzip.open(path, "rt") as f:
        return [json.loads(line) for line in f]


if __name__ == "__main__":
    m, b, h = load()
    print(f"markets {len(m)}, book rows {len(b)}, history points {len(h)}")
