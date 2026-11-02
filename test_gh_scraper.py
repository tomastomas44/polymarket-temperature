"""Offline test of gh_scraper with fake API responses (no network)."""
import csv
import gzip
import json
import os
import sys
import tempfile
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gh_scraper as G  # noqa: E402
import scraper as S  # noqa: E402

NOW = datetime(2026, 11, 2, 17, 0, tzinfo=timezone.utc)


def mk(i, label, v2=False, closed=False):
    m = {"id": str(100 + i), "conditionId": f"0xc{i}", "groupItemTitle": label,
         "question": f"Will the highest temperature in London be {label} on November 3?",
         "outcomes": '["Yes","No"]', "outcomePrices": '["0.3","0.7"]', "closed": closed,
         "volumeNum": 500, "liquidityNum": 50, "volume24hr": 10, "bestBid": 0.29, "bestAsk": 0.31}
    if v2:
        m.update(version="v2", positionIds=[f"Y{i}", f"N{i}"])
    else:
        m.update(version="v1", clobTokenIds=json.dumps([f"Y{i}", f"N{i}"]))
    return m


EV = [{"id": "1", "slug": "highest-temperature-in-london-on-november-3-2026",
       "title": "Highest temperature in London on November 3?", "endDate": "2026-11-03T12:00:00Z",
       "markets": [mk(0, "11°C or below"), mk(1, "12°C"), mk(2, "13°C or higher", closed=True)]},
      {"id": "2", "slug": "polyv2-highest-temperature-in-london-on-november-3-2026",
       "title": "Highest temperature in London on November 3?", "endDate": "2026-11-03T12:00:00Z",
       "markets": [mk(3, "12°C", v2=True)]},
      {"id": "3", "title": "Will it rain?", "markets": []}]


class Fake:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, params))
        if url.endswith("/tags/slug/daily-temperature"):
            return {"id": "103040"}
        if "/tags/slug/" in url:
            raise S.HttpError(404, url, "")
        if url.endswith("/events/keyset"):
            return {"events": EV, "next_cursor": None}
        if url.endswith("/v2/prices-history"):
            if "cursor" in params:
                return {"data": [{"timestamp": 1_800_000_120_000, "price": 0.4, "resolution_seconds": 60}],
                        "pagination": {"next_cursor": None}}
            return {"data": [{"timestamp": 1_800_000_000, "price": 0.3, "resolution_seconds": 60},
                             {"timestamp": 1_800_000_060, "price": 0.35, "resolution_seconds": 60}],
                    "pagination": {"next_cursor": "c2"}}
        raise AssertionError(url)

    def post(self, url, body):
        assert url.endswith("/books")
        out = []
        for it in body:
            a = it["token_id"]
            if a == "Y3":  # dead v2 copy: empty book
                out.append({"asset_id": a, "bids": [], "asks": []})
            else:
                out.append({"asset_id": a, "timestamp": "1762102800000",
                            "bids": [{"price": "0.01", "size": "1000"}, {"price": "0.27", "size": "50"},
                                     {"price": "0.29", "size": "20"}],
                            "asks": [{"price": "0.99", "size": "800"}, {"price": "0.31", "size": "30"},
                                     {"price": "0.40", "size": "10"}],
                            "last_trade_price": "0.30"})
        return out


def rd(path):
    with gzip.open(path, "rt") as f:
        return list(csv.DictReader(f))


tmp = tempfile.mkdtemp()
http = Fake()

# --- snapshot without --full
r = G.cmd_snapshot(os.path.join(tmp, "out"), full=False, http=http, now=NOW)
assert r == {"markets": 4, "assets": 3, "books": 2, "full": False}, r   # closed bracket skipped, empty v2 book dropped
d = os.path.join(tmp, "out", "data-2026-11-02")
assert sorted(os.listdir(d)) == ["books_20261102T1700.csv.gz"], os.listdir(d)
rows = rd(os.path.join(d, "books_20261102T1700.csv.gz"))
y1 = [x for x in rows if x["market_id"] == "101"][0]
assert (y1["best_bid"], y1["best_ask"], y1["bid_depth_5c"], y1["ask_depth_5c"]) == ("0.29", "0.31", "70.0", "30.0"), y1
assert y1["n_bid_levels"] == "3" and y1["market_id"] == "101"

# --- snapshot with --full
G.cmd_snapshot(os.path.join(tmp, "out"), full=True, http=http, now=NOW.replace(minute=5))
files = sorted(os.listdir(d))
assert files == ["books_20261102T1700.csv.gz", "books_20261102T1705.csv.gz",
                 "ladders_20261102T1705.jsonl.gz", "markets_20261102T1705.csv.gz"], files
mk_rows = rd(os.path.join(d, "markets_20261102T1705.csv.gz"))
assert len(mk_rows) == 4
m0 = [x for x in mk_rows if x["market_id"] == "100"][0]
assert (m0["city"], m0["kind"], m0["target_date"], m0["unit"], m0["low"], m0["high"], m0["version"]) == \
    ("London", "highest", "2026-11-03", "C", "", "11.0", "v1"), m0
with gzip.open(os.path.join(d, "ladders_20261102T1705.jsonl.gz"), "rt") as f:
    lad = [json.loads(x) for x in f]
assert lad[0]["bids"][0] == [0.29, 20.0] and lad[0]["asks"][0] == [0.31, 30.0], lad[0]

# --- history for target date 2026-11-03, run on 2026-11-05
inp = d  # markets file lives here
r = G.cmd_history("2026-11-03", inp, os.path.join(tmp, "out2"), http=http,
                  now=datetime(2026, 11, 5, 4, 17, tzinfo=timezone.utc))
assert r == {"assets": 4, "points": 12, "failed": 0, "bucket": 60}, r
h = rd(os.path.join(tmp, "out2", "data-2026-11-03", "history_20261103.csv.gz"))
assert len(h) == 12 and h[2]["ts"] == "1800000120", h[2]   # ms timestamp normalized
hp = [p for u, p in http.calls if u.endswith("/v2/prices-history") and "start" in p][0]
assert hp["end"] - hp["start"] <= 15 * 86400 and hp["bucket_seconds"] == 60, hp

# --- loader
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from load_data import load  # noqa: E402
import shutil  # noqa: E402
shutil.copytree(os.path.join(tmp, "out2", "data-2026-11-03"), os.path.join(tmp, "out", "data-2026-11-03"))
markets, books, history = load(os.path.join(tmp, "out"))
assert len(markets) == 4 and len(books) == 4 and len(history) == 12, (len(markets), len(books), len(history))
assert abs(books["spread"].iloc[0] - 0.02) < 1e-9
print("ALL GH TESTS PASSED")
