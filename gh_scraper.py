#!/usr/bin/env python3
"""
GitHub Actions version of the Polymarket temperature scraper.

No database: every run writes small gzip files that the workflow uploads to a
GitHub Release named data-YYYY-MM-DD (one release per UTC day).

Commands
  snapshot [--full]   every ~5 min. Writes books_<stamp>.csv.gz (top-of-book + depth summary).
                      With --full (once per hour) also markets_<stamp>.csv.gz and
                      ladders_<stamp>.jsonl.gz (complete order books).
  history --day D     daily. Reads markets_*.csv.gz of day D from --inp and archives the
                      1-minute price history of every bracket whose target date is D.

Reuses the parsing/HTTP helpers of scraper.py (must sit in the same folder).
"""
from __future__ import annotations

import argparse
import csv
import glob
import gzip
import json
import os
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Any, Optional

import scraper as S

MARKET_FIELDS = [
    "snapshot_ts", "event_id", "event_slug", "title", "kind", "city", "target_date", "end_date",
    "market_id", "condition_id", "version", "bracket_label", "unit", "low", "high",
    "yes_asset", "no_asset", "closed", "volume", "volume_24h", "liquidity",
    "gamma_best_bid", "gamma_best_ask", "gamma_last_trade", "outcome_prices", "winner",
]
BOOK_FIELDS = [
    "ts", "market_id", "book_ts",            # market_id -> yes_asset via markets_*.csv.gz
    "best_bid", "best_ask", "best_bid_size", "best_ask_size",
    "bid_depth_5c", "ask_depth_5c",          # shares within 5 cents of the best price
    "bid_depth_usd", "ask_depth_usd",        # whole book, price * size
    "n_bid_levels", "n_ask_levels", "last_trade_price",
]
HISTORY_FIELDS = ["market_id", "ts", "price", "resolution_seconds"]


# ----------------------------------------------------------------------------- discovery
def resolve_tags(http: S.Http) -> list[str]:
    ids = []
    for slug in S.TAG_SLUGS:
        try:
            t = http.get(f"{S.GAMMA}/tags/slug/{slug}")
            if isinstance(t, dict) and t.get("id") is not None:
                ids.append(str(t["id"]))
        except S.HttpError as e:
            if e.status not in (404, 422):
                raise
    return ids


def discover(http: S.Http, ts: int) -> list[dict]:
    events: dict[str, dict] = {}
    for tid in resolve_tags(http):
        for ev in S.list_open_events_for_tag(http, tid):
            if S.TITLE_RE.match(ev.get("title") or ""):
                events[str(ev["id"])] = ev
    if not events:
        for ev in S.search_fallback(http):
            events[str(ev["id"])] = ev
    for eid, ev in list(events.items()):
        ms = ev.get("markets") or []
        if not ms or not any(m.get("clobTokenIds") or m.get("positionIds") for m in ms):
            try:
                events[eid] = http.get(f"{S.GAMMA}/events/{eid}")
            except S.HttpError as e:
                print(f"warn: event {eid}: {e}")

    rows = []
    for ev in events.values():
        end_ts = S.parse_iso(ev.get("endDate"))
        info = S.parse_title(ev.get("title") or "", end_ts)
        if not info:
            continue
        for m in ev.get("markets") or []:
            yes, no = S.asset_ids(m)
            label = m.get("groupItemTitle") or m.get("question")
            unit, lo, hi = S.parse_bracket(label)
            if unit is None:
                unit, lo, hi = S.parse_bracket(m.get("question"))
            rows.append({
                "snapshot_ts": ts, "event_id": ev.get("id"), "event_slug": ev.get("slug"),
                "title": ev.get("title"), "kind": info["kind"], "city": info["city"],
                "target_date": info["target_date"], "end_date": ev.get("endDate"),
                "market_id": m.get("id"), "condition_id": m.get("conditionId"),
                "version": m.get("version"), "bracket_label": label, "unit": unit, "low": lo, "high": hi,
                "yes_asset": yes, "no_asset": no, "closed": int(bool(m.get("closed"))),
                "volume": S.fnum(m.get("volumeNum") if m.get("volumeNum") is not None else m.get("volume")),
                "volume_24h": S.fnum(m.get("volume24hr")),
                "liquidity": S.fnum(m.get("liquidityNum") if m.get("liquidityNum") is not None else m.get("liquidity")),
                "gamma_best_bid": S.fnum(m.get("bestBid")), "gamma_best_ask": S.fnum(m.get("bestAsk")),
                "gamma_last_trade": S.fnum(m.get("lastTradePrice")),
                "outcome_prices": json.dumps(S.jload(m.get("outcomePrices"))),
                "winner": S.winner_from(m),
            })
    return rows


# ----------------------------------------------------------------------------- books
def summarize(book: dict, ts: int, market_id: str) -> tuple[Optional[dict], Optional[dict]]:
    bids = sorted(S._levels(book.get("bids")), key=lambda x: -x[0])
    asks = sorted(S._levels(book.get("asks")), key=lambda x: x[0])
    if not bids and not asks:
        return None, None  # empty book (e.g. dead v2 copies): skip, saves space
    bb = bids[0][0] if bids else None
    ba = asks[0][0] if asks else None
    row = {
        "ts": ts, "market_id": market_id,
        "book_ts": int(S.fnum(book.get("timestamp")) or 0) or None,
        "best_bid": bb, "best_ask": ba,
        "best_bid_size": bids[0][1] if bids else None, "best_ask_size": asks[0][1] if asks else None,
        "bid_depth_5c": round(sum(s for p, s in bids if p >= bb - 0.05 - 1e-9), 2) if bids else 0,
        "ask_depth_5c": round(sum(s for p, s in asks if p <= ba + 0.05 + 1e-9), 2) if asks else 0,
        "bid_depth_usd": round(sum(p * s for p, s in bids), 2),
        "ask_depth_usd": round(sum(p * s for p, s in asks), 2),
        "n_bid_levels": len(bids), "n_ask_levels": len(asks),
        "last_trade_price": S.fnum(book.get("last_trade_price")),
    }
    ladder = {"ts": ts, "market_id": market_id, "bids": bids, "asks": asks}
    return row, ladder


def write_csv(path: str, fields: list[str], rows: list[dict]) -> None:
    with gzip.open(path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def cmd_snapshot(out: str, full: bool, http: Optional[S.Http] = None,
                 now: Optional[datetime] = None) -> dict:
    http = http or S.Http()
    now = now or datetime.now(timezone.utc)
    ts = int(now.timestamp())
    stamp = now.strftime("%Y%m%dT%H%M")
    d = os.path.join(out, "data-" + now.strftime("%Y-%m-%d"))
    os.makedirs(d, exist_ok=True)

    markets = discover(http, ts)
    want = {m["yes_asset"]: m["market_id"] for m in markets if m["yes_asset"] and not m["closed"]}

    summaries, ladders = [], []
    for batch in S.chunks(list(want), 400):
        try:
            books = http.post(f"{S.CLOB}/books", [{"token_id": a} for a in batch]) or []
        except S.HttpError as e:
            print(f"warn: books batch failed: {e}")
            continue
        for b in books:
            a = str(b.get("asset_id"))
            if a not in want:
                continue
            row, lad = summarize(b, ts, want[a])
            if row:
                summaries.append(row)
                ladders.append(lad)

    write_csv(os.path.join(d, f"books_{stamp}.csv.gz"), BOOK_FIELDS, summaries)
    if full:
        write_csv(os.path.join(d, f"markets_{stamp}.csv.gz"), MARKET_FIELDS, markets)
        with gzip.open(os.path.join(d, f"ladders_{stamp}.jsonl.gz"), "wt", encoding="utf-8") as f:
            for lad in ladders:
                f.write(json.dumps(lad, separators=(",", ":")) + "\n")
    res = {"markets": len(markets), "assets": len(want), "books": len(summaries), "full": full}
    print(f"snapshot {stamp}: {res}")
    return res


# ----------------------------------------------------------------------------- history
def cmd_history(day: str, inp: str, out: str, http: Optional[S.Http] = None,
                now: Optional[datetime] = None) -> dict:
    http = http or S.Http()
    now = now or datetime.now(timezone.utc)
    D = date.fromisoformat(day)
    assets: dict[str, str] = {}   # yes_asset -> market_id
    for path in glob.glob(os.path.join(inp, "markets_*.csv.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("target_date") == day and r.get("yes_asset"):
                    assets[r["yes_asset"]] = r["market_id"]
    start = int(datetime.combine(D - timedelta(days=4), dtime(0), timezone.utc).timestamp())
    end = min(int(now.timestamp()),
              int(datetime.combine(D + timedelta(days=1), dtime(12), timezone.utc).timestamp()))
    age_days = (now.timestamp() - end) / 86400
    bucket = 60 if age_days < 6 else (300 if age_days < 55 else 1800)

    rows: list[dict] = []
    failed = 0
    for a in sorted(assets):
        cursor = None
        try:
            for _ in range(200):
                params: dict[str, Any] = {"token_id": a}
                if cursor:
                    params["cursor"] = cursor
                else:
                    params.update({"start": start, "end": end, "bucket_seconds": bucket})
                res = http.get(f"{S.DATA}/v2/prices-history", params=params)
                for p in res.get("data") or []:
                    t = p.get("timestamp")
                    if t and t > 10**11:
                        t //= 1000
                    rows.append({"market_id": assets[a], "ts": t, "price": p.get("price"),
                                 "resolution_seconds": p.get("resolution_seconds")})
                cursor = (res.get("pagination") or {}).get("next_cursor")
                if not cursor:
                    break
        except S.HttpError as e:
            failed += 1
            print(f"warn: history {a}: {e}")
    d = os.path.join(out, f"data-{day}")
    os.makedirs(d, exist_ok=True)
    write_csv(os.path.join(d, f"history_{day.replace('-', '')}.csv.gz"), HISTORY_FIELDS, rows)
    res = {"assets": len(assets), "points": len(rows), "failed": failed, "bucket": bucket}
    print(f"history {day}: {res}")
    return res


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("snapshot")
    s.add_argument("--out", default="out")
    s.add_argument("--full", action="store_true")
    h = sub.add_parser("history")
    h.add_argument("--day", required=True, help="YYYY-MM-DD target date to archive")
    h.add_argument("--inp", default="in")
    h.add_argument("--out", default="out")
    a = ap.parse_args()
    if a.cmd == "snapshot":
        cmd_snapshot(a.out, a.full)
    else:
        cmd_history(a.day, a.inp, a.out)


if __name__ == "__main__":
    main()
