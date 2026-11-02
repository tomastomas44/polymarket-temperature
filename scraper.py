#!/usr/bin/env python3
"""
Polymarket daily-temperature market scraper (thesis data collection).

Collects, for every "Highest/Lowest temperature in <city> on <date>?" event:
  * market metadata, brackets and final resolution          (Gamma API)
  * volume / liquidity snapshots                             every 30 min
  * FULL order-book depth of every open bracket              every 5 min (CLOB /books)
  * all trades                                               hourly sync (Data API v2)
  * 1-minute price history, archived right after closing     (Data API v2; expires after ~7 days)

Storage: one SQLite file (WAL mode). Order-book ladders are de-duplicated by book hash
and stored zlib-compressed, so unchanged books cost ~100 bytes per snapshot.

Usage:
  python scraper.py discover     # one discovery pass, prints what it found (run this first)
  python scraper.py once         # one full cycle of every job, then exit
  python scraper.py run          # run forever (use with systemd)
  python scraper.py stats        # row counts / sanity check

Config via environment variables (all optional): see CONFIG below.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import signal
import sqlite3
import sys
import time
import zlib
from datetime import date, datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, Iterable, Optional

import requests

# --------------------------------------------------------------------------- CONFIG
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
DATA = "https://data-api.polymarket.com"

DB_PATH = os.environ.get("PM_DB", "temperature.db")
LOG_PATH = os.environ.get("PM_LOG", "scraper.log")
BOOK_INTERVAL = int(os.environ.get("PM_BOOK_INTERVAL", 300))         # 5 min
DISCOVERY_INTERVAL = int(os.environ.get("PM_DISCOVERY_INTERVAL", 1800))  # 30 min
TRADES_INTERVAL = int(os.environ.get("PM_TRADES_INTERVAL", 3600))    # 1 h
FINALIZE_INTERVAL = int(os.environ.get("PM_FINALIZE_INTERVAL", 3600))  # 1 h
# Polymarket books for YES and NO are mirror images; set to 1 to store both anyway.
BOOK_BOTH_SIDES = os.environ.get("PM_BOOK_BOTH_SIDES", "0") == "1"
# Tag slugs tried (in order) to find weather/temperature events.
TAG_SLUGS = [s.strip() for s in os.environ.get(
    "PM_TAG_SLUGS", "daily-temperature,temperature,weather").split(",") if s.strip()]

TITLE_RE = re.compile(
    r"^\s*(highest|lowest)\s+temperature\s+in\s+(.+?)\s+on\s+(.+?)\s*\??\s*$", re.I)
USER_AGENT = "thesis-temperature-scraper/1.0 (academic research, ULB Solvay)"

log = logging.getLogger("pm")
_STOP = False


# --------------------------------------------------------------------------- utils
def now_ts() -> int:
    return int(time.time())


def iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    try:
        s = s.strip().replace("Z", "+00:00")
        if "T" not in s and " " not in s and len(s) == 10:
            s += "T00:00:00+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        return None


def jload(v: Any) -> Any:
    """Gamma returns some arrays as JSON-encoded strings ('["Yes","No"]')."""
    if isinstance(v, str):
        try:
            return json.loads(v)
        except ValueError:
            return None
    return v


def fnum(v: Any) -> Optional[float]:
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None


def chunks(seq: list, n: int) -> Iterable[list]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# --------------------------------------------------------------------------- parsing
_RANGE_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*(?:-|–|to)\s*(-?\d+(?:\.\d+)?)\s*[°º]?\s*([CF])\b", re.I)
_SINGLE_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*[°º]?\s*([CF])\b", re.I)
_UP_RE = re.compile(r"or\s+(higher|above|more|greater)", re.I)
_DOWN_RE = re.compile(r"or\s+(lower|below|less|under)", re.I)
_MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], 1)}


def parse_bracket(text: Optional[str]) -> tuple[Optional[str], Optional[float], Optional[float]]:
    """'between 94-95°F' -> ('F',94,95); '80°F or higher' -> ('F',80,None);
    '23°C or below' -> ('C',None,23); '12°C' -> ('C',12,12). Bounds are inclusive integers
    as written in the market; open ends are None."""
    if not text:
        return None, None, None
    m = _RANGE_RE.search(text)
    if m:
        a, b = float(m.group(1)), float(m.group(2))
        return m.group(3).upper(), min(a, b), max(a, b)
    m = _SINGLE_RE.search(text)
    if not m:
        return None, None, None
    v, unit = float(m.group(1)), m.group(2).upper()
    if _UP_RE.search(text):
        return unit, v, None
    if _DOWN_RE.search(text):
        return unit, None, v
    return unit, v, v


def parse_title(title: str, end_ts: Optional[int]) -> Optional[dict]:
    m = TITLE_RE.match(title or "")
    if not m:
        return None
    kind, city, date_txt = m.group(1).lower(), m.group(2).strip(), m.group(3).strip()
    target = None
    dm = re.match(r"([A-Za-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?", date_txt)
    if dm and dm.group(1).lower() in _MONTHS:
        month, day = _MONTHS[dm.group(1).lower()], int(dm.group(2))
        if dm.group(3):
            years = [int(dm.group(3))]
        else:
            ref = datetime.fromtimestamp(end_ts or now_ts(), tz=timezone.utc).date()
            years = [ref.year - 1, ref.year, ref.year + 1]
            ref_ord = ref.toordinal()
            years.sort(key=lambda y: abs(_safe_ord(y, month, day) - ref_ord))
        try:
            target = date(years[0], month, day).isoformat()
        except ValueError:
            target = None
    return {"kind": kind, "city": city, "target_date": target}


def _safe_ord(y: int, m: int, d: int) -> int:
    try:
        return date(y, m, d).toordinal()
    except ValueError:
        return 10**9


# --------------------------------------------------------------------------- HTTP
class HttpError(Exception):
    def __init__(self, status: int, url: str, body: str):
        super().__init__(f"HTTP {status} {url} :: {body[:300]}")
        self.status = status


class Http:
    def __init__(self) -> None:
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

    def request(self, method: str, url: str, **kw: Any) -> Any:
        last = ""
        for attempt in range(6):
            try:
                r = self.s.request(method, url, timeout=30, **kw)
            except requests.RequestException as e:
                last = str(e)
                time.sleep(min(60, 2 ** attempt))
                continue
            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("Retry-After", "")
                try:
                    wait = float(ra)
                except ValueError:
                    wait = min(60, 2 ** attempt)
                last = f"{r.status_code}"
                log.warning("HTTP %s on %s, retrying in %.0fs", r.status_code, url, wait)
                time.sleep(wait)
                continue
            if r.status_code >= 400:
                raise HttpError(r.status_code, url, r.text)
            return r.json()
        raise HttpError(-1, url, f"gave up after retries ({last})")

    def get(self, url: str, params: Any = None) -> Any:
        return self.request("GET", url, params=params)

    def post(self, url: str, body: Any) -> Any:
        return self.request("POST", url, json=body)


# --------------------------------------------------------------------------- DB
SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);

CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  slug TEXT, title TEXT,
  kind TEXT,               -- 'highest' | 'lowest'
  city TEXT,
  target_date TEXT,        -- YYYY-MM-DD parsed from title
  start_ts INTEGER, end_ts INTEGER,
  closed INTEGER DEFAULT 0,
  resolved_ts INTEGER,     -- when we first saw it fully resolved
  trades_done INTEGER DEFAULT 0,
  history_done INTEGER DEFAULT 0,
  first_seen INTEGER, last_seen INTEGER,
  raw_json TEXT
);

CREATE TABLE IF NOT EXISTS markets (
  market_id TEXT PRIMARY KEY,
  event_id TEXT,
  condition_id TEXT,
  question TEXT, bracket_label TEXT,
  version TEXT,
  yes_asset TEXT, no_asset TEXT,
  unit TEXT, low REAL, high REAL,   -- inclusive bracket bounds; NULL = open-ended
  neg_risk INTEGER,
  closed INTEGER DEFAULT 0,
  outcome_prices TEXT,
  winner INTEGER,                   -- 1 = YES won, 0 = NO won, NULL = unresolved
  resolution_status TEXT,
  first_seen INTEGER, last_seen INTEGER,
  raw_json TEXT
);
CREATE INDEX IF NOT EXISTS ix_markets_event ON markets(event_id);
CREATE INDEX IF NOT EXISTS ix_markets_yes ON markets(yes_asset);

CREATE TABLE IF NOT EXISTS market_stats (
  ts INTEGER, market_id TEXT,
  volume REAL, volume_24h REAL, liquidity REAL,
  best_bid REAL, best_ask REAL, last_trade_price REAL,
  PRIMARY KEY (ts, market_id)
);

CREATE TABLE IF NOT EXISTS book_snapshots (
  ts INTEGER,              -- our snapshot time (epoch s, aligned to the 5-min grid)
  asset_id TEXT,
  market_id TEXT,
  side TEXT,               -- 'YES' | 'NO'
  book_ts INTEGER,         -- exchange timestamp of the book (epoch ms)
  hash TEXT,               -- key into book_levels
  best_bid REAL, best_ask REAL,
  best_bid_size REAL, best_ask_size REAL,
  bid_depth_shares REAL, ask_depth_shares REAL,
  bid_depth_usd REAL, ask_depth_usd REAL,
  n_bid_levels INTEGER, n_ask_levels INTEGER,
  last_trade_price REAL,
  tick_size REAL,
  PRIMARY KEY (ts, asset_id)
);
CREATE INDEX IF NOT EXISTS ix_books_asset ON book_snapshots(asset_id, ts);

CREATE TABLE IF NOT EXISTS book_levels (
  hash TEXT PRIMARY KEY,
  bids BLOB, asks BLOB     -- zlib(JSON [[price,size],...]) best price first
);

CREATE TABLE IF NOT EXISTS trades (
  transaction_hash TEXT, token_id TEXT, condition_id TEXT,
  proxy_wallet TEXT, side TEXT, outcome TEXT, outcome_index INTEGER,
  price REAL, size REAL, timestamp INTEGER,
  event_slug TEXT, slug TEXT,
  UNIQUE (transaction_hash, token_id, proxy_wallet, side, price, size, timestamp)
);
CREATE INDEX IF NOT EXISTS ix_trades_token ON trades(token_id, timestamp);

CREATE TABLE IF NOT EXISTS price_history (
  asset_id TEXT, ts INTEGER, price REAL, resolution_seconds INTEGER,
  PRIMARY KEY (asset_id, ts, resolution_seconds)
);

CREATE TABLE IF NOT EXISTS job_runs (
  ts INTEGER, job TEXT, n INTEGER, seconds REAL, error TEXT
);
"""


def connect(path: str = DB_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=60)
    con.executescript(SCHEMA)
    return con


def meta_get(con: sqlite3.Connection, key: str) -> Optional[str]:
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def meta_set(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value))


# --------------------------------------------------------------------------- discovery
def resolve_tag_ids(http: Http, con: sqlite3.Connection) -> list[str]:
    cached = meta_get(con, "tag_ids")
    if cached:
        return json.loads(cached)
    ids = []
    for slug in TAG_SLUGS:
        try:
            t = http.get(f"{GAMMA}/tags/slug/{slug}")
            if isinstance(t, dict) and t.get("id") is not None:
                ids.append(str(t["id"]))
                log.info("tag '%s' -> id %s", slug, t["id"])
        except HttpError as e:
            if e.status not in (404, 422):
                raise
            log.info("tag '%s' not found", slug)
    if ids:
        meta_set(con, "tag_ids", json.dumps(ids))
        con.commit()
    return ids


def list_open_events_for_tag(http: Http, tag_id: str) -> list[dict]:
    out, cursor = [], None
    for _ in range(200):  # hard cap on pages
        params: list[tuple[str, str]] = [("tag_id", tag_id), ("closed", "false"), ("limit", "100"),
                                         ("version", "v1"), ("version", "v2")]
        if cursor:
            params.append(("after_cursor", cursor))
        res = http.get(f"{GAMMA}/events/keyset", params=params)
        if isinstance(res, list):          # tolerate the older list-shaped response
            out.extend(res)
            break
        out.extend(res.get("events") or [])
        cursor = res.get("next_cursor")
        if not cursor:
            break
    return out


def search_fallback(http: Http) -> list[dict]:
    """Used only if tag lookup finds nothing: free-text search, then fetch full events."""
    ids = set()
    for q in ("highest temperature", "lowest temperature"):
        try:
            res = http.get(f"{GAMMA}/public-search", params={"q": q})
        except HttpError as e:
            log.warning("search fallback failed: %s", e)
            continue
        for ev in (res or {}).get("events") or []:
            if TITLE_RE.match(ev.get("title") or "") and not ev.get("closed"):
                ids.add(str(ev["id"]))
    events = []
    for eid in ids:
        try:
            events.append(http.get(f"{GAMMA}/events/{eid}"))
        except HttpError as e:
            log.warning("event %s: %s", eid, e)
    return events


def asset_ids(m: dict) -> tuple[Optional[str], Optional[str]]:
    """Return (yes_asset, no_asset). V2 markets use positionIds, CTF (v1) use clobTokenIds."""
    ids = jload(m.get("positionIds")) if m.get("version") == "v2" else None
    if not ids:
        ids = jload(m.get("clobTokenIds")) or jload(m.get("positionIds"))
    if not ids or len(ids) < 2:
        return None, None
    outcomes = jload(m.get("outcomes")) or ["Yes", "No"]
    try:
        yes_i = [str(o).lower() for o in outcomes].index("yes")
    except ValueError:
        yes_i = 0
    return str(ids[yes_i]), str(ids[1 - yes_i])


def winner_from(m: dict) -> Optional[int]:
    if not m.get("closed"):
        return None
    prices = jload(m.get("outcomePrices"))
    outcomes = jload(m.get("outcomes")) or ["Yes", "No"]
    if not prices or len(prices) < 2:
        return None
    try:
        p = [float(x) for x in prices]
    except (TypeError, ValueError):
        return None
    try:
        yes_i = [str(o).lower() for o in outcomes].index("yes")
    except ValueError:
        yes_i = 0
    if p[yes_i] >= 0.99:
        return 1
    if p[yes_i] <= 0.01:
        return 0
    return None  # not (yet) settled to 0/1


def upsert_event(con: sqlite3.Connection, ev: dict, ts: int) -> Optional[str]:
    end_ts = parse_iso(ev.get("endDate"))
    info = parse_title(ev.get("title") or "", end_ts)
    if not info:
        return None
    eid = str(ev["id"])
    markets = ev.get("markets") or []
    all_closed = bool(markets) and all(m.get("closed") for m in markets)
    con.execute(
        """INSERT INTO events(event_id,slug,title,kind,city,target_date,start_ts,end_ts,closed,
                              first_seen,last_seen,raw_json)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(event_id) DO UPDATE SET
             slug=excluded.slug, title=excluded.title, end_ts=excluded.end_ts,
             start_ts=COALESCE(events.start_ts, excluded.start_ts),
             closed=excluded.closed, last_seen=excluded.last_seen, raw_json=excluded.raw_json""",
        (eid, ev.get("slug"), ev.get("title"), info["kind"], info["city"], info["target_date"],
         parse_iso(ev.get("startDate") or ev.get("createdAt")), end_ts,
         int(bool(ev.get("closed")) or all_closed), ts, ts, json.dumps(ev)))

    resolved = bool(markets)
    for m in markets:
        mid = str(m.get("id"))
        yes, no = asset_ids(m)
        label = m.get("groupItemTitle") or m.get("question")
        unit, lo, hi = parse_bracket(label)
        if unit is None:
            unit, lo, hi = parse_bracket(m.get("question"))
        win = winner_from(m)
        if win is None:
            resolved = False
        con.execute(
            """INSERT INTO markets(market_id,event_id,condition_id,question,bracket_label,version,
                                   yes_asset,no_asset,unit,low,high,neg_risk,closed,outcome_prices,
                                   winner,resolution_status,first_seen,last_seen,raw_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(market_id) DO UPDATE SET
                 closed=excluded.closed, outcome_prices=excluded.outcome_prices,
                 winner=COALESCE(excluded.winner, markets.winner),
                 resolution_status=excluded.resolution_status,
                 yes_asset=COALESCE(excluded.yes_asset, markets.yes_asset),
                 no_asset=COALESCE(excluded.no_asset, markets.no_asset),
                 last_seen=excluded.last_seen, raw_json=excluded.raw_json""",
            (mid, eid, m.get("conditionId"), m.get("question"), label, m.get("version"),
             yes, no, unit, lo, hi, int(bool(m.get("negRisk"))), int(bool(m.get("closed"))),
             json.dumps(jload(m.get("outcomePrices"))), win,
             m.get("resolutionStatus") or m.get("umaResolutionStatus"), ts, ts, json.dumps(m)))
        con.execute(
            """INSERT OR IGNORE INTO market_stats VALUES(?,?,?,?,?,?,?,?)""",
            (ts, mid, fnum(m.get("volumeNum") if m.get("volumeNum") is not None else m.get("volume")),
             fnum(m.get("volume24hr")),
             fnum(m.get("liquidityNum") if m.get("liquidityNum") is not None else m.get("liquidity")),
             fnum(m.get("bestBid")), fnum(m.get("bestAsk")), fnum(m.get("lastTradePrice"))))
    if resolved:
        con.execute("UPDATE events SET resolved_ts=COALESCE(resolved_ts, ?), closed=1 WHERE event_id=?",
                    (ts, eid))
    return eid


def job_discover(http: Http, con: sqlite3.Connection, verbose: bool = False) -> int:
    ts = now_ts()
    tag_ids = resolve_tag_ids(http, con)
    raw: dict[str, dict] = {}
    for tid in tag_ids:
        for ev in list_open_events_for_tag(http, tid):
            if TITLE_RE.match(ev.get("title") or ""):
                raw[str(ev["id"])] = ev
    if not raw:
        log.warning("no temperature events via tags %s; trying search fallback", tag_ids)
        for ev in search_fallback(http):
            raw[str(ev["id"])] = ev
    # Listing responses may trim market fields; fetch the full event when markets lack token ids.
    for eid, ev in list(raw.items()):
        ms = ev.get("markets") or []
        if not ms or not any(m.get("clobTokenIds") or m.get("positionIds") for m in ms):
            try:
                raw[eid] = http.get(f"{GAMMA}/events/{eid}")
            except HttpError as e:
                log.warning("event %s: %s", eid, e)
    n = 0
    for ev in raw.values():
        if upsert_event(con, ev, ts):
            n += 1
    con.commit()
    if verbose:
        rows = con.execute(
            """SELECT e.city, e.kind, e.target_date, COUNT(m.market_id), e.title
               FROM events e JOIN markets m USING(event_id)
               WHERE e.closed=0 GROUP BY e.event_id ORDER BY e.city, e.target_date""").fetchall()
        print(f"\nOpen temperature events found: {len(rows)}  (tag ids used: {tag_ids})\n")
        for city, kind, d, nm, title in rows:
            print(f"  {city:<18} {kind:<8} {d}  {nm:>2} brackets   {title}")
    return n


# --------------------------------------------------------------------------- order books
def _levels(side: list) -> list[list[float]]:
    out = []
    for lv in side or []:
        p, s = fnum(lv.get("price")), fnum(lv.get("size"))
        if p is not None and s is not None:
            out.append([p, s])
    return out


def store_book(con: sqlite3.Connection, ts: int, book: dict, market_id: str, side: str) -> None:
    bids = sorted(_levels(book.get("bids")), key=lambda x: -x[0])   # best (highest) first
    asks = sorted(_levels(book.get("asks")), key=lambda x: x[0])    # best (lowest) first
    payload_b = json.dumps(bids, separators=(",", ":"))
    payload_a = json.dumps(asks, separators=(",", ":"))
    h = book.get("hash") or hashlib.sha1((payload_b + "|" + payload_a).encode()).hexdigest()
    # Our own content hash guards against an exchange hash that also covers timestamps.
    content_key = hashlib.sha1((payload_b + "|" + payload_a).encode()).hexdigest()
    con.execute("INSERT OR IGNORE INTO book_levels(hash,bids,asks) VALUES(?,?,?)",
                (content_key, zlib.compress(payload_b.encode()), zlib.compress(payload_a.encode())))
    con.execute(
        """INSERT OR REPLACE INTO book_snapshots VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (ts, str(book.get("asset_id")), market_id, side, int(fnum(book.get("timestamp")) or 0) or None,
         content_key,
         bids[0][0] if bids else None, asks[0][0] if asks else None,
         bids[0][1] if bids else None, asks[0][1] if asks else None,
         sum(s for _, s in bids), sum(s for _, s in asks),
         round(sum(p * s for p, s in bids), 4), round(sum(p * s for p, s in asks), 4),
         len(bids), len(asks), fnum(book.get("last_trade_price")), fnum(book.get("tick_size"))))
    _ = h  # exchange hash not stored separately; content_key is deterministic


def job_books(http: Http, con: sqlite3.Connection, ts: Optional[int] = None) -> int:
    ts = ts or now_ts()
    rows = con.execute(
        """SELECT m.market_id, m.yes_asset, m.no_asset FROM markets m JOIN events e USING(event_id)
           WHERE e.closed=0 AND m.closed=0 AND m.yes_asset IS NOT NULL""").fetchall()
    want: dict[str, tuple[str, str]] = {}
    for mid, yes, no in rows:
        want[yes] = (mid, "YES")
        if BOOK_BOTH_SIDES and no:
            want[no] = (mid, "NO")
    n = 0
    for batch in chunks(list(want), 400):
        try:
            books = http.post(f"{CLOB}/books", [{"token_id": a} for a in batch])
        except HttpError as e:
            log.warning("books batch failed (%d assets): %s", len(batch), e)
            continue
        for b in books or []:
            a = str(b.get("asset_id"))
            if a in want:
                store_book(con, ts, b, *want[a])
                n += 1
    con.commit()
    return n


# --------------------------------------------------------------------------- trades
def job_trades(http: Http, con: sqlite3.Connection) -> int:
    """Re-sync trades for open events and events that closed recently. Idempotent."""
    ts = now_ts()
    eids = [r[0] for r in con.execute(
        """SELECT event_id FROM events WHERE trades_done=0
           AND (end_ts IS NULL OR end_ts > ? )""", (ts - 30 * 86400,)).fetchall()]
    n = 0
    for batch in chunks(eids, 20):
        cursor: Optional[str] = None
        for _ in range(500):
            params = {"event_id": ",".join(batch)}
            if cursor:
                params["cursor"] = cursor
            else:
                params["limit"] = 1000
            try:
                res = http.get(f"{DATA}/v2/trades", params=params)
            except HttpError as e:
                log.warning("trades %s: %s", batch, e)
                break
            for t in res.get("data") or []:
                cur = con.execute(
                    """INSERT OR IGNORE INTO trades VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (t.get("transaction_hash"), t.get("token_id"), t.get("condition_id"),
                     t.get("proxy_wallet"), t.get("side"), t.get("outcome"), t.get("outcome_index"),
                     fnum(t.get("price")), fnum(t.get("size")), t.get("timestamp"),
                     t.get("event_slug"), t.get("slug")))
                n += cur.rowcount
            cursor = (res.get("pagination") or {}).get("next_cursor")
            if not cursor:
                break
        con.commit()
    # Mark trades complete once an event has been resolved for > 6 h (this pass was the final sync).
    con.execute("UPDATE events SET trades_done=1 WHERE resolved_ts IS NOT NULL AND resolved_ts < ?",
                (ts - 6 * 3600,))
    con.commit()
    return n


# --------------------------------------------------------------------------- finalize
def job_finalize(http: Http, con: sqlite3.Connection) -> int:
    """Refresh events past their end date until resolved, then archive fine price history."""
    ts = now_ts()
    n = 0
    pending = [r[0] for r in con.execute(
        """SELECT event_id FROM events WHERE resolved_ts IS NULL AND end_ts IS NOT NULL
           AND end_ts < ? AND end_ts > ?""", (ts, ts - 30 * 86400)).fetchall()]
    for eid in pending:
        try:
            ev = http.get(f"{GAMMA}/events/{eid}")
            upsert_event(con, ev, ts)
            n += 1
        except HttpError as e:
            log.warning("finalize event %s: %s", eid, e)
    con.commit()

    todo = con.execute(
        """SELECT event_id, start_ts, end_ts, resolved_ts FROM events
           WHERE history_done=0 AND resolved_ts IS NOT NULL""").fetchall()
    for eid, start_ts, end_ts, resolved_ts in todo:
        stop = min(ts, max(end_ts or resolved_ts, resolved_ts) + 3600)
        begin = max(start_ts or 0, stop - 15 * 86400 + 60)   # API: explicit ranges <= 15 days
        age = ts - stop
        bucket = 60 if age < 6 * 86400 else (300 if age < 55 * 86400 else 1800)
        assets = [r[0] for r in con.execute(
            "SELECT yes_asset FROM markets WHERE event_id=? AND yes_asset IS NOT NULL", (eid,))]
        ok = True
        for a in assets:
            try:
                n += fetch_price_history(http, con, a, begin, stop, bucket)
            except HttpError as e:
                log.warning("price history %s: %s", a, e)
                ok = False
        if ok:
            con.execute("UPDATE events SET history_done=1 WHERE event_id=?", (eid,))
        con.commit()
    return n


def fetch_price_history(http: Http, con: sqlite3.Connection, asset: str,
                        start: int, end: int, bucket: int) -> int:
    n, cursor = 0, None
    for _ in range(200):
        params: dict[str, Any] = {"token_id": asset}
        if cursor:
            params["cursor"] = cursor
        else:
            params.update({"start": int(start), "end": int(end), "bucket_seconds": bucket})
        res = http.get(f"{DATA}/v2/prices-history", params=params)
        for p in res.get("data") or []:
            t = p.get("timestamp")
            if t and t > 10**11:  # tolerate ms
                t //= 1000
            cur = con.execute("INSERT OR IGNORE INTO price_history VALUES(?,?,?,?)",
                              (asset, t, fnum(p.get("price")), p.get("resolution_seconds")))
            n += cur.rowcount
        cursor = (res.get("pagination") or {}).get("next_cursor")
        if not cursor:
            break
    return n


# --------------------------------------------------------------------------- runner
def run_job(con: sqlite3.Connection, name: str, fn: Any, *args: Any) -> None:
    t0 = time.time()
    err = None
    n = 0
    try:
        n = fn(*args)
    except Exception as e:  # never let one job kill the loop
        err = f"{type(e).__name__}: {e}"
        log.exception("job %s failed", name)
        try:
            con.rollback()
        except sqlite3.Error:
            pass
    dt = time.time() - t0
    con.execute("INSERT INTO job_runs VALUES(?,?,?,?,?)", (now_ts(), name, n, round(dt, 2), err))
    con.commit()
    log.info("%-9s n=%-6s %.1fs%s", name, n, dt, f"  ERROR {err}" if err else "")


def _handle_stop(*_: Any) -> None:
    global _STOP
    _STOP = True
    log.info("stop requested")


def run_forever(http: Http, con: sqlite3.Connection) -> None:
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    nxt = {"discover": 0.0, "trades": time.time() + 120, "finalize": time.time() + 300}
    next_book = (now_ts() // BOOK_INTERVAL + 1) * BOOK_INTERVAL
    run_job(con, "discover", job_discover, http, con)
    nxt["discover"] = time.time() + DISCOVERY_INTERVAL
    log.info("running; first book snapshot at %s", iso(next_book))
    while not _STOP:
        now = time.time()
        if now >= next_book:
            slot = int(next_book)
            run_job(con, "books", job_books, http, con, slot)
            next_book = (now_ts() // BOOK_INTERVAL + 1) * BOOK_INTERVAL
            continue
        if now >= nxt["discover"]:
            run_job(con, "discover", job_discover, http, con)
            nxt["discover"] = time.time() + DISCOVERY_INTERVAL
        elif now >= nxt["trades"]:
            run_job(con, "trades", job_trades, http, con)
            nxt["trades"] = time.time() + TRADES_INTERVAL
        elif now >= nxt["finalize"]:
            run_job(con, "finalize", job_finalize, http, con)
            nxt["finalize"] = time.time() + FINALIZE_INTERVAL
        else:
            time.sleep(min(5.0, max(0.5, next_book - time.time())))
    log.info("stopped cleanly")


def print_stats(con: sqlite3.Connection) -> None:
    q = lambda s: con.execute(s).fetchone()[0]  # noqa: E731
    print(f"events          {q('SELECT COUNT(*) FROM events')}  (open {q('SELECT COUNT(*) FROM events WHERE closed=0')},"
          f" resolved {q('SELECT COUNT(*) FROM events WHERE resolved_ts IS NOT NULL')})")
    print(f"cities          {q('SELECT COUNT(DISTINCT city) FROM events')}")
    print(f"markets         {q('SELECT COUNT(*) FROM markets')}")
    print(f"book snapshots  {q('SELECT COUNT(*) FROM book_snapshots')}  (distinct ladders {q('SELECT COUNT(*) FROM book_levels')})")
    print(f"trades          {q('SELECT COUNT(*) FROM trades')}")
    print(f"price history   {q('SELECT COUNT(*) FROM price_history')}")
    last = con.execute("SELECT MAX(ts) FROM book_snapshots").fetchone()[0]
    print(f"last snapshot   {iso(last)}")
    print("\nlast job runs:")
    for r in con.execute("SELECT ts, job, n, seconds, error FROM job_runs ORDER BY ts DESC LIMIT 10"):
        print(f"  {iso(r[0])}  {r[1]:<9} n={r[2]:<6} {r[3]}s {r[4] or ''}")
    size = os.path.getsize(DB_PATH) / 1e6 if os.path.exists(DB_PATH) else 0
    print(f"\nDB size         {size:.1f} MB")


def setup_logging(to_file: bool) -> None:
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%S")
    h: logging.Handler = logging.StreamHandler(sys.stdout)
    h.setFormatter(fmt)
    log.addHandler(h)
    if to_file:
        fh = RotatingFileHandler(LOG_PATH, maxBytes=10_000_000, backupCount=5)
        fh.setFormatter(fmt)
        log.addHandler(fh)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["discover", "once", "run", "stats"])
    args = ap.parse_args()
    setup_logging(to_file=args.cmd == "run")
    con = connect()
    if args.cmd == "stats":
        print_stats(con)
        return
    http = Http()
    if args.cmd == "discover":
        job_discover(http, con, verbose=True)
    elif args.cmd == "once":
        for name, fn in (("discover", job_discover), ("books", job_books),
                         ("trades", job_trades), ("finalize", job_finalize)):
            run_job(con, name, fn, http, con)
        print_stats(con)
    else:
        run_forever(http, con)


if __name__ == "__main__":
    main()
