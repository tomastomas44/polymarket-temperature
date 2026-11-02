# Polymarket temperature scraper — GitHub Actions version

Free, no server, no credit card. GitHub runs the scraper for you and stores the data as
**Releases** (one per UTC day, e.g. `data-2026-11-02`).

## What it collects

| File (in each day's release) | When | Contents |
|---|---|---|
| `books_<time>.csv.gz` | every ~5 min | per bracket: best bid/ask, sizes, depth within 5¢ of best price, total depth ($), number of levels, last trade |
| `markets_<time>.csv.gz` | hourly | every bracket: city, high/low, target date, bracket bounds, version (v1/v2), volume, liquidity, resolution once settled |
| `ladders_<time>.jsonl.gz` | hourly | full order books (every price level) |
| `history_<date>.csv.gz` | daily, 2 days after the target date | 1-minute price series of every bracket of that day (Polymarket deletes 1-min data after ~7 days) |

Not collected here (recoverable later, so we fetch them once at analysis time): **trades** and **final resolutions**.

Expected volume: ~50–100 MB/day, stored in GitHub Releases (doesn't fill the repo).

## Files

| File | Purpose |
|---|---|
| `gh_scraper.py` | the collector (`snapshot`, `history`) |
| `scraper.py` | shared helpers (same file as the server version) |
| `.github/workflows/snapshot.yml` | runs `snapshot` every 5 min |
| `.github/workflows/history.yml` | runs `history` daily at 04:17 UTC |
| `download_data.py` | downloads all releases to your Mac |
| `load_data.py` | loads them into pandas |
| `test_gh_scraper.py` | offline test |

## Setup

### 1. Quick live test on your Mac (2 min)

Copy `gh_scraper.py`, `load_data.py`, `download_data.py`, `test_gh_scraper.py` into `~/Desktop/thesis-scraper` (next to `scraper.py`). Then:

```bash
cd ~/Desktop/thesis-scraper
source venv/bin/activate
pip install pandas
python test_gh_scraper.py                       # ALL GH TESTS PASSED
python gh_scraper.py snapshot --out out --full  # real API
```

It should print something like `{'markets': 3486, 'assets': 3412, 'books': 2800, 'full': True}`.
Then `rm -rf out`.

### 2. Create the repository (5 min)

1. Sign in at **github.com** (create a free account if needed).
2. **+** (top right) → **New repository**
   - Name: `polymarket-temperature`
   - **Public** (required: public repos get unlimited free Actions minutes; private ones would run out in a week)
   - Don't add a README → **Create repository**
3. Click **uploading an existing file** → drag in:
   `gh_scraper.py`, `scraper.py`, `requirements.txt`, `download_data.py`, `load_data.py`, `test_gh_scraper.py`, `README.md`
   → **Commit changes**.
4. Create the two workflows (the `.github` folder is hidden on Mac, so create them in the browser):
   - **Add file → Create new file**
   - Name: `.github/workflows/snapshot.yml` (typing `/` creates the folders)
   - Paste the full content of `snapshot.yml` (open it in VS Code, `Cmd+A`, `Cmd+C`) → **Commit changes**
   - Repeat with `.github/workflows/history.yml`.

### 3. Allow it to write releases (1 min)

Repo **Settings → Actions → General → Workflow permissions** → **Read and write permissions** → **Save**.

### 4. First run (2 min)

1. **Actions** tab. If asked, click *I understand my workflows, go ahead and enable them*.
2. Left: **snapshot** → **Run workflow** → **Run workflow**.
3. Wait ~1 min for a green ✓. If red ✗, click it and send the error.
4. Repo home page → right sidebar **Releases** → `data-<today>` should contain `books_…`, `markets_…`, `ladders_…`.

From now on it runs by itself every ~5 minutes.

## Daily check (30 s)

Open **Actions**: runs should be green. Open today's **release**: the number of `books_` files should grow through the day.
GitHub often delays scheduled runs (typically 5–15 min, more around the full hour), so expect ~100–288 per day, not exactly 288. The real timestamp of every snapshot is in the file name and the `ts` column, so irregular spacing is fine for analysis.

## Get the data onto your Mac

```bash
cd ~/Desktop/thesis-scraper
source venv/bin/activate
python download_data.py <your-github-username>/polymarket-temperature
```

Re-run anytime; it only downloads new files. Then in Python / Jupyter:

```python
from load_data import load
markets, books, history = load("data")
# join: books.market_id / history.market_id -> markets.market_id
df = books.merge(markets[["market_id", "city", "kind", "target_date", "low", "high", "unit", "version"]], on="market_id")
```

## Limits to know

- **Timing is best-effort.** GitHub may delay or occasionally skip scheduled runs. Fine for a thesis; mention it in your data section.
- **The data is public** (public repo). It's public Polymarket data anyway.
- **Scheduled workflows stop after 60 days without commits** in public repos. The daily `history` job makes a tiny "heartbeat" commit to prevent that.
- If something breaks, GitHub emails you when a workflow fails.
