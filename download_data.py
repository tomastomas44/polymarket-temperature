#!/usr/bin/env python3
"""
Download all data files from the GitHub releases of this repo into ./data/<release>/.
Skips files you already have, so just re-run it whenever you want an update.

  python download_data.py <github-username>/<repo-name>

Optional: set GITHUB_TOKEN to avoid the 60 requests/hour limit for anonymous users.
"""
import os
import sys

import requests

repo = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("REPO", "")
if "/" not in repo:
    sys.exit("usage: python download_data.py <user>/<repo>")
s = requests.Session()
if os.environ.get("GITHUB_TOKEN"):
    s.headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"

new = 0
page = 1
while True:
    r = s.get(f"https://api.github.com/repos/{repo}/releases", params={"per_page": 100, "page": page}, timeout=30)
    r.raise_for_status()
    rels = r.json()
    if not rels:
        break
    for rel in rels:
        tag = rel["tag_name"]
        if not tag.startswith("data-"):
            continue
        d = os.path.join("data", tag)
        os.makedirs(d, exist_ok=True)
        # a day holds ~300 files: page through the release's asset list
        assets, ap = [], 1
        while True:
            ra = s.get(rel["assets_url"], params={"per_page": 100, "page": ap}, timeout=30)
            ra.raise_for_status()
            batch = ra.json()
            assets += batch
            if len(batch) < 100:
                break
            ap += 1
        for a in assets:
            path = os.path.join(d, a["name"])
            if os.path.exists(path) and os.path.getsize(path) == a["size"]:
                continue
            with s.get(a["browser_download_url"], stream=True, timeout=120) as resp:
                resp.raise_for_status()
                with open(path + ".part", "wb") as f:
                    for chunk in resp.iter_content(1 << 16):
                        f.write(chunk)
            os.replace(path + ".part", path)
            new += 1
        print(f"{tag}: {len(assets)} files")
    page += 1
print(f"done, {new} new files")
