#!/usr/bin/env python3
"""Fetch the CV/site database from nstarman.github.io.

That repo is the single source of truth: one JSON file per item under data/,
plus config/person.json. This module pulls them so the profile README renders
from the same records as the website and the CV PDFs, instead of keeping its own
copy of every blurb — which had already drifted (the README said "Strategic
Planning" while the site said "Strategic Committee").

Reads through the GitHub API so it works before the site is live and needs no
extra token beyond the one Actions already provides.
"""

from __future__ import annotations

import json
import os
import subprocess
from functools import lru_cache

REPO = "nstarman/nstarman.github.io"
# The branch the site is published from. Override to render this README from
# records on a branch before they are merged.
REF = os.environ.get("DATA_REF", "main")


def _gh(path: str) -> str:
    """Call the GitHub API via gh, which already holds the Actions token."""
    return subprocess.run(
        ["gh", "api", f"repos/{REPO}/{path}"],
        capture_output=True, text=True, check=True, timeout=60,
    ).stdout


def _fetch(url: str) -> str:
    return subprocess.run(
        ["curl", "-sfL", "--max-time", "30",
         "--retry", "4", "--retry-delay", "2", "--retry-all-errors", url],
        capture_output=True, text=True, check=True,
    ).stdout


@lru_cache(maxsize=1)
def items() -> tuple[dict, ...]:
    """Every item in data/, newest first."""
    listing = json.loads(_gh(f"contents/data?ref={REF}"))
    out = []
    for entry in listing:
        if entry["type"] == "file" and entry["name"].endswith(".json"):
            out.append(json.loads(_fetch(entry["download_url"])))
    # partial dates (YYYY, YYYY-MM) sort correctly as plain strings
    out.sort(key=lambda i: i.get("date", {}).get("start", ""), reverse=True)
    return tuple(out)


@lru_cache(maxsize=1)
def person() -> dict:
    meta = json.loads(_gh(f"contents/config/person.json?ref={REF}"))
    return json.loads(_fetch(meta["download_url"]))


def of_type(*types: str) -> list[dict]:
    return [i for i in items() if i.get("type") in types]


def link(item: dict, rel: str) -> str | None:
    for l in item.get("links", []):
        if l["rel"] == rel:
            return l["url"]
    return None


def ads_url(item: dict) -> str | None:
    """Synthesised from the bibcode, exactly as the website does — so an item
    can never carry a bibcode and a contradicting ADS link."""
    bib = item.get("bibcode")
    return f"https://ui.adsabs.harvard.edu/abs/{bib}/abstract" if bib else None


def initials(given: str) -> str:
    return " ".join(p if p.endswith(".") else f"{p[0].upper()}." for p in given.split())


def display_name(a: dict) -> str:
    if a.get("literal"):
        return a["literal"]
    return " ".join(x for x in (initials(a["given"]), a["family"], a.get("suffix")) if x)


def byline(item: dict, limit: int = 4) -> str:
    """Authors for display, bolding the `me` entry. Truncation happens here and
    never in the data, which keeps the BibTeX complete."""
    authors = item.get("authors", [])
    shown = authors[:limit]
    parts = [f"**{display_name(a)}**" if a.get("me") else display_name(a) for a in shown]
    line = ", ".join(parts)
    if item.get("collaboration"):
        line = f"{item['collaboration']}, {line}"
    if len(authors) > limit:
        line += ", et al."
    return line


def venue_line(item: dict) -> str:
    v = item.get("venue") or {}
    title = v.get("journal") or v.get("booktitle") or v.get("school") or ""
    tail = ", ".join(x for x in (v.get("volume"), v.get("pages")) if x)
    return " ".join(x for x in (title, tail) if x)
