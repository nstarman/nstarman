#!/usr/bin/env python3
"""Render README.md from the shared database.

Every word here comes from nstarman/nstarman.github.io — the same records that
render the website and the CV PDFs. Nothing about this profile is maintained in
two places any more.

    python3 scripts/build_readme.py          # writes README.md
    python3 scripts/build_readme.py --check  # exits 1 if it is out of date
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import shared_data as sd  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RAW = "https://raw.githubusercontent.com/nstarman/nstarman/main/assets"

# The icon trail, matching the CV's glyphs and the website's buttons. Keyed by
# the closed `rel` vocabulary the schema enforces.
REL_EMOJI = {
    "paper": "📄", "preprint": "📜", "doi": "📄", "repo": "🔨",
    "code": "💻", "docs": "📖", "data": "🗄️", "slides": "🖼️",
    "event": "🔗", "homepage": "🔗",
}
REL_LABEL = {
    "paper": "paper", "preprint": "preprint", "doi": "doi", "repo": "paper repo",
    "code": "code", "docs": "docs", "data": "data", "slides": "slides",
    "event": "event", "homepage": "homepage",
}
# Contacts that have a vendored mark; the rest fall back to an emoji.
LOGO = {"ads": "ads.svg", "arxiv": "arxiv.svg", "zenodo": "zenodo.svg", "orcid": "orcid.svg"}
CONTACT_EMOJI = {"email": "✉️", "github": "🐙", "google-scholar": "🎓"}

SELECT_PUBS = 6


def links_line(item: dict) -> str:
    """The trailing icon trail. ADS is synthesised from the bibcode, so an item
    cannot carry a bibcode and a contradicting ADS link."""
    parts = []
    for l in item.get("links", []):
        rel = l["rel"]
        parts.append(f'[{REL_EMOJI.get(rel, "🔗")}]({l["url"]} "{REL_LABEL.get(rel, rel)}")')
    ads = sd.ads_url(item)
    if ads:
        parts.append(f'[📄]({ads} "ADS")')
    return " ".join(parts)


def publications() -> list[str]:
    pubs = [p for p in sd.of_type("publication") if p.get("status") != "in-prep"]
    pubs = [p for p in pubs if p.get("featured")][:SELECT_PUBS] or pubs[:SELECT_PUBS]

    out = ["### 📄 Select Publications", "",
           "> 📄 paper · 🔨 paper repo · 💻 code · 📖 docs · 🗄️ data", ""]
    for p in pubs:
        year = (p.get("date", {}).get("start") or "")[:4]
        title = p["title"].rstrip(".")
        bits = [sd.byline(p), f"({year})." if year else "", f"*{title}.*"]
        venue = sd.venue_line(p)
        if venue:
            bits.append(f"{venue}.")
        if p.get("status") != "published":
            bits.append(f"*{p['status'].replace('-', ' ')}.*")
        trail = links_line(p)
        line = " ".join(b for b in bits if b)
        out.append(f"1. {line}{'&nbsp;&nbsp;' + trail if trail else ''}")
    ads = sd.person()["profiles"][0]["url"]
    out += ["", f"➡️ Full publication list on [**ADS**]({ads})."]
    return out


def software() -> list[str]:
    soft = sd.of_type("software")
    lead = [s for s in soft if s.get("tier") == "lead"]
    head = sorted((s for s in soft if s.get("tier") == "headline"), key=lambda s: s["title"].lower())
    rest = sorted((s for s in soft if s.get("tier") == "other"), key=lambda s: s["title"].lower())

    out = ["### 💻 Software", "", "<p>"]
    for s in lead + head:
        url = sd.link(s, "code") or sd.link(s, "docs") or "#"
        out.append(
            f'<a href="{url}"><picture>'
            f'<source media="(prefers-color-scheme: dark)" srcset="assets/cards/{s["id"]}-dark.svg">'
            f'<img src="assets/cards/{s["id"]}-light.svg" alt="{s["id"]}" width="400">'
            f"</picture></a>"
        )
    out += ["</p>", ""]

    if rest:
        out += ["<details>",
                "<summary><i>Click to expand:</i> the rest of the ecosystem.</summary>", "", ""]
        for s in rest:
            url = sd.link(s, "code") or sd.link(s, "docs") or "#"
            desc = s.get("short") or s.get("long") or ""
            trail = links_line(s)
            out.append(f'- [**{s["title"]}**]({url}): {desc}{" " + trail if trail else ""}')
        out += ["", "</details>"]
    return out


def build() -> str:
    p = sd.person()
    out = []

    for para in p["bio"]:
        out += [para, ""]

    out += ["### 🔗 Links", ""]
    contact = {c["icon"]: c for c in p["contacts"]}

    email = contact.get("email", {}).get("url", "").replace("mailto:", "")
    obfuscated = email.replace("@", " [at] ").replace(".", " [dot] ")
    row1 = [f"✉️ {obfuscated}", f'🌐 [{p["website"]}]({p["websiteUrl"]})']
    out += [" &nbsp;|&nbsp; ".join(row1), ""]

    row2 = []
    for icon in ("ads", "arxiv", "zenodo", "orcid", "google-scholar", "github"):
        c = contact.get(icon)
        if not c:
            continue
        label = "Publications" if icon == "ads" else c["label"]
        if icon in LOGO:
            mark = (f'<img src="{RAW}/{LOGO[icon]}" alt="{c["label"]}" '
                    f'width="16" height="16">')
        else:
            mark = CONTACT_EMOJI.get(icon, "🔗")
        suffix = f' {p["orcid"]}' if icon == "orcid" else ""
        row2.append(f'{mark} [**{label}**{suffix}]({c["url"]})')
    out += [" &nbsp;|&nbsp; ".join(row2), ""]

    out += publications() + [""] + software() + [""]
    return "\n".join(out).rstrip() + "\n"


if __name__ == "__main__":
    text = build()
    target = ROOT / "README.md"
    if "--check" in sys.argv:
        current = target.read_text(encoding="utf-8") if target.exists() else ""
        if current != text:
            print("README.md is out of date — run scripts/build_readme.py")
            sys.exit(1)
        print("README.md is up to date")
    else:
        target.write_text(text, encoding="utf-8")
        print(f"wrote {target} ({len(text)} bytes)")
