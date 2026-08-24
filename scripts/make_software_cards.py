#!/usr/bin/env python3
"""Render the Software-section cards of README.md as self-hosted SVGs.

Card content comes from the shared database in nstarman/nstarman.github.io, not
from this file. It used to keep its own copy of every blurb, and that copy had
already drifted from the website.

GitHub's markdown sanitizer strips `style`, `class` and `<style>`, so a CSS
card grid is impossible in a README. The cards are therefore images: one SVG
per library per colour scheme, selected at view time with <picture>.

Star pills are driven by the live star count, not a hardcoded flag — a repo
crossing THRESHOLD gains its pill on the next run with no edit here. Run
monthly by .github/workflows/refresh-software-cards.yml, or by hand:

    python3 scripts/make_software_cards.py
"""

import subprocess
import sys
from html import escape
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import shared_data as sd  # noqa: E402

# A pill below this reads as a liability rather than a credential.
THRESHOLD = 50
# galax is the flagship science library; the threshold is a presentation rule,
# not a judgement about it, so it keeps its pill at any count.
ALWAYS_PILL = {"galax"}

W, PAD = 400, 18
TITLE_Y = 38                      # title baseline
DESC_Y, DESC_LEADING = 66, 20     # first description baseline, then line step
DESC_LINES = 3                    # reserved regardless of actual length, so
                                  # every card is the same height in the grid
PILL_H, PILL_GAP = 22, 16         # pill box, and its clearance below the text
PILL_Y = DESC_Y + (DESC_LINES - 1) * DESC_LEADING + PILL_GAP
H = PILL_Y + PILL_H + PAD
THEMES = {
    # name:   (background, border,   title,     body,      pill bg,  pill text)
    "light": ("#ffffff", "#d0d7de", "#0969da", "#57606a", "#f6f8fa", "#57606a"),
    "dark": ("#0d1117", "#30363d", "#2f81f7", "#8b949e", "#161b22", "#8b949e"),
}
FONT = "system-ui,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif"

# Emoji are presentation, so they stay here; every word comes from the database.
EMOJI = {
    "astropy": "\U0001F52D", "galax": "\U0001F30C", "unxt": "\U0001F4CF",
    "coordinax": "\U0001F9ED", "quax": "\U0001F500",
}
DEFAULT_EMOJI = "\U0001F4E6"

TIER_ORDER = {"lead": 0, "headline": 1}


def cards():
    """The lead and headline packages, in the order the site shows them."""
    picked = [s for s in sd.of_type("software") if s.get("tier") in TIER_ORDER]
    picked.sort(key=lambda s: (TIER_ORDER[s["tier"]], s["title"].lower()))
    return picked


def star_count(repo):
    """Live count via gh. None on failure, which suppresses the pill rather
    than baking in a wrong number."""
    try:
        return int(subprocess.run(
            ["gh", "api", f"repos/{repo}", "--jq", ".stargazers_count"],
            capture_output=True, text=True, check=True, timeout=30).stdout)
    except Exception as exc:  # network, auth, rate limit, renamed repo
        print(f"  ! {repo}: {exc.__class__.__name__} — pill suppressed")
        return None


def pill_label(n):
    return f"★ {n/1000:.1f}k" if n >= 1000 else f"★ {n}"


def wrap(text, width_px, px_per_char=6.15, max_lines=3):
    """Greedy wrap on an average glyph width; good enough for 13px sans."""
    limit = int(width_px / px_per_char)
    lines, cur = [], ""
    for word in text.split():
        trial = f"{cur} {word}".strip()
        if len(trial) <= limit:
            cur = trial
        else:
            lines.append(cur)
            cur = word
            if len(lines) == max_lines:
                break
    if cur and len(lines) < max_lines:
        lines.append(cur)
    return lines


def render(item, theme, label):
    slug = item["id"]
    emoji = EMOJI.get(slug, DEFAULT_EMOJI)
    title = item["title"]
    desc = item.get("long") or item.get("short") or ""
    bg, border, title_c, body_c, pill_bg, pill_c = THEMES[theme]

    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}" role="img" aria-label="{escape(title)}">',
        f'<title>{escape(title)}</title>',
        f'<rect x="0.5" y="0.5" width="{W-1}" height="{H-1}" rx="12" '
        f'fill="{bg}" stroke="{border}"/>',
        f'<text x="{PAD}" y="{TITLE_Y}" font-family="{FONT}" font-size="17" '
        f'font-weight="600" fill="{title_c}">{emoji}  {escape(title)}</text>',
    ]
    for i, line in enumerate(wrap(desc, W - 2 * PAD, max_lines=DESC_LINES)):
        out.append(
            f'<text x="{PAD}" y="{DESC_Y + i*DESC_LEADING}" font-family="{FONT}" '
            f'font-size="13" fill="{body_c}">{escape(line)}</text>')
    if label:
        out += [
            f'<rect x="{PAD}" y="{PILL_Y}" width="{22 + 6*len(label)}" '
            f'height="{PILL_H}" rx="11" fill="{pill_bg}" stroke="{border}"/>',
            f'<text x="{PAD+11}" y="{PILL_Y + 15}" font-family="{FONT}" '
            f'font-size="11" fill="{pill_c}">{label}</text>']
    out.append("</svg>")
    return "\n".join(out)


def main():
    outdir = Path(__file__).resolve().parent.parent / "assets" / "cards"
    outdir.mkdir(parents=True, exist_ok=True)

    picked = cards()
    pilled, plain = [], []
    for item in picked:
        slug = item["id"]
        n = star_count(item["repo"]) if item.get("repo") else None
        show = n is not None and (n >= THRESHOLD or slug in ALWAYS_PILL)
        label = pill_label(n) if show else ""
        for theme in THEMES:
            (outdir / f"{slug}-{theme}.svg").write_text(
                render(item, theme, label), encoding="utf-8")
        (pilled if show else plain).append(f"{slug} ({n if n is not None else '?'})")

    keep = {i["id"] for i in picked}
    for stale in sorted(outdir.glob("*.svg")):
        if stale.stem.rsplit("-", 1)[0] not in keep:
            stale.unlink()
            print(f"  - removed {stale.name} (no longer lead/headline)")

    print(f"\nwrote {len(picked) * len(THEMES)} cards to {outdir}")
    print(f"pilled  (>= {THRESHOLD}, or exempt): {', '.join(pilled) or 'none'}")
    print(f"plain   (below threshold):          {', '.join(plain) or 'none'}")


if __name__ == "__main__":
    main()
