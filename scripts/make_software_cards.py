#!/usr/bin/env python3
"""Render the Software-section cards as self-hosted SVGs.

GitHub's markdown sanitizer strips `style`, `class` and `<style>`, so a CSS
card grid is impossible in a README. The cards are therefore images, one SVG
per library per colour scheme, selected at view time with <picture>.

Re-run after editing CARDS:  python3 scripts/make_software_cards.py
"""

import json
import subprocess
from html import escape
from pathlib import Path

W, H, PAD = 400, 150, 18
THEMES = {
    # name:   (background, border,   title,     body,      pill bg,  pill text)
    "light": ("#ffffff", "#d0d7de", "#0969da", "#57606a", "#f6f8fa", "#57606a"),
    "dark": ("#0d1117", "#30363d", "#2f81f7", "#8b949e", "#161b22", "#8b949e"),
}
FONT = "system-ui,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif"

# (slug, emoji, title, description, star badge?) -- see REPOS for the
# GitHub path each slug maps to.
REPOS = {
    "astropy": "astropy/astropy",
    "galax": "GalacticDynamics/galax",
    "unxt": "GalacticDynamics/unxt",
    "coordinax": "GalacticDynamics/coordinax",
    "quax": "nstarman/quax",
    "quaxed": "GalacticDynamics/quaxed",
    "mvgkde": "nstarman/mvgkde",
    "dataclassish": "GalacticDynamics/dataclassish",
    "jaxmore": "GalacticDynamics/jaxmore",
    "phasecurvefit": "GalacticDynamics/phasecurvefit",
}
CARDS = [
    ("astropy", "🔭", "Astropy",
     "The community core package for astronomy in Python. "
     "I'm a core developer, on the Coordination Committee, and on "
     "Strategic Planning.", True),
    ("galax", "🌌", "galax",
     "Galactic dynamics in JAX. Orbit integration, potentials and stream "
     "generation — GPU-accelerated and fully differentiable.", True),
    ("unxt", "📏", "unxt",
     "Units in JAX. Unit-aware quantities that survive jit, grad and vmap. "
     "Published in JOSS.", True),
    ("coordinax", "🧭", "coordinax",
     "Coordinates in JAX. Vectors, frames and transformations — "
     "differentiable, and unit-aware via unxt.", False),
    ("quax", "🔀", "quax",
     "Multiple dispatch in JAX. Custom array-ish types that work with JAX "
     "primitives — the substrate the rest of the stack builds on.", True),
    ("quaxed", "⚡", "quaxed",
     "Pre-quaxed libraries. Drop-in jax.numpy and friends, already wrapped "
     "for dispatch over abstract array types.", False),
    ("mvgkde", "📊", "mvgkde",
     "Multivariate Gaussian KDE. Kernel density estimation in JAX — "
     "differentiable, vectorized, bandwidth-tunable.", False),
    ("dataclassish", "🧩", "dataclassish",
     "dataclasses, for everything. replace, fields and asdict, generalized "
     "to any object rather than just dataclasses.", False),
    ("jaxmore", "➕", "jaxmore",
     "There's more to JAX. The utilities you keep re-writing, collected in "
     "one place.", False),
    ("phasecurvefit", "〰️", "phasecurvefit",
     "Paths through phase space. Fit a curve through phase-space points — "
     "streams, orbits, trajectories. Under JOSS & pyOpenSci review.", False),
]


def stars(slug):
    """Live star count via gh, so a regenerate is never stale."""
    try:
        n = int(subprocess.run(
            ["gh", "api", f"repos/{REPOS[slug]}", "--jq", ".stargazers_count"],
            capture_output=True, text=True, check=True, timeout=20).stdout)
    except Exception:
        return "\u2605"
    return f"\u2605 {n/1000:.1f}k" if n >= 1000 else f"\u2605 {n}"


def wrap(text, width_px, px_per_char=6.15, max_lines=3):
    """Greedy wrap using an average glyph width; good enough for 13px sans."""
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
    if len(lines) == max_lines and cur not in lines[-1]:
        lines[-1] = lines[-1].rstrip(",.;") + "…"
    return lines


def render(card, theme):
    slug, emoji, title, desc, starred = card
    bg, border, title_c, body_c, pill_bg, pill_c = THEMES[theme]
    inner = W - 2 * PAD

    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}" role="img" aria-label="{escape(title)}">',
        f'<title>{escape(title)}</title>',
        f'<rect x="0.5" y="0.5" width="{W-1}" height="{H-1}" rx="12" '
        f'fill="{bg}" stroke="{border}"/>',
        f'<text x="{PAD}" y="38" font-family="{FONT}" font-size="17" '
        f'font-weight="600" fill="{title_c}">{emoji}  {escape(title)}</text>',
    ]
    for i, line in enumerate(wrap(desc, inner)):
        out.append(
            f'<text x="{PAD}" y="{66 + i*20}" font-family="{FONT}" '
            f'font-size="13" fill="{body_c}">{escape(line)}</text>'
        )
    if starred:
        label = stars(slug)
        width = 22 + 6 * len(label)
        out += [
            f'<rect x="{PAD}" y="{H-40}" width="{width}" height="22" rx="11" '
            f'fill="{pill_bg}" stroke="{border}"/>',
            f'<text x="{PAD+11}" y="{H-25}" font-family="{FONT}" '
            f'font-size="11" fill="{pill_c}">{label}</text>',
        ]
    out.append("</svg>")
    return "\n".join(out)


def main():
    outdir = Path(__file__).resolve().parent.parent / "assets" / "cards"
    outdir.mkdir(parents=True, exist_ok=True)
    for card in CARDS:
        for theme in THEMES:
            (outdir / f"{card[0]}-{theme}.svg").write_text(
                render(card, theme), encoding="utf-8"
            )
    print(f"wrote {len(CARDS) * len(THEMES)} cards to {outdir}")


if __name__ == "__main__":
    main()
