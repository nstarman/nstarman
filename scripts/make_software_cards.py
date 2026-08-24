#!/usr/bin/env python3
"""Render the Software-section cards of README.md as self-hosted SVGs.

GitHub's markdown sanitizer strips `style`, `class` and `<style>`, so a CSS
card grid is impossible in a README. The cards are therefore images: one SVG
per library per colour scheme, selected at view time with <picture>.

Star pills are driven by the live star count, not a hardcoded flag — a repo
crossing THRESHOLD gains its pill on the next run with no edit here. Run
monthly by .github/workflows/refresh-software-cards.yml, or by hand:

    python3 scripts/make_software_cards.py
"""

import subprocess
from html import escape
from pathlib import Path

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

# slug -> (repo, emoji, title, description)
CARDS = {
    "astropy": ("astropy/astropy", "🔭", "Astropy",
        "The community core package for astronomy in Python. I'm a core "
        "developer, on the Coordination Committee, and on Strategic Planning."),
    "galax": ("GalacticDynamics/galax", "🌌", "galax",
        "Galactic dynamics in JAX. Orbit integration, potentials and stream "
        "generation — GPU-accelerated and fully differentiable."),
    "unxt": ("GalacticDynamics/unxt", "📏", "unxt",
        "Units in JAX. Unit-aware quantities that survive jit, grad and vmap. "
        "Published in JOSS."),
    "coordinax": ("GalacticDynamics/coordinax", "🧭", "coordinax",
        "Coordinates in JAX. Vectors, frames and transformations — "
        "differentiable, and unit-aware via unxt."),
    "quax": ("nstarman/quax", "🔀", "quax",
        "Multiple dispatch in JAX. Custom array-ish types that work with JAX "
        "primitives — the substrate the rest of the stack builds on."),
    "quaxed": ("GalacticDynamics/quaxed", "⚡", "quaxed",
        "Pre-quaxed libraries. Drop-in jax.numpy and friends, already wrapped "
        "for dispatch over abstract array types."),
    "mvgkde": ("nstarman/mvgkde", "📊", "mvgkde",
        "Multivariate Gaussian KDE. Kernel density estimation in JAX — "
        "differentiable, vectorized, bandwidth-tunable."),
    "dataclassish": ("GalacticDynamics/dataclassish", "🧩", "dataclassish",
        "dataclasses, for everything. replace, fields and asdict, generalized "
        "to any object rather than just dataclasses."),
    "jaxmore": ("GalacticDynamics/jaxmore", "➕", "jaxmore",
        "There's more to JAX. The utilities you keep re-writing, collected in "
        "one place."),
    "phasecurvefit": ("GalacticDynamics/phasecurvefit", "〰️", "phasecurvefit",
        "Paths through phase space. Fit a curve through phase-space points — "
        "streams, orbits, trajectories. Under JOSS & pyOpenSci review."),
}


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


def render(slug, theme, label):
    _repo, emoji, title, desc = CARDS[slug]
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

    pilled, plain = [], []
    for slug, (repo, *_) in CARDS.items():
        n = star_count(repo)
        show = n is not None and (n >= THRESHOLD or slug in ALWAYS_PILL)
        label = pill_label(n) if show else ""
        for theme in THEMES:
            (outdir / f"{slug}-{theme}.svg").write_text(
                render(slug, theme, label), encoding="utf-8")
        (pilled if show else plain).append(
            f"{slug} ({n if n is not None else '?'})")

    print(f"\nwrote {len(CARDS) * len(THEMES)} cards to {outdir}")
    print(f"pilled  (>= {THRESHOLD}, or exempt): {', '.join(pilled) or 'none'}")
    print(f"plain   (below threshold):          {', '.join(plain) or 'none'}")


if __name__ == "__main__":
    main()
