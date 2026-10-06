#!/usr/bin/env python3
"""Copy the Software-section cards of README.md from the website.

The cards are the website's own: nstarkman.space draws each lead and headline
package's card as an SVG at build (/cards/<id>-<light|dark>.svg, listed in
/cards/index.json), from the same presets as its Software page. Nothing about a
card's look lives here, so a change to the site's cards reaches the README on
the next run.

GitHub's markdown sanitizer strips `style`, `class` and `<style>`, so the cards
are images, one per package per colour scheme, selected at view time with
<picture>. Run monthly by .github/workflows/refresh-software-cards.yml, which builds the
site and reads them from its dist/cards (CARDS_DIR); by hand it downloads them from
the deployed site:

    python3 scripts/make_software_cards.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import shared_data as sd  # noqa: E402

THEMES = ("light", "dark")


def main():
    outdir = Path(__file__).resolve().parent.parent / "assets" / "cards"
    outdir.mkdir(parents=True, exist_ok=True)

    cards = sd.cards()
    for card in cards:
        for theme in THEMES:
            # Written as served: the site's drawing is the whole of the card.
            (outdir / f"{card['id']}-{theme}.svg").write_text(sd.card_file(f"{card['id']}-{theme}.svg"), encoding="utf-8")

    keep = {c["id"] for c in cards}
    for stale in sorted(outdir.glob("*.*")):
        if stale.suffix != ".svg" or stale.stem.rsplit("-", 1)[0] not in keep:
            stale.unlink()
            print(f"  - removed {stale.name}")

    print(f"wrote {len(cards) * len(THEMES)} cards to {outdir}")


if __name__ == "__main__":
    main()
