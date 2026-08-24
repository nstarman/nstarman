I am a **Brinson Prize Fellow** and postdoctoral associate at the **MIT Kavli Institute for Astrophysics and Space Research**. I work on dark matter — what it is, and how it shapes galaxies — mostly by reading the gravitational field written into stellar streams, in the Milky Way and now beyond it with *Euclid*. I received my PhD from the University of Toronto.

I also write much of the scientific software I use. I'm a core developer and Coordination Committee member of [Astropy](https://www.astropy.org), and I wrote most of the JAX-based galactic dynamics ecosystem (see below). I have strong opinions about good software development in science.

Ask me about dark matter, differentiable simulation, or cheese.

### 🔗 Links

✉️ starkman [at] mit [dot] edu &nbsp;|&nbsp; 🌐 [nstarman.github.io](https://nstarman.github.io/)

<img src="https://raw.githubusercontent.com/nstarman/nstarman/main/assets/ads.svg" alt="ADS" width="18" height="16"> [**Publications**](https://ui.adsabs.harvard.edu/search/p_=0&q=author%3A%22Starkman%2C%20Nathaniel%22&sort=date%20desc%2C%20bibcode%20desc) &nbsp;|&nbsp; <img src="https://raw.githubusercontent.com/nstarman/nstarman/main/assets/arxiv.svg" alt="arXiv" width="16" height="16"> [**arXiv**](https://arxiv.org/search/advanced?advanced=&terms-0-operator=AND&terms-0-term=Starkman%2C+Nathaniel&terms-0-field=author&classification-physics_archives=all&classification-include_cross_list=include&date-filter_by=all_dates&abstracts=show&size=50&order=-announced_date_first) &nbsp;|&nbsp; <img src="https://raw.githubusercontent.com/nstarman/nstarman/main/assets/zenodo.svg" alt="Zenodo" width="16" height="16"> [**Zenodo**](https://zenodo.org/search?q=%22Starkman%2C%20Nathaniel%22%20OR%20%22Nathaniel%20Starkman%22) &nbsp;|&nbsp; <img src="https://raw.githubusercontent.com/nstarman/nstarman/main/assets/orcid.svg" alt="ORCID" width="16" height="16"> [**ORCID** 0000-0003-3954-3291](https://orcid.org/0000-0003-3954-3291)

### 📄 Select Publications

> 📄 paper · 🔨 paper repo · 💻 code · 📖 docs · 🗄️ data

| Publication | |
|:---|---:|
| Euclid Collaboration, **N. Starkman**, et al. *Euclid: The Geometry of Dark Matter Halos from Extragalactic Streams — a Pilot Study.* Submitted to A&A. arXiv:2606.21774 | [📄](https://ui.adsabs.harvard.edu/abs/2026arXiv260621774E/abstract) |
| **N. Starkman**, et al. (2025). *unxt: A Python package for unit-aware computing with JAX.* JOSS 10(107), 7771. | [📄](https://doi.org/10.21105/joss.07771) [💻](https://github.com/GalacticDynamics/unxt) [📖](https://unxt.readthedocs.io/en/stable/) |
| **N. Starkman**, et al. (2025). *Stream Members Only: Data-Driven Characterization of Stellar Streams with Mixture Density Networks.* ApJ 979, 155. | [📄](https://iopscience.iop.org/article/10.3847/1538-4357/ad94f2) [🔨](https://github.com/nstarman/stellar_stream_density_ml_paper) [🗄️](https://zenodo.org/records/10211410) |
| **N. Starkman**, A. Kosowsky, G. Starkman (2024). *Angular Correlations of Cosmic Microwave Background Spectrum Distortions from Photon Diffusion.* MNRAS 529, 2274. | [📄](https://academic.oup.com/mnras/article/529/3/2274/7630233) [🔨](https://github.com/nstarman/Temperature-Diffusion-Spectral-Distortion-Paper) [🗄️](https://zenodo.org/record/8400583) |
| **N. Starkman**, et al. (2023). *Characterizing Stream Tracks and Comparing to Simulation.* MNRAS 522, 2735. | [📄](https://doi.org/10.1093/mnras/stad1166) [🔨](https://github.com/nstarman/trackstream_paper) [💻](https://github.com/nstarman/trackstream) [📖](https://trackstream.readthedocs.io/en/latest/) [🗄️](https://zenodo.org/record/7265571) |
| The Astropy Collaboration, A. M. Price-Whelan, …, **N. Starkman**, et al. (2022). *The Astropy Project: Sustaining and Growing a Community-oriented Open-source Project and the Latest Major Release (v5.0) of the Core Package.* ApJ 935, 167. | [📄](https://arxiv.org/abs/2206.14220) [🔨](https://github.com/astropy/astropy-v5.0-paper) [💻](https://github.com/astropy/astropy) [📖](https://www.astropy.org) [🗄️](https://doi.org/10.5281/zenodo.8325470) |

➡️ Full publication list on [**ADS**](https://ui.adsabs.harvard.edu/search/p_=0&q=author%3A%22Starkman%2C%20Nathaniel%22&sort=date%20desc%2C%20bibcode%20desc).

### 💻 Software

<!--
  MAINTENANCE NOTE — star badges
  Only repos with >= 50 stars carry a shields.io star badge; below that the number
  undersells the library more than it helps. Re-check every ~6 months and promote
  any repo that crosses the threshold:

      gh repo list GalacticDynamics --json name,stargazerCount \
        --jq '.[] | select(.stargazerCount >= 50) | "\(.stargazerCount) \(.name)"'
      gh api repos/nstarman/quax --jq .stargazers_count

  Last checked: 2026-08-24. Badged: astropy (5284), quax (143), unxt (67).
  Closest below the line: galax (49), coordinax (37) — galax is one star away.
-->

<table>
<tr>
<td width="50%" valign="top">

#### 🔭 [Astropy](https://github.com/astropy/astropy)

**Astronomy in Python.** The community core package. I'm a core developer, Coordination Committee member, and on the Strategic Planning committee.

[![stars](https://img.shields.io/github/stars/astropy/astropy?style=social)](https://github.com/astropy/astropy)

</td>
<td width="50%" valign="top">

#### 🌌 [galax](https://github.com/GalacticDynamics/galax)

**Galactic dynamics in JAX.** Orbit integration, potentials, and stream generation — GPU-accelerated and fully differentiable.

</td>
</tr>
<tr>
<td width="50%" valign="top">

#### 📏 [unxt](https://github.com/GalacticDynamics/unxt)

**Units in JAX.** Unit-aware quantities that survive `jit`, `grad`, and `vmap`. Published in JOSS.

[![stars](https://img.shields.io/github/stars/GalacticDynamics/unxt?style=social)](https://github.com/GalacticDynamics/unxt)

</td>
<td width="50%" valign="top">

#### 🧭 [coordinax](https://github.com/GalacticDynamics/coordinax)

**Coordinates in JAX.** Vectors, frames, and transformations — differentiable, and unit-aware via `unxt`.

</td>
</tr>
<tr>
<td width="50%" valign="top">

#### 🔀 [quax](https://github.com/nstarman/quax)

**Multiple dispatch in JAX.** Custom array-ish types that work with JAX primitives. The substrate the rest of the stack is built on.

[![stars](https://img.shields.io/github/stars/nstarman/quax?style=social)](https://github.com/nstarman/quax)

</td>
<td width="50%" valign="top">

#### ⚡ [quaxed](https://github.com/GalacticDynamics/quaxed)

**Pre-`quax`ed libraries.** Drop-in `jax.numpy` and friends, already wrapped for dispatch over abstract array types.

</td>
</tr>
<tr>
<td width="50%" valign="top">

#### 📊 [mvgkde](https://github.com/nstarman/mvgkde)

**Multivariate Gaussian KDE.** Kernel density estimation in JAX — differentiable, vectorized, and bandwidth-tunable.

</td>
<td width="50%" valign="top">

#### 🧩 [dataclassish](https://github.com/GalacticDynamics/dataclassish)

**`dataclasses`, for everything.** `replace`, `fields`, `asdict` — generalized to any object, not just dataclasses.

</td>
</tr>
<tr>
<td width="50%" valign="top">

#### ➕ [jaxmore](https://github.com/GalacticDynamics/jaxmore)

**There's more to JAX.** The utilities you keep re-writing, collected in one place.

</td>
<td width="50%" valign="top">

#### 〰️ [phasecurvefit](https://github.com/GalacticDynamics/phasecurvefit)

**Paths through phase space.** Fit a curve through phase-space points — streams, orbits, trajectories. Under JOSS & pyOpenSci review.

</td>
</tr>
</table>

<details>
<summary><i>Click to expand:</i> the rest of the ecosystem — JAX tooling, Python micro-libraries, and science codes.</summary>

<br>

**JAX / array computing**

- [**quax-blocks**](https://github.com/GalacticDynamics/quax-blocks): building blocks for constructing `quax` classes — mixins for arithmetic, comparison, and array protocols.
- [**diffraxtra**](https://github.com/GalacticDynamics/diffraxtra): extras for Diffrax — OOP interfaces and vectorization.
- [**oncequinox**](https://github.com/GalacticDynamics/oncequinox): singletons for Equinox.
- [**xmmutablemap**](https://github.com/GalacticDynamics/xmmutablemap): an immutable mapping, JAX-compatible.

**Python micro-libraries**

- [**optional_dependencies**](https://github.com/GalacticDynamics/optional_dependencies): construct checks for optional dependencies.
- [**plotting_backends**](https://github.com/GalacticDynamics/plotting_backends): dispatch over plotting backends.
- [**is_annotated**](https://github.com/GalacticDynamics/is_annotated): check whether a type hint is `Annotated`.
- [**zeroth**](https://github.com/GalacticDynamics/zeroth): efficiently get the index-0 element of any iterable.

**Science codes**

- [**trackstream**](https://github.com/nstarman/trackstream): characterize a stellar stream's track with minimal prior knowledge. Astropy-powered.
- [**macro_lightning**](https://github.com/nstarman/macro_lightning): constrain macroscopic dark matter with lightning, on Earth and Jupiter.
- [**galactic_dynamics_interoperability**](https://github.com/GalacticDynamics/galactic_dynamics_interoperability): interoperability between galactic dynamics libraries.

</details>
