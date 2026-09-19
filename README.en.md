# resale-home-val — Explainable comparable-sales valuation engine for resale homes (v2)

English | [简体中文](README.md)

> Transparent, evidence-chained comparable-sales valuation for resale
> residential properties in a bounded urban submarket — now with a governed
> quantified model layer. A valuation system that refuses false precision.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](03-估值引擎/pyproject.toml)

> Naming: the repository is `resale-home-val`. In v1 (the beta release) the
> Python package and CLI were named `compsval` (from *comparable-sales*, kept
> for historical reasons). **As of v2 (this release, the first stable release)
> the v1 package name `compsval` is retired and renamed to
> `gz_property_valuation`, with the CLI command `gzv`.** `compsval` survives
> only as a historical name in this note and in the upstream records.

## What is this

`resale-home-val` is a resale-home valuation engine built on the
sales-comparison approach. Version 2 consists of two layers:

1. **Comparable-sales core** (evolved from the v1 beta): for an ordinary resale
   apartment in a bounded target submarket, given an explicit valuation date
   and data cutoff, it outputs the central estimate, the credible range, the
   confidence level (high / medium / low / insufficient, with itemized
   reasons), and the comparables and judgments behind the result. Every
   valuation is in exactly one of four states: `formal`, `reference`,
   `insufficient-data`, `not-applicable` — the system would rather say
   "I don't know" than emit a number dressed up as precision;
2. **Quantified model layer** (new in v2): under one shared data contract and
   one governance discipline, the B0 naive baseline, the M1 explainable model
   and GBM challengers (CatBoost/LightGBM) are compared under controlled,
   budget-matched experiments; freeze & version fingerprinting, tiered
   fallback and missing-input degradation, candidate-only serving with shadow
   records, a global stop switch and release gates are all fail-closed —
   without an effective release record, no code path produces a formal price.

It is not a black-box AVM: the methodology is the appraisal industry's
sales-comparison approach, the quantified layer only produces candidate
references and comparison evidence, human review is a mandatory step, and the
evidence chain is traceable end to end.

## Features

Comparable-sales core:

- **Tiered comparable selection**: starts from same-community, same-product
  sales and relaxes exactly one major criterion at a time, keeping the full
  relaxation trail;
- **Time adjustment**: computed only from data available at the valuation
  date — no evidence, no adjustment;
- **Outlier-robust aggregation**: similarity-weighted median + weighted
  quantile range, with effective sample size exposing weight concentration;
- **Interval calibration**: width reflects dispersion, sample size, staleness,
  missing data and replay error;
- **Monotonicity guarantee**: weaker data ⇒ wider range, lower confidence —
  never the other way around;
- **Review lineage**: automated results cannot be silently overwritten;
  before/after values and reasons are always on record;
- **Out-of-time replay**: rolling historical replay against a simple baseline
  with grouped error analysis — random splits don't substitute;
- **Evidence chain**: immutable raw snapshots, source registry, field
  contracts, missing-value discipline (unknown ≠ 0).

Quantified model layer (v2):

- **Three models on one stage**: B0 (recent-median baseline with a
  community→block→district fallback chain), M1 (an explainable regularized
  model over a fold encoder) and GBM challengers (CatBoost/LightGBM) are
  compared at one shared information cutoff under one metric contract; the two
  use cases (standalone quoting / assisted adjustment) report separately, and
  complex models with no stable gain get an honest "no-gain" verdict;
- **Freeze & version fingerprinting**: the five components (model / features /
  market assets / calibration / coordination policy) plus code fingerprints are
  bound as one bundle; mixed-generation assets are rejected; prediction and
  label records live on an append-only hash chain — append only, never rewrite;
- **Degradation & fallback**: missing total floors and similar input defects
  follow registered, deterministic degradation paths — no silent extrapolation;
  cold-start behavior for unknown communities is deterministic and audited;
- **Candidate-only serving**: candidate outputs never touch the formal
  pipeline — M1-vs-B0 divergence guardrails, A1 conflict flags, block-vocabulary
  translation and ambiguity disclosures are always printed with the result;
- **Shadow records**: every request's input snapshot and frozen result are
  stored; incoming transaction labels only append matched-pair records;
- **Stop switch & fail-closed release gates**: a missing/expired/fingerprint-
  mismatched release record ⇒ no formal price (`version_disabled`); after the
  global stop switch fires, no formal output remains anywhere; eligibility
  gates check population, freshness, support and divergence caps per request
  and explain every rejection.

## Quick start

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/kerwin-li-8888/resale-home-val.git
cd resale-home-val/03-估值引擎
uv sync
uv run pytest              # full offline test suite
uv run gzv version         # CLI smoke test
uv run python examples/synthetic_phase2_demo.py   # end-to-end synthetic demo
```

> The engine subfolder name contains CJK characters (`03-估值引擎`); modern
> terminals handle this out of the box.

The synthetic demo (fictional communities, fixed seed, fully offline,
bit-for-bit reproducible) walks the whole quantified layer: synthetic data →
B0/M1 training & evaluation → freeze & version fingerprints → candidate calls
with degradation paths → a release-gate demo in the unreleased state. See
[03-估值引擎/examples/README.md](03-估值引擎/examples/README.md).

## Scope

The engine hard-codes no city or district: the research scope defaults to a
**configurable fictional value** (Example City · Yunxi District, with a neutral
`district` fallback tier). To connect your own city, follow
[ADAPTATION.md](ADAPTATION.md) and supply data you have lawfully obtained.

## Repository layout

```text
resale-home-val/
├─ 03-估值引擎/                     # engine workspace
│   ├─ src/gz_property_valuation/   # engine source (contract/entities/ingest/valuation/phase2/reporting)
│   ├─ tests/                       # full offline test suite
│   ├─ examples/                    # synthetic demos and their artifacts
│   └─ upstream/                    # archived upstream LICENSE
├─ openspec/
│   ├─ specs/                       # current behavioral authority (13 comparable-sales specs + release gate + 10 quantified-layer specs)
│   ├─ schemas/                     # OpenSpec workflow templates
│   └─ adopt/                       # OpenSpec governance adoption records
├─ LICENSE / NOTICE / UPSTREAM.md   # MIT + upstream attribution & provenance registry
└─ ADAPTATION.md                    # guide for porting to your own city
```

## Data & compliance

- This repository **contains no scraped platform data** — no transaction
  records, no listing snapshots, no community catalogs;
- Everything in `examples/` is **synthetic** (fictional communities, placeholder
  IDs), included only to demonstrate the data contract, the quantified model
  layer and its governance;
- You are responsible for ensuring that any data you collect and use complies
  with the target platforms' terms of service, `robots` rules and the laws of
  your jurisdiction;
- The engineering skeleton derives from
  [Philly Fair Measure](https://github.com/nickhand/philly-fair-measure) (MIT);
  attribution in [NOTICE](NOTICE) and [UPSTREAM.md](UPSTREAM.md).

## Disclaimer

The output of this project is **decision support**: it is not a statutory
real-estate appraisal report, not investment advice, and no substitute for
on-site inspection, title verification or a licensed appraiser. **The
quantified model layer is currently positioned as research/candidate
reference only; its formal-enablement process has not been opened** — release
gates are fail-closed by default and no code path produces a formal price. The
software is provided "AS IS"; the authors accept no liability for any
valuation result.

## Governance

This project is governed by
[OpenSpec](https://github.com/Fission-AI/OpenSpec): `openspec/specs/` is the
single source of truth for current behavior, and any behavioral change must go
through the change workflow (propose → verify → archive). When porting to your
own city, start with [ADAPTATION.md](ADAPTATION.md).

## Acknowledgements

- [Philly Fair Measure](https://github.com/nickhand/philly-fair-measure) —
  engineering foundation (MIT)
- [mcp-imo](https://github.com/zedd75/mcp-imo),
  [open-comps](https://github.com/property-hackers/open-comps),
  [Cook County model-res-avm](https://github.com/ccao-data/model-res-avm) —
  methodological references
- Method basis: China's *Real Estate Appraisal Standard* GB/T 50291-2015, IVS,
  IAAO Standard on AVMs, Fannie Mae comparable-sales guidance

## License

[MIT](LICENSE) (upstream attribution in [NOTICE](NOTICE))
