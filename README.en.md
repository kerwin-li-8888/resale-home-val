# resale-home-val — Explainable comparable-sales valuation engine for resale homes

English | [简体中文](README.md)

> Transparent, evidence-chained comparable-sales valuation for resale
> residential properties in a bounded urban submarket. A valuation system that
> refuses false precision.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](pyproject.toml)

> Naming: the repository is `resale-home-val`; the Python package and CLI are
> `compsval` (from *comparable-sales*, kept for historical reasons).

## What is this

`resale-home-val` is a resale-home valuation engine built on the
sales-comparison approach. For an ordinary resale apartment in a bounded
target submarket, given an explicit valuation date and data cutoff, it outputs:

1. where the **central estimate** lies;
2. how wide the **credible range** is;
3. **how confident** the result is (high / medium / low / insufficient, with
   itemized reasons);
4. **which comparables and judgments** produced the result.

Every valuation is in exactly one of four states: `formal`, `reference`,
`insufficient-data`, `not-applicable`. The system would rather say "I don't
know" than emit a number dressed up as precision. It is not a black-box AVM:
the methodology is the appraisal industry's sales-comparison approach, human
review is a mandatory step, and the evidence chain is traceable end to end.

## Features

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

## Quick start

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/kerwin-li-8888/resale-home-val.git
cd resale-home-val/03-估值引擎
uv sync
uv run pytest              # full offline test suite
uv run compsval version    # CLI smoke test
```

> The engine subfolder name contains CJK characters (`03-估值引擎`); modern
> terminals handle this out of the box.

Synthetic sample data (fictional community "ChunhuiLi", end-to-end demo:
standardized contract layer → valuation chain → report) lives in `examples/`.
Fully offline, bit-for-bit reproducible.

## Repository layout

```text
resale-home-val/
├─ 03-估值引擎/            # engine workspace
│   ├─ src/compsval/       # engine source (contract/entities/ingest/valuation/reporting)
│   ├─ tests/              # full offline test suite
│   ├─ UPSTREAM.md         # per-file upstream provenance registry (OSS compliance audit)
│   └─ upstream/           # archived upstream LICENSE
├─ openspec/
│   ├─ specs/              # current behavioral authority (capability specs + release gates)
│   └─ adopt/              # OpenSpec governance adoption records
├─ LICENSE / NOTICE        # MIT + upstream attribution
└─ ADAPTATION.md           # guide for porting to your own city
```

## Data & compliance

- This repository **contains no scraped platform data** — no transaction
  records, no listing snapshots, no community catalogs;
- Everything in `examples/` is **synthetic** (fictional communities, placeholder
  IDs), included only to demonstrate the data contract and valuation flow;
- You are responsible for ensuring that any data you collect and use complies
  with the target platforms' terms of service, `robots` rules and the laws of
  your jurisdiction;
- The engineering skeleton derives from
  [Philly Fair Measure](https://github.com/nickhand/philly-fair-measure) (MIT);
  attribution in [NOTICE](NOTICE) and
  [UPSTREAM.md](03-估值引擎/UPSTREAM.md).

## Disclaimer

The output of this project is **decision support**. It is not a statutory
real-estate appraisal report, not investment advice, and no substitute for
on-site inspection, title verification or a licensed appraiser. The software is
provided "AS IS"; the authors accept no liability for any valuation result.

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
