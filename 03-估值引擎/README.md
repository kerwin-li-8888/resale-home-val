# gz_property_valuation engine (resale-home-val v2)

The project README lives at the repository root: [../README.md](../README.md)
([English](../README.en.md)); the porting guide is
[../ADAPTATION.md](../ADAPTATION.md).

This directory contains the Python package `gz_property_valuation` (CLI:
`gzv`; v1 historical package name: `compsval`), its test suite, the synthetic
demos ([examples/](examples/README.md)) and the archived upstream license
([upstream/](upstream/)). Upstream provenance is documented in the repository
root [UPSTREAM.md](../UPSTREAM.md).

Common commands (run from this directory):

```bash
uv sync                                           # install (Python 3.12+)
uv run pytest                                     # full offline test suite
uv run gzv version                                # CLI smoke test
uv run python examples/synthetic_phase2_demo.py   # end-to-end synthetic demo
```
