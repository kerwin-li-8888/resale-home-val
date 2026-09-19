"""Scalar narrowing at dataframe boundaries (Robust Python: don't let ``None``
masquerade as a number).

polars aggregations (``Series.mean()``, ``.median()``, ``.quantile()``, …) are
typed as broad unions including ``None`` because empty/all-null inputs yield
``None`` at runtime. Wrapping them straight in ``float()`` hides that case
until it crashes with an unhelpful ``TypeError``. ``as_float`` states the
intent — this value must be a real number here — and turns the empty-input
case into a targeted error.
"""

from __future__ import annotations


def as_float(value: object) -> float:
    """Narrow a scalar from an untyped boundary to ``float``.

    Rejects ``None`` (an empty/all-null aggregation upstream) and any
    non-numeric scalar with an explicit error instead of ``float()``'s generic
    ``TypeError``.
    """
    if isinstance(value, (int, float)):
        return float(value)
    raise TypeError(f"expected a numeric scalar, got {value!r}")