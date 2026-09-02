"""Scalar narrowing: as_float rejects None/non-numeric, accepts ints and floats."""

import pytest

from compsval.scalars import as_float


def test_accepts_int_and_float() -> None:
    assert as_float(3) == 3.0
    assert as_float(3.5) == 3.5
    assert as_float(-1.25) == -1.25


def test_rejects_none() -> None:
    with pytest.raises(TypeError):
        as_float(None)


def test_rejects_non_numeric() -> None:
    with pytest.raises(TypeError):
        as_float("3.5")
    with pytest.raises(TypeError):
        as_float(object())