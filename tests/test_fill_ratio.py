import pytest

from linewerk.quality.fill_ratio import line_fill, station_fill, total_filled_cells

# 4 rows. S0: both features filled on the same 2 rows -> min == max.
# S1: one feature on 3 rows, the other on 1 -> breaks the pattern. L1_S24: empty.
NON_NULL = {
    "L0_S0_F0": 2,
    "L0_S0_F2": 2,
    "L0_S1_F4": 3,
    "L0_S1_F6": 1,
    "L1_S24_F1": 0,
}
TOTAL_ROWS = 4


def test_station_fill():
    by_station = {(s.line, s.station): s for s in station_fill(NON_NULL, TOTAL_ROWS)}

    assert list(by_station) == [(0, 0), (0, 1), (1, 24)]
    s0, s1, s24 = by_station.values()
    assert (s0.n_features, s0.min_non_null, s0.max_non_null) == (2, 2, 2)
    assert s0.fill_ratio == pytest.approx(4 / 8)
    assert s0.all_or_nothing
    assert (s1.min_non_null, s1.max_non_null) == (1, 3)
    assert not s1.all_or_nothing
    assert s24.fill_ratio == 0.0
    assert s24.all_or_nothing  # empty everywhere is "together" too


def test_line_fill():
    l0, l1 = line_fill(NON_NULL, TOTAL_ROWS)
    assert (l0.line, l0.n_features, l0.filled_cells) == (0, 4, 8)
    assert l0.fill_ratio == pytest.approx(8 / 16)
    assert (l1.line, l1.n_features, l1.fill_ratio) == (1, 1, 0.0)


def test_total_filled_cells():
    assert total_filled_cells(NON_NULL) == 8


def test_zero_rows_gives_zero_ratio():
    assert all(s.fill_ratio == 0.0 for s in station_fill(NON_NULL, 0))


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="Unknown column"):
        station_fill({"Id": 1}, TOTAL_ROWS)
