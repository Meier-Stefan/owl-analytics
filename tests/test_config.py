from pathlib import Path

from config import (
    SYMBOLS, FIELDNAMES, NUMERIC_COLS, TIME_COLS,
    RATE_LIMIT, RATE_WINDOW, MAX_WORKERS,
    DATA_DIR, CLEAN_DIR, MESSY_DIR, RESULTS_DIR, REPORTS_DIR,
)


class TestSymbols:

    def test_all_symbols_are_uppercase(self):
        for s in SYMBOLS:
            assert s == s.upper()

    def test_all_symbols_end_with_usdt(self):
        for s in SYMBOLS:
            assert s.endswith("USDT")

    def test_there_are_exactly_ten_symbols(self):
        assert len(SYMBOLS) == 10

    def test_no_duplicate_symbols(self):
        assert len(set(SYMBOLS)) == len(SYMBOLS)


class TestFieldColumns:

    def test_numeric_cols_are_subset_of_fieldnames(self):
        for c in NUMERIC_COLS:
            assert c in FIELDNAMES

    def test_time_cols_are_subset_of_fieldnames(self):
        for c in TIME_COLS:
            assert c in FIELDNAMES

    def test_numeric_and_time_cols_do_not_overlap(self):
        overlap = set(NUMERIC_COLS) & set(TIME_COLS)
        assert len(overlap) == 0


class TestRateLimiterConfig:

    def test_rate_limit_is_positive(self):
        assert RATE_LIMIT > 0

    def test_rate_window_is_positive(self):
        assert RATE_WINDOW > 0

    def test_max_workers_is_positive(self):
        assert MAX_WORKERS > 0


class TestPaths:

    def test_all_paths_are_pathlib_paths(self):
        for p in [DATA_DIR, CLEAN_DIR, MESSY_DIR, RESULTS_DIR, REPORTS_DIR]:
            assert isinstance(p, Path)

    def test_all_paths_are_relative(self):
        for p in [DATA_DIR, CLEAN_DIR, MESSY_DIR, RESULTS_DIR, REPORTS_DIR]:
            assert not p.is_absolute()
