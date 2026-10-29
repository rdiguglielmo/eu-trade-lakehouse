"""The names config gives, and the window of months a run reads from two parameters."""

from __future__ import annotations

import pytest

import config


def test_a_table_is_named_in_the_catalog() -> None:
    assert config.qualified("bronze", "comext_trade") == "eu_trade.bronze.comext_trade"
    assert config.qualified("ops") == "eu_trade.ops"


def test_a_month_names_its_archive_and_its_csv() -> None:
    assert config.archive_name("202605") == "full_v2_202605.7z"
    assert config.csv_name("202605") == "full_v2_202605.csv"


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        ("202601", "", ("202601", None)),
        (" 202601 ", " 202608 ", ("202601", "202608")),
        ("202612", "   ", ("202612", None)),
    ],
)
def test_the_window_is_read_from_its_parameters(
    start: str, end: str, expected: tuple[str, str | None]
) -> None:
    assert config.window_from(start, end) == expected


@pytest.mark.parametrize(
    ("start", "end", "named"),
    [
        ("", "", "start_period"),
        ("2026-01", "", "start_period"),
        ("202613", "", "start_period"),
        ("202600", "", "start_period"),
        ("202601", "20260", "end_period"),
        ("202601", "202601,202602", "end_period"),
    ],
)
def test_a_malformed_month_names_its_parameter(
    start: str, end: str, named: str
) -> None:
    with pytest.raises(ValueError, match=named):
        config.window_from(start, end)


# The listing of 2026-10-28 around the window, as months_in_window() reads it:
# the yearly archive of 2025, the subset of a month under another prefix, and
# a file that is no archive.
ARCHIVES = (
    "full_v2_202512.7z",
    "full_v2_202552.7z",
    "full_v2_202601.7z",
    "full_v2_202607.7z",
    "full_v2_202608.7z",
    "full_partxixu_v2_202608.7z",
    "full_v2_202608.7z.bak",
    "Important note of 6 January 2026.txt",
)


def test_an_open_window_reaches_the_last_month() -> None:
    assert config.months_in_window(ARCHIVES, "202601", None) == (
        "202601",
        "202607",
        "202608",
    )


def test_a_closed_window_stops_at_its_end_and_reads_no_yearly_archive() -> None:
    assert config.months_in_window(ARCHIVES, "202501", "202607") == (
        "202512",
        "202601",
        "202607",
    )


def test_a_window_past_the_last_month_is_empty() -> None:
    assert config.months_in_window(ARCHIVES, "202609", None) == ()


def test_a_window_that_ends_before_it_starts_is_refused() -> None:
    with pytest.raises(ValueError, match="ends before"):
        config.months_in_window(ARCHIVES, "202608", "202601")
