"""The expected failures of the gold checks, compared by the rule of the silver checks.

Starts no JVM, though gold_checks imports pyspark.
"""

from __future__ import annotations

from typing import Any

import pytest

import gold_checks as checks
from silver_checks import mismatches

MAY = "full_v2_202605.csv"


def result(
    check_name: str,
    rows_failed: int | None,
    expected: int | None,
    *,
    table_name: str = "gold.fct_trade_detail",
    source_file: str | None = MAY,
) -> dict[str, Any]:
    return {
        "check_name": check_name,
        "table_name": table_name,
        "source_file": source_file,
        "rows_failed": rows_failed,
        "expected_failures": expected,
    }


def every_check(**changed: dict[str, Any]) -> list[dict[str, Any]]:
    """A result per check, each as expected, with some replaced by name."""
    return [changed.get(name, result(name, 0, 0)) for name in checks.CHECKS]


def test_every_expectation_names_a_check() -> None:
    assert set(checks.EXPECTED) <= set(checks.CHECKS)
    assert set(checks.EVERY_MONTH_CHECKS) <= set(checks.CHECKS)


@pytest.mark.parametrize(
    ("check_name", "expected"),
    [
        # Every month: France's two groups.
        ("07_detail_matches_totals", 2),
        ("08_version_covers_month", 0),
        ("01_rows_match_silver", 0),
        # A check of a dimension, which has no file.
        ("05_dimension_key_unique", 0),
    ],
)
def test_expected_failures(check_name: str, expected: int | None) -> None:
    assert checks.expected_failures(check_name) == expected


def test_every_check_as_expected_is_no_mismatch() -> None:
    results = every_check(
        **{
            "05_dimension_key_unique": result(
                "05_dimension_key_unique",
                0,
                0,
                table_name="gold.dim_country",
                source_file=None,
            ),
            "07_detail_matches_totals": result("07_detail_matches_totals", 2, 2),
        }
    )
    assert mismatches(results, checks.CHECKS) == []


def test_a_dimension_off_its_expectation_is_named_without_a_file() -> None:
    name = "06_no_overlapping_versions"
    off = result(name, 1, 0, table_name="gold.dim_country", source_file=None)
    assert mismatches(every_check(**{name: off}), checks.CHECKS) == [
        f"{name} on gold.dim_country: 1 failed, 0 expected"
    ]


def test_a_gold_check_without_a_result_and_a_silver_name() -> None:
    results = [r for r in every_check() if r["check_name"] != "02_no_null_key"]
    results.append(result("09_no_duplicate_on_grain", 0, 0))
    assert mismatches(results, checks.CHECKS) == [
        "02_no_null_key: no result",
        "09_no_duplicate_on_grain: not a check",
    ]
