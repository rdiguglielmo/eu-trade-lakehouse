"""The expected failures of the silver checks, and the comparison in both directions.

Starts no JVM, though silver_checks imports pyspark.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

import silver_checks as checks

MAY = "full_v2_202605.csv"
AUGUST = "full_v2_202608.csv"
KG_ZERO = checks.Rate(Decimal("0.050"), Decimal("0.060"))


def result(
    check_name: str, rows_failed: int | None, expected: int | None
) -> dict[str, Any]:
    return {
        "check_name": check_name,
        "table_name": "silver.trade_detail",
        "source_file": MAY,
        "rows_failed": rows_failed,
        "expected_failures": expected,
    }


def every_check(**changed: dict[str, Any]) -> list[dict[str, Any]]:
    """A result per check, each as expected, with some replaced by name."""
    return [changed.get(name, result(name, 0, 0)) for name in checks.CHECKS]


def test_every_expectation_names_a_check() -> None:
    assert set(checks.EXPECTED) <= set(checks.CHECKS)


@pytest.mark.parametrize(
    ("check_name", "has_copy", "expected"),
    [
        # Every month: the rates of 06a and 06b, and 0 for any other check.
        ("06a_kg_zero_with_value", True, KG_ZERO),
        (
            "06b_unit_zero_with_value",
            True,
            checks.Rate(Decimal("0.0040"), Decimal("0.0060")),
        ),
        ("01_country_valid", True, 0),
        ("10_totals_match_published", True, 0),
        # Landed before the table held its figures: check 10 is not evaluated,
        # and nothing else changes.
        ("10_totals_match_published", False, None),
        ("06a_kg_zero_with_value", False, KG_ZERO),
    ],
)
def test_expected_failures(
    check_name: str, has_copy: bool, expected: checks.Expectation
) -> None:
    assert checks.expected_failures(check_name, has_copy=has_copy) == expected


@pytest.mark.parametrize(
    ("expectation", "rows_checked", "bounds"),
    [
        (7_725, 151_835, (7_725, None, None)),
        (None, None, (None, None, None)),
        # 5% and 6% of 4,981,646 rows are 249,082.3 and 298,898.76: rounded
        # inward, to whole rows inside the rate, where rounding to the nearest
        # would give 249,082 and 298,899.
        (KG_ZERO, 4_981_646, (None, 249_083, 298_898)),
        # A rate over nothing checked allows nothing, so not evaluated fails.
        (KG_ZERO, None, (None, 0, 0)),
    ],
)
def test_expected_bounds(
    expectation: checks.Expectation,
    rows_checked: int | None,
    bounds: tuple[int | None, int | None, int | None],
) -> None:
    assert checks.expected_bounds(expectation, rows_checked) == bounds


def test_every_check_as_expected_is_no_mismatch() -> None:
    results = every_check(
        **{
            "06a_kg_zero_with_value": result("06a_kg_zero_with_value", 7_725, 7_725),
            "10_totals_match_published": result(
                "10_totals_match_published", None, None
            ),
        }
    )
    assert checks.mismatches(results) == []


@pytest.mark.parametrize(
    ("rows_failed", "expected"),
    [
        # A new failure.
        (7_726, 7_725),
        # A documented failure that shrank.
        (7_724, 7_725),
        # Not evaluated where a count was expected.
        (None, 0),
        # A count where nothing could be compared.
        (0, None),
    ],
)
def test_a_count_off_its_expectation_either_way(
    rows_failed: int | None, expected: int | None
) -> None:
    name = "06a_kg_zero_with_value"
    found = checks.mismatches(
        every_check(**{name: result(name, rows_failed, expected)})
    )
    assert found == [
        f"{name} on silver.trade_detail, {MAY}: "
        f"{rows_failed} failed, {expected} expected"
    ]


def ranged(rows_failed: int | None) -> dict[str, Any]:
    """A result of 06a on a month, expected within 254,550 to 305,459."""
    return {
        **result("06a_kg_zero_with_value", rows_failed, None),
        "source_file": AUGUST,
        "expected_min": 254_550,
        "expected_max": 305_459,
    }


@pytest.mark.parametrize("rows_failed", [254_550, 281_201, 305_459])
def test_a_count_within_its_range(rows_failed: int) -> None:
    results = every_check(**{"06a_kg_zero_with_value": ranged(rows_failed)})
    assert checks.mismatches(results) == []


@pytest.mark.parametrize("rows_failed", [254_549, 305_460, None])
def test_a_count_outside_its_range_or_not_evaluated(rows_failed: int | None) -> None:
    found = checks.mismatches(
        every_check(**{"06a_kg_zero_with_value": ranged(rows_failed)})
    )
    assert found == [
        f"06a_kg_zero_with_value on silver.trade_detail, {AUGUST}: "
        f"{rows_failed} failed, 254550 to 305459 expected"
    ]


def test_a_check_without_a_result_and_a_name_that_is_no_check() -> None:
    results = [
        r for r in every_check() if r["check_name"] != "09_no_duplicate_on_grain"
    ]
    results.append(result("09_no_duplicates", 0, 0))
    assert checks.mismatches(results) == [
        "09_no_duplicate_on_grain: no result",
        "09_no_duplicates: not a check",
    ]
