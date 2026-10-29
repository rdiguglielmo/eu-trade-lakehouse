"""The watermark: which months are pending, and its rows once months are recorded.

Starts no JVM, though watermark imports pyspark.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

import watermark
from manifest import Publication

SEPTEMBER = "2026-09-15T09:38:30"
OCTOBER = "2026-10-16T10:00:08"

# The Volume's manifest after 202605 was landed again and 202607 for the first
# time.
MANIFEST = {
    "full_v2_202604.7z": Publication(46_952_767, OCTOBER),
    "full_v2_202605.7z": Publication(46_148_464, OCTOBER),
    "full_v2_202607.7z": Publication(46_802_030, "2026-10-16T10:00:09"),
}

# What the pipeline processed: 202605 from the publication before.
PROCESSED = {
    "full_v2_202604.csv": OCTOBER,
    "full_v2_202605.csv": SEPTEMBER,
}


@pytest.mark.parametrize(
    ("periods", "pending"),
    [
        # Processed from the publication landed: nothing to do.
        (("202604",), ()),
        # Landed again since, and new to the watermark.
        (("202604", "202605", "202607"), ("202605", "202607")),
    ],
)
def test_pending_periods(periods: tuple[str, ...], pending: tuple[str, ...]) -> None:
    assert (
        watermark.pending_periods(periods, manifest=MANIFEST, processed=PROCESSED)
        == pending
    )


def test_a_month_the_manifest_does_not_name_stops_the_run() -> None:
    with pytest.raises(ValueError, match=r"manifest: full_v2_202606\.7z"):
        watermark.pending_periods(("202605", "202606"), manifest=MANIFEST, processed={})


def test_the_csv_of_each_month_in_the_volume() -> None:
    landing = Path("/Volumes/eu_trade/landing/comext")
    assert watermark.landed_csv_paths(landing, ("202605", "202607")) == [
        landing / "full_v2_202605.csv",
        landing / "full_v2_202607.csv",
    ]


def test_watermark_rows_replace_the_months_recorded_and_keep_the_rest() -> None:
    before = datetime(2026, 10, 28, 9, 0, tzinfo=UTC)
    now = datetime(2026, 10, 28, 16, 0, tzinfo=UTC)
    current = [
        ("full_v2_202605.csv", "202605", SEPTEMBER, 46_127_843, "seed", before),
        ("full_v2_202604.csv", "202604", OCTOBER, 46_952_767, "seed", before),
    ]
    rows = watermark.watermark_rows(
        current,
        ("202605", "202607"),
        manifest=MANIFEST,
        batch_id="run",
        processed_at=now,
    )
    assert rows == [
        ("full_v2_202604.csv", "202604", OCTOBER, 46_952_767, "seed", before),
        ("full_v2_202605.csv", "202605", OCTOBER, 46_148_464, "run", now),
        ("full_v2_202607.csv", "202607", "2026-10-16T10:00:09", 46_802_030, "run", now),
    ]
