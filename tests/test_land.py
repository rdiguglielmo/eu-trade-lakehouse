"""The landing: the access policy, the atomic download, the archive layout,
the copy of the published figures, the comparison of the Volume's manifest
with the listing, and what a run writes to a Volume, here a folder of tmp_path."""

from __future__ import annotations

import io
import json
import shutil
from pathlib import Path
from typing import Any

import py7zr
import pytest
import requests

import land


def response(
    status: int, *, headers: dict[str, str] | None = None, body: bytes = b""
) -> requests.Response:
    """Build a real requests.Response, as the server would send it."""
    result = requests.Response()
    result.status_code = status
    result.headers.update(headers or {})
    result.raw = io.BytesIO(body)
    result.url = "https://example.test/file"
    return result


def serve(monkeypatch: pytest.MonkeyPatch, *outcomes: object) -> list[str]:
    """Answer each call to requests.get with the next outcome: a response, or an
    exception to raise. Return the list of URLs requested."""
    calls: list[str] = []
    remaining = list(outcomes)

    def fake_get(url: str, **_: object) -> requests.Response:
        calls.append(url)
        outcome = remaining.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        assert isinstance(outcome, requests.Response)
        return outcome

    monkeypatch.setattr(land.requests, "get", fake_get)
    return calls


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record the waits between attempts instead of waiting."""
    recorded: list[float] = []
    monkeypatch.setattr(land, "MIN_REQUEST_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(land.time, "sleep", recorded.append)
    return recorded


# Retry-After
# -----------


@pytest.mark.parametrize(
    ("retry_after", "expected"),
    [
        (None, 1.0),
        ("5", 5.0),
        ("0", 0.0),
        ("120", 30.0),
        ("Wed, 21 Oct 2026 07:28:00 GMT", 1.0),
        # time.sleep() refuses a negative or NaN delay, so these fall back to
        # the backoff too, as does a value no wait could honour.
        ("-1", 1.0),
        ("nan", 1.0),
        ("inf", 1.0),
    ],
)
def test_retry_delay_honours_seconds_and_falls_back_to_the_backoff(
    retry_after: str | None, expected: float
) -> None:
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    assert land._retry_delay(response(503, headers=headers), attempt=1) == expected


def test_backoff_doubles_up_to_its_cap() -> None:
    assert [land._backoff(n) for n in range(1, 7)] == [1, 2, 4, 8, 16, 30]


# The access policy
# -----------------


def test_get_retries_a_server_error_and_returns_the_next_answer(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    busy = response(503)
    calls = serve(monkeypatch, busy, response(200))
    assert land._get("https://example.test/file").status_code == 200
    assert (len(calls), sleeps) == (2, [1.0])
    assert busy.raw.closed


def test_get_raises_a_not_found_at_once(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    calls = serve(monkeypatch, response(404))
    with pytest.raises(requests.HTTPError, match="404"):
        land._get("https://example.test/file")
    assert (len(calls), sleeps) == (1, [])


def test_get_gives_up_after_five_server_errors(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    calls = serve(monkeypatch, *(response(503) for _ in range(5)))
    with pytest.raises(requests.HTTPError, match="503"):
        land._get("https://example.test/file")
    assert (len(calls), sleeps) == (5, [1.0, 2.0, 4.0, 8.0])


def test_get_retries_a_dropped_connection(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    serve(monkeypatch, requests.ConnectionError("reset"), response(200))
    assert land._get("https://example.test/file").status_code == 200
    assert sleeps == [1.0]


def test_get_gives_up_after_five_timeouts(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    calls = serve(monkeypatch, *(requests.Timeout("slow") for _ in range(5)))
    with pytest.raises(requests.Timeout):
        land._get("https://example.test/file")
    assert (len(calls), sleeps) == (5, [1.0, 2.0, 4.0, 8.0])


@pytest.mark.parametrize("retry_after", ["-1", "nan"])
def test_get_survives_a_retry_after_that_sleep_refuses(
    monkeypatch: pytest.MonkeyPatch, retry_after: str
) -> None:
    # The real time.sleep(), which raises ValueError on a negative or NaN
    # delay; a short backoff keeps the test fast.
    monkeypatch.setattr(land, "MIN_REQUEST_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(land, "INITIAL_BACKOFF_SECONDS", 0.01)
    serve(
        monkeypatch, response(429, headers={"Retry-After": retry_after}), response(200)
    )
    assert land._get("https://example.test/file").status_code == 200


def test_get_asks_for_the_body_without_content_encoding(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    # Content-Length counts the bytes on the wire, and the size check counts
    # the bytes written: with no encoding they are the same bytes.
    received: dict[str, object] = {}

    def fake_get(url: str, **arguments: object) -> requests.Response:
        received.update(arguments)
        return response(200)

    monkeypatch.setattr(land.requests, "get", fake_get)
    land._get("https://example.test/file")
    assert received["headers"] == {"Accept-Encoding": "identity"}


# The atomic download
# -------------------


def test_download_to_renames_a_complete_transfer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = b"archive bytes"
    answer = response(200, headers={"Content-Length": str(len(body))}, body=body)
    monkeypatch.setattr(land, "_get", lambda _: answer)
    target = tmp_path / "full_v2_202605.7z"
    land._download_to("https://example.test/file", target)
    assert target.read_bytes() == body
    assert [path.name for path in tmp_path.iterdir()] == [target.name]


def test_download_to_discards_a_short_transfer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = b"archive bytes"
    answer = response(200, headers={"Content-Length": str(len(body) + 10)}, body=body)
    monkeypatch.setattr(land, "_get", lambda _: answer)
    with pytest.raises(OSError, match="incomplete download"):
        land._download_to("https://example.test/file", tmp_path / "file.7z")
    assert list(tmp_path.iterdir()) == []


def test_download_to_keeps_a_transfer_without_a_content_length(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # With no size announced there is nothing to compare against, and the file
    # is kept as received.
    answer = response(200, body=b"archive bytes")
    monkeypatch.setattr(land, "_get", lambda _: answer)
    target = tmp_path / "file.7z"
    land._download_to("https://example.test/file", target)
    assert target.read_bytes() == b"archive bytes"


def dropping(body: bytes, error: Exception) -> requests.Response:
    """A response whose body breaks off with error after its first bytes."""

    class Raw(io.BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            if self.tell():
                raise error
            return super().read(4)

    answer = response(200, headers={"Content-Length": str(len(body))})
    answer.raw = Raw(body)
    return answer


def test_download_to_retries_a_transfer_dropped_mid_body(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    body = b"archive bytes"
    answers = [
        dropping(body, requests.exceptions.ChunkedEncodingError("broken")),
        response(200, headers={"Content-Length": str(len(body))}, body=body),
    ]
    monkeypatch.setattr(land, "_get", lambda _: answers.pop(0))
    target = tmp_path / "file.7z"
    land._download_to("https://example.test/file", target)
    assert target.read_bytes() == body
    assert (answers, sleeps) == ([], [1.0])
    assert [path.name for path in tmp_path.iterdir()] == [target.name]


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.ChunkedEncodingError("broken"),
        # What requests raises for a read that times out while the body streams.
        requests.ConnectionError("read timed out"),
    ],
)
def test_download_to_gives_up_after_five_dropped_transfers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sleeps: list[float],
    error: Exception,
) -> None:
    calls: list[str] = []

    def get(url: str) -> requests.Response:
        calls.append(url)
        return dropping(b"archive bytes", error)

    monkeypatch.setattr(land, "_get", get)
    with pytest.raises(type(error)):
        land._download_to("https://example.test/file", tmp_path / "file.7z")
    assert (len(calls), sleeps) == (5, [1.0, 2.0, 4.0, 8.0])
    assert list(tmp_path.iterdir()) == []


# The archive layout
# ------------------


def make_archive(path: Path, members: dict[str, bytes]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with py7zr.SevenZipFile(path, "w") as handle:
        for name, data in members.items():
            handle.writestr(data, name)
    return path


def test_extract_csv_renames_the_single_member(tmp_path: Path) -> None:
    data = b"REPORTER,PARTNER\nAT,GB\n"
    archive = make_archive(
        tmp_path / "in" / "full_v2_202605.7z",
        {"full202605.dat": data},
    )
    output_dir = tmp_path / "out"
    csv_path = land.extract_csv(archive, output_dir)
    assert csv_path == output_dir / "full_v2_202605.csv"
    assert csv_path.read_bytes() == data
    # The temporary folder of the extraction is gone.
    assert list(output_dir.iterdir()) == [csv_path]


@pytest.mark.parametrize(
    ("members", "message"),
    [
        ({"a.dat": b"1", "b.dat": b"2"}, "expected one .dat member"),
        ({"a.txt": b"1"}, "expected one .dat member"),
        ({"sub/a.dat": b"1"}, "sub/a.dat"),
    ],
)
def test_extract_csv_refuses_another_layout(
    tmp_path: Path, members: dict[str, bytes], message: str
) -> None:
    archive = make_archive(tmp_path / "in" / "file.7z", members)
    output_dir = tmp_path / "out"
    with pytest.raises(ValueError, match=message):
        land.extract_csv(archive, output_dir)
    assert list(output_dir.iterdir()) == []


def test_extract_csv_refuses_a_member_above_the_ceiling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = b"REPORTER,PARTNER\nAT,GB\n"
    archive = make_archive(tmp_path / "in" / "file.7z", {"file.dat": data})
    monkeypatch.setattr(land, "MAX_EXTRACTED_BYTES", len(data) - 1)
    output_dir = tmp_path / "out"
    with pytest.raises(ValueError, match="above the ceiling"):
        land.extract_csv(archive, output_dir)
    assert list(output_dir.iterdir()) == []


def test_extract_csv_skips_a_csv_already_there(tmp_path: Path) -> None:
    existing = tmp_path / "file.csv"
    existing.write_bytes(b"already landed")
    # The archive does not exist: a skipped extraction never opens it.
    assert land.extract_csv(tmp_path / "file.7z", tmp_path) == existing
    assert existing.read_bytes() == b"already landed"


# File names and URLs
# -------------------


@pytest.fixture
def transfers(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, Path]]:
    """Record each transfer instead of making it."""
    recorded: list[tuple[str, Path]] = []
    monkeypatch.setattr(
        land, "_download_to", lambda url, target: recorded.append((url, target))
    )
    return recorded


def test_download_archive_requests_the_published_name(
    tmp_path: Path, transfers: list[tuple[str, Path]]
) -> None:
    target = land.download_archive("202605", tmp_path)
    assert transfers == [
        (
            "https://ec.europa.eu/eurostat/api/dissemination/files/"
            "?file=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_202605.7z",
            tmp_path / "full_v2_202605.7z",
        )
    ]
    assert target == tmp_path / "full_v2_202605.7z"


def test_download_code_list_quotes_the_path_and_keeps_the_name(
    tmp_path: Path, transfers: list[tuple[str, Path]]
) -> None:
    land.download_code_list("RELATIONS/GEOZONES-PARTNERS_ISO.txt", tmp_path)
    assert transfers == [
        (
            "https://ec.europa.eu/eurostat/api/dissemination/files/"
            "?file=comext%2FCOMEXT_METADATA%2FCLASSIFICATIONS_AND_RELATIONS"
            "%2FRELATIONS%2FGEOZONES-PARTNERS_ISO.txt",
            tmp_path / "GEOZONES-PARTNERS_ISO.txt",
        )
    ]


def test_a_file_already_there_is_not_downloaded_again(
    tmp_path: Path, transfers: list[tuple[str, Path]]
) -> None:
    (tmp_path / "full_v2_202605.7z").write_bytes(b"landed")
    (tmp_path / "CN.txt").write_bytes(b"landed")
    land.download_archive("202605", tmp_path)
    land.download_code_list("CLASSIFICATIONS/ENGLISH/CN.txt", tmp_path)
    assert transfers == []


# Published figures
# -----------------

# The published table's answer for 2026-05, as revised on 2026-10-16, cut to
# what the parser reads. The answer lists the partners as EU27_2020, UK,
# EXT_EU27_2020: neither the order of the request nor alphabetical.
TABLE_202605: dict[str, Any] = {
    "id": ["freq", "stk_flow", "indic_et", "partner", "sitc06", "geo", "time"],
    "size": [1, 2, 1, 3, 1, 1, 1],
    "dimension": {
        "freq": {"category": {"index": {"M": 0}}},
        "stk_flow": {"category": {"index": {"IMP": 0, "EXP": 1}}},
        "indic_et": {"category": {"index": {"TRD_VAL": 0}}},
        "partner": {
            "category": {"index": {"EU27_2020": 0, "UK": 1, "EXT_EU27_2020": 2}}
        },
        "sitc06": {"category": {"index": {"TOTAL": 0}}},
        "geo": {"category": {"index": {"EU27_2020": 0}}},
        "time": {"category": {"index": {"2026-05": 0}}},
    },
    "value": {
        "3": 360873.5,
        "5": 216603.9,
        "4": 29973.6,
        "0": 347519.6,
        "2": 231875.5,
        "1": 14639.6,
    },
    "updated": "2026-10-16T11:00:00+0200",
}

# A month not published yet: the API answers 200, with no time and no value.
TABLE_NOT_YET: dict[str, Any] = {
    **TABLE_202605,
    "size": [1, 2, 1, 3, 1, 1, 0],
    "value": {},
}


def test_parse_published_figures_follows_the_order_of_the_answer() -> None:
    rows = land.parse_published_figures(TABLE_202605, "202605")
    assert [(r["partner"], r["stk_flow"], r["value_meur"]) for r in rows] == [
        ("EU27_2020", "EXP", "360873.5"),
        ("EU27_2020", "IMP", "347519.6"),
        ("EXT_EU27_2020", "EXP", "216603.9"),
        ("EXT_EU27_2020", "IMP", "231875.5"),
        ("UK", "EXP", "29973.6"),
        ("UK", "IMP", "14639.6"),
    ]
    assert {(r["period"], r["table_updated"]) for r in rows} == {
        ("202605", "2026-10-16T11:00:00+0200")
    }


def test_a_month_not_in_the_table_yet_writes_no_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sleeps: list[float],
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls = serve(monkeypatch, response(200, body=json.dumps(TABLE_NOT_YET).encode()))
    assert not land.save_published_figures("202609", tmp_path / "published" / "x.csv")
    assert "&time=2026-09&" in calls[0]
    assert not (tmp_path / "published").exists()
    assert "no figure for 202609" in caplog.text


# The listing and the manifest
# ----------------------------

Publication = land.Publication

# Two rows of the listing as served on 2026-10-28, and the note listed after
# them like any other file.
LISTING = (
    '<table><tr><td> &nbsp;<a href="/eurostat/api/dissemination/files/?sort=1&amp;'
    'file=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_202607.7z" rel="external" '
    'target="_blank">full_v2_202607.7z</a></td><td title="46802030 bytes">44.63 MB'
    '</td><td class="center">7z</td><td> &nbsp;16/10/2026 10:00:09</td>'
    '<td class="center"><a href="/eurostat/api/dissemination/files/?sort=1&amp;'
    'downfile=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_202607.7z">Download</a>'
    '</td></tr><tr><td> &nbsp;<a href="/eurostat/api/dissemination/files/?sort=1'
    '&amp;file=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_202608.7z" '
    'rel="external" target="_blank">full_v2_202608.7z</a></td>'
    '<td title="25760599 bytes">24.56 MB</td><td class="center">7z</td>'
    '<td> &nbsp;20/10/2026 16:03:31</td><td class="center"><a href="/eurostat/'
    "api/dissemination/files/?sort=1&amp;downfile=comext%2FCOMEXT_DATA%2FPRODUCTS"
    '%2Ffull_v2_202608.7z">Download</a></td></tr><tr><td> &nbsp;<a href="/eurostat'
    "/api/dissemination/files/?sort=1&amp;file=comext%2FCOMEXT_DATA%2FPRODUCTS%2F"
    "Important note of 6 January 2026%E2%80%93 CPA22 code added and NAC of BG "
    'converted.txt" rel="external" target="_blank">Important note of 6 January '
    "2026\N{EN DASH} CPA22 code added and NAC of BG converted.txt</a></td>"
    '<td title="732 bytes">732 bytes</td><td class="center">txt</td>'
    '<td> &nbsp;06/01/2026 12:30:43</td><td class="center"><a href="/eurostat/'
    "api/dissemination/files/?sort=1&amp;downfile=comext%2FCOMEXT_DATA%2FPRODUCTS"
    "%2FImportant note of 6 January 2026%E2%80%93 CPA22 code added and NAC of BG "
    'converted.txt">Download</a></td></tr></table>'
)


def test_parse_listing_reads_each_file_size_and_time() -> None:
    files = land.parse_listing(LISTING)
    assert files["full_v2_202607.7z"] == Publication(46_802_030, "2026-10-16T10:00:09")
    assert files["full_v2_202608.7z"] == Publication(25_760_599, "2026-10-20T16:03:31")
    assert len(files) == 3


def test_parse_listing_refuses_a_page_without_files() -> None:
    with pytest.raises(ValueError, match="format changed"):
        land.parse_listing("<html><body><table></table></body></html>")


def test_fetch_listing_reads_the_folder_of_the_archives(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    calls = serve(monkeypatch, response(200, body=LISTING.encode("utf-8")))
    files = land.fetch_listing()
    assert calls == [
        "https://ec.europa.eu/eurostat/api/dissemination/files/"
        "?dir=comext%2FCOMEXT_DATA%2FPRODUCTS"
    ]
    assert len(files) == 3


def test_the_manifest_reads_back_what_was_written(tmp_path: Path) -> None:
    path = tmp_path / "manifest.csv"
    assert land.read_manifest(path) == {}
    manifest = {
        "full_v2_202607.7z": Publication(46_802_030, "2026-10-16T10:00:09"),
        "full_v2_202601.7z": Publication(44_955_883, "2026-10-16T10:00:07"),
    }
    land.write_manifest(manifest, path)
    assert land.read_manifest(path) == manifest
    # In bytes: read as text, CRLF line endings would pass as LF.
    assert path.read_bytes() == (
        b"archive,archive_bytes,published_at\n"
        b"full_v2_202601.7z,44955883,2026-10-16T10:00:07\n"
        b"full_v2_202607.7z,46802030,2026-10-16T10:00:09\n"
    )
    assert [p.name for p in tmp_path.iterdir()] == ["manifest.csv"]


def test_plan_months_tells_each_month_what_it_needs(tmp_path: Path) -> None:
    listed = Publication(46_148_464, "2026-10-16T10:00:08")
    months = ("01", "02", "03", "04", "05")
    listing = {f"full_v2_2026{m}.7z": listed for m in months}
    for month in ("01", "02", "03", "05"):
        (tmp_path / f"full_v2_2026{month}.csv").write_bytes(b"rows")
    manifest = {
        "full_v2_202601.7z": listed,
        # Another size at the same time, and the same size at another time.
        "full_v2_202602.7z": Publication(46_127_843, "2026-10-16T10:00:08"),
        "full_v2_202603.7z": Publication(46_148_464, "2026-09-15T09:38:30"),
        # Recorded as listed, and its CSV missing from the Volume.
        "full_v2_202604.7z": listed,
        # 202605 not recorded, though its CSV is there.
    }
    planned = land.plan_months(
        tuple(f"2026{m}" for m in months), listing, manifest, tmp_path
    )
    assert [(month.period, month.status) for month in planned] == [
        ("202601", "current"),
        ("202602", "replace"),
        ("202603", "replace"),
        ("202604", "replace"),
        ("202605", "new"),
    ]
    assert land.describe(planned[1]) == (
        "202602 full_v2_202602.7z replace: landed 46,127,843 bytes of "
        "2026-10-16T10:00:08, listed 46,148,464 bytes of 2026-10-16T10:00:08"
    )
    assert land.describe(planned[4]).endswith(
        "new: landed none, listed 46,148,464 bytes of 2026-10-16T10:00:08"
    )


# A run of land()
# ---------------


def land_old_month(volume: Path, period: str) -> None:
    """Write a month's CSV and its copy of the figures, as of an earlier publication."""
    (volume / "published").mkdir(parents=True, exist_ok=True)
    (volume / f"full_v2_{period}.csv").write_bytes(b"rows of 09-15")
    (volume / "published" / f"full_v2_{period}.csv").write_bytes(b"figures of 09-15")


def files_below(folder: Path) -> dict[str, bytes]:
    return {
        path.relative_to(folder).as_posix(): path.read_bytes()
        for path in folder.rglob("*")
        if path.is_file()
    }


@pytest.fixture
def volume(tmp_path: Path) -> Path:
    folder = tmp_path / "volume"
    folder.mkdir()
    return folder


@pytest.fixture
def eurostat(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, list[str]]:
    """Serve a real archive of 202605 to every download, and the figures of 2026-05.

    No code list is landed unless a test names some. Return the archive and the
    names of the files downloaded."""
    archive = make_archive(
        tmp_path_factory.mktemp("eurostat") / "full_v2_202605.7z",
        {"full202605.dat": b"rows of 10-16"},
    )
    downloaded: list[str] = []

    def transfer(url: str, target: Path) -> None:
        downloaded.append(target.name)
        shutil.copyfile(archive, target)

    monkeypatch.setattr(land, "_download_to", transfer)
    monkeypatch.setattr(
        land,
        "fetch_published_figures",
        lambda period: land.parse_published_figures(TABLE_202605, period),
    )
    monkeypatch.setattr(land, "CODE_LISTS", ())
    return archive, downloaded


def listing_of(archive: Path, *, extra_bytes: int = 0) -> dict[str, Publication]:
    """A listing that holds 202605 as the archive served, published on 2026-10-16.

    And a newer month, 202606, as the newest one of the listing, published on
    its own four days later, which an open window leaves out."""
    return {
        "full_v2_202605.7z": Publication(
            archive.stat().st_size + extra_bytes, "2026-10-16T10:00:08"
        ),
        "full_v2_202606.7z": Publication(archive.stat().st_size, "2026-10-20T16:03:31"),
    }


def test_a_new_month_is_landed_and_recorded(
    volume: Path, monkeypatch: pytest.MonkeyPatch, eurostat: tuple[Path, list[str]]
) -> None:
    archive, downloaded = eurostat
    listing = listing_of(archive)
    monkeypatch.setattr(land, "fetch_listing", lambda: listing)
    planned = land.land(volume, "202601", None)
    assert [(month.period, month.status) for month in planned] == [("202605", "new")]
    assert downloaded == ["full_v2_202605.7z"]
    files = files_below(volume)
    # The CSV and its copy of the figures; the archive stays on the driver.
    assert sorted(files) == [
        "full_v2_202605.csv",
        "manifest.csv",
        "published/full_v2_202605.csv",
    ]
    assert files["full_v2_202605.csv"] == b"rows of 10-16"
    lines = files["published/full_v2_202605.csv"].decode("utf-8").splitlines()
    assert lines[0] == "period,partner,stk_flow,value_meur,table_updated"
    assert lines[1] == "202605,EU27_2020,EXP,360873.5,2026-10-16T11:00:00+0200"
    assert len(lines) == 7
    assert land.read_manifest(volume / "manifest.csv") == {
        "full_v2_202605.7z": listing["full_v2_202605.7z"]
    }


def test_a_current_month_is_not_downloaded_again(
    volume: Path, monkeypatch: pytest.MonkeyPatch, eurostat: tuple[Path, list[str]]
) -> None:
    archive, downloaded = eurostat
    monkeypatch.setattr(land, "fetch_listing", lambda: listing_of(archive))
    land.land(volume, "202601", None)
    before = files_below(volume)
    planned = land.land(volume, "202601", None)
    assert [month.status for month in planned] == ["current"]
    assert downloaded == ["full_v2_202605.7z"]
    assert files_below(volume) == before


def test_a_republished_month_is_replaced(
    volume: Path, monkeypatch: pytest.MonkeyPatch, eurostat: tuple[Path, list[str]]
) -> None:
    archive, _ = eurostat
    listing = listing_of(archive)
    land_old_month(volume, "202605")
    old = Publication(46_127_843, "2026-09-15T09:38:30")
    land.write_manifest({"full_v2_202605.7z": old}, volume / "manifest.csv")
    monkeypatch.setattr(land, "fetch_listing", lambda: listing)

    planned = land.land(volume, "202601", None)
    assert [month.status for month in planned] == ["replace"]
    files = files_below(volume)
    assert files["full_v2_202605.csv"] == b"rows of 10-16"
    assert files["published/full_v2_202605.csv"].startswith(b"period,partner")
    assert land.read_manifest(volume / "manifest.csv") == {
        "full_v2_202605.7z": listing["full_v2_202605.7z"]
    }


def test_a_republished_month_without_figures_yet_loses_its_old_copy(
    volume: Path,
    monkeypatch: pytest.MonkeyPatch,
    eurostat: tuple[Path, list[str]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # An old copy would reconcile the new file against another publication.
    archive, _ = eurostat
    land_old_month(volume, "202605")
    old = Publication(46_127_843, "2026-09-15T09:38:30")
    land.write_manifest({"full_v2_202605.7z": old}, volume / "manifest.csv")
    monkeypatch.setattr(land, "fetch_listing", lambda: listing_of(archive))
    monkeypatch.setattr(land, "fetch_published_figures", lambda period: [])

    land.land(volume, "202601", None)
    files = files_below(volume)
    assert files["full_v2_202605.csv"] == b"rows of 10-16"
    assert "published/full_v2_202605.csv" not in files
    assert "no figure for 202605" in caplog.text


def test_a_download_that_differs_from_the_listing_is_not_landed(
    volume: Path, monkeypatch: pytest.MonkeyPatch, eurostat: tuple[Path, list[str]]
) -> None:
    archive, _ = eurostat
    listing = listing_of(archive, extra_bytes=1)
    monkeypatch.setattr(land, "fetch_listing", lambda: listing)
    with pytest.raises(OSError, match="not landed"):
        land.land(volume, "202601", None)
    assert files_below(volume) == {}


def test_the_window_lands_only_its_months(
    volume: Path, monkeypatch: pytest.MonkeyPatch, eurostat: tuple[Path, list[str]]
) -> None:
    archive, downloaded = eurostat
    listing = {
        **listing_of(archive),
        "full_v2_202604.7z": Publication(1, "2026-10-16T10:00:07"),
        "full_v2_202606.7z": Publication(1, "2026-10-16T10:00:08"),
    }
    monkeypatch.setattr(land, "fetch_listing", lambda: listing)
    planned = land.land(volume, "202605", "202605")
    assert [month.period for month in planned] == ["202605"]
    assert downloaded == ["full_v2_202605.7z"]


def test_an_open_window_leaves_the_newest_month_out(
    volume: Path, monkeypatch: pytest.MonkeyPatch, eurostat: tuple[Path, list[str]]
) -> None:
    # Published without the intra-EU trade of most countries, which comes with
    # the next release.
    archive, downloaded = eurostat
    monkeypatch.setattr(land, "fetch_listing", lambda: listing_of(archive))
    planned = land.land(volume, "202601", None)
    assert [month.period for month in planned] == ["202605"]
    assert downloaded == ["full_v2_202605.7z"]
    assert "full_v2_202606.7z" not in land.read_manifest(volume / "manifest.csv")


def test_a_window_that_names_the_newest_month_lands_it(
    volume: Path, monkeypatch: pytest.MonkeyPatch, eurostat: tuple[Path, list[str]]
) -> None:
    archive, downloaded = eurostat
    monkeypatch.setattr(land, "fetch_listing", lambda: listing_of(archive))
    planned = land.land(volume, "202601", "202606")
    assert [(month.period, month.status) for month in planned] == [
        ("202605", "new"),
        ("202606", "new"),
    ]
    assert downloaded == ["full_v2_202605.7z", "full_v2_202606.7z"]


def test_an_empty_window_stops_the_run_before_any_download(
    volume: Path, monkeypatch: pytest.MonkeyPatch, eurostat: tuple[Path, list[str]]
) -> None:
    # From the newest month, an open window holds none.
    archive, downloaded = eurostat
    monkeypatch.setattr(land, "fetch_listing", lambda: listing_of(archive))
    with pytest.raises(
        ValueError, match="no monthly archive from 202606 to the month before"
    ):
        land.land(volume, "202606", None)
    assert downloaded == []
    assert files_below(volume) == {}


def test_every_code_list_is_downloaded_over_the_one_in_the_volume(
    volume: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    downloaded: list[str] = []

    def transfer(url: str, target: Path) -> None:
        downloaded.append(target.name)
        target.write_bytes(b"list")

    monkeypatch.setattr(land, "_download_to", transfer)
    monkeypatch.setattr(
        land,
        "CODE_LISTS",
        ("CLASSIFICATIONS/ENGLISH/CN.txt", "CLASSIFICATIONS/ENGLISH/FLOW.txt"),
    )
    # A list landed by an earlier run, which Eurostat may have republished since.
    (volume / "code_lists").mkdir()
    (volume / "code_lists" / "CN.txt").write_bytes(b"landed")
    land.land_code_lists(volume)
    assert downloaded == ["CN.txt", "FLOW.txt"]
    assert files_below(volume) == {
        "code_lists/CN.txt": b"list",
        "code_lists/FLOW.txt": b"list",
    }
