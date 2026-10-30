"""Land the Eurostat COMEXT monthly files and the code lists in the Volume.

The first task of the Job. Each month is published as a 7z archive holding a
single .dat member, which is a comma separated file with a header row despite
its extension. The archive is downloaded and extracted on the driver's disk, in
a temporary folder, and the CSV is then copied into the Volume, which does not
take the random writes of an extraction. The archive itself is not kept.

When a month is landed, the figures Eurostat publishes for it are saved with
it, in a subfolder of the Volume and under the name of its CSV, so that the
reconciliation in silver compares the file with the figures of its own
publication. A month whose figures the table does not hold yet lands without a
copy, and the reconciliation does not evaluate it.

The window runs from a start to an end. Without an end, it stops at the month
before the last one listed. Eurostat publishes the detail of a month's extra-EU
trade about 46 days after it ends, and that of its intra-EU trade usually with
the next monthly release, except for the few countries that send both at once.
The newest month of the listing therefore lacks most of its intra-EU trade, and
the reconciliation in silver would stop the run on it: it lands with the next
release, once complete.

Eurostat republishes months under the same name. The listing of the bulk
download facility is compared with the Volume's manifest, which records the
size and the publication time of each landed archive, and each month of the
window is new, current or to be replaced:

- new: the manifest does not name it;
- current: the manifest records the size and time the listing shows, and the
  Volume holds its CSV;
- replace: the manifest records another publication, or the CSV is missing.

A new month and a replaced one land the same way: the CSV is written over any
CSV of that name, and the copy of the figures too, or deleted if the table
holds none yet, since an old copy would reconcile the new file against the
figures of another publication. What was there before stays in the history of
the Delta tables built from it. A month enters the manifest after its files,
so the manifest never records a publication whose files are not there yet.

The code lists are downloaded if the Volume does not hold them.
"""

from __future__ import annotations

import csv
import html
import logging
import math
import re
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from urllib.parse import quote

import py7zr
import requests

from config import (
    CODE_LIST_URL_TEMPLATE,
    CODE_LISTS,
    CODE_LISTS_SUBDIR,
    DOWNLOAD_CHUNK_BYTES,
    EUROSTAT_FILE_URL_TEMPLATE,
    EUROSTAT_LISTING_URL,
    MANIFEST_NAME,
    MAX_EXTRACTED_BYTES,
    MIN_REQUEST_INTERVAL_SECONDS,
    PUBLISHED_SUBDIR,
    PUBLISHED_TABLE_URL_TEMPLATE,
    REQUEST_TIMEOUT_SECONDS,
    archive_name,
    csv_name,
    months_in_window,
)
from manifest import Publication, read_manifest, write_manifest

LOGGER = logging.getLogger("land")

# Which failures are worth retrying, and which are not.
#
# Retried: 429 and the 5xx family, plus dropped connections and timeouts. They
# describe a server that is busy or briefly broken, and the same request will
# very likely succeed shortly.
#
# Not retried: everything else, 403 and 404 included. A 404 means the file name
# is wrong or the month is not published yet, and a 403 means the request was
# refused; neither clears by asking again.
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

MAX_ATTEMPTS = 5

# 1, 2, 4, 8 seconds between the five attempts. Capped so that a server sending
# an implausible Retry-After cannot park the run for an hour.
INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 30.0

# The size check compares the bytes written with Content-Length, which counts
# the bytes on the wire. Asked for no content encoding, the two are the same.
REQUEST_HEADERS = {"Accept-Encoding": "identity"}

# A connection that drops while the body streams in. _get() retries the same
# failure when it comes before the headers; requests raises a read that times
# out mid-body as a ConnectionError.
DROPPED_TRANSFER_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.ConnectionError,
)

_last_request_at = 0.0


def _throttle() -> None:
    """Hold off until the configured request interval has passed."""
    global _last_request_at
    wait_seconds = MIN_REQUEST_INTERVAL_SECONDS - (time.monotonic() - _last_request_at)
    if wait_seconds > 0:
        time.sleep(wait_seconds)
    _last_request_at = time.monotonic()


def _backoff(attempt: int) -> float:
    return min(INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1)), MAX_BACKOFF_SECONDS)


def _retry_delay(response: requests.Response, attempt: int) -> float:
    """Return how long to wait before the next attempt.

    Retry-After is honoured when the server sends it in seconds. An HTTP date, an
    unparseable value, and a number no wait can honour, negative, NaN or
    infinite, fall back to the backoff rather than failing: time.sleep() raises
    on a negative or NaN delay.
    """
    try:
        seconds = float(response.headers.get("Retry-After", ""))
    except ValueError:
        return _backoff(attempt)
    if not math.isfinite(seconds) or seconds < 0:
        return _backoff(attempt)
    return min(seconds, MAX_BACKOFF_SECONDS)


def _get(url: str) -> requests.Response:
    """GET a Eurostat URL as a stream, retrying transient failures.

    This is the one place the access policy is implemented. Returns a response
    that has already passed raise_for_status; the caller owns closing it.
    """
    for attempt in range(1, MAX_ATTEMPTS + 1):
        _throttle()
        try:
            response = requests.get(
                url,
                stream=True,
                timeout=REQUEST_TIMEOUT_SECONDS,
                headers=REQUEST_HEADERS,
            )
        except (requests.ConnectionError, requests.Timeout) as error:
            if attempt == MAX_ATTEMPTS:
                raise
            delay = _backoff(attempt)
            LOGGER.warning(
                "%s on attempt %d/%d for %s, retrying in %.1fs",
                type(error).__name__,
                attempt,
                MAX_ATTEMPTS,
                url,
                delay,
            )
            time.sleep(delay)
            continue

        if response.status_code not in RETRYABLE_STATUS_CODES:
            response.raise_for_status()
            return response

        if attempt == MAX_ATTEMPTS:
            response.raise_for_status()

        delay = _retry_delay(response, attempt)
        LOGGER.warning(
            "HTTP %d on attempt %d/%d for %s, retrying in %.1fs",
            response.status_code,
            attempt,
            MAX_ATTEMPTS,
            url,
            delay,
        )
        response.close()
        time.sleep(delay)

    # Unreachable: the loop either returns or raises on its last attempt.
    raise RuntimeError(f"_get exhausted its attempts without resolving {url}")


def download_archive(period: str, output_dir: Path) -> Path:
    """Download one monthly archive into output_dir and return its path.

    Returns immediately if the archive is already there.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / archive_name(period)
    if target.exists():
        LOGGER.info(
            "%s already present (%d bytes), skipping",
            target.name,
            target.stat().st_size,
        )
        return target

    url = EUROSTAT_FILE_URL_TEMPLATE.format(file_name=target.name)
    _download_to(url, target)
    return target


def download_code_list(relative_path: str, output_dir: Path) -> Path:
    """Download one code list into output_dir and return its path.

    relative_path is the list's path below CLASSIFICATIONS_AND_RELATIONS/, for
    instance CLASSIFICATIONS/ENGLISH/PARTNERS_ISO.txt; the file is saved under
    its own name. A list that is already present is skipped, as an archive is.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / PurePosixPath(relative_path).name
    if target.exists():
        LOGGER.info(
            "%s already present (%d bytes), skipping",
            target.name,
            target.stat().st_size,
        )
        return target

    url = CODE_LIST_URL_TEMPLATE.format(path=quote(relative_path, safe=""))
    _download_to(url, target)
    return target


def _download_to(url: str, target: Path) -> None:
    """Download url to target, which only appears once the transfer is complete.

    The transfer writes to a temporary name and renames only once the size
    announced by the server has arrived, so an interrupted download never leaves
    a truncated file that a later run would mistake for a complete one.

    A connection that drops while the body streams in is retried from the
    start, with the attempts and the backoff of _get().
    """
    part_file = target.with_name(target.name + ".part")

    LOGGER.info("Downloading %s", url)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        with _get(url) as response:
            expected_bytes = int(response.headers.get("Content-Length", 0))
            received_bytes = 0
            try:
                with part_file.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                        handle.write(chunk)
                        received_bytes += len(chunk)
                break
            except DROPPED_TRANSFER_ERRORS as error:
                if attempt == MAX_ATTEMPTS:
                    part_file.unlink(missing_ok=True)
                    raise
                delay = _backoff(attempt)
                LOGGER.warning(
                    "%s after %d bytes on attempt %d/%d for %s, retrying in %.1fs",
                    type(error).__name__,
                    received_bytes,
                    attempt,
                    MAX_ATTEMPTS,
                    url,
                    delay,
                )
        time.sleep(delay)

    if expected_bytes and received_bytes != expected_bytes:
        part_file.unlink()
        raise OSError(
            f"{target.name}: incomplete download, expected {expected_bytes} bytes "
            f"but received {received_bytes}"
        )

    part_file.replace(target)
    LOGGER.info("Saved %s (%d bytes)", target.name, received_bytes)


def extract_csv(archive: Path, output_dir: Path) -> Path:
    """Extract the single .dat member of an archive as <archive stem>.csv.

    Returns immediately if the CSV is already there. The member is extracted
    into a temporary folder inside output_dir and renamed into place, so the
    CSV only exists under its final name once it is complete.

    Raises ValueError if the archive does not hold exactly one .dat member at
    its root: anything else means the publication format changed, and loading
    whatever happens to be inside would hide it. Also if the member would unpack
    to more than MAX_EXTRACTED_BYTES, read from the archive's listing before
    anything is written.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{archive.stem}.csv"
    if target.exists():
        LOGGER.info("%s already present, skipping", target.name)
        return target

    with py7zr.SevenZipFile(archive, "r") as handle:
        members = handle.getnames()
        if len(members) != 1 or not members[0].endswith(".dat"):
            raise ValueError(
                f"{archive.name}: expected one .dat member, found {members}"
            )
        member = members[0]
        if "/" in member or "\\" in member:
            raise ValueError(
                f"{archive.name}: member {member!r} is not at the archive root"
            )
        unpacked_bytes = handle.list()[0].uncompressed
        if unpacked_bytes > MAX_EXTRACTED_BYTES:
            raise ValueError(
                f"{archive.name}: {member} unpacks to {unpacked_bytes} bytes, "
                f"above the ceiling of {MAX_EXTRACTED_BYTES}"
            )

        with tempfile.TemporaryDirectory(dir=output_dir) as scratch:
            handle.extractall(path=scratch)
            (Path(scratch) / member).replace(target)

    LOGGER.info("Extracted %s from %s as %s", member, archive.name, target.name)
    return target


PUBLISHED_COLUMNS = ("period", "partner", "stk_flow", "value_meur", "table_updated")


def fetch_published_figures(period: str) -> list[dict[str, str]]:
    """Return the figures the published table holds today for one month.

    Empty if the month is not in the table yet: the API answers 200 with no
    value, not an error.
    """
    url = PUBLISHED_TABLE_URL_TEMPLATE.format(month=f"{period[:4]}-{period[4:]}")
    with _get(url) as response:
        return parse_published_figures(response.json(), period)


def parse_published_figures(stat: dict[str, Any], period: str) -> list[dict[str, str]]:
    """Read a JSON-stat answer of the published table into one row per figure.

    JSON-stat holds the values in one flat list, or a dict keyed by position,
    in row-major order over the dimensions in `id`. Each dimension's codes are
    ordered by their index in the answer, which is neither the order of the
    request nor alphabetical.
    """
    codes = {}
    for dimension in stat["id"]:
        index = stat["dimension"][dimension]["category"]["index"]
        codes[dimension] = sorted(index, key=index.get)
    values = stat["value"]
    positions = values.items() if isinstance(values, dict) else enumerate(values)
    rows = []
    for flat_index, value in positions:
        if value is None:
            continue
        position, remainder = {}, int(flat_index)
        for dimension, size in reversed(
            list(zip(stat["id"], stat["size"], strict=True))
        ):
            position[dimension] = codes[dimension][remainder % size]
            remainder //= size
        rows.append(
            {
                "period": period,
                "partner": position["partner"],
                "stk_flow": position["stk_flow"],
                "value_meur": str(value),
                "table_updated": stat["updated"],
            }
        )
    return sorted(rows, key=lambda row: (row["partner"], row["stk_flow"]))


def write_published_figures(rows: list[dict[str, str]], target: Path) -> None:
    """Write the figures as a CSV, which only appears once it is complete."""
    target.parent.mkdir(parents=True, exist_ok=True)
    part_file = target.with_name(target.name + ".part")
    with part_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=PUBLISHED_COLUMNS, lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)
    part_file.replace(target)
    LOGGER.info("Saved %d published figures as %s", len(rows), target.name)


def save_published_figures(period: str, target: Path) -> bool:
    """Fetch the figures of one month and save them, if the table has any.

    Returns whether a copy was written.
    """
    rows = fetch_published_figures(period)
    if not rows:
        LOGGER.warning(
            "The published table holds no figure for %s yet: %s not written, "
            "and the reconciliation will not evaluate this file",
            period,
            target.name,
        )
        return False
    write_published_figures(rows, target)
    return True


# The listing
# -----------

# One file of the listing: the link that names it, a cell with its size in
# bytes in the title, its type, and the time it was published, as DD/MM/YYYY
# HH:MM:SS after a non-breaking space.
_LISTING_ROW = re.compile(
    r">([^<>]+)</a></td>"
    r'<td title="(\d+) bytes">[^<]*</td>'
    r'<td class="center">[^<]*</td>'
    r"<td>[^<\d]*(\d\d)/(\d\d)/(\d{4}) (\d\d:\d\d:\d\d)</td>"
)


def parse_listing(page: str) -> dict[str, Publication]:
    """Read the listing into each file's publication, by file name.

    Raises ValueError if the page holds no file: the listing changed its
    format, and reading it as empty would make every month look unpublished.
    """
    files = {
        html.unescape(name): Publication(int(size), f"{year}-{month}-{day}T{at}")
        for name, size, day, month, year, at in _LISTING_ROW.findall(page)
    }
    if not files:
        raise ValueError("The bulk download listing holds no file: its format changed")
    return files


def fetch_listing() -> dict[str, Publication]:
    """Return the publication of every file in the folder of the monthly archives."""
    # Served as UTF-8, and decoded as such rather than by guessing.
    with _get(EUROSTAT_LISTING_URL) as response:
        return parse_listing(response.content.decode("utf-8"))


# What each month needs
# ---------------------

Status = Literal["new", "current", "replace"]


@dataclass(frozen=True)
class PlannedMonth:
    """One month of the window: its archive, what it needs, and both publications."""

    period: str
    archive: str
    status: Status
    landed: Publication | None
    listed: Publication


def plan_months(
    periods: tuple[str, ...],
    listing: dict[str, Publication],
    manifest: dict[str, Publication],
    volume: Path,
) -> list[PlannedMonth]:
    """Compare each month's landed archive with the listing.

    listing has to hold every month of periods, as months_in_window() over it
    ensures; manifest is the Volume's. The statuses are those of the module
    docstring.
    """
    planned = []
    for period in periods:
        archive = archive_name(period)
        landed = manifest.get(archive)
        listed = listing[archive]
        status: Status
        if landed is None:
            status = "new"
        elif landed == listed and (volume / csv_name(period)).exists():
            status = "current"
        else:
            status = "replace"
        planned.append(PlannedMonth(period, archive, status, landed, listed))
    return planned


def describe(month: PlannedMonth) -> str:
    """One line of the plan: the month, its archive, its status and publications."""

    def show(entry: Publication | None) -> str:
        return f"{entry.size:,} bytes of {entry.published_at}" if entry else "none"

    return (
        f"{month.period} {month.archive} {month.status}: "
        f"landed {show(month.landed)}, listed {show(month.listed)}"
    )


# Landing
# -------


def land_month(period: str, listed: Publication, volume: Path) -> None:
    """Download and extract one month, and copy its CSV and figures to the Volume.

    The figures are fetched right after the archive. Raises OSError, before
    anything reaches the Volume, if the archive is not the size listed: the
    listing was read before the download, and a file of another size means it
    changed in between, so its time would be wrong.
    """
    name = csv_name(period)
    with tempfile.TemporaryDirectory() as scratch:
        work = Path(scratch)
        archive = download_archive(period, work)
        received = archive.stat().st_size
        if received != listed.size:
            raise OSError(
                f"{archive.name}: {received} bytes downloaded, {listed.size} listed: "
                "not landed"
            )
        local_copy = work / PUBLISHED_SUBDIR / name
        has_copy = save_published_figures(period, local_copy)
        extracted = extract_csv(archive, work)

        shutil.copyfile(extracted, volume / name)
        LOGGER.info("Copied %s to %s", name, volume)
        copy = volume / PUBLISHED_SUBDIR / name
        if has_copy:
            copy.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(local_copy, copy)
        elif copy.exists():
            copy.unlink()
            LOGGER.info("Deleted the copy of the figures of an earlier publication")


def write_volume_manifest(manifest: dict[str, Publication], volume: Path) -> None:
    """Write the manifest on the driver's disk and copy it over the Volume's."""
    with tempfile.TemporaryDirectory() as scratch:
        local = Path(scratch) / MANIFEST_NAME
        write_manifest(manifest, local)
        shutil.copyfile(local, volume / MANIFEST_NAME)


def land_code_lists(volume: Path) -> None:
    """Download each code list the Volume does not hold yet."""
    target_dir = volume / CODE_LISTS_SUBDIR
    with tempfile.TemporaryDirectory() as scratch:
        for relative_path in CODE_LISTS:
            name = PurePosixPath(relative_path).name
            if (target_dir / name).exists():
                LOGGER.info("%s already in the Volume, skipping", name)
                continue
            downloaded = download_code_list(relative_path, Path(scratch))
            target_dir.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(downloaded, target_dir / name)


def land(volume: Path, start: str, end: str | None) -> list[PlannedMonth]:
    """Land every month of the window that is new or republished, and the code lists.

    The window runs from start to end, or to the month before the last one the
    listing holds. Returns the plan of each month.
    """
    listing = fetch_listing()
    periods = months_in_window(listing, start, end)
    if end is None:
        # The newest month waits for its intra-EU trade (module docstring).
        periods = periods[:-1]
    if not periods:
        raise ValueError(
            f"The listing holds no monthly archive from {start} to "
            f"{end or 'the month before the last one'}"
        )
    manifest = read_manifest(volume / MANIFEST_NAME)
    planned = plan_months(periods, listing, manifest, volume)
    for month in planned:
        LOGGER.info("%s", describe(month))

    for month in planned:
        if month.status == "current":
            continue
        land_month(month.period, month.listed, volume)
        manifest[month.archive] = month.listed
        write_volume_manifest(manifest, volume)
        LOGGER.info("Recorded %s in %s", month.archive, MANIFEST_NAME)

    land_code_lists(volume)
    return planned
