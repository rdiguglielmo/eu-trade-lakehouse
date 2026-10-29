"""The manifest: the publication of each archive landed in the Volume.

src/land.py writes it when it lands a month, and the notebooks read it to tell
which months were landed from a publication they have not processed yet. It
needs neither py7zr nor requests, which only the landing task installs, nor
pyspark.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Publication:
    """An archive's size in bytes and the time it was published, as listed."""

    size: int
    published_at: str  # YYYY-MM-DDTHH:MM:SS


MANIFEST_COLUMNS = ("archive", "archive_bytes", "published_at")


def parse_manifest(text: str) -> dict[str, Publication]:
    """Return the publication of each archive the text of a manifest names."""
    return {
        row["archive"]: Publication(int(row["archive_bytes"]), row["published_at"])
        for row in csv.DictReader(io.StringIO(text))
    }


def format_manifest(manifest: dict[str, Publication]) -> str:
    """Return the text of a manifest, by archive name, with LF line endings."""
    text = io.StringIO()
    writer = csv.writer(text, lineterminator="\n")
    writer.writerow(MANIFEST_COLUMNS)
    for archive in sorted(manifest):
        entry = manifest[archive]
        writer.writerow((archive, entry.size, entry.published_at))
    return text.getvalue()


def read_manifest(path: Path) -> dict[str, Publication]:
    """Return the publication of each landed archive, or nothing without a manifest."""
    if not path.exists():
        return {}
    return parse_manifest(path.read_bytes().decode("utf-8"))


def write_manifest(manifest: dict[str, Publication], path: Path) -> None:
    """Write the manifest by archive name, which only appears once it is complete."""
    part_file = path.with_name(path.name + ".part")
    # In bytes: written as text on Windows, the lines would end in CRLF.
    part_file.write_bytes(format_manifest(manifest).encode("utf-8"))
    part_file.replace(path)
