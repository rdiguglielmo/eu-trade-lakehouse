"""Create the Databricks Job from its definition, or replace the one there.

jobs/trade_pipeline_job.json holds the settings of the Job as the Jobs API
takes them. The Job is found by its name: absent, it is created; present, its
settings are replaced by the file's, so the workspace runs what the repository
holds. Two Jobs of that name stop the run, since which one to replace is
unknown.

Creating a Job starts no compute: it runs when started, from the Jobs page or
the API.

Authentication follows the Databricks SDK default chain: the environment
variables DATABRICKS_HOST and DATABRICKS_TOKEN, or a profile in
~/.databrickscfg, DEFAULT unless DATABRICKS_CONFIG_PROFILE names another. A
personal access token needs the scope jobs.

Usage:
    python src/deploy_job.py
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import fields
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings

from config import PROJECT_ROOT

LOGGER = logging.getLogger("deploy")

JOB_FILE = PROJECT_ROOT / "jobs" / "trade_pipeline_job.json"


def read_job_settings(path: Path = JOB_FILE) -> JobSettings:
    """The settings of the Job, as the file holds them."""
    return JobSettings.from_dict(json.loads(path.read_text(encoding="utf-8")))


def deploy(client: WorkspaceClient, settings: JobSettings) -> tuple[int, bool]:
    """Create the Job, or replace the settings of the one of the same name.

    Returns the id of the Job, and whether it was created.
    """
    found = [
        job.job_id
        for job in client.jobs.list(name=settings.name)
        if job.job_id is not None
    ]
    if len(found) > 1:
        raise ValueError(
            f"{len(found)} jobs are named {settings.name!r} "
            f"({', '.join(str(job_id) for job_id in found)}): delete all but one"
        )
    if found:
        client.jobs.reset(found[0], settings)
        return found[0], False

    created = client.jobs.create(
        **{field.name: getattr(settings, field.name) for field in fields(settings)}
    )
    if created.job_id is None:
        raise RuntimeError(f"The workspace returned no id for {settings.name!r}")
    return created.job_id, True


def main() -> None:
    argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0]).parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    settings = read_job_settings()
    job_id, created = deploy(WorkspaceClient(), settings)
    LOGGER.info(
        "%s job %d, %s",
        "Created" if created else "Replaced the settings of",
        job_id,
        settings.name,
    )


if __name__ == "__main__":
    main()
