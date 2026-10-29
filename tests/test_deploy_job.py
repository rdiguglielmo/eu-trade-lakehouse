"""Creating the Job, or replacing the settings of the one in the workspace.

The Jobs API is replaced by a small fake. No test builds a WorkspaceClient,
which would read the credentials of the machine it runs on.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

import pytest
from databricks.sdk.service.jobs import JobSettings

import deploy_job
from deploy_job import deploy, read_job_settings


class FakeJobs:
    """The Jobs API of a workspace holding Jobs by id and name.

    created and replaced record what each call received.
    """

    def __init__(self, names: dict[int, str]) -> None:
        self.names = dict(names)
        self.created: list[dict[str, Any]] = []
        self.replaced: list[tuple[int, JobSettings]] = []

    def list(self, *, name: str) -> list[SimpleNamespace]:
        return [
            SimpleNamespace(job_id=job_id)
            for job_id, job_name in self.names.items()
            if job_name.lower() == name.lower()
        ]

    def create(self, **settings: Any) -> SimpleNamespace:
        self.created.append(settings)
        job_id = max(self.names, default=0) + 1
        self.names[job_id] = settings["name"]
        return SimpleNamespace(job_id=job_id)

    def reset(self, job_id: int, new_settings: JobSettings) -> None:
        self.replaced.append((job_id, new_settings))


@pytest.fixture
def settings() -> JobSettings:
    return read_job_settings()


def test_a_job_not_in_the_workspace_is_created(settings: JobSettings) -> None:
    jobs = FakeJobs({7: "another_job"})
    assert deploy(SimpleNamespace(jobs=jobs), settings) == (8, True)
    assert [JobSettings(**received) for received in jobs.created] == [settings]
    assert jobs.replaced == []


def test_a_job_in_the_workspace_gets_the_settings_of_the_file(
    settings: JobSettings,
) -> None:
    jobs = FakeJobs({7: "another_job", 12: settings.name or ""})
    assert deploy(SimpleNamespace(jobs=jobs), settings) == (12, False)
    assert jobs.replaced == [(12, settings)]
    assert jobs.created == []


def test_two_jobs_of_the_same_name_stop_the_run(settings: JobSettings) -> None:
    jobs = FakeJobs({12: settings.name or "", 13: settings.name or ""})
    with pytest.raises(ValueError, match=r"2 jobs are named .* \(12, 13\)"):
        deploy(SimpleNamespace(jobs=jobs), settings)
    assert jobs.created == []
    assert jobs.replaced == []


def test_main_creates_the_job_of_the_repository(
    monkeypatch: pytest.MonkeyPatch, settings: JobSettings
) -> None:
    jobs = FakeJobs({})
    monkeypatch.setattr(
        deploy_job, "WorkspaceClient", lambda: SimpleNamespace(jobs=jobs)
    )
    monkeypatch.setattr(sys, "argv", ["deploy_job.py"])
    deploy_job.main()
    assert [JobSettings(**received) for received in jobs.created] == [settings]
