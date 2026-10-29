"""The definition of the Databricks Job, against the notebooks and the lock.

The Job cannot run outside Databricks, so these tests read its JSON: each task
runs a notebook of the repository, in their order; the parameters are the two
widgets of the window, which every notebook that reads them creates with the
same defaults; and the landing task installs the py7zr the lock pins.
"""

from __future__ import annotations

import json
import re
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest
from databricks.sdk.service.jobs import JobSettings

import config

JOB_FILE = config.PROJECT_ROOT / "jobs" / "trade_pipeline_job.json"
NOTEBOOKS = sorted((config.PROJECT_ROOT / "notebooks").glob("0*.py"))

# The notebooks that read the window. The views read every month of gold.
WINDOWED = [path for path in NOTEBOOKS if not path.name.startswith("06_")]


@pytest.fixture(scope="module")
def job() -> dict[str, Any]:
    return json.loads(JOB_FILE.read_text(encoding="utf-8"))


def test_every_key_is_a_setting_of_the_jobs_api(job: dict[str, Any]) -> None:
    # The SDK drops a key it does not know, so a misspelled one would never
    # reach the workspace.
    assert JobSettings.from_dict(job).as_dict() == job


def test_the_tasks_run_the_notebooks_in_order(job: dict[str, Any]) -> None:
    tasks = job["tasks"]
    assert [task["notebook_task"]["notebook_path"] for task in tasks] == [
        f"notebooks/{path.stem}" for path in NOTEBOOKS
    ]
    assert all(task["notebook_task"]["source"] == "GIT" for task in tasks)
    assert "depends_on" not in tasks[0]
    for before, task in pairwise(tasks):
        assert task["depends_on"] == [{"task_key": before["task_key"]}]


def test_one_run_at_a_time_on_serverless(job: dict[str, Any]) -> None:
    # Two runs at once would find the same pending months, since the gold
    # checks write the watermark last.
    assert job["max_concurrent_runs"] == 1
    for task in job["tasks"]:
        assert not {"existing_cluster_id", "new_cluster", "job_cluster_key"} & set(task)


def test_only_the_landing_installs_py7zr_and_at_the_version_locked(
    job: dict[str, Any],
) -> None:
    lock = (config.PROJECT_ROOT / "requirements.txt").read_text(encoding="utf-8")
    locked = re.search(r"^py7zr==(\S+)", lock, flags=re.MULTILINE)
    assert locked is not None
    (environment,) = job["environments"]
    assert environment["spec"]["dependencies"] == [f"py7zr=={locked.group(1)}"]
    keys = {task["task_key"]: task.get("environment_key") for task in job["tasks"]}
    assert keys.pop("land") == environment["environment_key"]
    assert set(keys.values()) == {None}


def test_the_monthly_schedule_is_paused(job: dict[str, Any]) -> None:
    # A run costs quota: the schedule shows when it would run, and runs nothing.
    assert job["schedule"]["pause_status"] == "PAUSED"


def test_the_parameters_are_the_window(job: dict[str, Any]) -> None:
    # A job parameter reaches every task as the widget of the same name.
    defaults = {
        parameter["name"]: parameter["default"] for parameter in job["parameters"]
    }
    assert defaults == {"start_period": "202601", "end_period": ""}
    assert config.window_from(defaults["start_period"], defaults["end_period"]) == (
        "202601",
        None,
    )


@pytest.mark.parametrize("notebook", WINDOWED, ids=lambda path: path.name)
def test_each_notebook_creates_the_widgets_before_it_reads_them(
    notebook: Path, job: dict[str, Any]
) -> None:
    # In a cell of their own, since Databricks never shows a widget created by
    # a command that fails, and with the defaults of the Job.
    text = notebook.read_text(encoding="utf-8")
    start = text.index("# DBTITLE 1,Run settings\n")
    end = text.index("# COMMAND ----------", start)
    cell = text[start:end]
    for parameter in job["parameters"]:
        name, default = parameter["name"], parameter["default"]
        assert f'dbutils.widgets.text("{name}", "{default}")' in cell
        assert text.index(f'dbutils.widgets.get("{name}")') > end
