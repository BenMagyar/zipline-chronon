"""Cloud-agnostic integration tests for the schedule lifecycle: deploy, verify, delete.

Replaces the former test_gcp_schedule.py.
"""

from collections import Counter

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_schedule
from .helpers.hub_api import delete_schedule, find_schedules_by_test_id

DEMO_BACKFILL = {
    "gcp": "compiled/joins/gcp/demo.v1__1",
    "aws": "compiled/joins/aws/demo.v1__1",
    "azure": "compiled/joins/azure/demo.v2",
}

EXPECTED_SCHEDULE_CONF_COUNTS = {
    "gcp": {
        "gcp.demo_{test_id}.v1__1": 2,
        "gcp.dim_listings_{test_id}.v1__0": 1,
        "gcp.dim_merchants_{test_id}.v1__0": 1,
    },
    "aws": {
        "aws.demo_{test_id}.v1__1": 2,
        "aws.dim_listings_{test_id}.v1__0": 1,
        "aws.dim_merchants_{test_id}.v1__0": 1,
    },
    "azure": {
        "azure.demo_{test_id}.v2": 2,
        "azure.dim_listings_{test_id}.v3": 1,
        "azure.dim_merchants_{test_id}.v2": 1,
    },
}


EXPECTED_SCHEDULE_MODE_COUNTS = {
    "gcp": {
        ("gcp.demo_{test_id}.v1__1", 0): 1,
        ("gcp.demo_{test_id}.v1__1", 1): 1,
        ("gcp.dim_listings_{test_id}.v1__0", 1): 1,
        ("gcp.dim_merchants_{test_id}.v1__0", 1): 1,
    },
    "aws": {
        ("aws.demo_{test_id}.v1__1", 0): 1,
        ("aws.demo_{test_id}.v1__1", 1): 1,
        ("aws.dim_listings_{test_id}.v1__0", 1): 1,
        ("aws.dim_merchants_{test_id}.v1__0", 1): 1,
    },
    "azure": {
        ("azure.demo_{test_id}.v2", 0): 1,
        ("azure.demo_{test_id}.v2", 1): 1,
        ("azure.dim_listings_{test_id}.v3", 1): 1,
        ("azure.dim_merchants_{test_id}.v2", 1): 1,
    },
}


def _expected_schedule_conf_counts(cloud: str, test_id: str) -> Counter:
    return Counter(
        {
            conf_name.format(test_id=test_id): count
            for conf_name, count in EXPECTED_SCHEDULE_CONF_COUNTS[cloud].items()
        }
    )


def _normalize_mode(mode):
    if isinstance(mode, str):
        mode_by_name = {"BACKFILL": 0, "DEPLOY": 1, "MONITOR": 2}
        if mode.upper() in mode_by_name:
            return mode_by_name[mode.upper()]
        return int(mode)
    return mode


def _expected_schedule_mode_counts(cloud: str, test_id: str) -> Counter:
    return Counter(
        {
            (conf_name.format(test_id=test_id), mode): count
            for (conf_name, mode), count in EXPECTED_SCHEDULE_MODE_COUNTS[cloud].items()
        }
    )


@pytest.mark.integration
def test_schedule_lifecycle(confs, test_id, chronon_root, hub_url, cloud):
    """Deploy a schedule, verify it exists, delete it, verify it's gone."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    submit_schedule(runner, chronon_root, hub_url, confs(DEMO_BACKFILL[cloud]))

    assertion_error = None
    try:
        schedules = find_schedules_by_test_id(hub_url, test_id)
        actual_counts = Counter(s["confName"] for s in schedules)
        expected_counts = _expected_schedule_conf_counts(cloud, test_id)
        if actual_counts != expected_counts:
            assertion_error = AssertionError(
                f"Unexpected schedules for test_id={test_id}: {schedules}"
            )
        elif all("mode" in schedule for schedule in schedules):
            actual_mode_counts = Counter(
                (schedule["confName"], _normalize_mode(schedule["mode"]))
                for schedule in schedules
            )
            expected_mode_counts = _expected_schedule_mode_counts(cloud, test_id)
            if actual_mode_counts != expected_mode_counts:
                assertion_error = AssertionError(
                    f"Unexpected schedule modes for test_id={test_id}: {schedules}"
                )
    finally:
        schedules = find_schedules_by_test_id(hub_url, test_id)
        for conf_name in sorted({s["confName"] for s in schedules}):
            delete_schedule(hub_url, conf_name)

    schedules = find_schedules_by_test_id(hub_url, test_id)
    assert len(schedules) == 0, f"Schedules still present after delete: {schedules}"
    if assertion_error:
        raise assertion_error
