"""Direct HTTP helpers for Hub API endpoints not exposed via CLI."""

import os
from urllib.parse import quote

import requests


def _get_auth_headers() -> dict:
    headers = {}
    zipline_token = os.environ.get("ZIPLINE_TOKEN")
    if zipline_token:
        from ai.chronon.repo.token_exchange import resolve_token

        auth_url = os.environ.get("ZIPLINE_AUTH_URL")
        jwt = resolve_token(zipline_token, auth_url)
        headers["Authorization"] = f"Bearer {jwt}"
    else:
        token = os.environ.get("GCP_ID_TOKEN") or os.environ.get("AWS_ID_TOKEN")
        if token:
            headers["Authorization"] = f"Bearer {token}"
    return headers


def list_schedules(hub_url: str) -> list[dict]:
    """GET /schedule/v2/schedules — return all schedules."""
    resp = requests.get(f"{hub_url}/schedule/v2/schedules", headers=_get_auth_headers())
    resp.raise_for_status()
    return resp.json().get("schedules", [])


def delete_schedule(hub_url: str, conf_name: str) -> dict:
    """DELETE /schedule/v2/schedules?confName=... — delete all schedules for a conf."""
    resp = requests.delete(
        f"{hub_url}/schedule/v2/schedules",
        params={"confName": conf_name},
        headers=_get_auth_headers(),
    )
    resp.raise_for_status()
    return resp.json()


def find_schedules_by_test_id(hub_url: str, test_id: str) -> list[dict]:
    """List schedules and filter to those whose confName contains *test_id*."""
    return [s for s in list_schedules(hub_url) if test_id in s.get("confName", "")]


def cancel_streaming_node(hub_url: str, node_name: str) -> None:
    """POST /streaming/v1/cancel — cancel a named streaming (Flink) node via the Hub."""
    resp = requests.post(
        f"{hub_url}/streaming/v1/cancel",
        json={"nodeNames": [node_name]},
        headers=_get_auth_headers(),
    )
    resp.raise_for_status()


def get_continuous_node_names(hub_url: str, workflow_id: str) -> list[str]:
    """Return names of continuous (streaming) nodes from the workflow plan."""
    resp = requests.get(f"{hub_url}/workflow/v2/{workflow_id}", headers=_get_auth_headers())
    resp.raise_for_status()
    workflow = resp.json().get("workflow", resp.json())
    nodes = workflow.get("workflowPlan", {}).get("nodes", [])
    return [n["name"] for n in nodes if n.get("continuous")]


def get_flink_job_ids(hub_url: str, workflow_id: str) -> list[str]:
    """Return Dataproc job IDs for continuous (Flink) steps tracked by *workflow_id*."""
    headers = _get_auth_headers()

    resp = requests.get(f"{hub_url}/workflow/v2/{workflow_id}", headers=headers)
    resp.raise_for_status()
    workflow = resp.json().get("workflow", resp.json())

    conf_name = workflow["confName"]
    mode = workflow["mode"]
    start = workflow["startPartition"]
    end = workflow["endPartition"]

    resp = requests.get(
        f"{hub_url}/confs/v2/{quote(conf_name, safe='')}/status/{quote(mode, safe='')}",
        params={"start": start, "end": end, "workflowId": workflow_id},
        headers=headers,
    )
    resp.raise_for_status()

    job_ids = []
    for node in resp.json().get("nodeExecutions", []):
        for step in node.get("stepRuns", []):
            if step.get("continuous"):
                tracking = step.get("jobTrackingInfo") or {}
                if tracking.get("jobId"):
                    job_ids.append(tracking["jobId"])
    return job_ids
