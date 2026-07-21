from integration.helpers import hub_api


class _Response:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data

    def raise_for_status(self):
        pass


def test_get_flink_job_ids_fetches_step_details_for_continuous_nodes(monkeypatch):
    hub_url = "https://hub.example"
    workflow_id = "workflow-123"
    calls = []

    responses = [
        _Response({
            "workflow": {
                "confName": "team/demo.v1",
                "mode": "backfill",
                "startPartition": "2026-07-01",
                "endPartition": "2026-07-01",
                "workflowPlan": {
                    "nodes": [
                        {"name": "streaming-node", "continuous": True},
                        {"name": "batch-node", "continuous": False},
                    ]
                },
            }
        }),
        _Response({
            "nodeExecutions": [
                {
                    "nodeName": "streaming-node",
                    "stepRuns": [{"runId": "stream-run"}],
                },
                {
                    "nodeName": "batch-node",
                    "stepRuns": [{"runId": "batch-run"}],
                },
            ]
        }),
        _Response({"stepRun": {"jobTrackingInfo": {"jobId": "dataproc-job-1"}}}),
    ]

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return responses.pop(0)

    monkeypatch.setattr(hub_api, "_get_auth_headers", lambda: {"Authorization": "Bearer token"})
    monkeypatch.setattr(hub_api.requests, "get", fake_get)

    assert hub_api.get_flink_job_ids(hub_url, workflow_id) == ["dataproc-job-1"]
    assert calls[2][0] == f"{hub_url}/confs/v2/steps/stream-run/detail"
    assert all("batch-run" not in url for url, _ in calls)
