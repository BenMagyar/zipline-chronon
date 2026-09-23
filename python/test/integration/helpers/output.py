"""Pure output-parsing helpers with no heavy dependencies."""

import json


def extract_workflow_id(output: str) -> str:
    """Extract the workflowId from CLI JSON output.

    Uses raw_decode so trailing content (e.g. ZIPLINE_ALLOW_DIRTY warnings
    mixed into stdout by CliRunner) doesn't break JSON parsing.
    """
    json_start = output.rfind("{")
    assert json_start != -1, f"No JSON in output:\n{output}"
    response, _ = json.JSONDecoder().raw_decode(output[json_start:])
    workflow_id = response.get("workflowId")
    assert workflow_id, f"No workflowId in response:\n{output}"
    return workflow_id
