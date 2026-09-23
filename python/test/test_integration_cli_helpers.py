"""Tests for integration/helpers/output.py."""

import pytest

from integration.helpers.output import extract_workflow_id


WORKFLOW_ID = "ca5f6502-e3ce-46e7-806f-12c4d66518ad"
JSON_BLOCK = f'{{\n    "workflowId": "{WORKFLOW_ID}"\n}}'


def test_clean_output():
    """No extra content — baseline case."""
    assert extract_workflow_id(JSON_BLOCK) == WORKFLOW_ID


def test_compilation_preamble():
    """Compilation output before the JSON (normal case)."""
    output = f"""
────── COMPILATION SUMMARY ──────
  prod  ✓ OK → compiled (229 written)
{JSON_BLOCK}
"""
    assert extract_workflow_id(output) == WORKFLOW_ID


def test_warning_after_json():
    """ZIPLINE_ALLOW_DIRTY warning printed after JSON (CI ordering)."""
    output = f"""{JSON_BLOCK}
⚠️  ZIPLINE_ALLOW_DIRTY is set — proceeding despite dirty git state:
   M chronon/python/test/canary/compiled/group_bys/aws/user_activities.v1__1
   ... and 304 more
"""
    assert extract_workflow_id(output) == WORKFLOW_ID


def test_warning_before_json():
    """ZIPLINE_ALLOW_DIRTY warning printed before JSON (local ordering)."""
    output = f"""
⚠️  ZIPLINE_ALLOW_DIRTY is set — proceeding despite dirty git state:
   M python/test/canary/compiled/joins/gcp/demo.v1__1
{JSON_BLOCK}
"""
    assert extract_workflow_id(output) == WORKFLOW_ID


def test_no_json_raises():
    """Assert fails clearly when there is no JSON at all."""
    with pytest.raises(AssertionError, match="No JSON in output"):
        extract_workflow_id("something went wrong\nno json here")


def test_missing_workflow_id_raises():
    """Assert fails clearly when JSON has no workflowId key."""
    with pytest.raises(AssertionError, match="No workflowId in response"):
        extract_workflow_id('{"status": "ok"}')
