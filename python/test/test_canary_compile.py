"""End-to-end `zipline compile` checks against the canary confs.

These don't require a cluster — they just exercise the compile CLI locally and
assert post-conditions on the compiled thriftjson under `canary/compiled/`.
Kept in the unit suite (not `test/integration/`) so PR CI runs them via
`./mill python.test`; the integration suite only runs post-merge via
`push_to_canary.yaml` and filters to `-k test_run_quickstart`.
"""

import json
import os

import pytest
from click.testing import CliRunner

from ai.chronon.repo.compile import compile
from ai.chronon.utils import OUTPUT_NAMESPACE_PLACEHOLDER


def _compile_canary(canary_root):
    """Run `zipline compile --chronon-root <canary_root> --force`. Raises on non-zero
    exit with the full CLI output for actionable failure messages."""
    runner = CliRunner()
    result = runner.invoke(
        compile,
        ["--chronon-root", canary_root, "--force"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"Canary compile failed:\n{result.output}"


def test_canary_compile_resolves_namespace_placeholder(canary):
    """Run `zipline compile` against the canary confs and assert no compiled
    thriftjson contains the internal `OUTPUT_NAMESPACE_PLACEHOLDER` token. The
    placeholder is emitted by `utils.output_table_name` when `.table` is accessed
    at Python authoring time before namespace propagation; the compile pass must
    substitute every occurrence before the Thrift is serialized. Every
    fully-qualified table name must land as a literal `<namespace>.<name>`.
    """
    _compile_canary(canary)

    compiled_dir = os.path.join(canary, "compiled")
    assert os.path.isdir(compiled_dir), f"Expected compiled/ at {compiled_dir}"

    violations = []
    for root, _, files in os.walk(compiled_dir):
        for f in files:
            path = os.path.join(root, f)
            with open(path) as fh:
                contents = fh.read()
            if OUTPUT_NAMESPACE_PLACEHOLDER in contents:
                violations.append(os.path.relpath(path, canary))

    assert not violations, (
        f"Canary compile leaked {OUTPUT_NAMESPACE_PLACEHOLDER!r} into compiled output:\n"
        + "\n".join(violations)
    )


# (description, compiled_relative_path, expected_namespace) — each exercises team-default
# namespace inheritance for a conf type whose authoring file sets no explicit `output_namespace=`.
_TEAM_DEFAULT_FIXTURES = [
    (
        "StagingQuery (no output_namespace= -> gcp team default 'data')",
        "compiled/staging_queries/gcp/team_default_ns_example.v1__0",
        "data",
    ),
    (
        "Join (no output_namespace= -> gcp team default 'data')",
        "compiled/joins/gcp/team_default_ns_join.v1__0",
        "data",
    ),
    (
        "GroupBy (no output_namespace= -> gcp team default 'data')",
        "compiled/group_bys/gcp/team_default_ns_gb.v1__0",
        "data",
    ),
    (
        "ModelTransforms (no output_namespace= -> gcp team default 'data')",
        "compiled/model_transforms/gcp/team_default_ns_mt.v1__1",
        "data",
    ),
    (
        "Model (no output_namespace= -> gcp team default 'data')",
        "compiled/models/gcp/click_through_rate.ctr_model__1.0",
        "data",
    ),
]


@pytest.mark.parametrize("description,relpath,expected_namespace", _TEAM_DEFAULT_FIXTURES)
def test_team_default_namespace_inheritance_per_conf_type(
    canary, description, relpath, expected_namespace
):
    """For each conf type, a canary fixture that omits `output_namespace=` must compile
    with the team's default namespace stamped onto `metaData.outputNamespace`. Guards
    against regressions in `_propagate_namespace` — every supported conf type must
    participate in inheritance.
    """
    _compile_canary(canary)

    path = os.path.join(canary, relpath)
    assert os.path.exists(path), f"Expected compiled output at {relpath} for {description}"

    payload = json.loads(open(path).read())
    ns = payload.get("metaData", {}).get("outputNamespace")
    assert ns == expected_namespace, (
        f"{description}: expected outputNamespace={expected_namespace!r}, got {ns!r}"
    )


def test_team_default_placeholder_resolves_through_cross_config_table_refs(canary):
    """When a consumer config captures a producer's `.table` at Python authoring time and
    the producer had no `output_namespace=` set, the compile pipeline's placeholder pass
    must resolve the token to the producer's team-default namespace in the consumer's
    compiled output.

    Exercised by: `joins/gcp/team_default_ns_join.py` reading
    `team_default_ns_example.v1.table` (a StagingQuery with no output_namespace).
    """
    _compile_canary(canary)

    path = os.path.join(canary, "compiled/joins/gcp/team_default_ns_join.v1__0")
    payload = json.loads(open(path).read())

    left_table = payload["left"]["events"]["table"]
    assert left_table == "data.gcp_team_default_ns_example_v1__0", (
        f"Join.left.events.table should resolve the internal placeholder against the "
        f"producer's team-default namespace, got {left_table!r}"
    )
