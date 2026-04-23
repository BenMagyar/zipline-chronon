import os
import json
import sys
from textwrap import dedent
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from gen_thrift.api.ttypes import (
    EventSource,
    GroupBy,
    Join,
    JoinPart,
    JoinSource,
    MetaData,
    ModelTransforms,
    Source,
    Team,
)
from gen_thrift.common.ttypes import ExecutionInfo

from ai.chronon.cli.compile import parse_configs
from ai.chronon.cli.compile.compile_context import CONFIG_INFOS, CompileContext
from ai.chronon.cli.compile.parse_teams import update_metadata
from ai.chronon.repo.compile import __compile, compile
from ai.chronon.utils import OUTPUT_NAMESPACE_PLACEHOLDER


def user_authoring_folders():
    """Folder names under `chronon_root` that users place authoring `.py` files in.
    Derived from the canonical `CONFIG_INFOS` registry so adding a new conf type
    automatically propagates to the test helpers below. Lives in the test file (not
    in production code) because nothing in the compile pipeline itself needs it."""
    return [ci.folder_name for ci in CONFIG_INFOS if ci.config_type is not None]


def test_compile(repo):
    import sys
    sys.path.append(repo)
    results = __compile(chronon_root=repo, ignore_python_errors=True)
    assert len(results) != 0


def test_compile_gcp_resources(gcp_resources):
    """Test compilation of GCP resource examples."""
    import sys
    sys.path.insert(0, gcp_resources)
    results = __compile(chronon_root=gcp_resources, ignore_python_errors=True)
    assert len(results) != 0

def test_compile_aws_resources(aws_resources):
    """Test compilation of AWS resource examples."""
    import sys
    sys.path.insert(0, aws_resources)
    results = __compile(chronon_root=aws_resources, ignore_python_errors=True)
    assert len(results) != 0

def test_compile_azure_resources(azure_resources):
    """Test compilation of Azure resource examples."""
    import sys
    sys.path.insert(0, azure_resources)
    results = __compile(chronon_root=azure_resources, ignore_python_errors=True)
    assert len(results) != 0

def test_parse_configs_relative_source_file():
    """Test that sourceFile is stored as a path relative to chronon_root."""
    # Setup
    test_root = "/fake/root/path"
    test_file_path = "/fake/root/path/group_bys/team/test_group_by.py"
    test_input_dir = os.path.join(test_root, "group_bys")

    # Create a properly initialized GroupBy object with MetaData
    mock_obj = GroupBy()
    mock_obj.metaData = MetaData()

    # Create mock context
    mock_compile_context = MagicMock(spec=CompileContext)
    mock_compile_context.chronon_root = test_root
    mock_compile_context.teams_dict = {}
    mock_compile_context.validator = MagicMock()
    mock_compile_context.validator.validate_obj.return_value = []
    mock_compile_context.compile_status = MagicMock()

    # Configure mocks
    with patch('ai.chronon.cli.compile.parse_configs.from_file') as mock_from_file, \
         patch('ai.chronon.cli.compile.serializer.thrift_simple_json') as mock_serialize, \
         patch('glob.glob', return_value=[test_file_path]), \
         patch('ai.chronon.cli.compile.parse_teams.update_metadata'), \
         patch('ai.chronon.cli.compile.parse_configs.populate_column_hashes'):

        # Configure mock return values
        mock_from_file.return_value = {"team.test_group_by.test_var": mock_obj}
        mock_serialize.return_value = "{}"

        # Call the function being tested
        results = parse_configs.from_folder(GroupBy, test_input_dir, mock_compile_context)

    # Assertions
    assert len(results) == 1
    assert results[0].obj is not None
    assert hasattr(results[0].obj, 'metaData')
    assert results[0].obj.metaData is not None

    # The sourceFile should be a relative path from chronon_root
    expected_relative_path = "group_bys/team/test_group_by.py"
    assert results[0].obj.metaData.sourceFile == expected_relative_path
    assert not results[0].obj.metaData.sourceFile.startswith("/")  # Should be relative, not absolute


def test_compile_with_json_format(canary):
    """Test that compile command with --format json returns valid JSON output."""
    import sys
    sys.path.append(canary)

    runner = CliRunner()
    result = runner.invoke(compile, [
        '--chronon-root', canary,
        '-f', 'json',
        '--ignore-python-errors',
        '--force',
    ])

    # Check that the command executed successfully
    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    # Verify that the output is valid JSON (should be clean JSON with no tracebacks)
    try:
        output_json = json.loads(result.output)
    except json.JSONDecodeError as e:
        assert False, f"Output is not valid JSON: {result.output}\nError: {e}"

    # Verify the JSON structure contains expected fields
    assert "status" in output_json, f"Output missing 'status' field: {output_json}"
    assert output_json["status"] == "success", f"Expected status 'success', got: {output_json['status']}"

    assert "results" in output_json, f"Output missing 'results' field: {output_json}"
    assert isinstance(output_json["results"], dict), f"'results' should be a dict, got: {type(output_json['results'])}"




def _make_team_dict(namespace="test_namespace"):
    """Helper to create a minimal team dict for update_metadata tests."""
    team = Team(outputNamespace=namespace)
    team.executionInfo = ExecutionInfo()
    default_team = Team(outputNamespace="default_ns")
    default_team.executionInfo = ExecutionInfo()
    return {"test_team": team, "default": default_team}


def test_update_metadata_propagates_namespace_to_groupby_join_source():
    """When a GroupBy uses a JoinSource whose Join has no outputNamespace,
    update_metadata should propagate the GroupBy's namespace to the embedded Join."""
    inner_join = Join(
        metaData=MetaData(name="test_team.parent_join", team="test_team"),
        left=Source(events=EventSource(table="some_table")),
    )
    assert inner_join.metaData.outputNamespace is None

    group_by = GroupBy(
        sources=[Source(joinSource=JoinSource(join=inner_join))],
        keyColumns=["user_id"],
        metaData=MetaData(name="test_team.my_gb", team="test_team"),
    )

    update_metadata(group_by, _make_team_dict("gb_namespace"))

    assert group_by.metaData.outputNamespace == "gb_namespace"
    assert inner_join.metaData.outputNamespace == "gb_namespace"


def test_update_metadata_does_not_overwrite_existing_join_source_namespace():
    """If the JoinSource's Join already has an outputNamespace, don't overwrite it."""
    inner_join = Join(
        metaData=MetaData(name="test_team.parent_join", team="test_team", outputNamespace="explicit_ns"),
        left=Source(events=EventSource(table="some_table")),
    )

    group_by = GroupBy(
        sources=[Source(joinSource=JoinSource(join=inner_join))],
        keyColumns=["user_id"],
        metaData=MetaData(name="test_team.my_gb", team="test_team"),
    )

    update_metadata(group_by, _make_team_dict("gb_namespace"))

    assert inner_join.metaData.outputNamespace == "explicit_ns"


def test_update_metadata_propagates_namespace_to_model_transforms_join_source():
    """Same propagation should work for ModelTransforms with JoinSource."""
    inner_join = Join(
        metaData=MetaData(name="test_team.parent_join", team="test_team"),
        left=Source(events=EventSource(table="some_table")),
    )

    mt = ModelTransforms(
        sources=[Source(joinSource=JoinSource(join=inner_join))],
        metaData=MetaData(name="test_team.my_mt", team="test_team"),
    )

    update_metadata(mt, _make_team_dict("mt_namespace"))

    assert mt.metaData.outputNamespace == "mt_namespace"
    assert inner_join.metaData.outputNamespace == "mt_namespace"


def test_update_metadata_propagates_namespace_through_join_to_groupby_join_source():
    """Join -> GroupBy (with JoinSource) -> inner Join: namespace should reach the inner Join.

    This is the chaining pattern: 6 groupbys -> join with groupby_v1,
    where groupby_v1 reads from a previous join via JoinSource.
    """
    inner_join = Join(
        metaData=MetaData(name="test_team.inner_join", team="test_team"),
        left=Source(events=EventSource(table="inner_table")),
    )

    chained_gb = GroupBy(
        sources=[Source(joinSource=JoinSource(join=inner_join))],
        keyColumns=["user_id"],
        metaData=MetaData(name="test_team.chained_gb", team="test_team"),
    )

    outer_join = Join(
        joinParts=[JoinPart(groupBy=chained_gb)],
        metaData=MetaData(name="test_team.outer_join", team="test_team"),
        left=Source(events=EventSource(table="outer_table")),
    )

    update_metadata(outer_join, _make_team_dict("ns"))

    assert outer_join.metaData.outputNamespace == "ns"
    assert chained_gb.metaData.outputNamespace == "ns"
    assert inner_join.metaData.outputNamespace == "ns"


def test_update_metadata_propagates_namespace_deeply_nested():
    """Full chain: Join -> GroupBy -> JoinSource(Join) -> GroupBy -> JoinSource(Join).

    Verifies namespace propagates to arbitrary depth.
    """
    deepest_join = Join(
        metaData=MetaData(name="test_team.deepest_join", team="test_team"),
        left=Source(events=EventSource(table="deepest_table")),
    )

    mid_gb = GroupBy(
        sources=[Source(joinSource=JoinSource(join=deepest_join))],
        keyColumns=["id"],
        metaData=MetaData(name="test_team.mid_gb", team="test_team"),
    )

    mid_join = Join(
        joinParts=[JoinPart(groupBy=mid_gb)],
        metaData=MetaData(name="test_team.mid_join", team="test_team"),
        left=Source(events=EventSource(table="mid_table")),
    )

    top_gb = GroupBy(
        sources=[Source(joinSource=JoinSource(join=mid_join))],
        keyColumns=["id"],
        metaData=MetaData(name="test_team.top_gb", team="test_team"),
    )

    outer_join = Join(
        joinParts=[JoinPart(groupBy=top_gb)],
        metaData=MetaData(name="test_team.outer_join", team="test_team"),
        left=Source(events=EventSource(table="outer_table")),
    )

    update_metadata(outer_join, _make_team_dict("deep_ns"))

    assert outer_join.metaData.outputNamespace == "deep_ns"
    assert top_gb.metaData.outputNamespace == "deep_ns"
    assert mid_join.metaData.outputNamespace == "deep_ns"
    assert mid_gb.metaData.outputNamespace == "deep_ns"
    assert deepest_join.metaData.outputNamespace == "deep_ns"


def test_update_metadata_propagates_namespace_to_join_left_join_source():
    """Join whose left source is a JoinSource should also get namespace propagated."""
    inner_join = Join(
        metaData=MetaData(name="test_team.left_source_join", team="test_team"),
        left=Source(events=EventSource(table="some_table")),
    )

    outer_join = Join(
        joinParts=[],
        metaData=MetaData(name="test_team.outer_join", team="test_team"),
        left=Source(joinSource=JoinSource(join=inner_join)),
    )

    update_metadata(outer_join, _make_team_dict("left_ns"))

    assert outer_join.metaData.outputNamespace == "left_ns"
    assert inner_join.metaData.outputNamespace == "left_ns"


def test_update_metadata_preserves_existing_namespace_in_deep_chain():
    """If a node in the middle of a chain already has a namespace, don't overwrite it,
    but still propagate to its children that don't have one."""
    deepest_join = Join(
        metaData=MetaData(name="test_team.deepest_join", team="test_team"),
        left=Source(events=EventSource(table="deepest_table")),
    )

    mid_gb = GroupBy(
        sources=[Source(joinSource=JoinSource(join=deepest_join))],
        keyColumns=["id"],
        metaData=MetaData(name="test_team.mid_gb", team="test_team", outputNamespace="explicit_ns"),
    )

    outer_join = Join(
        joinParts=[JoinPart(groupBy=mid_gb)],
        metaData=MetaData(name="test_team.outer_join", team="test_team"),
        left=Source(events=EventSource(table="outer_table")),
    )

    update_metadata(outer_join, _make_team_dict("outer_ns"))

    assert outer_join.metaData.outputNamespace == "outer_ns"
    assert mid_gb.metaData.outputNamespace == "explicit_ns"  # preserved
    # deepest_join gets outer_ns because _propagate_namespace uses the top-level namespace
    assert deepest_join.metaData.outputNamespace == "outer_ns"


def test_update_metadata_groupby_standalone_with_join_source():
    """Standalone GroupBy (not inside a Join) with a JoinSource — the original fix case."""
    inner_join = Join(
        metaData=MetaData(name="test_team.source_join", team="test_team"),
        left=Source(events=EventSource(table="some_table")),
    )

    gb = GroupBy(
        sources=[Source(joinSource=JoinSource(join=inner_join))],
        keyColumns=["user_id"],
        metaData=MetaData(name="test_team.standalone_gb", team="test_team"),
    )

    update_metadata(gb, _make_team_dict("standalone_ns"))

    assert gb.metaData.outputNamespace == "standalone_ns"
    assert inner_join.metaData.outputNamespace == "standalone_ns"


def test_update_metadata_user_reported_chain():
    """Reproduces the user-reported issue:

    acct_enriched_join (4 groupbys -> join)
      -> acct_enriched_by_cardholder (groupby reading from acct_enriched_join via JoinSource)
        -> txn_join (6 groupbys -> join, one of which is acct_enriched_by_cardholder)

    The second join fails because acct_enriched_by_cardholder's JoinSource inner join
    has null outputNamespace, causing table resolution to look for "null.tablename".
    """
    # --- First join: acct_enriched_join (4 groupbys -> join) ---
    acct_combined = GroupBy(
        keyColumns=["cardholder_id"],
        metaData=MetaData(name="test_team.acct_combined", team="test_team"),
    )
    acct_by_company = GroupBy(
        keyColumns=["cardholder_id"],
        metaData=MetaData(name="test_team.acct_by_company", team="test_team"),
    )
    acct_by_segment = GroupBy(
        keyColumns=["cardholder_id"],
        metaData=MetaData(name="test_team.acct_by_segment", team="test_team"),
    )
    acct_by_tier = GroupBy(
        keyColumns=["cardholder_id"],
        metaData=MetaData(name="test_team.acct_by_tier", team="test_team"),
    )

    acct_enriched_join = Join(
        left=Source(events=EventSource(table="acct_enriched_left")),
        joinParts=[
            JoinPart(groupBy=acct_combined),
            JoinPart(groupBy=acct_by_company),
            JoinPart(groupBy=acct_by_segment),
            JoinPart(groupBy=acct_by_tier),
        ],
        metaData=MetaData(name="test_team.acct_enriched_join", team="test_team"),
    )

    # --- GroupBy that reads from the first join via JoinSource ---
    acct_enriched_by_cardholder = GroupBy(
        sources=[Source(joinSource=JoinSource(join=acct_enriched_join))],
        keyColumns=["cardholder_id"],
        metaData=MetaData(name="test_team.acct_enriched_by_cardholder", team="test_team"),
    )

    # --- Second join: uses acct_enriched_by_cardholder as a join part ---
    txn_by_cardholder = GroupBy(
        keyColumns=["cardholder_id"],
        metaData=MetaData(name="test_team.txn_by_cardholder", team="test_team"),
    )

    txn_join = Join(
        left=Source(events=EventSource(table="txn_source")),
        joinParts=[
            JoinPart(groupBy=txn_by_cardholder),
            JoinPart(groupBy=acct_enriched_by_cardholder),  # <- this was the problem
        ],
        metaData=MetaData(name="test_team.txn_join", team="test_team"),
    )

    # Before the fix, update_metadata on txn_join would NOT propagate namespace
    # into acct_enriched_by_cardholder's JoinSource -> acct_enriched_join,
    # leaving acct_enriched_join.metaData.outputNamespace as None.
    update_metadata(txn_join, _make_team_dict("warehouse_db"))

    # All top-level objects get the namespace
    assert txn_join.metaData.outputNamespace == "warehouse_db"
    assert txn_by_cardholder.metaData.outputNamespace == "warehouse_db"
    assert acct_enriched_by_cardholder.metaData.outputNamespace == "warehouse_db"

    # The critical assertion: the inner join embedded inside the GroupBy's JoinSource
    # must also have the namespace, otherwise table resolution produces "null.tablename"
    assert acct_enriched_join.metaData.outputNamespace == "warehouse_db", (
        f"Expected 'warehouse_db' but got '{acct_enriched_join.metaData.outputNamespace}'. "
        f"This causes 'null.tablename' resolution errors at runtime."
    )

    # The groupbys inside the inner join should also get the namespace
    assert acct_combined.metaData.outputNamespace == "warehouse_db"
    assert acct_by_company.metaData.outputNamespace == "warehouse_db"
    assert acct_by_segment.metaData.outputNamespace == "warehouse_db"
    assert acct_by_tier.metaData.outputNamespace == "warehouse_db"


# ---------------------------------------------------------------------------
# End-to-end compile invariants:
#   (1) no compiled thriftjson contains the internal namespace placeholder
#       (`OUTPUT_NAMESPACE_PLACEHOLDER`) — every occurrence must be resolved
#       to a literal namespace by the compile pass.
#   (2) compile fails fast when a config has no outputNamespace and no team default.
# ---------------------------------------------------------------------------


_TEAMS_PY = dedent(
    """
    from ai.chronon.types import ConfigProperties, Team

    _DEFAULT_CONF = ConfigProperties(common={"spark.chronon.partition.column": "ds"})

    default = Team(outputNamespace="default_ns", conf=_DEFAULT_CONF)
    sample_team = Team(outputNamespace="sample_ns", conf=_DEFAULT_CONF)
    """
).strip()


def _write(path, contents):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


def _init_pkg(path):
    path.mkdir(parents=True, exist_ok=True)
    (path / "__init__.py").write_text("")


def _scaffold_repo(tmp_path, teams_py_body=_TEAMS_PY):
    """Scaffold the full set of user-authoring conf subdirs so compile discovery
    works regardless of which conf types a given test actually writes. Derives
    the folder list from the canonical `CONFIG_INFOS` registry so adding a new
    conf type to chronon doesn't silently skip it here."""
    (tmp_path / "teams.py").write_text(teams_py_body)
    for folder in user_authoring_folders():
        _init_pkg(tmp_path / folder)
        _init_pkg(tmp_path / folder / "sample_team")
    return tmp_path


def _run_compile(tmp_path, monkeypatch, ignore_python_errors=True):
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.chdir(tmp_path)
    # The compile loop imports user modules by name; drop stale caches so repeated
    # tests don't pick up a previous tmp repo's files. Include every user-authoring
    # conf folder plus `teams` so a new conf type never silently bypasses the reset.
    prefixes_to_reset = set(user_authoring_folders()) | {"teams"}
    for name in list(sys.modules):
        if name.split(".")[0] in prefixes_to_reset:
            del sys.modules[name]
    return __compile(chronon_root=str(tmp_path), ignore_python_errors=ignore_python_errors)


def _assert_no_namespace_placeholder_in_compiled(compiled_dir):
    violations = []
    for root, _, files in os.walk(compiled_dir):
        for f in files:
            full = os.path.join(root, f)
            rel = os.path.relpath(full, compiled_dir)
            if OUTPUT_NAMESPACE_PLACEHOLDER in open(full).read():
                violations.append(f"{rel}: found {OUTPUT_NAMESPACE_PLACEHOLDER!r} in compiled output")
    assert not violations, "\n".join(violations)


def test_compile_never_leaves_namespace_placeholder_in_thriftjson(tmp_path, monkeypatch):
    """Regression guard: compiled thriftjson must never contain the internal
    `OUTPUT_NAMESPACE_PLACEHOLDER` token. The token is emitted by `utils.output_table_name`
    when `.table` is accessed at authoring time before namespace propagation; the compile
    pass must substitute every occurrence before the Thrift is serialized."""
    _scaffold_repo(tmp_path)
    _write(
        tmp_path / "group_bys" / "sample_team" / "gb_ok.py",
        dedent(
            """
            from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, selects

            v1 = GroupBy(
                sources=[
                    EventSource(
                        table="external.events",
                        query=Query(
                            selects=selects(event="event_expr", group_by_subject="user_id"),
                            time_column="ts",
                        ),
                    )
                ],
                keys=["group_by_subject"],
                aggregations=[Aggregation(input_column="event", operation=Operation.SUM, windows=["1d"])],
            )
            """
        ).strip(),
    )

    _run_compile(tmp_path, monkeypatch)
    _assert_no_namespace_placeholder_in_compiled(tmp_path / "compiled")


def test_compile_fails_when_config_has_no_output_namespace(tmp_path, monkeypatch):
    """If a config has no outputNamespace and the team has no default, compile must fail."""
    teams_py = dedent(
        """
        from ai.chronon.types import ConfigProperties, Team
        _CONF = ConfigProperties(common={"spark.chronon.partition.column": "ds"})
        default = Team(conf=_CONF)
        sample_team = Team(conf=_CONF)
        """
    ).strip()
    _scaffold_repo(tmp_path, teams_py_body=teams_py)
    _write(
        tmp_path / "group_bys" / "sample_team" / "gb_no_ns.py",
        dedent(
            """
            from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, selects

            v1 = GroupBy(
                sources=[
                    EventSource(
                        table="external.events",
                        query=Query(
                            selects=selects(event="event_expr", group_by_subject="user_id"),
                            time_column="ts",
                        ),
                    )
                ],
                keys=["group_by_subject"],
                aggregations=[Aggregation(input_column="event", operation=Operation.SUM, windows=["1d"])],
            )
            """
        ).strip(),
    )

    _, has_errors, _ = _run_compile(tmp_path, monkeypatch, ignore_python_errors=True)
    assert has_errors, "Expected compile to surface an error for missing outputNamespace"
    compiled_path = tmp_path / "compiled" / "group_bys" / "sample_team" / "gb_no_ns.v1"
    assert not compiled_path.exists()
