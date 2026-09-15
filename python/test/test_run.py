"""
Basic tests for namespace and breaking changes in run.py
"""

#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import json
import os
import tempfile
import time

import click
import pytest
from click.testing import CliRunner

from ai.chronon.repo import default_runner, run, utils
from ai.chronon.repo.constants import RunMode

DEFAULT_ENVIRONMENT = os.environ.copy()


def context():
    """Basic click Context for tests relative to the main arguments of run.py"""
    context = click.Context(run.main)
    context.params = {
        "repo": None,
        "conf": None,
        "mode": None,
        "env": None,
        "app_name": None,
        "chronon_jar": None,
        "online_jar": None,
        "online_class": None,
        "render_info": None,
        "sub_help": None,
    }
    run.set_defaults(context)
    return context


@pytest.fixture
def test_conf_location():
    """Sample test conf for tests"""
    return "compiled/joins/sample_team/sample_online_join.v1__0"


def reset_env(default_env):
    set_keys = os.environ.keys()
    for key in set_keys:
        os.environ.pop(key)
    for k, v in default_env.items():
        os.environ[k] = v


def test_download_jar(monkeypatch, sleepless):
    def mock_cmd(url, path, skip_download):
        return url

    monkeypatch.setattr(time, "sleep", sleepless)
    monkeypatch.setattr(utils, "download_only_once", mock_cmd)
    jar_path = utils.download_jar(
        "version", jar_type="uber", release_tag=None, spark_version="2.4.0"
    )
    tmp = tempfile.gettempdir()
    assert jar_path == f"{tmp}/spark_uber_2.11-version-assembly.jar"
    jar_path = utils.download_jar(
        "version", jar_type="uber", release_tag=None, spark_version="3.1.1"
    )
    assert jar_path == f"{tmp}/spark_uber_2.12-version-assembly.jar"
    with pytest.raises(Exception):
        utils.download_jar(
            "version", jar_type="uber", release_tag=None, spark_version="2.1.0"
        )

def test_environment(teams_json, repo, test_conf_location):
    default_environment = DEFAULT_ENVIRONMENT.copy()
    # If nothing is passed.
    ctx = context()
    run.set_runtime_env_v3(ctx.params, test_conf_location)

    # If repo is passed common_env is loaded.
    reset_env(default_environment)
    ctx = context()
    ctx.params["repo"] = repo
    ctx.params['mode'] = RunMode.BACKFILL
    run.set_runtime_env_v3(ctx.params, test_conf_location)
    assert os.environ["VERSION"] == "latest"

    # For chronon_metadata_export is passed. APP_NAME should be set.
    reset_env(default_environment)
    ctx = context()
    ctx.params["mode"] = "metadata-export"
    run.set_runtime_env_v3(ctx.params, test_conf_location)
    assert os.environ["APP_NAME"] == "chronon_metadata_export"

    # If APP_NAME is set, should be respected.
    reset_env(default_environment)
    os.environ["APP_NAME"] = "fake-name"
    ctx = context()
    ctx.params["mode"] = "metadata-export"
    run.set_runtime_env_v3(ctx.params, test_conf_location)
    assert os.environ["APP_NAME"] == "fake-name"

    # If app_name can be passed from cli.
    reset_env(default_environment)
    ctx = context()
    ctx.params["mode"] = "metadata-export"
    ctx.params["app_name"] = "fake-name"
    run.set_runtime_env_v3(ctx.params, test_conf_location)
    assert os.environ["APP_NAME"] == "fake-name"

    # Check default backfill for a team sets parameters accordingly.
    reset_env(default_environment)
    ctx = context()
    ctx.params["mode"] = "backfill"
    ctx.params["conf"] = test_conf_location
    ctx.params["repo"] = repo
    ctx.params["env"] = "production"
    ctx.params["online_jar"] = test_conf_location
    run.set_runtime_env_v3(ctx.params, test_conf_location)
    # from common env.
    assert os.environ["VERSION"] == "latest"
    # derived from args.
    assert (
        os.environ["APP_NAME"]
        == "chronon_joins_backfill_production_sample_team.sample_online_join.v1__0"
    )

    # Check dev backfill for a team sets parameters accordingly.
    reset_env(default_environment)
    ctx = context()
    ctx.params["mode"] = "backfill"
    ctx.params["conf"] = test_conf_location
    ctx.params["repo"] = repo
    ctx.params["online_jar"] = test_conf_location
    run.set_runtime_env_v3(ctx.params, test_conf_location)

    # Check conf set environment overrides most.
    reset_env(default_environment)
    ctx = context()
    ctx.params["mode"] = "backfill"
    ctx.params["conf"] = "production/joins/sample_team/sample_join.v1__0"
    ctx.params["repo"] = repo
    ctx.params["env"] = "production"
    run.set_runtime_env_v3(ctx.params, test_conf_location)
    assert os.environ['APP_NAME'] == 'chronon_joins_backfill_production_sample_team.sample_online_join.v1__0'

    # Check metadata export run.py
    reset_env(default_environment)
    ctx = context()
    ctx.params["mode"] = "metadata-export"
    ctx.params["conf"] = "production/joins//"
    ctx.params["repo"] = repo
    run.set_runtime_env_v3(ctx.params, test_conf_location)
    # without conf still works.
    assert os.environ["APP_NAME"] == "chronon_joins_metadata-export_dev_sample_team.sample_online_join.v1__0"

    reset_env(default_environment)
    ctx = context()
    ctx.params["mode"] = "metadata-upload"
    ctx.params["conf"] = "production/joins//"
    ctx.params["repo"] = repo
    run.set_runtime_env_v3(ctx.params, test_conf_location)
    assert os.environ["APP_NAME"] == "chronon_joins_metadata-upload_dev_sample_team.sample_online_join.v1__0"
    reset_env(default_environment)


def test_property_default_update(repo, test_conf_location):
    reset_env(DEFAULT_ENVIRONMENT.copy())
    assert "VERSION" not in os.environ
    ctx = context()
    ctx.params["mode"] = "backfill"
    ctx.params["conf"] = test_conf_location
    ctx.params["repo"] = repo
    assert "version" not in ctx.params
    run.set_runtime_env_v3(ctx.params, test_conf_location)
    assert "VERSION" in os.environ
    assert "version" not in ctx.params
    run.set_defaults(ctx)
    reparsed = ctx.params
    assert reparsed["version"] is not None


def test_render_info_setting_update(repo, test_conf_location):
    default_environment = DEFAULT_ENVIRONMENT.copy()

    ctx = context()
    run.set_defaults(ctx)
    ctx.params["mode"] = "info"
    ctx.params["conf"] = test_conf_location
    ctx.params["repo"] = repo
    run.set_defaults(ctx)
    assert ctx.params["render_info"] == os.path.join(
        ".", run.RENDER_INFO_DEFAULT_SCRIPT
    )

    reset_env(default_environment)
    run.set_runtime_env_v3(ctx.params, test_conf_location)
    os.environ["CHRONON_REPO_PATH"] = repo
    ctx = context()
    ctx.params["mode"] = "info"
    ctx.params["conf"] = test_conf_location
    ctx.params["repo"] = repo
    run.set_defaults(ctx)
    assert ctx.params["render_info"] == os.path.join(
        repo, run.RENDER_INFO_DEFAULT_SCRIPT
    )

    reset_env(default_environment)
    ctx = context()
    somewhere = "/tmp/somewhere/script.py"
    ctx.params["mode"] = "info"
    ctx.params["conf"] = test_conf_location
    ctx.params["render_info"] = somewhere
    run.set_defaults(ctx)
    assert ctx.params["render_info"] == somewhere


def test_render_info(repo, test_conf_location, monkeypatch):
    actual_cmd = None

    def mock_check_call(cmd):
        nonlocal actual_cmd
        actual_cmd = cmd
        return cmd

    def mock_exists(_):
        return True

    monkeypatch.setattr(utils, "check_call", mock_check_call)
    monkeypatch.setattr(os.path, "exists", mock_exists)

    monkeypatch.setenv("CUSTOMER_ID", "test")

    ctx = context()
    run.set_defaults(ctx)
    ctx.params["mode"] = "info"
    ctx.params["conf"] = test_conf_location
    ctx.params["repo"] = repo
    args = ctx.params

    args["args"] = ctx.args
    runner = default_runner.Runner(args, "some.jar")
    runner.run()

    assert run.RENDER_INFO_DEFAULT_SCRIPT in actual_cmd


def test_upload_to_kv_forwards_timestamp(repo, test_online_group_by):
    ctx = context()
    run.set_defaults(ctx)
    ctx.params.update(
        {
            "mode": RunMode.UPLOAD_TO_KV,
            "conf": test_online_group_by,
            "repo": repo,
            "online_jar": "online.jar",
            "online_class": "example.OnlineApi",
            "warehouse_bucket": "test-warehouse",
            "args": "",
            "timestamp": 1788541200000,
        }
    )

    runner = default_runner.Runner(ctx.params, "chronon.jar")

    assert "--timestamp=1788541200000" in runner._gen_final_args()


def test_upload_to_kv_exposes_publish_timestamp_option():
    result = CliRunner().invoke(run.main, ["--help"])

    assert result.exit_code == 0
    assert "--publish-timestamp" in result.output
    assert "--timestamp PARTITION_DATE" not in result.output


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        ("2026-09-04", 1788480000000),
        ("2026/09/04", 1788480000000),
        ("2026-09-04-01", 1788483600000),
        ("2026-09-04-01-30", 1788485400000),
        ("2026-09-04T01:30", 1788485400000),
        ("2026-09-04 01:30:00", 1788485400000),
        ("1788541200000", 1788541200000),
        ("-1", -1),
        ("253402300800000", 253402300800000),
    ],
)
def test_parse_upload_timestamp_accepts_date_or_epoch_millis(value, expected):
    assert run.parse_upload_timestamp(None, None, value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "2026-09-04T12:00:00Z",
    ],
)
def test_parse_upload_timestamp_rejects_invalid_values(value):
    with pytest.raises(click.BadParameter, match="epoch milliseconds or a supported date"):
        run.parse_upload_timestamp(None, None, value)


def test_parse_upload_timestamp_rejects_sub_minute_precision():
    with pytest.raises(click.BadParameter, match="minute precision"):
        run.parse_upload_timestamp(None, None, "2026-09-04T00:00:01")


def test_upload_to_kv_omits_timestamp_by_default(repo, test_online_group_by):
    ctx = context()
    run.set_defaults(ctx)
    ctx.params.update(
        {
            "mode": RunMode.UPLOAD_TO_KV,
            "conf": test_online_group_by,
            "repo": repo,
            "online_jar": "online.jar",
            "online_class": "example.OnlineApi",
            "warehouse_bucket": "test-warehouse",
            "args": "",
        }
    )

    runner = default_runner.Runner(ctx.params, "chronon.jar")

    assert "--timestamp=" not in runner._gen_final_args()


@pytest.mark.parametrize("timestamp", [None, 1788541200000])
def test_validate_upload_timestamp_accepts_upload_to_kv(timestamp):
    run.validate_upload_timestamp(RunMode.UPLOAD_TO_KV, timestamp)


def test_validate_upload_timestamp_rejects_other_modes():
    with pytest.raises(
        click.UsageError,
        match="--publish-timestamp is only supported with --mode=upload-to-kv",
    ):
        run.validate_upload_timestamp(RunMode.UPLOAD, 1788541200000)


def test_streaming_client(repo, test_online_group_by, monkeypatch):
    """Test mode compiles properly and uses the same app name by default, killing if necessary."""
    calls = []

    def mock_check_call(cmd):
        nonlocal calls
        calls += [cmd]
        return cmd

    def mock_check_output(cmd):
        print(cmd)
        return "[]".encode("utf8")

    monkeypatch.setattr(utils, "check_output", mock_check_output)
    monkeypatch.setattr(utils, "check_call", mock_check_call)

    monkeypatch.setenv("CUSTOMER_ID", "test")

    ctx = context()
    run.set_defaults(ctx)
    # Follow the same flow as __main__: Do a first pass (no env), do a second pass and run.
    ctx.params["mode"] = "streaming"
    ctx.params["conf"] = test_online_group_by
    ctx.params["repo"] = repo
    run.set_runtime_env_v3(ctx.params, test_online_group_by)
    run.set_defaults(ctx)
    ctx.params["mode"] = "streaming"
    ctx.params["conf"] = test_online_group_by
    ctx.params["repo"] = repo
    ctx.params["args"] = ""
    runner = default_runner.Runner(ctx.params, "some.jar")
    runner.run()
    streaming_app_name = runner.app_name
    # Repeat for streaming-client
    ctx = context()
    ctx.params["mode"] = "streaming-client"
    ctx.params["conf"] = test_online_group_by
    ctx.params["repo"] = repo
    run.set_runtime_env_v3(ctx.params, test_online_group_by)
    run.set_defaults(ctx)
    ctx.params["mode"] = "streaming-client"
    ctx.params["conf"] = test_online_group_by
    ctx.params["repo"] = repo
    ctx.params["args"] = ""
    runner = default_runner.Runner(ctx.params, "some.jar")
    runner.run()
    assert streaming_app_name == runner.app_name

    # Check job its not killed if found and submitted by a different user.
    def mock_check_output_with_app_other_user(cmd):
        return json.dumps(
            {
                "app_name": streaming_app_name,
                "kill_cmd": "<kill app cmd>",
                "user": "notcurrent",
            }
        ).encode("utf8")

    monkeypatch.setattr(utils, "check_output", mock_check_output_with_app_other_user)
    assert "<kill app cmd>" not in calls
    runner = default_runner.Runner(ctx.params, "some.jar")
    with pytest.raises(RuntimeError):
        runner.run()


def test_split_date_range():
    start_date = "2022-01-01"
    end_date = "2022-01-11"
    parallelism = 5
    expected_result = [
        ("2022-01-01", "2022-01-02"),
        ("2022-01-03", "2022-01-04"),
        ("2022-01-05", "2022-01-06"),
        ("2022-01-07", "2022-01-08"),
        ("2022-01-09", "2022-01-11"),
    ]

    result = utils.split_date_range(start_date, end_date, parallelism)
    assert result == expected_result


def _runner_args(repo, conf, **overrides):
    ctx = context()
    ctx.params.update(
        {
            "repo": repo,
            "conf": conf,
            "mode": RunMode.BACKFILL,
            "ds": "2026-09-03",
            "args": "",
            "warehouse_bucket": "test-warehouse",
        }
    )
    ctx.params.update(overrides)
    return ctx.params


def test_force_overwrite_is_exposed_in_run_help():
    result = CliRunner().invoke(run.main, ["--help"])

    assert result.exit_code == 0
    assert "--force-overwrite" in result.output


def test_force_overwrite_defaults_to_the_requested_partition(repo):
    args = _runner_args(
        repo,
        "compiled/staging_queries/sample_team/sample_staging_query.v1__0",
        force_overwrite=True,
    )

    runner = default_runner.Runner(args, "some.jar")
    final_args = runner._gen_final_args(runner.start_ds)

    assert runner.start_ds == "2026-09-03"
    assert "--start-partition=2026-09-03" in final_args
    assert "--force-overwrite" in final_args


def test_force_overwrite_preserves_an_explicit_range(repo):
    args = _runner_args(
        repo,
        "compiled/staging_queries/sample_team/sample_staging_query.v1__0",
        ds=None,
        start_ds="2026-09-01",
        end_ds="2026-09-03",
        force_overwrite=True,
    )

    runner = default_runner.Runner(args, "some.jar")
    final_args = runner._gen_final_args(runner.start_ds)

    assert runner.start_ds == "2026-09-01"
    assert runner.ds == "2026-09-03"
    assert "--start-partition=2026-09-01" in final_args
    assert "--force-overwrite" in final_args


def test_force_overwrite_rejects_non_staging_query_confs(repo, test_conf_location):
    args = _runner_args(repo, test_conf_location, force_overwrite=True)

    with pytest.raises(ValueError, match="only supported for staging query backfills"):
        default_runner.Runner(args, "some.jar")


def test_force_overwrite_cli_rejects_non_staging_query_confs(repo, test_conf_location):
    result = CliRunner().invoke(
        run.main,
        [
            test_conf_location,
            "--chronon-root",
            repo,
            "--mode",
            "backfill",
            "--ds",
            "2026-09-03",
            "--force-overwrite",
        ],
    )

    assert result.exit_code == 2
    assert "only supported for staging query backfills" in result.output


def test_force_overwrite_rejects_non_backfill_modes():
    result = CliRunner().invoke(
        run.main,
        [
            "compiled/staging_queries/sample_team/sample_staging_query.v1__0",
            "--mode",
            "upload",
            "--ds",
            "2026-09-03",
            "--force-overwrite",
        ],
    )

    assert result.exit_code == 2
    assert "only supported for staging query backfills" in result.output


@pytest.mark.parametrize(
    "date_args",
    [
        [],
        ["--start-ds", "2026-09-03"],
        ["--end-ds", "2026-09-03"],
        ["--ds", "2026-09-03", "--start-ds", "2026-09-03", "--end-ds", "2026-09-03"],
    ],
)
def test_force_overwrite_rejects_ambiguous_date_arguments(date_args):
    result = CliRunner().invoke(
        run.main,
        [
            "compiled/staging_queries/sample_team/sample_staging_query.v1__0",
            "--mode",
            "backfill",
            "--force-overwrite",
            *date_args,
        ],
    )

    assert result.exit_code == 2
    assert "Pass either --ds or both --start-ds and --end-ds" in result.output


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "gs://bucket/path/jar.jar",
        "s3://bucket/path/jar.jar",
        "http://artifactory.example.com/path/jar.jar",
        "https://artifactory.example.com/path/jar.jar",
        "file:/opt/jars/jar.jar",
        "file:///opt/jars/jar.jar",
        "local:/opt/jars/jar.jar",
        "local:///opt/jars/jar.jar",
        "gs://bucket/a.jar,https://artifactory.example.com/b.jar,s3://bucket/c.jar",
    ],
)
def test_validate_additional_jars_accepts_supported_schemes(value):
    assert run.validate_additional_jars(None, None, value) == value


@pytest.mark.parametrize(
    "value,expected",
    [
        ("  gs://bucket/a.jar  ", "gs://bucket/a.jar"),
        ("gs://bucket/a.jar, https://artifactory.example.com/b.jar",
         "gs://bucket/a.jar,https://artifactory.example.com/b.jar"),
        ("\tgs://bucket/a.jar\t,\nhttps://artifactory.example.com/b.jar\n",
         "gs://bucket/a.jar,https://artifactory.example.com/b.jar"),
    ],
)
def test_validate_additional_jars_strips_whitespace(value, expected):
    assert run.validate_additional_jars(None, None, value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "ftp://example.com/jar.jar",
        "/absolute/path/jar.jar",
        "relative/path/jar.jar",
        "hdfs://nn/path/jar.jar",
        "gs://bucket/a.jar,ftp://example.com/b.jar",
    ],
)
def test_validate_additional_jars_rejects_unsupported_schemes(value):
    with pytest.raises(click.BadParameter):
        run.validate_additional_jars(None, None, value)
