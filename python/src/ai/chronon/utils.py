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

import gc
import importlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Iterable, Sequence
from typing import List, Optional, Union, cast, get_args

import ai.chronon.repo.extract_objects as eo
import gen_thrift.api.ttypes as api
from ai.chronon.repo import FOLDER_NAME_TO_CLASS

ChrononJobTypes = Union[api.GroupBy, api.Join, api.StagingQuery]

# Common type definition for any source type used across the codebase
ANY_SOURCE_TYPE = Union[
    api.Source, api.EventSource, api.EntitySource, api.JoinSource, api.ModelTransforms
]

chronon_root_path = ""  # passed from compile.py


def normalize_source(source: ANY_SOURCE_TYPE, output_namespace: str = None) -> api.Source:
    """Convert any source type to a properly wrapped api.Source"""
    if isinstance(source, api.EventSource):
        return api.Source(events=source)
    elif isinstance(source, api.EntitySource):
        return api.Source(entities=source)
    elif isinstance(source, api.JoinSource):
        __set_name(source.join, api.Join, "joins")
        if output_namespace and not source.join.metaData.outputNamespace:
            source.join.metaData.outputNamespace = output_namespace
        return api.Source(joinSource=source)
    elif isinstance(source, api.ModelTransforms):
        return api.Source(modelTransforms=source)
    elif isinstance(source, api.Source):
        if source.entities:
            return normalize_source(source.entities, output_namespace)
        elif source.events:
            return normalize_source(source.events, output_namespace)
        elif source.joinSource:
            return normalize_source(source.joinSource, output_namespace)
        elif source.modelTransforms:
            return normalize_source(source.modelTransforms, output_namespace)
        else:
            return source
    else:
        print("unrecognized " + str(source))


def normalize_sources(
    sources: Union[Sequence[ANY_SOURCE_TYPE], ANY_SOURCE_TYPE], output_namespace: str = None
) -> List[api.Source]:
    """Convert a source or source sequence into wrapped api.Source objects."""
    source_types = get_args(ANY_SOURCE_TYPE)

    if isinstance(sources, source_types):
        sources = [sources]
    elif not isinstance(sources, Sequence):
        raise TypeError("sources must be a source or a sequence of sources")

    for index, source in enumerate(sources):
        if not isinstance(source, source_types):
            raise TypeError(
                f"sources[{index}] must be a supported source type, got {type(source).__name__}"
            )

    return [normalize_source(source, output_namespace) for source in sources]


def edit_distance(str1, str2):
    m = len(str1) + 1
    n = len(str2) + 1
    dp = [[0 for _ in range(n)] for _ in range(m)]
    for i in range(m):
        for j in range(n):
            if i == 0:
                dp[i][j] = j
            elif j == 0:
                dp[i][j] = i
            elif str1[i - 1] == str2[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                dp[i][j] = 1 + min(dp[i][j - 1], dp[i - 1][j], dp[i - 1][j - 1])
    return dp[m - 1][n - 1]


class JsonDiffer:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()
        self.new_name = "new.json"
        self.old_name = "old.json"

    def diff(self, new_json_str: object, old_json_str: object, skipped_keys=None) -> str:
        if skipped_keys is None:
            skipped_keys = []
        new_json = {k: v for k, v in json.loads(new_json_str).items() if k not in skipped_keys}
        old_json = {k: v for k, v in json.loads(old_json_str).items() if k not in skipped_keys}

        with (
            open(os.path.join(self.temp_dir, self.old_name), mode="w") as old,
            open(os.path.join(self.temp_dir, self.new_name), mode="w") as new,
        ):
            old.write(json.dumps(old_json, sort_keys=True, indent=2))
            new.write(json.dumps(new_json, sort_keys=True, indent=2))
        diff_str = subprocess.run(
            ["diff", old.name, new.name], stdout=subprocess.PIPE
        ).stdout.decode("utf-8")
        return diff_str

    def clean(self):
        shutil.rmtree(self.temp_dir)


def check_contains_single(candidate, valid_items, type_name, name, print_function=repr):
    name_suffix = f"for {name}" if name else ""
    candidate_str = print_function(candidate)
    if not valid_items:
        assert f"{candidate_str}, is not a valid {type_name} because no {type_name}s are specified {name_suffix}"
    elif candidate not in valid_items:
        sorted_items = sorted(
            map(print_function, valid_items),
            key=lambda item: edit_distance(candidate_str, item),
        )
        printed_items = "\n    ".join(sorted_items)
        assert (
            candidate in valid_items
        ), f"""{candidate_str}, is not a valid {type_name} {name_suffix}
Please pick one from:
    {printed_items}
"""


def check_contains(candidates, *args):
    if isinstance(candidates, Iterable) and not isinstance(candidates, str):
        for candidate in candidates:
            check_contains_single(candidate, *args)
    else:
        check_contains_single(candidates, *args)


def get_streaming_sources(group_by: api.GroupBy) -> List[api.Source]:
    """Checks if the group by has a source with streaming enabled."""
    return [source for source in group_by.sources if is_streaming(source)]


def is_streaming(source: api.Source) -> bool:
    """Checks if the source has streaming enabled."""
    return (source.entities and source.entities.mutationTopic is not None) or (
        source.events and source.events.topic is not None
    )


def _get_underlying_source(
    source: api.Source,
) -> Union[api.EventSource, api.EntitySource, api.JoinSource]:
    if source.entities:
        return source.entities
    elif source.events:
        return source.events
    else:
        return source.joinSource


def get_root_source(
    source: api.Source,
) -> Union[api.EventSource, api.EntitySource]:
    if source.entities:
        return source.entities
    elif source.events:
        return source.events
    else:
        return get_root_source(source.joinSource.join.left)


def get_query(source: api.Source) -> api.Query:
    return _get_underlying_source(source).query


def get_table(source: api.Source) -> str:
    if source.entities:
        table = source.entities.snapshotTable
    elif source.events:
        table = source.events.table
    else:
        from ai.chronon.join import _get_output_table_name

        table = _get_output_table_name(source.joinSource.join, True)
    return table.split("/")[0]


def get_topic(source: api.Source) -> str:
    return source.entities.mutationTopic if source.entities else source.events.topic


def get_columns(source: api.Source):
    query = get_query(source)
    assert query.selects is not None, "Please specify selects in your Source/Query"
    columns = query.selects.keys()
    return columns


def get_mod_name_from_gc(obj, mod_prefix):
    """get an object's module information from garbage collector"""
    mod_name = None
    # get obj's module info from garbage collector
    gc.collect()

    referrers = gc.get_referrers(obj)

    valid_referrers = [
        ref for ref in referrers if (isinstance(ref, Iterable) and "__name__" in ref)
    ]

    if len(valid_referrers) == 1:
        return valid_referrers[0]["__name__"]

    for ref in valid_referrers:
        if ref["__name__"].startswith(mod_prefix):
            mod_name = ref["__name__"]
            break

    return mod_name


def get_mod_and_var_name_from_gc(obj, mod_prefix):
    # Find the variable name within the module
    mod_name = get_mod_name_from_gc(obj, mod_prefix)
    """Get the variable name that points to the obj in the module"""
    if not mod_name:
        return None

    module = importlib.import_module(mod_name)
    for var_name, value in vars(module).items():
        if value is obj:
            return mod_name, var_name

    return mod_name, None


def __set_name(obj, cls, mod_prefix):
    module_qualifier = get_mod_name_from_gc(obj, mod_prefix)

    module = importlib.import_module(module_qualifier)
    eo.import_module_set_name(module, cls)


def sanitize(name):
    """
    From api.Extensions.scala
    Option(name).map(_.replaceAll("[^a-zA-Z0-9_]", "_")).orNull
    """
    if name is not None:
        return re.sub("[^a-zA-Z0-9_]", "_", name)
    return None


def dict_to_bash_commands(d):
    """
    Convert a dict into a bash command substring
    """
    if not d:
        return ""
    bash_commands = []
    for key, value in d.items():
        cmd = f"--{key.replace('_', '-')}={value}" if value else f"--{key.replace('_', '-')}"
        bash_commands.append(cmd)
    return " ".join(bash_commands)


def dict_to_exports(d):
    if not d:
        return ""
    exports = []
    for key, value in d.items():
        exports.append(f"export {key.upper()}={value}")
    return " && ".join(exports)


def output_table_name(obj, full_name: bool):
    table_name = sanitize(obj.metaData.name)
    db = obj.metaData.outputNamespace
    db = db or "{{ db }}"
    if full_name:
        return db + "." + table_name
    else:
        return table_name


def join_part_name(jp):
    if jp.groupBy is None:
        raise NotImplementedError("Join Part names for non group bys is not implemented.")
    if not jp.groupBy.metaData.name and isinstance(jp.groupBy, api.GroupBy):
        __set_name(jp.groupBy, api.GroupBy, "group_bys")
    return "_".join(
        [
            component
            for component in [jp.prefix, sanitize(jp.groupBy.metaData.name)]
            if component is not None
        ]
    )


def join_part_output_table_name(join, jp, full_name: bool = False):
    """
    From api.Extensions.scala

    Join Part output table name.
    To be synced with Scala API.
    def partOutputTable(jp: JoinPart): String = (Seq(join.metaData.outputTable) ++ Option(jp.prefix) :+
      jp.groupBy.metaData.cleanName).mkString("_")
    """
    if not join.metaData.name and isinstance(join, api.Join):
        __set_name(join, api.Join, "joins")
    return "_".join(
        [
            component
            for component in [
                output_table_name(join, full_name),
                join_part_name(jp),
            ]
            if component is not None
        ]
    )


def log_table_name(obj, full_name: bool = False):
    return output_table_name(obj, full_name=full_name) + "_logged"


def get_team_conf_from_py(team, key):
    team_module = importlib.import_module(f"teams.{team}")
    return getattr(team_module, key)


def wait_for_simple_schema(table, lag, start, end):
    if not table:
        return None
    table_tokens = table.split("/")
    clean_name = table_tokens[0]
    subpartition_spec = "/".join(table_tokens[1:]) if len(table_tokens) > 1 else ""
    return {
        "name": "wait_for_{}_ds{}".format(clean_name, "" if lag == 0 else f"_minus_{lag}"),
        "spec": "{}/ds={}{}".format(
            clean_name,
            "{{ ds }}" if lag == 0 else "{{{{ macros.ds_add(ds, -{}) }}}}".format(lag),
            "/{}".format(subpartition_spec) if subpartition_spec else "",
        ),
        "start": start,
        "end": end,
    }


def wait_for_name(dep):
    replace_nonalphanumeric = re.sub("[^a-zA-Z0-9]", "_", dep)
    name = f"wait_for_{replace_nonalphanumeric}"
    return re.sub("_+", "_", name).rstrip("_")


def dedupe_in_order(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def has_topic(group_by: api.GroupBy) -> bool:
    """Find if there's topic or mutationTopic for a source helps define streaming tasks"""
    return any(
        (source.entities and source.entities.mutationTopic)
        or (source.events and source.events.topic)
        for source in group_by.sources
    )


def get_offline_schedule(conf: ChrononJobTypes, default: Optional[str] = "@daily") -> Optional[str]:
    offline_schedule = conf.metaData.executionInfo.offlineSchedule
    if offline_schedule is None:
        offline_schedule = default
    # "@never" explicitly disables backfill
    if offline_schedule == "@never":
        return None
    return offline_schedule


def requires_log_flattening_task(conf: ChrononJobTypes) -> bool:
    return (conf.metaData.samplePercent or 0) > 0


def get_applicable_modes(conf: ChrononJobTypes) -> List[str]:
    """Based on a conf and mode determine if a conf should define a task."""
    modes = []  # type: List[str]

    if isinstance(conf, api.GroupBy):
        group_by = cast(api.GroupBy, conf)
        # For GroupBys, offline_schedule=None means no backfill (no default)
        if get_offline_schedule(conf, default=None) is not None:
            modes.append("backfill")

        online = group_by.metaData.online or False

        if online:
            modes.append("upload")

            temporal_accuracy = group_by.accuracy or False
            streaming = has_topic(group_by)
            if temporal_accuracy or streaming:
                modes.append("streaming")

    elif isinstance(conf, api.Join):
        join = cast(api.Join, conf)

        # For Joins, offline_schedule=None defaults to "@daily"
        if get_offline_schedule(conf, default="@daily") is not None:
            modes.append("backfill")
            modes.append("stats-summary")

        if join.metaData.consistencyCheck is True:
            modes.append("consistency-metrics-compute")

        if requires_log_flattening_task(join):
            modes.append("log-flattener")

    elif isinstance(conf, api.StagingQuery):
        modes.append("backfill")
    else:
        raise ValueError(f"Unsupported job type {type(conf).__name__}")

    return modes


def get_related_table_names(conf: ChrononJobTypes) -> List[str]:
    table_name = output_table_name(conf, full_name=True)

    applicable_modes = set(get_applicable_modes(conf))
    related_tables = []  # type: List[str]

    if "upload" in applicable_modes:
        related_tables.append(f"{table_name}_upload")
    if "stats-summary" in applicable_modes:
        related_tables.append(f"{table_name}_daily_stats")
    if "label-join" in applicable_modes:
        related_tables.append(f"{table_name}_labels")
        related_tables.append(f"{table_name}_labeled")
        related_tables.append(f"{table_name}_labeled_latest")
    if "log-flattener" in applicable_modes:
        related_tables.append(f"{table_name}_logged")
    if "consistency-metrics-compute" in applicable_modes:
        related_tables.append(f"{table_name}_consistency")

    if isinstance(conf, api.Join) and conf.bootstrapParts:
        related_tables.append(f"{table_name}_bootstrap")

    return related_tables


class DotDict(dict):
    def __getattr__(self, attr):
        if attr in self:
            value = self[attr]
            return DotDict(value) if isinstance(value, dict) else value
        return None


def convert_json_to_obj(d):
    if isinstance(d, dict):
        return DotDict({k: convert_json_to_obj(v) for k, v in d.items()})
    elif isinstance(d, list):
        return [convert_json_to_obj(item) for item in d]
    else:
        return d


def chronon_path(file_path: str) -> str:
    conf_types = FOLDER_NAME_TO_CLASS.keys()
    splits = file_path.split("/")
    conf_occurences = [splits.index(typ) for typ in conf_types if typ in splits]
    assert len(conf_occurences) > 0, (
        f"Path: {file_path} doesn't contain folder with name among {conf_types}"
    )

    index = min([splits.index(typ) for typ in conf_types if typ in splits])
    rel_path = "/".join(splits[index:])
    return rel_path


def module_path(file_path: str) -> str:
    adjusted_path = chronon_path(file_path)
    assert adjusted_path.endswith(".py"), f"Path: {file_path} doesn't end with '.py'"
    without_extension = adjusted_path[:-3]
    mod_path = without_extension.replace("/", ".")
    return mod_path


def compose(arg, *methods):
    """
    Allows composing deeply nested method calls - typically used in selects & derivations
    The first arg is what is threaded into methods, methods can have more than one arg.

    Example:

    .. code-block:: python
        compose(
            "user_id_approx_distinct_count_by_query",
            "map_entries",
            "array_sort (x, y) -> IF(y.value > x.value, -1, IF(y.value < x.value, 1, 0))",
            "transform entry -> entry.key"
        )

    would produce (without the new lines or indents):

    .. code-block:: text

        transform(
            array_sort(
                map_entries(
                    user_id_approx_distinct_count_by_query
                ),
                (x, y) -> IF(y.value > x.value, -1, IF(y.value < x.value, 1, 0))
            ),
            entry -> entry.key
        )
    """

    indent = "    " * (len(methods))

    result = [indent + arg]

    for method in methods:
        method_parts = method.split(" ", 1)
        method = method_parts[0]

        if len(method_parts) > 1:
            remaining_args = method_parts[1]
            last = result.pop()
            result = result + [last + ",", indent + remaining_args]

        indent = indent[:-4]
        result = [f"{indent}{method}("] + result + [f"{indent})"]

    return "\n".join(result)


def clean_expression(expr):
    """
    Cleans up an expression by removing leading and trailing whitespace and newlines.
    """
    return re.sub(r"\s+", " ", expr).strip()
