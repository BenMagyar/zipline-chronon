import importlib
import importlib.util
import os
import sys
from copy import deepcopy
from enum import Enum
from typing import Any, Dict, Optional, Union

from ai.chronon.cli.logger import get_logger
from ai.chronon.cli.theme import console
from ai.chronon.utils import OUTPUT_NAMESPACE_PLACEHOLDER
from gen_thrift.api.ttypes import (
    GroupBy,
    Join,
    MetaData,
    Model,
    ModelTransforms,
    StagingQuery,
    Team,
)
from gen_thrift.common.ttypes import (
    ClusterConfigProperties,
    ConfigProperties,
    EnvironmentVariables,
    ExecutionInfo,
)

logger = get_logger()

_DEFAULT_CONF_TEAM = "default"


def import_module_from_file(file_path):
    # Get the module name from the file path (without .py extension)
    module_name = file_path.split("/")[-1].replace(".py", "")

    # Create the module spec
    spec = importlib.util.spec_from_file_location(module_name, file_path)

    # Create the module based on the spec
    module = importlib.util.module_from_spec(spec)

    # Add the module to sys.modules
    sys.modules[module_name] = module

    # Execute the module
    spec.loader.exec_module(module)

    return module


_DEPRECATED_CATALOG = "DelegatingBigQueryMetastoreCatalog"
_REPLACEMENT_CATALOG = "org.apache.iceberg.spark.SparkCatalog"


def _check_deprecated_catalog(team_name: str, conf):
    """Check if a team's config references the deprecated DelegatingBigQueryMetastoreCatalog."""
    if conf is None:
        return
    if conf.common:
        for key, value in conf.common.items():
            val_str = value if isinstance(value, str) else str(value or "")
            if _DEPRECATED_CATALOG in val_str:
                raise ValueError(
                    f"Team '{team_name}' uses deprecated {_DEPRECATED_CATALOG} in conf key '{key}'. "
                    f"Please migrate to {_REPLACEMENT_CATALOG} with BigQueryMetastoreCatalog as the catalog-impl."
                )
    mode_configs = getattr(conf, "modeConfigs", None) or getattr(conf, "modeClusterConfigs", None)
    if mode_configs:
        for mode, mode_map in mode_configs.items():
            if mode_map:
                for key, value in mode_map.items():
                    val_str = value if isinstance(value, str) else str(value or "")
                    if _DEPRECATED_CATALOG in val_str:
                        raise ValueError(
                            f"Team '{team_name}' uses deprecated {_DEPRECATED_CATALOG} in conf key '{key}' "
                            f"(mode: {mode}). Please migrate to {_REPLACEMENT_CATALOG} with BigQueryMetastoreCatalog as the catalog-impl."
                        )


def load_teams(conf_root: str, print: bool = True) -> Dict[str, Team]:
    teams_file = os.path.join(conf_root, "teams.py")

    assert os.path.exists(teams_file), (
        f"Team config file: {teams_file} not found. You might be running this from the wrong directory."
    )

    team_module = import_module_from_file(teams_file)

    assert team_module is not None, (
        f"Team config file {teams_file} is not on the PYTHONPATH. You might need to add the your config "
        f"directory to the PYTHONPATH."
    )

    team_dict = {}

    if print:
        console.print(f"Pulling configuration from [cyan italic]{teams_file}[/cyan italic]")

    for name, obj in team_module.__dict__.items():
        if isinstance(obj, Team):
            obj.name = name
            _check_deprecated_catalog(name, obj.conf)
            team_dict[name] = obj

    return team_dict


def update_metadata(obj: Any, team_dict: Dict[str, Team]):
    assert obj is not None, "Cannot update metadata None object"

    metadata = obj.metaData

    assert obj.metaData is not None, "Cannot update empty metadata"

    name = obj.metaData.name
    team = obj.metaData.team

    assert team is not None, (
        f"Team name is required in metadata for {name}. This usually set by compiler. Internal error."
    )

    assert team in team_dict, f"Team '{team}' not found in teams.py. Please add an entry 🙏"

    assert _DEFAULT_CONF_TEAM in team_dict, (
        f"'{_DEFAULT_CONF_TEAM}' team not found in teams.py, please add an entry 🙏."
    )

    if not metadata.outputNamespace:
        metadata.outputNamespace = team_dict[team].outputNamespace

    namespace = metadata.outputNamespace

    # Three passes, one traversal. `_walk_nodes` yields every chronon node reachable
    # from `obj`; each pass is a pure function of that node plus closure-captured
    # context. Adding a new nesting edge (or a new conf type) means updating the
    # walker once — no chance of drift where (e.g.) propagate reaches
    # `Join.left.joinSource.join` but resolve doesn't.
    for node in _walk_nodes(obj):
        _propagate_namespace_onto(node, team_dict, team, namespace)
    for node in _walk_nodes(obj):
        _require_output_namespace_on(node)
    for node in _walk_nodes(obj):
        _resolve_namespace_placeholders_on(node)


def _walk_nodes(node: Any):
    """Generator yielding every chronon config node reachable from `node`: the node
    itself, then all nested configs via joinParts, joinSource.join, modelTransforms,
    and models. Single source of truth for tree traversal — used by every pass in
    `update_metadata`."""
    if node is None:
        return
    yield node

    if isinstance(node, Join):
        for jp in node.joinParts or []:
            # Per-edge effect that needs the parent Join in scope: propagate
            # useLongNames from the Join onto each JoinPart.
            jp.useLongNames = getattr(node, "useLongNames", jp.useLongNames)
            if jp.groupBy:
                yield from _walk_nodes(jp.groupBy)
        if node.left:
            yield from _walk_source_nodes(node.left)

    if isinstance(node, (GroupBy, ModelTransforms)):
        for src in node.sources or []:
            yield from _walk_source_nodes(src)

    if isinstance(node, ModelTransforms):
        for m in node.models or []:
            yield from _walk_nodes(m)


def _walk_source_nodes(source: Any):
    """Yield nested chronon nodes reachable through a Source wrapper (joinSource.join,
    modelTransforms). Source.events / Source.entities don't wrap chronon objects so
    they're handled per-pass when visiting the enclosing node, not here."""
    if source is None:
        return
    if source.joinSource and source.joinSource.join:
        yield from _walk_nodes(source.joinSource.join)
    if source.modelTransforms:
        yield from _walk_nodes(source.modelTransforms)


def _propagate_namespace_onto(
    node: Any,
    team_dict: Dict[str, Team],
    default_team: str,
    default_namespace: Optional[str],
):
    """Populate `metaData.team` and `metaData.outputNamespace` on a node. Falls back
    to the top-level `default_team` / `default_namespace` only when the node has no
    team set and its own team's lookup yields no namespace."""
    if not isinstance(node, (GroupBy, Join, Model, ModelTransforms, StagingQuery)):
        return
    if not node.metaData:
        node.metaData = MetaData()
    if not node.metaData.team:
        node.metaData.team = default_team
    if not node.metaData.outputNamespace:
        resolved_team = team_dict.get(node.metaData.team)
        node.metaData.outputNamespace = (
            resolved_team.outputNamespace
            if resolved_team and resolved_team.outputNamespace
            else default_namespace
        )
    if node.metaData.team not in team_dict:
        raise ValueError(
            f"Team '{node.metaData.team}' referenced by '{node.metaData.name}' not found in teams.py"
        )
    merge_team_execution_info(node.metaData, team_dict, node.metaData.team)


def _require_output_namespace_on(node: Any):
    """Fail compile if this node has a null/empty `metaData.outputNamespace` after
    propagation. Runs per-node; the walker (`_walk_nodes`) handles recursion."""
    if node is None or node.metaData is None:
        return
    if not node.metaData.outputNamespace:
        name = node.metaData.name or type(node).__name__
        raise ValueError(
            f"{name}: outputNamespace is not set. Set output_namespace on the config "
            f"or configure outputNamespace on the team in teams.py."
        )


def _substitute(value: Optional[str], namespace: str) -> Optional[str]:
    """Replace the internal `OUTPUT_NAMESPACE_PLACEHOLDER` with `namespace`."""
    if value is None or OUTPUT_NAMESPACE_PLACEHOLDER not in value:
        return value
    return value.replace(OUTPUT_NAMESPACE_PLACEHOLDER, namespace)


def _resolve_namespace_placeholders_on(node: Any):
    """Substitute the internal namespace placeholder in every user-authored string
    field of `node` using the node's own post-propagation `outputNamespace`. Runs
    per-node — the walker handles recursion, so this only touches fields that belong
    to `node` itself, not nested configs.

    Covers: Source table names (Events/Entities on `node.sources` or `node.left`),
    Join bootstrapParts tables, StagingQuery SQL bodies + setups + tableDependencies,
    and `metaData.customJson` (for StagingQuery Airflow dep specs built at Python
    authoring time before namespace propagation)."""
    if node is None or node.metaData is None:
        return
    namespace = node.metaData.outputNamespace
    if not namespace:
        # `_require_output_namespace_on` ran before us and would have raised.
        # Defensive skip only for benign propagate-from-null cases.
        return

    if isinstance(node, Join):
        _substitute_source_tables(node.left, namespace)
        for bp in node.bootstrapParts or []:
            bp.table = _substitute(bp.table, namespace)

    if isinstance(node, (GroupBy, ModelTransforms)):
        for src in node.sources or []:
            _substitute_source_tables(src, namespace)

    if isinstance(node, StagingQuery):
        node.query = _substitute(node.query, namespace)
        if node.setups:
            node.setups = [_substitute(s, namespace) for s in node.setups]
        for dep in node.tableDependencies or []:
            if dep and dep.tableInfo:
                dep.tableInfo.table = _substitute(dep.tableInfo.table, namespace)

    # `metaData.customJson` captures Airflow dep specs that StagingQuery's Python
    # wrapper builds at construction time from user-authored `TableDependency.table`
    # values — so a placeholder can bake into the JSON string. For Join/GroupBy
    # customJson is filled later by `set_airflow_deps` (after this pass), so this
    # line is a no-op there.
    if node.metaData.customJson:
        node.metaData.customJson = _substitute(node.metaData.customJson, namespace)


def _substitute_source_tables(source: Any, namespace: str):
    """Substitute the namespace placeholder in Source.events / Source.entities table
    fields using `namespace`. Does NOT recurse into joinSource / modelTransforms —
    that's the walker's job."""
    if source is None:
        return
    if source.events:
        source.events.table = _substitute(source.events.table, namespace)
    if source.entities:
        source.entities.snapshotTable = _substitute(source.entities.snapshotTable, namespace)
        source.entities.mutationTable = _substitute(source.entities.mutationTable, namespace)


def merge_team_execution_info(metadata: MetaData, team_dict: Dict[str, Team], team_name: str):
    default_team = team_dict.get(_DEFAULT_CONF_TEAM)
    if not metadata.executionInfo:
        metadata.executionInfo = ExecutionInfo()

    metadata.executionInfo.env = _merge_mode_maps(
        default_team.env if default_team else {},
        team_dict[team_name].env,
        metadata.executionInfo.env,
        env_or_config_attribute=EnvOrConfigAttribute.ENV,
    )

    metadata.executionInfo.conf = _merge_mode_maps(
        default_team.conf if default_team else {},
        team_dict[team_name].conf,
        metadata.executionInfo.conf,
        env_or_config_attribute=EnvOrConfigAttribute.CONFIG,
    )

    metadata.executionInfo.clusterConf = _merge_mode_maps(
        default_team.clusterConf if default_team else {},
        team_dict[team_name].clusterConf,
        metadata.executionInfo.clusterConf,
        env_or_config_attribute=EnvOrConfigAttribute.CLUSTER_CONFIG,
    )


def _merge_maps(*maps: Optional[Dict[str, str]]):
    """
    Merges multiple maps into one - with the later maps overriding the earlier ones.
    """

    result = {}

    for m in maps:
        if m is None:
            continue

        for key, value in m.items():
            result[key] = value

    return result


class EnvOrConfigAttribute(str, Enum):
    ENV = "modeEnvironments"
    CONFIG = "modeConfigs"
    CLUSTER_CONFIG = "modeClusterConfigs"


def _merge_mode_maps(
    *mode_maps: Optional[Union[EnvironmentVariables, ConfigProperties, ClusterConfigProperties]],
    env_or_config_attribute: EnvOrConfigAttribute,
):
    """
    Merges multiple environment variables into one - with the later maps overriding the earlier ones.
    """

    # Merge `common` to each individual mode map. Creates a new map
    def push_common_to_modes(
        mode_map: Union[EnvironmentVariables, ConfigProperties], mode_key: EnvOrConfigAttribute
    ):
        final_mode_map = deepcopy(mode_map)
        common = final_mode_map.common
        modes = getattr(final_mode_map, mode_key)

        if modes:
            for _ in modes:
                modes[_] = _merge_maps(common, modes[_])

        return final_mode_map

    filtered_mode_maps = [m for m in mode_maps if m]

    if not filtered_mode_maps:
        return None

    # Initialize the result with the first mode map
    result = push_common_to_modes(filtered_mode_maps[0], env_or_config_attribute)

    # Merge each new mode map into the result
    for m in filtered_mode_maps[1:]:
        # We want to prepare the individual modes with `common` in incoming_mode_map
        incoming_mode_map = push_common_to_modes(m, env_or_config_attribute)

        # create new common
        incoming_common = incoming_mode_map.common
        new_common = _merge_maps(result.common, incoming_common)
        result.common = new_common

        current_modes = getattr(result, env_or_config_attribute)
        incoming_modes = getattr(incoming_mode_map, env_or_config_attribute)

        current_modes_keys = list(current_modes.keys()) if current_modes else []
        incoming_modes_keys = list(incoming_modes.keys()) if incoming_modes else []

        all_modes_keys = list(set(current_modes_keys + incoming_modes_keys))

        for mode in all_modes_keys:
            current_mode = current_modes.get(mode, {}) if current_modes else {}

            # if the incoming_mode is not found, we NEED to default to incoming_common
            incoming_mode = (
                incoming_modes.get(mode, incoming_common) if incoming_modes else incoming_common
            )

            # first to last with later ones overriding the earlier ones
            # common -> current mode level -> incoming mode level

            new_mode = _merge_maps(new_common, current_mode, incoming_mode)

            if current_modes is None:
                current_modes = {}
                setattr(result, env_or_config_attribute, current_modes)

            current_modes[mode] = new_mode

    return result
