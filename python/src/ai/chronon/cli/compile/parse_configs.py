import copy
import glob
import importlib
import os
import sys
from typing import Any, List

import gen_thrift.common.ttypes as common
from ai.chronon import airflow_helpers, utils
from ai.chronon.cli.compile import parse_teams, serializer
from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.display.compiled_obj import CompiledObj
from ai.chronon.cli.logger import get_logger
from gen_thrift.api.ttypes import GroupBy, Join

logger = get_logger()


def populate_table_names(obj: Any):
    """
    Populate the outputTableInfo.table field in the object's metadata with the
    resolved output table name for the object and any nested output targets.
    """
    for output_obj in utils.get_output_table_targets(obj):
        execution_info = output_obj.metaData.executionInfo
        if execution_info.outputTableInfo is None:
            execution_info.outputTableInfo = common.TableInfo()
        execution_info.outputTableInfo.table = utils.output_table_name(
            output_obj,
            full_name=True,
            output_namespace=obj.metaData.outputNamespace,
        )


def from_folder(cls: type, input_dir: str, compile_context: CompileContext) -> List[CompiledObj]:
    """
    Recursively consumes a folder, and constructs a map of
    object qualifier to StagingQuery, GroupBy, or Join
    """

    python_files = glob.glob(os.path.join(input_dir, "**/*.py"), recursive=True)

    results = []

    for f in python_files:
        try:
            results_dict = from_file(f, cls, input_dir)

            for name, obj in results_dict.items():
                parse_teams.update_metadata(obj, compile_context.teams_dict)
                # Populate columnHashes field with semantic hashes
                populate_column_hashes(obj)
                populate_table_names(obj)

                # Airflow deps must be set AFTER updating metadata
                airflow_helpers.set_airflow_deps(obj)

                obj.metaData.sourceFile = os.path.relpath(f, compile_context.chronon_root)

                tjson = serializer.thrift_simple_json(obj)

                # Perform validation
                errors = compile_context.validator.validate_obj(obj)

                result = CompiledObj(
                    name=name,
                    obj=obj,
                    file=f,
                    errors=errors if len(errors) > 0 else None,
                    obj_type=cls.__name__,
                    tjson=tjson,
                )
                results.append(result)

                compile_context.compile_status.add_object_update_display(result, cls.__name__)

        except Exception as e:
            result = CompiledObj(
                name=None,
                obj=None,
                file=f,
                errors=[e],
                obj_type=cls.__name__,
                tjson=None,
            )

            results.append(result)

            compile_context.compile_status.add_object_update_display(result, cls.__name__)

    return results


def from_file(file_path: str, cls: type, input_dir: str):
    # this is where the python path should have been set to
    chronon_root = os.path.dirname(input_dir)
    rel_path = os.path.relpath(file_path, chronon_root)

    rel_path_without_extension = os.path.splitext(rel_path)[0]

    module_name = rel_path_without_extension.replace("/", ".")

    conf_type, team_name_with_path = module_name.split(".", 1)
    mod_path = team_name_with_path.replace("/", ".")

    modules_before = set(sys.modules.keys())
    try:
        module = importlib.import_module(module_name)
    except Exception as e:
        # Remove any partially-loaded modules from the cache so that downstream
        # files importing this one don't get cascading import errors.
        for mod in set(sys.modules.keys()) - modules_before:
            del sys.modules[mod]
        # Python removes the failed module from sys.modules but leaves it as an
        # attribute on the parent package. Clean that up too to prevent cascade.
        sys.modules.pop(module_name, None)
        parent_name, _, child_name = module_name.rpartition(".")
        parent = sys.modules.get(parent_name)
        if parent is not None and hasattr(parent, child_name):
            delattr(parent, child_name)
        raise ValueError(f"Error parsing {os.path.relpath(file_path)}: {e}") from None

    result = {}

    for var_name, obj in list(module.__dict__.items()):
        if isinstance(obj, cls):
            copied_obj = copy.deepcopy(obj)

            name = utils.get_object_name(module_name, var_name, copied_obj.metaData.version)

            copied_obj.metaData.name = name
            copied_obj.metaData.team = mod_path.split(".")[0]

            result[name] = copied_obj

    return result


def populate_column_hashes(obj: Any):
    """
    Populate the columnHashes field in the object's metadata with semantic hashes
    for each output column.
    """
    # Import here to avoid circular imports
    from ai.chronon.cli.compile.column_hashing import (
        compute_group_by_columns_hashes,
        compute_join_column_hashes,
    )

    if isinstance(obj, GroupBy):
        # For GroupBy objects, get column hashes
        column_hashes = compute_group_by_columns_hashes(obj, exclude_keys=False)
        obj.metaData.columnHashes = column_hashes

    elif isinstance(obj, Join):
        # For Join objects, get column hashes
        column_hashes = compute_join_column_hashes(obj)
        obj.metaData.columnHashes = column_hashes

        if obj.joinParts:
            for jp in obj.joinParts or []:
                group_by = jp.groupBy
                group_by_hashes = compute_group_by_columns_hashes(group_by)
                group_by.metaData.columnHashes = group_by_hashes
