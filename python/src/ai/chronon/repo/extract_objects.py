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

import glob
import importlib.machinery
import importlib.util
import logging
import os

from ai.chronon import utils
from ai.chronon.logger import get_logger
from ai.chronon.repo import FOLDER_NAME_TO_CLASS


def from_folder(full_path: str, cls: type, log_level=logging.INFO):
    """
    Recursively consumes a folder, and constructs a map
    Creates a map of object qualifier to
    """
    if full_path.endswith("/"):
        full_path = full_path[:-1]

    python_files = glob.glob(os.path.join(full_path, "**/*.py"), recursive=True)
    result = {}
    for f in python_files:
        try:
            result.update(from_file(f, cls, log_level))
        except Exception as e:
            logging.error(f"Failed to extract: {f}")
            logging.exception(e)
    return result


def from_folderV2(full_path: str, target_file: str, cls: type):
    """
    Recursively consumes a folder, and constructs a map of
    object qualifier to StagingQuery, GroupBy, or Join
    """
    if full_path.endswith("/"):
        full_path = full_path[:-1]

    python_files = glob.glob(os.path.join(full_path, "**/*.py"), recursive=True)
    results = {}
    errors = {}
    target_file_error = None
    for f in python_files:
        try:
            results_dict = from_file(f, cls, log_level=logging.NOTSET)
            for k, v in results_dict.items():
                results[k] = (v, f)
        except Exception as e:
            if f == target_file:
                target_file_error = e
            errors[f] = e
    return results, errors, target_file_error


def import_module_set_name(module, cls):
    """
    evaluate imported modules to assign object name.
    """
    for name, obj in list(module.__dict__.items()):
        if isinstance(obj, cls):
            # the name would be `team_name.python_script_name.[group_by_name|join_name|staging_query_name]__version`
            # example module.__name__=group_bys.user.avg_session_length, version=1
            # obj.metaData.name=user.avg_session_length.v1__1
            # obj.metaData.team=user
            version = obj.metaData.version if hasattr(obj.metaData, "version") else None
            base_name = utils.get_object_name(module.__name__, name, version)

            obj.metaData.name = base_name
            obj.metaData.team = module.__name__.split(".")[1]
    return module


def from_file(file_path: str, cls: type, log_level=logging.INFO):
    logger = get_logger(log_level)
    logger.debug("Loading objects of type {cls} from {file_path}".format(**locals()))

    # mod_qualifier includes team name and python script name without `.py`
    # this line takes the full file path as input, strips the root path on the left side
    # strips `.py` on the right side and finally replaces the slash sign to dot
    # eg: the output would be `team_name.python_script_name`
    module_qualifier = module_path(file_path)
    mod = importlib.import_module(module_qualifier)

    # the key of result dict would be `team_name.python_script_name.[group_by_name|join_name|staging_query_name]`
    # real world case: psx.reservation_status.v1
    import_module_set_name(mod, cls)

    result = {}
    for obj in [o for o in mod.__dict__.values() if isinstance(o, cls)]:
        result[obj.metaData.name] = obj

    return result


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
