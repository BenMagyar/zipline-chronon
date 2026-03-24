import inspect
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

import ai.chronon.airflow_helpers as airflow_helpers
import gen_thrift.api.ttypes as ttypes
import gen_thrift.common.ttypes as common
from ai.chronon import query_utils, utils
from ai.chronon.constants import AIRFLOW_DEPENDENCIES_KEY


def _get_output_table_name(staging_query: ttypes.StagingQuery, full_name: bool = False):
    """generate output table name for staging query job"""
    name = utils.get_name(staging_query)
    staging_query.metaData.name = name
    return utils.output_table_name(staging_query, full_name=full_name)


# Wrapper for EngineType
class EngineType:
    SPARK = ttypes.EngineType.SPARK
    BIGQUERY = ttypes.EngineType.BIGQUERY
    SNOWFLAKE = ttypes.EngineType.SNOWFLAKE


@dataclass
class TableDependency:
    table: str
    partition_column: Optional[str] = None
    partition_format: Optional[str] = None
    additional_partitions: Optional[List[str]] = None
    offset: Optional[int] = None
    time_partitioned: Optional[bool] = None

    def to_thrift(self):
        if self.partition_column is not None and self.offset is None:
            raise ValueError(f"Dependency offset for table {self.table} must be specified.")
        offset_window = common.Window(length=self.offset, timeUnit=common.TimeUnit.DAYS)
        return common.TableDependency(
            tableInfo=common.TableInfo(
                table=self.table,
                partitionColumn=self.partition_column,
                partitionFormat=self.partition_format,
                partitionInterval=common.Window(1, common.TimeUnit.DAYS),
                timePartitioned=self.time_partitioned,
            ),
            startOffset=offset_window,
            endOffset=offset_window,
            startCutOff=None,
            endCutOff=None,
        )


def StagingQuery(
    query: str,
    version: Optional[int] = None,
    output_namespace: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    setups: Optional[List[str]] = None,
    engine_type: Optional[EngineType] = None,
    dependencies: Optional[List[Union[TableDependency, Dict]]] = None,
    tags: Optional[Dict[str, str]] = None,
    # execution params
    offline_schedule: str = "@daily",
    conf: Optional[common.ConfigProperties] = None,
    env_vars: Optional[common.EnvironmentVariables] = None,
    cluster_conf: common.ClusterConfigProperties = None,
    step_days: Optional[int] = None,
    recompute_days: Optional[int] = None,
    additional_partitions: List[str] = None,
) -> ttypes.StagingQuery:
    """
    Creates a StagingQuery object for executing arbitrary SQL queries with templated date parameters.

    :param query:
        Arbitrary spark query that should be written with template parameters:
        - `{{ start_date }}`: Initial run uses start_date, future runs use latest partition + 1 day
        - `{{ end_date }}`: The end partition of the computing range
        - `{{ latest_date }}`: End partition independent of the computing range (for cumulative sources)
        - `{{ max_date(table=namespace.my_table) }}`: Max partition available for a given table
        These parameters can be modified with offset and bounds:
        - `{{ start_date(offset=-10, lower_bound='2023-01-01', upper_bound='2024-01-01') }}`
    :type query: str
    :param setups:
        Spark SQL setup statements. Used typically to register UDFs.
    :type setups: List[str]
    :type partition_column: str
    :param engine_type:
        By default, spark is the compute engine. You can specify an override (eg. bigquery, etc.)
        Use the EngineType class constants: EngineType.SPARK, EngineType.BIGQUERY, etc.
    :type engine_type: int
    :param tags:
        Additional metadata that does not directly affect computation, but is useful for management.
    :type tags: Dict[str, str]
    :param offline_schedule:
        The offline schedule interval for batch jobs. Supports standard cron expressions
        that run at most once per day. Examples:
        '@daily': Legacy format for midnight daily execution
        '0 2 * * *': Daily at 2:00 AM
        '30 14 * * MON-FRI': Weekdays at 2:30 PM
        '0 9 * * 1': Mondays at 9:00 AM
        '15 23 * * SUN': Sundays at 11:15 PM
        '@never': Explicitly disable offline scheduling
        Note: Hourly, sub-hourly, or multi-daily schedules are not supported.
    :type offline_schedule: str
    :param conf:
        Configuration properties for the StagingQuery.
    :type conf: common.ConfigProperties
    :param env_vars:
        Environment variables for the StagingQuery.
    :type env_vars: common.EnvironmentVariables
    :param cluster_conf:
        Cluster configuration properties for the join.
    :param step_days:
        The maximum number of days to process at once
    :type step_days: int
    :param dependencies:
        List of dependencies for the StagingQuery. Each dependency can be either a TableDependency object
        or a dictionary with 'name' and 'spec' keys.
    :type dependencies: List[Union[TableDependency, Dict]]
    :param recompute_days:
        Used by orchestrator to determine how many days are recomputed on each incremental scheduled run. Should be
        set when the source data is changed in-place (i.e. existing partitions overwritten with new data each day up to
        X days later) or when you want partially mature aggregations (i.e. a 7 day window, but start computing it from
        day 1, and refresh it for the next 6 days)
    :type recompute_days: int
    :return:
        A StagingQuery object
    """
    # Get caller's filename to assign team
    team = inspect.stack()[1].filename.split("/")[-2]

    assert version is None or isinstance(version, int), (
        f"Version must be an integer or None, but found {type(version).__name__}"
    )
    tables_in_query = [
        query_utils.normalize_table_name(t)
        for t in query_utils.tables_in_query(
            query,
            dialect=ttypes.EngineType._VALUES_TO_NAMES[engine_type].lower()
            if engine_type
            else "spark",
        )
    ]
    if tables_in_query:
        assert dependencies is not None, "Dependencies must be specified if tables are in the query"
        spec_names = set()
        for d in dependencies:
            if isinstance(d, TableDependency):
                spec_names.add(query_utils.normalize_table_name(d.table))
            elif isinstance(d, dict):
                spec_names.add(query_utils.normalize_table_name(d["spec"].split("/")[0]))
        mismatch = set(tables_in_query) - set(spec_names)
        if mismatch:
            # There can be staging queries that run on top of views which are not handled as dependencies right now.
            # So it's better to avoid this validation until views are handled for both Eval and / or Hub
            # raise ValueError(f"Tables in query but not in dependencies: {mismatch}\nDependencies: {spec_names}\nTables in query: {tables_in_query}")
            pass

    # Create execution info
    exec_info = common.ExecutionInfo(
        offlineSchedule=offline_schedule,
        conf=conf,
        env=env_vars,
        stepDays=step_days,
        clusterConf=cluster_conf,
    )

    airflow_dependencies = []

    if dependencies:
        for d in dependencies:
            if isinstance(d, TableDependency) and d.partition_column is not None:
                # Create an Airflow dependency object for the table
                airflow_dependency = airflow_helpers.create_airflow_dependency(
                    d.table,
                    d.partition_column,
                    d.additional_partitions,
                    d.offset,
                )
                airflow_dependencies.append(airflow_dependency)
            elif isinstance(d, dict):
                # If it's already a dictionary, just append it
                airflow_dependencies.append(d)
            elif isinstance(d, TableDependency) and d.partition_column is None:
                # NoOp
                pass
            else:
                raise ValueError(
                    "Dependencies must be either TableDependency instances or dictionaries."
                )

    custom_json = json.dumps({AIRFLOW_DEPENDENCIES_KEY: airflow_dependencies})

    # Create metadata
    meta_data = ttypes.MetaData(
        outputNamespace=output_namespace,
        team=team,
        executionInfo=exec_info,
        tags=tags,
        customJson=custom_json,
        tableProperties=table_properties,
        version=str(version) if version is not None else None,
        additionalOutputPartitionColumns=additional_partitions,
    )

    thrift_deps = []
    if dependencies and len(dependencies) > 0:
        for d in dependencies:
            if d and isinstance(d, TableDependency):
                thrift_deps.append(d.to_thrift())

    # Create and return the StagingQuery object with camelCase parameter names
    staging_query = ttypes.StagingQuery(
        metaData=meta_data,
        query=query,
        setups=setups,
        engineType=engine_type,
        tableDependencies=thrift_deps,
        recomputeDays=recompute_days,
    )

    # Add the table property that calls the private function
    staging_query.__class__.table = property(
        lambda self: _get_output_table_name(self, full_name=True)
    )

    return staging_query
