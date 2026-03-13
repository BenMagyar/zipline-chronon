from gen_thrift.api.ttypes import (
    Aggregation,
    BootstrapPart,
    EventSource,
    GroupBy,
    Join,
    JoinPart,
    MetaData,
    Operation,
    Query,
    Source,
    StagingQuery,
)

from ai.chronon.cli.compile.conf_validator import ConfValidator


def _make_group_by(name="team.my_gb", online=False, table="test_table", price_expr="price"):
    return GroupBy(
        sources=[
            Source(
                events=EventSource(
                    table=table,
                    query=Query(
                        selects={"user_id": "user_id", "price": price_expr},
                        timeColumn="timestamp",
                    ),
                )
            )
        ],
        keyColumns=["user_id"],
        aggregations=[
            Aggregation(inputColumn="price", operation=Operation.SUM),
        ],
        metaData=MetaData(name=name, online=online),
    )


def _make_join(name="team.my_join", online=False, group_by=None):
    if group_by is None:
        group_by = _make_group_by()
    return Join(
        left=Source(
            events=EventSource(
                table="left_table",
                query=Query(
                    selects={"user_id": "user_id"},
                    timeColumn="timestamp",
                ),
            )
        ),
        joinParts=[JoinPart(groupBy=group_by)],
        metaData=MetaData(name=name, online=online),
    )


def _make_validator(existing_gbs=None, existing_joins=None, existing_staging_queries=None):
    return ConfValidator(
        input_root="/fake",
        output_root="compiled",
        existing_gbs=existing_gbs or {},
        existing_joins=existing_joins or {},
        existing_staging_queries=existing_staging_queries or {},
    )


def _has_online_in_place_error(errors):
    return any("is online and cannot be changed in-place" in str(e) for e in errors)


def _has_time_partitioned_missing_partition_column_error(errors, context=None):
    message = "timePartitioned sources must have partitionColumn set to the timestamp/date column name"
    return any(message in str(e) and (context is None or context in str(e)) for e in errors)


class TestOnlineConfNotChangedInPlace:
    def test_online_group_by_changed_in_place_is_blocked(self):
        old_gb = _make_group_by(online=True, price_expr="price")
        new_gb = _make_group_by(online=True, price_expr="price * 2")
        validator = _make_validator(existing_gbs={old_gb.metaData.name: old_gb})

        errors = validator.validate_obj(new_gb)
        assert _has_online_in_place_error(errors)

    def test_online_group_by_unchanged_passes(self):
        old_gb = _make_group_by(online=True)
        new_gb = _make_group_by(online=True)
        validator = _make_validator(existing_gbs={old_gb.metaData.name: old_gb})

        errors = validator.validate_obj(new_gb)
        assert not _has_online_in_place_error(errors)

    def test_offline_group_by_changed_in_place_is_allowed(self):
        old_gb = _make_group_by(online=False, price_expr="price")
        new_gb = _make_group_by(online=False, price_expr="price * 2")
        validator = _make_validator(existing_gbs={old_gb.metaData.name: old_gb})

        errors = validator.validate_obj(new_gb)
        assert not _has_online_in_place_error(errors)

    def test_new_group_by_no_old_object_passes(self):
        new_gb = _make_group_by(online=True)
        validator = _make_validator()

        errors = validator.validate_obj(new_gb)
        assert not _has_online_in_place_error(errors)

    def test_online_join_changed_in_place_is_blocked(self):
        old_join = _make_join(online=True)
        gb = _make_group_by(online=True, table="different_table")
        new_join = _make_join(online=True, group_by=gb)
        validator = _make_validator(existing_joins={old_join.metaData.name: old_join})

        errors = validator.validate_obj(new_join)
        assert _has_online_in_place_error(errors)

    def test_online_join_unchanged_passes(self):
        old_join = _make_join(online=True)
        new_join = _make_join(online=True)
        validator = _make_validator(existing_joins={old_join.metaData.name: old_join})

        errors = validator.validate_obj(new_join)
        assert not _has_online_in_place_error(errors)

    def test_online_group_by_metadata_only_change_is_allowed(self):
        """metaData is a skipped field during diff, so metadata-only changes should pass."""
        old_gb = _make_group_by(online=True)
        new_gb = _make_group_by(online=True)
        new_gb.metaData.team = "different_team"
        new_gb.metaData.production = True
        validator = _make_validator(existing_gbs={old_gb.metaData.name: old_gb})

        errors = validator.validate_obj(new_gb)
        assert not _has_online_in_place_error(errors)

    def test_error_message_includes_config_name(self):
        old_gb = _make_group_by(name="team.important_gb", online=True, price_expr="price")
        new_gb = _make_group_by(name="team.important_gb", online=True, price_expr="price * 2")
        validator = _make_validator(existing_gbs={old_gb.metaData.name: old_gb})

        errors = validator.validate_obj(new_gb)
        online_errors = [e for e in errors if "is online and cannot be changed in-place" in str(e)]
        assert len(online_errors) == 1
        assert "team.important_gb" in str(online_errors[0])
        assert "GroupBy" in str(online_errors[0])


class TestTimePartitionedValidation:
    def test_group_by_source_requires_partition_column_when_time_partitioned(self):
        group_by = _make_group_by()
        group_by.sources[0].events.query.timePartitioned = True
        group_by.sources[0].events.query.partitionColumn = None
        validator = _make_validator()

        errors = validator.validate_obj(group_by)
        assert _has_time_partitioned_missing_partition_column_error(
            errors, "group_by team.my_gb source[0]"
        )

    def test_join_left_requires_partition_column_when_time_partitioned(self):
        join = _make_join()
        join.left.events.query.timePartitioned = True
        join.left.events.query.partitionColumn = None
        validator = _make_validator()

        errors = validator.validate_obj(join)
        assert _has_time_partitioned_missing_partition_column_error(
            errors, "join team.my_join left"
        )

    def test_join_bootstrap_part_requires_partition_column_when_time_partitioned(self):
        join = _make_join()
        bootstrap_query = Query()
        bootstrap_query.timePartitioned = True
        bootstrap_query.partitionColumn = None
        join.bootstrapParts = [
            BootstrapPart(
                table="test.bootstrap_table",
                query=bootstrap_query,
            )
        ]
        validator = _make_validator()

        errors = validator.validate_obj(join)
        assert _has_time_partitioned_missing_partition_column_error(
            errors, "join team.my_join bootstrapParts[0]"
        )

    def test_time_partitioned_with_partition_column_passes(self):
        group_by = _make_group_by()
        group_by.sources[0].events.query.timePartitioned = True
        group_by.sources[0].events.query.partitionColumn = "created_at"
        validator = _make_validator()

        errors = validator.validate_obj(group_by)
        assert not _has_time_partitioned_missing_partition_column_error(errors)

    def test_non_time_partitioned_without_partition_column_passes_this_validation(self):
        group_by = _make_group_by()
        validator = _make_validator()

        errors = validator.validate_obj(group_by)
        assert not _has_time_partitioned_missing_partition_column_error(errors)
