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

"""Tests for per-aggregation `filter` / `column_alias` compile-layer sugar."""

import pytest

import gen_thrift.common.ttypes as common
from ai.chronon import group_by
from gen_thrift.api import ttypes


def _event_source(table="db.events"):
    return ttypes.EventSource(
        table=table,
        query=ttypes.Query(
            selects={"user": "user", "amount": "amount", "status": "status"},
            timeColumn="ts",
        ),
    )


def _selects(gb, source_index=0):
    src = gb.sources[source_index]
    inner = src.events or src.entities or src.joinSource
    return inner.query.selects


def _day(n):
    return common.Window(length=n, timeUnit=common.TimeUnit.DAYS)


def test_filter_desugars_to_case_when():
    gb = group_by.GroupBy(
        sources=[_event_source()],
        keys=["user"],
        aggregations=[
            group_by.Aggregation(
                input_column="amount",
                operation=ttypes.Operation.SUM,
                windows=[_day(1)],
                filter="status = 'paid'",
                column_alias="paid_amount",
            ),
        ],
        accuracy=ttypes.Accuracy.TEMPORAL,
    )
    selects = _selects(gb)
    assert selects["paid_amount"] == "CASE WHEN (status = 'paid') THEN amount ELSE NULL END"
    # aggregation repointed to the generated alias column
    assert gb.aggregations[0].inputColumn == "paid_amount"
    # output feature named after the alias, not the original input column
    assert group_by.get_output_col_names(gb.aggregations[0]) == ["paid_amount_sum_1d"]


def test_filter_injected_into_every_source():
    gb = group_by.GroupBy(
        sources=[_event_source("db.events_a"), _event_source("db.events_b")],
        keys=["user"],
        aggregations=[
            group_by.Aggregation(
                input_column="amount",
                operation=ttypes.Operation.SUM,
                windows=[_day(1)],
                filter="status = 'paid'",
                column_alias="paid_amount",
            ),
        ],
        accuracy=ttypes.Accuracy.TEMPORAL,
    )
    expected = "CASE WHEN (status = 'paid') THEN amount ELSE NULL END"
    assert _selects(gb, 0)["paid_amount"] == expected
    assert _selects(gb, 1)["paid_amount"] == expected


def test_original_aggregation_not_mutated():
    # GroupBy desugars on a copy, so a caller reusing the Aggregation object is unaffected.
    agg = group_by.Aggregation(
        input_column="amount",
        operation=ttypes.Operation.SUM,
        windows=[_day(1)],
        filter="status = 'paid'",
        column_alias="paid_amount",
    )
    group_by.GroupBy(
        sources=[_event_source()],
        keys=["user"],
        aggregations=[agg],
        accuracy=ttypes.Accuracy.TEMPORAL,
    )
    assert agg.inputColumn == "amount"


def test_filter_requires_column_alias():
    with pytest.raises(ValueError, match="requires a column_alias"):
        group_by.GroupBy(
            sources=[_event_source()],
            keys=["user"],
            aggregations=[
                group_by.Aggregation(
                    input_column="amount",
                    operation=ttypes.Operation.SUM,
                    windows=[_day(1)],
                    filter="status = 'paid'",
                ),
            ],
            accuracy=ttypes.Accuracy.TEMPORAL,
        )


def test_column_alias_without_filter_rejected():
    with pytest.raises(ValueError, match="without a filter"):
        group_by.GroupBy(
            sources=[_event_source()],
            keys=["user"],
            aggregations=[
                group_by.Aggregation(
                    input_column="amount",
                    operation=ttypes.Operation.SUM,
                    windows=[_day(1)],
                    column_alias="paid_amount",
                ),
            ],
            accuracy=ttypes.Accuracy.TEMPORAL,
        )


def test_conflicting_alias_definitions_rejected():
    with pytest.raises(ValueError, match="two different filtered expressions"):
        group_by.GroupBy(
            sources=[_event_source()],
            keys=["user"],
            aggregations=[
                group_by.Aggregation(
                    input_column="amount", operation=ttypes.Operation.SUM, windows=[_day(1)],
                    filter="status = 'paid'", column_alias="amt",
                ),
                group_by.Aggregation(
                    input_column="amount", operation=ttypes.Operation.SUM, windows=[_day(1)],
                    filter="status = 'failed'", column_alias="amt",
                ),
            ],
            accuracy=ttypes.Accuracy.TEMPORAL,
        )


def test_duplicate_output_column_rejected():
    with pytest.raises(ValueError, match="duplicate output column"):
        group_by.GroupBy(
            sources=[_event_source()],
            keys=["user"],
            aggregations=[
                group_by.Aggregation(
                    input_column="amount", operation=ttypes.Operation.SUM, windows=[_day(1)],
                    filter="status = 'paid'", column_alias="amt",
                ),
                group_by.Aggregation(
                    input_column="amount", operation=ttypes.Operation.SUM, windows=[_day(1)],
                    filter="status = 'paid'", column_alias="amt",
                ),
            ],
            accuracy=ttypes.Accuracy.TEMPORAL,
        )


def test_alias_collides_with_existing_select_rejected():
    with pytest.raises(ValueError, match="collides with an existing select"):
        group_by.GroupBy(
            sources=[_event_source()],
            keys=["user"],
            aggregations=[
                group_by.Aggregation(
                    input_column="amount",
                    operation=ttypes.Operation.SUM,
                    windows=[_day(1)],
                    filter="amount > 0",
                    column_alias="status",  # already a source select column
                ),
            ],
            accuracy=ttypes.Accuracy.TEMPORAL,
        )


def test_no_filter_group_by_unchanged():
    gb = group_by.GroupBy(
        sources=[_event_source()],
        keys=["user"],
        aggregations=[
            group_by.Aggregation(
                input_column="amount", operation=ttypes.Operation.SUM, windows=[_day(1)]
            ),
        ],
        accuracy=ttypes.Accuracy.TEMPORAL,
    )
    assert gb.aggregations[0].inputColumn == "amount"
    # no synthetic columns injected
    assert set(_selects(gb).keys()) == {"user", "amount", "status"}


def _no_selects_source(table="db.events"):
    # Query(selects=None): keys/inputs get auto-added as identity selects during
    # normalization -- exactly where an alias collision must still be caught.
    return ttypes.EventSource(table=table, query=ttypes.Query(timeColumn="ts"))


def test_alias_colliding_with_key_rejected():
    # 'user' (a key) would be auto-added as an identity select; an alias must not clobber it.
    with pytest.raises(ValueError, match="reserved column name"):
        group_by.GroupBy(
            sources=[_no_selects_source()],
            keys=["user"],
            aggregations=[
                group_by.Aggregation(
                    input_column="amount", operation=ttypes.Operation.SUM, windows=[_day(1)],
                    filter="status = 'paid'", column_alias="user",
                ),
            ],
            accuracy=ttypes.Accuracy.TEMPORAL,
        )


def test_alias_ts_rejected():
    with pytest.raises(ValueError, match="reserved column name"):
        group_by.GroupBy(
            sources=[_no_selects_source()],
            keys=["user"],
            aggregations=[
                group_by.Aggregation(
                    input_column="amount", operation=ttypes.Operation.SUM, windows=[_day(1)],
                    filter="status = 'paid'", column_alias="ts",
                ),
            ],
            accuracy=ttypes.Accuracy.TEMPORAL,
        )


def test_alias_colliding_with_other_agg_input_rejected():
    with pytest.raises(ValueError, match="reserved column name"):
        group_by.GroupBy(
            sources=[_no_selects_source()],
            keys=["user"],
            aggregations=[
                group_by.Aggregation(
                    input_column="region", operation=ttypes.Operation.COUNT, windows=[_day(1)]
                ),
                group_by.Aggregation(
                    input_column="amount", operation=ttypes.Operation.SUM, windows=[_day(1)],
                    filter="status = 'paid'", column_alias="region",  # collides with agg1 input
                ),
            ],
            accuracy=ttypes.Accuracy.TEMPORAL,
        )
