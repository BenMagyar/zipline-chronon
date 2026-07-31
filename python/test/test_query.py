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

import copy

from ai.chronon.query import CaseWhen, when


def test_when_single_branch_no_else():
    expr = when("intent = 'SMS'", "phoneNumber")
    assert expr == "CASE WHEN intent = 'SMS' THEN phoneNumber END"
    assert isinstance(expr, str)


def test_when_else_none_renders_null():
    expr = when("intent = 'SMS'", "phoneNumber").else_(None)
    assert expr == "CASE WHEN intent = 'SMS' THEN phoneNumber ELSE NULL END"


def test_when_multi_branch():
    expr = (
        when("region = 'US'", "amount * 1.1")
        .when("region = 'EU'", "amount * 1.2")
        .else_("amount")
    )
    assert expr == (
        "CASE WHEN region = 'US' THEN amount * 1.1 "
        "WHEN region = 'EU' THEN amount * 1.2 ELSE amount END"
    )


def test_otherwise_is_alias_for_else():
    assert when("x > 0", "1").otherwise("0") == when("x > 0", "1").else_("0")
    assert when("x > 0", "1").otherwise("0") == "CASE WHEN x > 0 THEN 1 ELSE 0 END"


def test_numeric_and_none_values():
    assert when("x > 0", 1).else_(0) == "CASE WHEN x > 0 THEN 1 ELSE 0 END"
    assert when("x IS NULL", None).else_(1) == "CASE WHEN x IS NULL THEN NULL ELSE 1 END"


def test_builder_is_immutable_when_chained():
    base = when("a", "1")
    extended = base.when("b", "2")
    # chaining returns a new object; the original is unchanged
    assert base == "CASE WHEN a THEN 1 END"
    assert extended == "CASE WHEN a THEN 1 WHEN b THEN 2 END"


def test_usable_as_select_value_and_deepcopy():
    # A GroupBy's _sanitize_columns deepcopies the source (and its selects); the builder
    # must survive that and remain a plain-string-compatible value.
    expr = when("intent = 'SMS'", "phoneNumber").else_(None)
    original = {"sms_phone": expr}
    copied = copy.deepcopy(original)
    assert copied["sms_phone"] == "CASE WHEN intent = 'SMS' THEN phoneNumber ELSE NULL END"
    assert isinstance(copied["sms_phone"], str)
    assert isinstance(copied["sms_phone"], CaseWhen)
