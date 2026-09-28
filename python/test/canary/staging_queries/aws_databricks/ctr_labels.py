from joins.aws_databricks import demo

from ai.chronon.types import StagingQuery, TableDependency

_ACTIVITIES = "workspace.demo.user_activities"

# The label was `rand() < 0.5`, which made every score this pipeline has ever produced
# a measurement of noise — and meant no feature added to it could ever help.
#
# It is now the outcome the event actually recorded. Each row of the join is one
# impression, and whether that impression was engaged with is a fact already sitting in
# the source table rather than a number invented here. That matters more than it sounds:
# a formula would be a guess at what makes people click, and would reward features that
# match the guess. The observed value rewards whatever genuinely predicts.
#
# It only became worth reading once the generator stopped drawing users, listings and
# event types independently (platform: "Make the demo data something a model can
# actually learn from"). With persistent user appetite and listing quality behind it,
# a user's past engagement rate predicts their next one — measured at AUC 0.70 for user
# history, 0.70 for listing history, and 0.78 for the two together.
#
# Point-in-time correctness is what keeps this honest: the aggregations are windowed
# strictly before each impression's own timestamp, so no feature can see the outcome it
# is predicting.
v1 = StagingQuery(
    query=f"""
SELECT
    joined.*,
    IF(activity.event_type IN ('click', 'purchase', 'favorite', 'add_to_cart'), 1, 0) AS label
FROM {demo.derivations_v1.table} joined
JOIN {_ACTIVITIES} activity
  ON joined.row_id = activity.event_id
 AND joined.ds = activity.ds
WHERE joined.ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
""",
    output_namespace="workspace_iceberg.poc",
    dependencies=[
        TableDependency(table=demo.derivations_v1.table, partition_column="ds", start_offset=0, end_offset=0),
        TableDependency(table=_ACTIVITIES, partition_column="ds", start_offset=0, end_offset=0),
    ],
    version=3,
)
