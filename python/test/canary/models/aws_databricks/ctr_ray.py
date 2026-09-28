"""CTR trainer submitted through Hub TRAIN_MODEL on the standard Ray image.

Reads a real labeled dataset from Databricks Unity Catalog via the Iceberg REST
catalog: the ``ctr_labels`` staging query already carries the derivation feature
columns plus a materialized ``label`` column, so the trainer no longer generates
synthetic labels. ``TrainingSpec.image`` is omitted so Crucible uses the
configured standard Ray image and fetches trainer code from
``{model_artifact_base_uri}/builds/listing_ctr_model-{version}.zip``.
"""

from staging_queries.aws_databricks import ctr_labels

from ai.chronon.data_types import DataType
from ai.chronon.types import (
    EventSource,
    InferenceSpec,
    Model,
    ModelBackend,
    Query,
    ResourceConfig,
    TimeUnit,
    TrainingSpec,
    Window,
    selects,
)

# One ordered feature contract drives source selection, trainer arguments, and
# inference input mapping. Add new features here, not inside the trainer.
_features = [
    ("user_id_click_event_average_7d", "user_id_click_event_average_7d"),
    ("listing_id_price_cents", "listing_price_cents"),
    ("price_log", "price_log"),
    ("price_bucket", "price_bucket"),
    ("listing_id_inventory_count", "inventory_count"),
    ("listing_id_weight_grams", "weight_grams"),
    ("listing_id_is_in_stock", "is_in_stock"),
    ("is_listing_heavy", "is_listing_heavy"),
    ("inventory_log", "inventory_log"),
    ("price_per_kg", "price_per_kg"),
]
_label_column = "label"
_partition_column = "ds"
# Keep the staging query's Spark name: Hub links the model to its producer by exact
# table name, so a model backfill recomputes the join and labels first. The trainer
# strips the catalog qualifier for the Iceberg REST lookup.
_training_table = ctr_labels.v1.table
_artifact_base = "s3://zipline-warehouse-crucible"
_databricks_host = "https://dbc-050d6f00-dcb3.cloud.databricks.com"
_training_source = EventSource(
    table=_training_table,
    query=Query(
        selects=selects(**{column: column for column in
                           [source for source, _ in _features] + [_label_column, _partition_column]}),
        start_partition="2025-07-01",
    ),
)

ctr_ray_model = Model(
    # Content address of the deterministic trainer archive; never reuse it for
    # different trainer bytes.
    version="afce3c0e00af",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.RAY,
        model_backend_params={
            "model_name": "listing_ctr_model",
            "model_type": "custom",
        },
    ),
    input_mapping={
        "instance": "named_struct(" + ", ".join(f"'{alias}', {source}" for source, alias in _features) + ")",
    },
    output_mapping={"ctr": "score"},
    value_fields=[("score", DataType.DOUBLE)],
    model_artifact_base_uri=_artifact_base,
    training_conf=TrainingSpec(
        training_data_source=_training_source,
        # Piyush's validated smoke range was 2026-08-20 through 2026-08-24.
        training_data_window=Window(length=4, time_unit=TimeUnit.DAYS),
        python_module="trainer.train",
        resource_config=ResourceConfig(min_replica_count=0, max_replica_count=0),
        job_configs={
            "features": ",".join(f"{source}:{alias}" for source, alias in _features),
            "label-column": _label_column,
            "partition-column": _partition_column,
            "seed": "42",
            "catalog-type": "rest",
            "catalog-uri": f"{_databricks_host}/api/2.1/unity-catalog/iceberg-rest",
            "oauth2-server-uri": f"{_databricks_host}/oidc/v1/token",
            "scope": "all-apis",
            "warehouse": "workspace",
            # Env-var names only; the Hub injects the secret values into the Ray
            # pods (see platform #1307). The trainer never sees them on argv.
            "client-id-env": "DATABRICKS_CLIENT_ID",
            "client-secret-env": "DATABRICKS_CLIENT_SECRET",
            "vended-credentials": "true",
            "eta": "0.1",
            "max-depth": "4",
            "num-boost-round": "50",
            # Hold out the last partition for evaluation. The trainer splits by
            # date, never at random, so it cannot score against rows it trained on.
            "eval-days": "1",
        },
    ),
)
