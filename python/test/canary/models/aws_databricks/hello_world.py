"""Smoke-test TRAIN_MODEL configuration for AWS in-cluster Ray.

The job runs ``python -m this`` (the standard-library Zen of Python module), so
the base Ray image needs no project-specific trainer package. ``trainer.train``
would require the archive delivery added by the follow-up CTR connector. Extra
CLI arguments are harmless. The source is an existing Crucible UC table so Hub
can resolve dependencies; this smoke job does not read it.
"""

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

# TRAIN_MODEL currently requires a typed source for planning and date-window
# construction. This stdlib smoke entrypoint deliberately does not read it.
_training_source = EventSource(
    table="workspace.poc.dim_listings",
    query=Query(selects=selects(listing_id="listing_id", ds="ds"), start_partition="2025-01-01"),
)

hello_world_model = Model(
    version="1.0",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.RAY,
        model_backend_params={"model_name": "hello_world_model", "model_type": "custom"},
    ),
    input_mapping={"instance": "named_struct('listing_id', listing_id)"},
    output_mapping={"score": "score"},
    value_fields=[("score", DataType.DOUBLE)],
    training_conf=TrainingSpec(
        training_data_source=_training_source,
        training_data_window=Window(length=1, time_unit=TimeUnit.DAYS),
        python_module="this",
        image="rayproject/ray:2.55.1",
        resource_config=ResourceConfig(min_replica_count=0, max_replica_count=0),
    ),
)
