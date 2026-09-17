"""
XGBoost CTR trainer, forked from ctr_model/ for Ray on Kubernetes.

Differences from the original:
  - Reads training data from an Iceberg table via PyIceberg. Catalog backend
    (glue, rest, hive, sql, ...) and object-store (s3/gs/abfss/local) are chosen
    via CLI flags and ambient env — no cloud SDK imports live in this module.
  - Model artifacts are written through pyarrow.fs, so `s3://`, `gs://`,
    `abfss://`, and plain filesystem paths all use one code path.
  - No Vertex/SageMaker-specific env handling (AIP_MODEL_DIR, GCSFuse rewrites).
  - CLI accepts Slice B ``--key=value`` argv.
    and unique ``--catalog-region``.
"""
import io
import json
import os
import sys
from pathlib import Path
from typing import List, Tuple

import joblib
import pandas as pd
import pyarrow.fs as pafs
import xgboost as xgb

from trainer.cli import DEFAULT_FEATURES, parse_features, catalog_properties_from_args, parse_args

FEATURE_COLUMNS = [alias for _, alias in parse_features(DEFAULT_FEATURES)]


def load_iceberg_catalog(args):
    # CLI flags win over pyiceberg's own config discovery (~/.pyiceberg.yaml,
    # PYICEBERG_CATALOG__* env). Any prop not passed here is left for pyiceberg
    # to resolve on its own — that's how a fully env-driven deployment works.
    from pyiceberg.catalog import load_catalog

    return load_catalog(args.catalog_name, **catalog_properties_from_args(args))


def shape_training_frame(
    df: pd.DataFrame,
    features=None,
    label_column="label",
) -> Tuple[pd.DataFrame, List[int]]:
    """Rename, impute, and split labeled rows into features and labels."""
    features = features if features is not None else parse_features(DEFAULT_FEATURES)
    sources = [source for source, _ in features]
    missing = [col for col in sources + [label_column] if col not in df.columns]
    if missing:
        raise ValueError(f"training frame is missing columns: {missing}")
    if df.empty:
        raise ValueError("training frame has no rows")
    labels = df[label_column]
    if labels.isna().any() or not labels.isin([0, 1]).all():
        raise ValueError("CTR labels must be non-null binary values (0 or 1)")
    x_train_df = df[sources].copy()
    x_train_df.columns = [alias for _, alias in features]
    # Nullable booleans arrive from Arrow as object dtype. Normalize numeric and
    # boolean columns before imputation so XGBoost does not reject mixed bool/0.
    x_train_df = x_train_df.apply(pd.to_numeric, errors="raise").fillna(0)
    return x_train_df, labels.astype(int).tolist()


def rest_identifier(table: str) -> str:
    # Hub links a model to its upstream staging query by the Spark table name
    # (catalog.namespace.table), but the Iceberg REST catalog addresses the same
    # table as namespace.table inside the configured warehouse.
    parts = table.split(".")
    return ".".join(parts[-2:]) if len(parts) > 2 else table


def get_training_data_df(
    catalog,
    input_table: str,
    start_ds: str,
    end_ds: str,
    features=None,
    label_column="label",
    partition_column="ds",
) -> Tuple[pd.DataFrame, List[int]]:
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

    print(f"Scanning Iceberg table {input_table} for ds in [{start_ds}, {end_ds}]")

    features = features if features is not None else parse_features(DEFAULT_FEATURES)
    selected = list(dict.fromkeys([source for source, _ in features] + [partition_column, label_column]))

    table = catalog.load_table(rest_identifier(input_table))
    arrow_tbl = table.scan(
        row_filter=And(
            GreaterThanOrEqual(partition_column, start_ds),
            LessThanOrEqual(partition_column, end_ds),
        ),
        selected_fields=tuple(selected),
    ).to_arrow()

    frame = arrow_tbl.to_pandas()
    if frame.empty:
        raise ValueError(
            f"no training rows found in {input_table} for ds in [{start_ds}, {end_ds}]"
        )
    x_train, labels = shape_training_frame(frame, features, label_column)
    x_train.attrs["training_data"] = {
        "input_table": input_table, "start_ds": start_ds, "end_ds": end_ds,
        "partition_column": partition_column, "label_column": label_column,
        "observed_start_ds": str(frame[partition_column].min()),
        "observed_end_ds": str(frame[partition_column].max()), "row_count": len(frame),
        "source_features": [source for source, _ in features],
    }
    return x_train, labels


def train_model(
    X_train: pd.DataFrame,
    y_train: List[int],
    max_depth: int = 4,
    eta: float = 0.1,
    num_boost_round: int = 50,
    seed: int = 42,
) -> xgb.Booster:
    print(f"Training with {len(X_train.columns)} features: {list(X_train.columns)}")

    params = {
        "max_depth": max_depth,
        "eta": eta,
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "seed": seed,
    }

    dtrain = xgb.DMatrix(X_train, label=y_train)
    print(f"Training model with params: {params}")

    model = xgb.train(params, dtrain, num_boost_round=num_boost_round)
    model.set_attr(training_metadata=json.dumps({
        "training_data": X_train.attrs.get("training_data", {}),
        "parameters": {**params, "num_boost_round": num_boost_round},
    }))
    print("Training complete!")
    return model


def _write_bytes(output_dir: str, filename: str, data: bytes) -> str:
    # pyarrow.fs.FileSystem.from_uri handles s3/gs/abfss/local uniformly and
    # picks credentials up from the ambient env (IAM role, GOOGLE_APPLICATION_CREDENTIALS, ...).
    if "://" in output_dir:
        fs, base = pafs.FileSystem.from_uri(output_dir)
        target = f"{base.rstrip('/')}/{filename}"
    else:
        fs = pafs.LocalFileSystem()
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        target = str(Path(output_dir) / filename)

    with fs.open_output_stream(target) as sink:
        sink.write(data)
    return target


def save_model(model: xgb.Booster, output_dir: str) -> None:
    buf = io.BytesIO()
    joblib.dump(model, buf)
    model_path = _write_bytes(output_dir, "model.joblib", buf.getvalue())
    print(f"Model saved to {model_path}")

    metadata = {
        "num_features": model.num_features(),
        "feature_names": model.feature_names,
        **json.loads(model.attr("training_metadata") or "{}"),
        "model_type": "xgboost",
        "version": "1.0",
    }
    meta_path = _write_bytes(
        output_dir, "metadata.json", json.dumps(metadata, indent=2).encode("utf-8")
    )
    print(f"Metadata saved to {meta_path}")


def finalize_remote_filesystems(output_dir: str) -> None:
    """Force a prompt, successful process exit after a remote (S3) write.

    PyArrow's S3 support (bundled aws-sdk-cpp) starts a native ``AwsEventLoop``
    thread the first time an ``s3://``/``s3a://`` path is touched. Under
    pyarrow 19.x on the Ray image that thread is never joined at interpreter
    shutdown, and ``pyarrow.fs.finalize_s3()`` *itself* deadlocks waiting on it
    -- the Ray driver then hangs forever and the RayJob is stuck ``RUNNING``
    even though training finished and ``model.joblib``/``metadata.json`` were
    already uploaded. The output streams flush synchronously when they close
    (in ``save_model``, well before this runs), so there is nothing left to
    clean up: bypass the hanging shutdown and exit immediately with success so
    the RayJob can report SUCCEEDED. Local runs have no S3 thread and simply
    return to exit normally.
    """
    if output_dir.lower().startswith(("s3://", "s3a://")):
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)


def main() -> None:
    args = parse_args()
    if not args.model_dir:
        raise SystemExit("--model-dir (or $MODEL_DIR) is required")

    print("=" * 80)
    print("XGBoost CTR Model Training (Ray fork)")
    print("=" * 80)
    print(f"  Table:          {args.input_table}")
    print(f"  Partitions:     [{args.start_ds}, {args.end_ds}]")
    print(f"  Catalog:        name={args.catalog_name} type={args.catalog_type}")
    print(f"  Warehouse:      {args.warehouse}")
    print(f"  Catalog region: {args.catalog_region}")
    print(f"  Max depth:      {args.max_depth}")
    print(f"  Learning rate:  {args.eta}")
    print(f"  Boost rounds:   {args.num_boost_round}")
    print(f"  Model output:   {args.model_dir}")
    print("=" * 80)

    catalog = load_iceberg_catalog(args)
    X_train, y_train = get_training_data_df(
        catalog, args.input_table, args.start_ds, args.end_ds,
        args.features, args.label_column, args.partition_column,
    )
    print(f"Loaded {len(X_train)} training rows.")

    model = train_model(
        X_train,
        y_train,
        max_depth=args.max_depth,
        eta=args.eta,
        num_boost_round=args.num_boost_round,
        seed=args.seed,
    )

    save_model(model, args.model_dir)

    # Cheap end-to-end sanity: predict on the first row of the training set.
    X_test = X_train.iloc[[0]]
    pred = model.predict(xgb.DMatrix(X_test))[0]
    print(f"Sample prediction: {pred:.4f} (label: {y_train[0]})")
    print("=" * 80)
    print("Training complete!")
    print("=" * 80)
    finalize_remote_filesystems(args.model_dir)


if __name__ == "__main__":
    main()
