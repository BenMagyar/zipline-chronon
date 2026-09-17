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
from typing import Any, Dict, List, Tuple

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
    partition_column=None,
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
    if partition_column and partition_column in df.columns:
        # Carried beside the features so callers can hold out whole days. The
        # partition itself must never become a feature.
        x_train_df.attrs["partitions"] = [str(value) for value in df[partition_column]]
    return x_train_df, labels.astype(int).tolist()


def rest_identifier(table: str) -> str:
    # Hub links a model to its upstream staging query by the Spark table name
    # (catalog.namespace.table), but the Iceberg REST catalog addresses the same
    # table as namespace.table inside the configured warehouse.
    parts = table.split(".")
    return ".".join(parts[-2:]) if len(parts) > 2 else table


def split_by_date(
    features: pd.DataFrame,
    labels: List[int],
    eval_days: int = 1,
) -> Tuple[pd.DataFrame, List[int], pd.DataFrame, List[int], Dict[str, Any]]:
    """Hold out the last ``eval_days`` partitions; never split at random.

    Rows are time-ordered, so a random split trains on rows that come after the
    ones it is scored against and reports optimistic metrics.
    """
    partitions = features.attrs.get("partitions")
    empty = features.iloc[0:0]
    if eval_days <= 0:
        return features, labels, empty, [], {
            "eval_days": eval_days,
            "evaluation_status": "disabled",
            "skipped": "eval-days is 0",
        }
    if not partitions:
        return features, labels, empty, [], {
            "eval_days": eval_days,
            "evaluation_status": "no_partition_column",
            "skipped": "no partition column in frame",
        }

    distinct = sorted(set(partitions))
    if len(distinct) <= eval_days:
        return features, labels, empty, [], {
            "eval_days": eval_days,
            "evaluation_status": "insufficient_partitions",
            "skipped": f"only {len(distinct)} partition(s) available; holding one out would leave no training rows",
        }

    eval_from = distinct[-eval_days]
    is_eval = [partition >= eval_from for partition in partitions]
    train_frame = features.loc[[not flag for flag in is_eval]]
    eval_frame = features.loc[is_eval]
    train_labels = [label for label, flag in zip(labels, is_eval) if not flag]
    eval_labels = [label for label, flag in zip(labels, is_eval) if flag]
    # Each side keeps only its own partitions; an unfiltered copy would report
    # the other side's dates and make a leak check pass on sorted input alone.
    train_frame.attrs = {**features.attrs,
                         "partitions": [p for p, flag in zip(partitions, is_eval) if not flag]}
    eval_frame.attrs = {**features.attrs,
                        "partitions": [p for p, flag in zip(partitions, is_eval) if flag]}
    split = {
        "eval_days": eval_days,
        "evaluation_status": "evaluated",
        "train_start_ds": distinct[0],
        "train_end_ds": distinct[-eval_days - 1],
        "eval_start_ds": eval_from,
        "eval_end_ds": distinct[-1],
        "train_rows": len(train_frame),
        "eval_rows": len(eval_frame),
    }
    return train_frame, train_labels, eval_frame, eval_labels, split


def _binary_classification_metrics(
    labels: List[int],
    probabilities: List[float],
    threshold: float = 0.5,
) -> Dict[str, float]:
    if len(labels) != len(probabilities):
        raise ValueError("labels and probabilities must be the same length")

    predicted = [1 if probability >= threshold else 0 for probability in probabilities]
    true_positive = sum(1 for actual, guess in zip(labels, predicted) if actual == 1 and guess == 1)
    true_negative = sum(1 for actual, guess in zip(labels, predicted) if actual == 0 and guess == 0)
    false_positive = sum(1 for actual, guess in zip(labels, predicted) if actual == 0 and guess == 1)
    false_negative = sum(1 for actual, guess in zip(labels, predicted) if actual == 1 and guess == 0)

    precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else 0.0
    recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else 0.0
    accuracy = (true_positive + true_negative) / len(labels) if labels else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if precision + recall else 0.0

    return {
        "threshold": threshold,
        "precision": precision,
        "recall": recall,
        "accuracy": accuracy,
        "f1": f1,
        "true_positives": true_positive,
        "true_negatives": true_negative,
        "false_positives": false_positive,
        "false_negatives": false_negative,
        "positive_rate": (sum(labels) / len(labels)) if labels else 0.0,
        "predicted_positive_rate": (sum(predicted) / len(predicted)) if predicted else 0.0,
    }


def _feature_importance(model: xgb.Booster, feature_names: List[str]) -> List[Dict[str, Any]]:
    gain_scores = model.get_score(importance_type="gain")
    weight_scores = model.get_score(importance_type="weight")
    cover_scores = model.get_score(importance_type="cover")
    total_gain = sum(gain_scores.values()) or 1.0

    ranked = [
        {
            "feature": name,
            "gain": float(gain_scores.get(name, 0.0)),
            "gain_share": float(gain_scores.get(name, 0.0)) / total_gain,
            "weight": int(weight_scores.get(name, 0.0)),
            "cover": float(cover_scores.get(name, 0.0)),
        }
        for name in feature_names
    ]
    ranked.sort(key=lambda item: item["gain"], reverse=True)
    for index, entry in enumerate(ranked, start=1):
        entry["rank"] = index
    return ranked


def _precision_recall_curve(
    labels: List[int],
    probabilities: List[float],
    point_count: int = 101,
) -> List[Dict[str, float]]:
    if not labels:
        return []
    if point_count < 2:
        raise ValueError("point_count must be at least 2")
    curve = []
    for step in range(point_count):
        threshold = 1.0 - (step / (point_count - 1))
        metrics = _binary_classification_metrics(labels, probabilities, threshold=threshold)
        curve.append({
            "threshold": threshold,
            "precision": metrics["precision"],
            "recall": metrics["recall"],
        })
    return curve


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
    x_train, labels = shape_training_frame(frame, features, label_column, partition_column)
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
    X_eval: pd.DataFrame = None,
    y_eval: List[int] = None,
    split: Dict[str, Any] = None,
    pr_curve_points: int = 101,
) -> xgb.Booster:
    print(f"Training with {len(X_train.columns)} features: {list(X_train.columns)}")

    has_eval = bool(X_eval is not None and len(X_eval) > 0 and y_eval)
    score_auc = has_eval and len(set(y_train)) > 1 and len(set(y_eval)) > 1

    params = {
        "max_depth": max_depth,
        "eta": eta,
        "objective": "binary:logistic",
        # AUC is undefined with one class present and XGBoost reports it as NaN,
        # which is not valid JSON for a strict reader. Ask for it only when both
        # sides can define it; logloss and the confusion metrics still apply.
        "eval_metric": ["logloss", "auc"] if score_auc else "logloss",
        "seed": seed,
    }

    dtrain = xgb.DMatrix(X_train, label=y_train)
    print(f"Training model with params: {params}")

    evals_result: Dict[str, Any] = {}
    watchlist = [(dtrain, "train")]
    if has_eval:
        watchlist.append((xgb.DMatrix(X_eval, label=y_eval), "validation"))

    model = xgb.train(
        params,
        dtrain,
        num_boost_round=num_boost_round,
        evals=watchlist,
        evals_result=evals_result,
        verbose_eval=False,
    )

    metrics: Dict[str, float] = {}
    for dataset, history in evals_result.items():
        for metric_name, values in history.items():
            metrics[f"{dataset}_{metric_name}"] = float(values[-1])
    metrics["training_rows"] = len(X_train)
    metrics.update({
        f"train_{key}": value
        for key, value in _binary_classification_metrics(y_train, model.predict(dtrain).tolist()).items()
    })
    if has_eval:
        metrics["validation_rows"] = len(X_eval)
        eval_probabilities = model.predict(xgb.DMatrix(X_eval)).tolist()
        metrics.update({
            f"validation_{key}": value
            for key, value in _binary_classification_metrics(y_eval, eval_probabilities).items()
        })
        curve = _precision_recall_curve(y_eval, eval_probabilities, point_count=pr_curve_points)
    else:
        curve = []

    status = (split or {}).get("evaluation_status", "disabled")
    if has_eval and not score_auc:
        status = "single_class_holdout"

    model.set_attr(training_metadata=json.dumps({
        "training_data": X_train.attrs.get("training_data", {}),
        "parameters": {**params, "num_boost_round": num_boost_round},
        "split": {**(split or {}), "evaluation_status": status},
    }))
    model.set_attr(validation_metrics=json.dumps(metrics))
    model.set_attr(feature_importance=json.dumps(_feature_importance(model, list(X_train.columns))))
    model.set_attr(precision_recall_curve=json.dumps(curve))
    print(f"Final metrics: {json.dumps(metrics, indent=2)}")
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


def _json_bytes(payload) -> bytes:
    # allow_nan=False: an undefined metric must fail here rather than reach an
    # artifact as NaN, which strict JSON readers reject.
    return json.dumps(payload, indent=2, allow_nan=False).encode("utf-8")


def save_model(model: xgb.Booster, output_dir: str) -> None:
    buf = io.BytesIO()
    joblib.dump(model, buf)
    model_path = _write_bytes(output_dir, "model.joblib", buf.getvalue())
    print(f"Model saved to {model_path}")

    metrics = json.loads(model.attr("validation_metrics") or "{}")
    feature_importance = json.loads(model.attr("feature_importance") or "[]")
    precision_recall_curve = json.loads(model.attr("precision_recall_curve") or "[]")

    metadata = {
        "num_features": model.num_features(),
        "feature_names": model.feature_names,
        **json.loads(model.attr("training_metadata") or "{}"),
        "model_type": "xgboost",
        "version": "1.0",
        "validation_metrics": metrics,
        "feature_importance": feature_importance,
    }
    meta_path = _write_bytes(output_dir, "metadata.json", _json_bytes(metadata))
    print(f"Metadata saved to {meta_path}")

    # Field names match the GCP CTR example's report so one reader can consume
    # either model's output.
    split = metadata.get("split", {})
    experiment_report = {
        "model_type": "xgboost",
        "feature_names": model.feature_names,
        "metrics": metrics,
        "feature_importance": feature_importance,
        "precision_recall_curve": precision_recall_curve,
        # A consumer must be able to tell "scored badly" from "never scored".
        "evaluation_status": split.get("evaluation_status", "disabled"),
        "split": split,
    }
    report_path = _write_bytes(output_dir, "experiment_report.json", _json_bytes(experiment_report))
    print(f"Experiment report saved to {report_path}")


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
    print(f"  Eval days:      {args.eval_days}")
    print(f"  Model output:   {args.model_dir}")
    print("=" * 80)

    catalog = load_iceberg_catalog(args)
    X_train, y_train = get_training_data_df(
        catalog, args.input_table, args.start_ds, args.end_ds,
        args.features, args.label_column, args.partition_column,
    )
    print(f"Loaded {len(X_train)} training rows.")

    X_train, y_train, X_eval, y_eval, split = split_by_date(X_train, y_train, args.eval_days)
    if split.get("skipped"):
        print(f"No evaluation split: {split['skipped']}. Metrics will cover training data only.")
    else:
        print(f"Training on ds [{split['train_start_ds']}, {split['train_end_ds']}] ({split['train_rows']} rows); "
              f"evaluating on ds [{split['eval_start_ds']}, {split['eval_end_ds']}] ({split['eval_rows']} rows).")

    model = train_model(
        X_train,
        y_train,
        max_depth=args.max_depth,
        eta=args.eta,
        num_boost_round=args.num_boost_round,
        seed=args.seed,
        X_eval=X_eval,
        y_eval=y_eval,
        split=split,
        pr_curve_points=args.pr_curve_points,
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
