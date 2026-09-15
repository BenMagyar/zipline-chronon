"""In-process smoke test that does not need Glue, S3, or a Ray cluster.

Builds a tiny synthetic feature frame, trains XGBoost, and writes
``model.joblib`` plus ``metadata.json`` to ``--model-dir``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from trainer.train import FEATURE_COLUMNS, save_model, shape_training_frame, train_model


def _synthetic_rows(n: int = 8) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "user_id_click_event_average_7d": [float(i % 3) for i in range(n)],
            "listing_id_price_cents": [1000 + 250 * i for i in range(n)],
            "price_log": [6.9 + 0.1 * i for i in range(n)],
            "price_bucket": [i % 5 for i in range(n)],
            "label": [i % 2 for i in range(n)],
            "ds": ["2026-08-20"] * n,
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Local CTR trainer smoke test")
    parser.add_argument("--model-dir", required=True, help="Directory for smoke artifacts")
    args = parser.parse_args()

    x_train, y_train = shape_training_frame(_synthetic_rows())
    assert list(x_train.columns) == FEATURE_COLUMNS
    model = train_model(x_train, y_train, num_boost_round=2)
    save_model(model, args.model_dir)

    model_path = Path(args.model_dir) / "model.joblib"
    meta_path = Path(args.model_dir) / "metadata.json"
    if not model_path.is_file() or not meta_path.is_file():
        raise SystemExit(f"expected artifacts under {args.model_dir}")
    metadata = json.loads(meta_path.read_text())
    if metadata.get("feature_names") != FEATURE_COLUMNS:
        raise SystemExit(f"unexpected metadata: {metadata}")
    print(f"Smoke artifacts written to {args.model_dir}")


if __name__ == "__main__":
    main()
