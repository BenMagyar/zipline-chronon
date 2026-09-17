"""Command-line contract for the AWS CTR Ray trainer."""

import argparse
import os
import re
from datetime import date
from typing import Dict, Optional, Sequence


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"expected a boolean, got {value!r}")


def _compose_credential(args: argparse.Namespace) -> Optional[str]:
    """Join the service-principal id/secret read from the named env vars.

    Only env-var *names* travel through argv (and therefore the RayJob spec /
    logs); the secret values are read from the process environment, so nothing
    sensitive is ever passed on the command line.
    """
    id_env = args.client_id_env
    secret_env = args.client_secret_env
    if bool(id_env) != bool(secret_env):
        raise ValueError("set both --client-id-env and --client-secret-env")
    if not id_env:
        return None
    client_id = os.environ.get(id_env)
    client_secret = os.environ.get(secret_env)
    missing = [name for name, value in ((id_env, client_id), (secret_env, client_secret)) if not value]
    if missing:
        raise ValueError(f"missing required env var(s) for REST credential: {', '.join(missing)}")
    return f"{client_id}:{client_secret}"


def catalog_properties_from_args(args: argparse.Namespace) -> Dict[str, str]:
    """Translate the small AWS CLI surface into PyIceberg properties."""
    props: Dict[str, str] = {}
    if args.catalog_type:
        props["type"] = args.catalog_type
    if args.warehouse:
        props["warehouse"] = args.warehouse

    # REST catalog (Iceberg REST / Databricks Unity Catalog / Polaris): OAuth2
    # client-credentials, with the underlying S3 creds vended by the catalog.
    if args.catalog_type == "rest":
        if args.catalog_uri:
            props["uri"] = args.catalog_uri
        if args.oauth2_server_uri:
            props["oauth2-server-uri"] = args.oauth2_server_uri
        if args.scope:
            props["scope"] = args.scope
        credential = _compose_credential(args)
        if credential:
            props["credential"] = credential
        if args.vended_credentials:
            props["header.X-Iceberg-Access-Delegation"] = "vended-credentials"
        return props

    region = args.catalog_region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:
        props["s3.region"] = region
        props["client.region"] = region
    return props



# Ordered source[:model_name] pairs. Defaults preserve the original smoke model.
DEFAULT_FEATURES = "user_id_click_event_average_7d,listing_id_price_cents:listing_price_cents,price_log,price_bucket"


def parse_features(value):
    features = []
    for item in value.split(","):
        parts = item.split(":")
        if len(parts) not in (1, 2) or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in parts):
            raise argparse.ArgumentTypeError("features must be comma-separated source[:alias] identifiers")
        features.append((parts[0], parts[-1]))
    if len({s for s, _ in features}) != len(features) or len({a for _, a in features}) != len(features):
        raise argparse.ArgumentTypeError("feature sources and aliases must be unique")
    return features


def column_name(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise argparse.ArgumentTypeError("expected a column identifier")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train the AWS XGBoost CTR model")
    parser.add_argument("--input-table", required=True)
    parser.add_argument("--start-ds", required=True)
    parser.add_argument("--end-ds", required=True)
    parser.add_argument("--catalog-name", default="default")
    parser.add_argument("--catalog-type", default="glue")
    parser.add_argument("--warehouse")
    parser.add_argument("--catalog-region")
    # REST (Iceberg REST / Databricks UC / Polaris) catalog options.
    parser.add_argument("--catalog-uri")
    parser.add_argument("--oauth2-server-uri")
    parser.add_argument("--scope", default="all-apis")
    parser.add_argument("--client-id-env")
    parser.add_argument("--client-secret-env")
    parser.add_argument("--vended-credentials", type=_parse_bool, default=False)
    parser.add_argument("--features", type=parse_features, default=DEFAULT_FEATURES)
    parser.add_argument("--label-column", type=column_name, default="label")
    parser.add_argument("--partition-column", type=column_name, default="ds")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-depth", type=int, default=4)
    parser.add_argument("--eta", type=float, default=0.1)
    parser.add_argument("--num-boost-round", type=int, default=50)
    # Held out by date, never at random: the rows are time-ordered, so a random
    # split trains on rows that come after the ones it is scored against.
    parser.add_argument("--eval-days", type=int, default=1,
                        help="trailing ds partitions held out for evaluation; 0 trains without metrics")
    parser.add_argument("--pr-curve-points", type=int, default=101)
    parser.add_argument("--model-dir", default=os.environ.get("MODEL_DIR"), required=os.environ.get("MODEL_DIR") is None)
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if date.fromisoformat(args.start_ds) > date.fromisoformat(args.end_ds):
            raise ValueError("start-ds must not be after end-ds")
    except ValueError as error:
        parser.error(str(error))
    if any(source in {args.label_column, args.partition_column} for source, _ in args.features):
        parser.error("label and partition columns must not be used as features")
    if args.eval_days < 0:
        parser.error("--eval-days must not be negative")
    if args.pr_curve_points < 2:
        parser.error("--pr-curve-points must be at least 2")
    return args
