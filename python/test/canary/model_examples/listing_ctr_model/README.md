# CTR training through Hub and Ray

The example reads a materialized Databricks Unity Catalog table through Iceberg REST
with OAuth client credentials and vended object-store credentials. The staging query
currently generates random labels: this is a pipeline test, not evidence of CTR quality.
A separate Polaris deployment has not been validated.

## Training contract

Configure `TrainingSpec.job_configs`:

- `features`: ordered comma-separated `source[:alias]` column identifiers. For example
  `listing_id_price_cents:price,rooms,area`. Only these columns become features.
- `label-column` (default `label`) and `partition-column` (default `ds`).
- `max-depth`, `eta`, `num-boost-round`, and `seed`.
- `catalog-type`, `catalog-uri`, `oauth2-server-uri`, `warehouse`, `scope`,
  `client-id-env`, `client-secret-env`, and `vended-credentials`.
- `model-dir`: artifact output location. Secrets are resolved from named environment
  variables, never supplied in arguments.

Hub supplies input-table, start-ds, and end-ds from the model source/window and run date.
The date filter is inclusive; a four-day lookback ends up covering five daily partitions.
Query SQL transformations are not applied by the direct reader; materialize them upstream.
Column aliases affect model feature names. Missing feature values are filled with zero;
labels must be non-null 0/1 values. The trainer is ordinary driver-side XGBoost, not distributed.

`metadata.json` records actual feature names, training parameters, source table, requested
and observed date bounds, and row count. It does not store credentials.

## Packaging and validation

Run `./mill python.test_ctr_ray`. For a local synthetic smoke run:
`python -m trainer.smoke_local --model-dir /tmp/ctr-smoke` from this directory.

Build with `python build_archive.py`. Upload the printed archive to the configured
artifact base under `builds/`, preserving its filename. Use the printed SHA prefix as
Model.version. Never overwrite a published archive with different bytes. The builder
currently names this example `test_listing_ctr_model`; keep model_name consistent.

Platform #1307 is required, including comma-safe feature argument validation.
Without TrainingSpec.image, Crucible uses its standard Ray image. A custom image still
receives the working-directory archive when model_artifact_base_uri is set. For an
image-only trainer, omit that base and include trainer code in the image.

## E2E scenarios

`ZIPLINE_CTR_RAY_E2E=1` enables the AWS integration test. Optional inputs:
`ZIPLINE_CTR_END_DS`, `ZIPLINE_CTR_MODEL_CONF` (compiled path relative to Chronon root),
`ZIPLINE_CTR_ARTIFACT_BASE_URI`, and `AWS_REGION`/`AWS_DEFAULT_REGION`.
The baseline date is a known data fixture, not the trainer's hardcoded date.
Outputs are isolated per invocation and cleaned after assertions.
