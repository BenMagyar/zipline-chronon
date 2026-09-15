# CTR Ray training source

- UC table: `workspace_iceberg.poc.aws_databricks_ctr_labels_v1__0`
  (PyIceberg identifier `poc.aws_databricks_ctr_labels_v1__0`; the UC catalog
  name `workspace` is sent as the REST `warehouse` param, not in the identifier)
- Catalog: Databricks Unity Catalog via the Iceberg REST endpoint (OAuth2
  client-credentials); underlying S3 data files are read with UC-vended creds
- Owner: Chronon/Crucible model-platform maintainers
- Purpose: materialized labeled input for the Hub → Crucible → Ray training path
- Produced by: the `aws_databricks.ctr_labels.v1` staging query, which is
  `SELECT *, <label> FROM aws_databricks.demo.derivations_v1`, so the table
  already carries every derivation feature column **plus** a `label` column
- Required training partition: `ds=2026-08-24`, with at least one row
- Required columns: `user_id_click_event_average_7d`, `listing_id_price_cents`,
  `price_log`, `price_bucket`, `ds`, and `label`
- Labels: materialized in the source table by the staging query — the trainer no
  longer generates synthetic labels itself. The upstream staging query currently
  uses rand() to create labels; this fixture validates plumbing, not CTR accuracy

The integration test treats this table as shared input and never deletes it.
Every invocation writes model artifacts to a unique prefix and deletes only
those per-run outputs.
