# CTR Ray training source

- UC table: `workspace_iceberg.poc.aws_databricks_ctr_labels_v1__2`
  (PyIceberg identifier `poc.aws_databricks_ctr_labels_v1__2`; the UC catalog
  name `workspace` is sent as the REST `warehouse` param, not in the identifier)
- Catalog: Databricks Unity Catalog via the Iceberg REST endpoint (OAuth2
  client-credentials); underlying S3 data files are read with UC-vended creds
- Owner: Chronon/Crucible model-platform maintainers
- Purpose: materialized labeled input for the Hub → Crucible → Ray training path
- Produced by: the `aws_databricks.ctr_labels.v1` staging query, which is
  `SELECT *, <label> FROM aws_databricks.demo.derivations_v1`, so the table
  already carries every derivation feature column **plus** a `label` column
- Versions: labels `__2` consumes Join `__3`, whose activity features use
  `aws_databricks.user_activities.v2__2`. This GroupBy preserves the source's
  epoch-millisecond clock for temporal windows and event-history structs.
- Hub receives the full Spark table name to resolve upstream dependencies;
  only the trainer strips the catalog prefix for the Iceberg REST lookup.
- Required training partition: `ds=2026-08-24`, with at least one row
- Required columns: `user_id_click_event_average_7d`, `listing_id_price_cents`,
  `price_log`, `price_bucket`, `listing_id_inventory_count`,
  `listing_id_weight_grams`, `listing_id_is_in_stock`, `is_listing_heavy`,
  `inventory_log`, `price_per_kg`, `ds`, and `label`
- Check click-history coverage before treating a training run as healthy:
  the old activity timestamp expression produced all-null history, which the
  trainer silently imputed to zero. Genuine cold-start rows may still be null.
- Labels: materialized in the source table by the staging query — the trainer no
  longer generates synthetic labels itself. The upstream staging query currently
  uses rand() to create labels; this fixture validates plumbing, not CTR accuracy

The integration test treats this table as shared input and never deletes it.
Every invocation writes model artifacts to a unique prefix and deletes only
those per-run outputs.
