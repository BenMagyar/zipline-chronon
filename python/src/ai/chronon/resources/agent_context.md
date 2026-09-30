---
name: zipline
description: Author, change, compile, evaluate, or debug Chronon feature configurations and Zipline pipelines, including GroupBys, Joins, StagingQueries, Models, ModelTransforms, source-table resolution, data-quality investigation, Tecton migration, and model trials and experiments. Do not use for general Python review, repository maintenance, CLI implementation, or unrelated work merely because it is in the Chronon repository.
---

# Zipline and Chronon

Help users author and debug Chronon feature definitions and operate the Zipline CLI. Prefer repository evidence and the installed Python API over generic examples.

## Non-negotiable rules

- Before changing a Chronon config, run `git branch --show-current`. If it returns `main`, create and switch to a new branch such as `feature/<descriptive-name>` before editing files or running commands that generate repository artifacts. Never modify `main` directly.
- Inspect the relevant team directories and nearby configs before designing a change. Follow their imports, naming, formatting, source patterns, and environment variables.
- Import public APIs from `ai.chronon.*`. Never import feature APIs directly from `gen_thrift.api.ttypes` or another generated thrift module.
- Before creating a GroupBy, search the same team for the same source and key set. Normally version the existing GroupBy (`v1` to `v2`) instead of creating a duplicate. Ask before separating unusually large or latency-sensitive features.
- Treat table identity, keys, timestamp semantics, and streaming topics as facts to verify. Ask only when repository inspection, metadata, and the user request do not resolve them.
- A `time_column` used for temporal or windowed computation must produce epoch milliseconds as a long. Do not wrap a value already stored in milliseconds in a timestamp-to-milliseconds conversion.
- Every `Aggregation.input_column` must name a column exposed by the source query's `selects`; put SQL expressions in `selects`, not in `input_column`.
- Prefer scalable approximate operations for high-cardinality features, such as `Operation.APPROX_UNIQUE_COUNT_LGK(k=8)` and `Operation.APPROX_PERCENTILE(...)`. `UNIQUE_TOP_K` does not support struct inputs; use `LAST_K` when a complex value is required.
- Preserve existing production variables and create the next variable version unless the user explicitly authorizes an in-place change. Keep existing variable names and their `version` values unchanged; give the new config a new variable name and an explicit version supported by the public API.
- GroupBy and Join files in the same team must not share a filename; use a distinct suffix such as `_join`.
- A Join currently needs `row_ids`: expose the row-id column on the left and pass it to `row_ids=[...]`.
- After each Chronon config file change, compile and evaluate that config before editing the next config. Report failures accurately; never claim validation that did not pass.
- Every training submission belonging to a trial must carry both `ZIPLINE_TRIAL_ID` and its matching `ZIPLINE_EXPERIMENT_ID` on that command. Show both in proposed training commands too; recording an experiment does not automatically attribute a later backfill to it.
- Present comparison work as one chronological workflow: discover prior attempts and start/resume the trial before configuration or archive changes; then prepare, record, train, retrieve reports, and close out with explicit summary/status/journal commands. This order applies to plans as well as execution.

For example, move an aggregation expression into a named selection (fragments):

```python
# Before: input_column contains SQL instead of a selected column name.
Aggregation(input_column="price * quantity", operation=Operation.SUM, windows=["7d"])

# After: compute per row, then aggregate the selected result.
query = Query(selects=selects("user_id", revenue="price * quantity"), time_column="ts")
aggregations = [Aggregation(input_column="revenue", operation=Operation.SUM, windows=["7d"])]
```

## Repository and CLI conventions

For this repository, run all `zipline` commands from `python/test/canary`, the Chronon root containing `teams.py`, `group_bys/`, `joins/`, and related directories.

Use the CLI's current positional forms:

```bash
# Discover tables when a name is missing, vague, or not an exact match.
zipline hub list-tables <schema> --engine-type BIGQUERY --team <team>

# Inspect a confirmed table.
zipline hub eval-table '<schema.table>' --engine-type BIGQUERY --team <team> -f json

# Compile from the Chronon root.
zipline compile --force

# Evaluate an already compiled config.
zipline hub eval --skip-compile compiled/group_bys/<team>/<name>
zipline hub eval --skip-compile compiled/joins/<team>/<name>
```

If a command's syntax differs in the installed CLI, read that subcommand's `--help` once and use the documented form. Do not repeatedly retry guessed flag combinations.

The GCP team's eval URL is normally configured by `teams.py` as `http://localhost:3904`; omit `--eval-url` when configuration or `ZIPLINE_EVAL_URL` supplies it. A connection or authorization failure leaves data validation incomplete; it does not rule out config defects established by repository inspection. Report both independently and ask the user to start/configure the service only when necessary.

For BigQuery identifiers containing dashes, preserve backticks, for example `demo.\`user-activities\``.

### GroupBy backfills

For historical batch output, use `zipline hub backfill` with the actual compiled
GroupBy path and explicit date bounds. `run-adhoc` submits a one-off online deploy;
`schedule` creates a recurring schedule. Neither substitutes for a bounded backfill.

```bash
# From python/test/canary, after compiling and evaluating the intended config:
zipline hub backfill compiled/group_bys/<team>/<name> --start-ds 2025-01-01 --end-ds 2025-01-07
```

Verify the target config/version, output table, source partitions, and full window
lookback before submitting. For chains, inspect the upstream Join and its sources;
the backfill must compute the needed upstream date ranges before the child.
Reject reversed date bounds and flag dates whose source data has not landed.
Use a distinct config version/output table for a development experiment alongside
production. If the user asks only for a plan, provide commands without submitting
jobs. Distinguish a submitted workflow from a completed, validated backfill.

## Resolve source tables before authoring

1. If the user's table name is not known to be exact—because it is misspelled, vague, descriptive, or absent—run `list-tables` first. Do not try `eval-table` against guessed spellings.
2. Present close candidates and wait for the user to confirm the exact table. Never silently substitute even an obvious-looking match.
3. Once confirmed, run `eval-table` using the source engine and inspect columns and types.
4. If discovery is unavailable, ask for the exact table name and pasted schema rather than inventing them.

Even when a raw table already appears behind a staging export, validate its metadata when authoring against it.

### External warehouses

Spark-backed sources cannot directly consume BigQuery, Snowflake, or Redshift tables. Search `staging_queries/<team>/` for an existing export. Reuse it when present. Otherwise create a `StagingQuery` using the warehouse's `EngineType`, date-template bounds, output namespace, and `TableDependency`, then reference the exported `.table` from the source. Match a nearby export rather than inventing provider-specific syntax.

## Authoring workflow

### GroupBy

1. Understand the intended use, team, online/batch requirement, and target source.
2. Search for existing GroupBys with the same source and keys.
3. Resolve and inspect the source table as described above.
4. Choose the source:
   - `EventSource` for time-series events. Use `table` only for batch, or `table` plus `topic` for streaming.
   - `EntitySource` for profiles and dimensions. Use `snapshot_table`; add mutation table/topic only when real-time entity mutations are actually required.
   - `JoinSource` when aggregating an already enriched Join.
5. Verify keys, selected input columns, timestamp units, filters, windows, accuracy, and online behavior, including on reused sources. Before editing, state the timestamp type, units, and supporting source-specific evidence. Resolve conflicts between nearby examples and verified schemas before copying expressions.
6. Make the smallest versioned change, then compile and evaluate it.

Set `online=False` for offline-only experiments, even when a nearby example uses
`online=True`. For temporal/windowed sources, determine timestamp units from the
schema and sample values before choosing an expression. If live metadata is
unavailable, inspect `test_data/`, source-generating code, and fixture parsers for
the same physical table or export. Check how sample timestamps are constructed
(including custom YAML tags), rather than inferring units from a column name or
another provider's config. Source-specific samples and schemas take precedence
over conflicting comments in unrelated examples. An authentication failure does
not invalidate this local evidence. If the evidence still conflicts, explicitly
report the unresolved clock contract instead of silently copying the old
expression and calling the new feature complete.

In a Spark source query:

```python
# Before: epoch seconds interpreted as milliseconds.
time_column = "event_time_seconds"
# After: epoch milliseconds as a long.
time_column = "CAST(event_time_seconds AS BIGINT) * 1000"

# Parse timestamp strings using the source format and timezone.
time_column = "unix_timestamp(event_time_string) * 1000"
# Already-millisecond longs need no conversion.
time_column = "event_time_ms"
```

When a source defect is established by a schema, fixture, or other repository
evidence, correct it in the new version. Preserve the published version by giving
the new config its own source/query; do not copy a known defect merely to match
the old config. For an already-millisecond long, for example:

```python
# Before: TIMESTAMP(number) interprets the number as seconds in Spark.
time_column = "unix_millis(TIMESTAMP(event_time_ms))"
# After: preserve milliseconds in the new source.
time_column = "event_time_ms"
```

Apply the same correction to selected structs or other expressions embedding
that timestamp. Report the semantic correction and why the old published
version remains unchanged. Compilation alone cannot establish correct units or
window semantics; blocked live validation does not prevent fixing a defect
already verified from code and schemas.

For temporal entity lookups, keep the snapshot and historical mutation table;
add the confirmed mutation topic when live updates are needed. Check the Query's
`mutation_time_column` and `reversal_column` against the real schema: mutation
times must be epoch milliseconds, and the reversal flag identifies before-images.
Event time and mutation time are distinct. A snapshot-only passthrough does not
need a fabricated topic or time column.

Compact event pattern:

```python
from ai.chronon.types import Accuracy, Aggregation, EventSource, GroupBy, Operation, Query, selects

v1 = GroupBy(
    sources=[EventSource(
        table="data.events",
        topic=None,
        query=Query(
            selects=selects("user_id", "item_id", amount="price * quantity"),
            time_column="event_time_ms",
        ),
    )],
    keys=["user_id"],
    aggregations=[Aggregation(
        input_column="item_id",
        operation=Operation.APPROX_UNIQUE_COUNT_LGK(k=8),
        windows=["7d"],
    )],
    online=True,
    accuracy=Accuracy.TEMPORAL,
    version=1,
)
```

Compact entity passthrough pattern:

```python
from ai.chronon.types import EntitySource, GroupBy, Query, selects

v1 = GroupBy(
    sources=[EntitySource(
        snapshot_table="data.users",
        query=Query(selects=selects("user_id", "country")),
    )],
    keys=["user_id"],
    aggregations=None,
    online=True,
    version=1,
)
```

Use query-level `wheres` to remove whole rows for all features. Use named `IF(...)` expressions in `selects` when filtering or transforming only one feature. Both may be combined.

For example, keep all event counts while averaging purchase amounts only:

```python
# Before: the purchase filter also removes non-purchases from the event count.
query = Query(
    selects=selects("user_id", "event_id", "price"),
    wheres=["event_type = 'purchase'"],
    time_column="ts",
)

# After: only purchase_amount is conditional; the row filter applies to both features.
query = Query(
    selects=selects("user_id", "event_id", purchase_amount="IF(event_type = 'purchase', price, NULL)"),
    wheres=["user_id IS NOT NULL"],
    time_column="ts",
)
aggregations = [
    Aggregation(input_column="event_id", operation=Operation.COUNT, windows=["7d"]),
    Aggregation(input_column="purchase_amount", operation=Operation.AVERAGE, windows=["7d"]),
]
```

Use `NULL` for excluded values in an average; zero would change its denominator.
For conditional counts, sum a selected `IF(condition, 1, 0)` flag.

### Join

- Confirm left-side columns and types match every right-side GroupBy key. Use `key_mapping` when names differ and `prefix` to avoid collisions.
- Check every `row_ids` entry against the left query's projected names, including when reusing an existing Join. In `selects(row_id="event_id")`, the projected name is `row_id`, not `event_id`; either use `row_ids=["row_id"]` or explicitly select `event_id` under that name. A raw-table column's existence does not establish that it survives the projection.
- Joins pass through left and right columns by default. Omit `derivations` if no transformations are needed.
- Once any derivation is present, add `Derivation("*", "*")` only if the original columns must remain alongside derived columns.
- For production configs, create the next variable version.
- For offline-only experiments, set `online=False` instead of copying an online example's setting.
- Compile and evaluate the compiled Join immediately.

```python
from ai.chronon.types import Derivation, Join, JoinPart

v2 = Join(
    left=left_source,  # must select the row-id and join keys
    right_parts=[
        JoinPart(group_by=user_features),
        JoinPart(group_by=item_features, key_mapping={"listing_id": "id"}),
    ],
    row_ids=["event_id"],
    derivations=[
        Derivation("*", "*"),
        Derivation("ratio", "amount / NULLIF(user_id_average_7d, 0)"),
    ],
    online=True,
    version=2,
)
```

### StagingQuery

Use a StagingQuery for warehouse exports, multi-table ETL, complex preprocessing, or reusable Join left sides. Declare every source in `dependencies`, use the repository's date templates (`{{ start_date }}`, `{{ end_date }}`, `{{ latest_date }}`, and `{{ max_date(table=...) }}` where supported), set the correct `EngineType`, output namespace, step size, and current required `version`. Inspect nearby provider-specific examples before writing SQL.

Check every physical column referenced by the SQL against the supplied schema or
source-specific repository fixtures, including projected values as well as join
keys and partitions. A business description such as "the listing price" does not
establish its physical column name or units. Preserve the verified input name;
use an explicit output alias when helpful. This check also applies to read-only
sketches, where execution cannot catch an invented column name.

Validate in dependency order: compile the StagingQuery and evaluate its exact
`compiled/staging_queries/<team>/<name>` target before authoring and validating
the downstream GroupBy or Join. End a read-only review of this pipeline with a
brief validation handoff, for example: "When implementation is authorized, compile
and evaluate the export's exact compiled target first; then author, compile, and
evaluate the downstream GroupBy or Join." Include this sequence even though no
commands are run during the review. Identify any SQL/template assumptions still
requiring validation and do not present an unexecuted sketch as validated.

### Chained GroupBy

Wrap a parent Join in `JoinSource`, select the enriched output columns, and give the child query a valid time column. Right-side Join columns are normally prefixed by key. Any aggregation over an unbounded/streaming parent must have a window. Consider a StagingQuery instead when it makes the dependency or cost clearer.

```python
# Before: price_cents is a listing attribute, absent from the raw activity stream.
# After: select the parent's actual enriched column, then aggregate its alias.
source = JoinSource(
    join=parent_join,
    query=Query(
        selects=selects("user_id", price_cents="listing_id_price_cents"),
        time_column="ts",
    ),
)
aggregations = [
    Aggregation(input_column="price_cents", operation=Operation.LAST_K(100), windows=["7d"]),
]
```

Before editing the child, inspect the parent left query's projected names and
compare them to its `row_ids` and join keys, then inspect its actual enriched
output names and timestamp. An existing parent or successful child compilation
does not establish that this contract is valid. Online children also require an
online-capable parent and upstream dependencies.

If a reused parent has a defect, explicitly report the mismatch and its impact on
the child, even when the parent is outside the requested edit scope. If correcting
the parent is necessary within scope, preserve its published version, create a
corrected version, and compile/evaluate it before editing the child. Validate the
child against the intended parent and distinguish any remaining dependency defect
from an external eval-service blocker in the final response.

Shared transformations can live in the parent's JoinSource; child-specific
expressions belong in the child's query.

### Models and ModelTransforms

- A `Model` defines backend, request mapping, response schema, and output mapping. Confirm the deployed model's actual request and response schemas instead of guessing them.
- `ModelTransforms` consumes `JoinSource`, not a bare GroupBy. Wrap a GroupBy in a Join when necessary.
- `input_mapping["instance"]` produces the raw inference instance. Backend params other than `model_name` and `model_type` become request parameters.
- `value_fields` must match the response object, and `output_mapping` must navigate the parsed field name generated by the config.
- Use `ModelBackend.VERTEXAI` for Vertex AI and `ModelBackend.SAGEMAKER` for SageMaker/Bedrock patterns. Inspect existing provider examples and public API signatures for exact fields.
- Custom training additionally needs training data, window, image/module, resources, artifact location, deployment config, and rollout strategy. Do not fabricate infrastructure identifiers.

## Debugging data correctness

For null or incorrect Join features, inspect the relevant Join, GroupBy, and staging export before forming a hypothesis. Separate facts verified from code/data from checks the user still needs to run.

Check in this order:

1. **Timestamp expression:** Confirm both sides produce epoch milliseconds and reasonable dates. A 10-digit value is usually seconds; a 13-digit value is usually milliseconds. Applying a timestamp conversion to an already-millisecond integer can move events far into the future.
2. **Key compatibility:** Compare types, formatting, casing, nullability, and actual overlap between left and right keys.
3. **Source filters and exports:** Apply the GroupBy query's `selects`, `wheres`, and staging logic to a small sample.
4. **Window coverage:** Query the right side over the full lookback interval and check late or missing partitions.
5. **Duplicates and aggregation semantics:** Recompute the expected feature manually for one key and timestamp.

Use the eval service or warehouse access when available. Otherwise provide exact SQL/checks and label conclusions as verified findings or hypotheses.

Useful diagnostic shape:

```sql
SELECT
  key,
  <time_expression> AS ts_ms,
  FROM_UNIXTIME(<time_expression> / 1000) AS readable_ts
FROM <table>
WHERE ds BETWEEN '<start>' AND '<end>'
LIMIT 20;
```

## Tecton migration

Translate concepts, then validate each resulting Chronon component before continuing:

| Tecton | Chronon |
|---|---|
| Entity / join keys | GroupBy `keys` |
| Batch dimension source | `EntitySource` |
| Batch event source | `EventSource(table=..., topic=None)` |
| Stream source | `EventSource` with table and topic |
| Batch/stream feature view | `GroupBy` |
| Realtime/on-demand feature | Join `Derivation` using SQL |
| Feature service | `Join` with `JoinPart`s |
| Feature aggregation | `Aggregation` plus `Operation` |
| `timestamp_field` | millisecond `time_column` |
| `timedelta` window | Chronon window string/object |

Common operation mappings are `sum` to `SUM`, `count` to `COUNT`, `mean` to `AVERAGE`, `min`/`max` to `MIN`/`MAX`, `first`/`last` to `FIRST`/`LAST`, count-distinct to `APPROX_UNIQUE_COUNT_LGK`, and secondary-key aggregation to `buckets`.

Move row-level Python transformations into named SQL expressions in `selects` or Join `Derivation`s. For complex shared transformations, use a StagingQuery. Convert sources first, then leaf GroupBys, derived features, and finally multi-source Joins; compile and evaluate after each config.

## Running trials and experiments

A trial records one attempt to answer a modelling question; its experiments are
individual variants. When a comparison is the deliverable (feature ablation,
window comparison, hyperparameter tuning, or an iterative metric target), check
the journal and open a trial before training, without waiting for the user to
ask for tracking. Use one trial per question. A single smoke-training run or a
standalone feature backfill does not need a trial. For a plan or read-only review,
describe this workflow without creating records, copying archives, or starting jobs.

### Metrics and variant identity

Never submit a metric value yourself. `experiments metrics` takes the variant's
`--model-conf-name`; Hub locates its trainer report and reads the measurements.
Wait for the run and report to finish. Missing reports, log snippets, or estimates
cannot establish a winner. Summaries may quote retrieved measurements, but must
identify incomplete or failed variants instead of marking the comparison successful.
Check the trainer's actual report schema before requiring fields. Data fingerprints
or snapshot IDs can be recorded separately for reproducibility; do not claim they
are emitted in a trainer report unless the implementation actually writes them.

Each variant needs its own model name and compiled config, not merely a distinct
experiment ID. Trainer archive versions are content hashes: configuration-only
changes can share a version, but must not share the model name. Outputs live at
`{base}/models/{model_name}/{version}`; reusing that location overwrites reports.
If Hub rejects a duplicate report URI, correct the model identity, train that
variant again, and then retrieve its metrics; do not relabel the shared report.

A new model name also needs an archive at
`{base}/builds/{model_name}-{version}.zip`. If trainer bytes are unchanged, copy
the existing archive to the new name, preserving the content version. If trainer
code changes, rebuild and publish its archive and use the resulting version.
If a variant changes both trainer code and configuration, say explicitly that
its result cannot be attributed to the configuration change alone. Keep trainer
bytes fixed for a configuration-only comparison, or test the code change separately.
Verify actual names/paths from the configs and build conventions before copying.
A missing archive can fail Ray setup before any training starts.

```bash
# Same trainer bytes, distinct model names; substitute verified storage paths.
aws s3 cp s3://<bucket>/builds/<existing_model>-<version>.zip \
          s3://<bucket>/builds/<variant_model>-<version>.zip
```

### Trial workflow

From the Chronon root, use `zipline hub trial`. Trial verbs resolve **default-team**
Hub settings, whereas training resolves the target config's team/environment. Do
not assume these match. Inspect the selected compiled metadata, then explicitly
carry its `--hub-url`, `--cloud`, `--customer-id`, and `--env` to trial commands.
Match `compiled/` with `prod` and `compiled_canary/` with `canary`; directory labels
do not necessarily match the deployment's hostname. If the intended environment
is unresolved, make that choice explicit before proposing submissions. Use
`--format json` when parsing output, and keep all trial records, metrics retrieval,
and training in the same resolved Hub/customer/environment.

1. Start the operational plan with `trial journal list --run-name <question-name>`
   before config changes, archive publication, or training. This search crosses
   scopes; do not unnecessarily restrict it to the current team. Inspect matching
   attempts with `trial list`/`show` and decide whether to resume before starting anew.
2. Start with `trial start <run-name> --task "<question>" --model <existing-conf>`.
   Bind the trial to a real model under study, not an invented family name. Scope
   defaults to the repository root. Starting with the same run name creates a
   **new attempt**, not a resume; use `trial list`/`show` and an existing trial ID
   to continue an attempt instead.
3. Set `ZIPLINE_TRIAL_ID` from the returned ID for subsequent commands. Shell
   exports do not carry across isolated tool calls: propagate the value explicitly
   or use the optional trial-ID argument for trial verbs.
4. Give each variant its own model name and published trainer archive, then
   compile/evaluate its config. Record the experiment as `RUNNING` **before**
   training, so failed runs leave a record. Supply that experiment's
   `ZIPLINE_EXPERIMENT_ID` and the trial ID to its training process.
5. After each run finishes, read metrics for its exact model. Inspect the experiment
   list, compare measured outcomes, write the trial summary and appropriate status,
   and append a journal retrospective with what changed and what remains untested.

In an operational plan, choose concrete proposed variable and backend model names
for each variant, label them as new, and list their distinct report paths. Map
each metadata name `<team>.<module>.<variable>__<version>` to
`compiled/models/<team>/<module>.<variable>__<version>` consistently, retaining the
module prefix. Read the actual compiler output before executing a new target.

Example for one prepared variant; repeat record/train/metrics for each variant.
Verify its compiled target and publish its archive before submitting it. Replace
`<returned-id>` in every command so separate shell calls retain attribution.

```bash
# In each shell, set these from the selected compiled metadata, not default-team guesses.
trial_cmd() {
    zipline hub trial "$@" --hub-url "<verified-hub-url>" --cloud "<verified-cloud>" \
        --customer-id "<verified-customer>" --env "<selected-compile-env>"
}
trial_cmd journal list --run-name ctr-windows --format json
trial_cmd list --run-name ctr-windows --format json
# Resume a matching attempt by ID, or start a new one if needed:
trial_cmd start ctr-windows --task "Does 30d beat 7d for CTR?" \
    --model aws_databricks.ctr_ray.ctr_ray_model__afce3c0e00af --format json
trial_cmd experiments record exp-7d <returned-id> --title "7 day window" --status RUNNING \
    --model-conf-name aws_databricks.ctr_ray.ctr_model_7d__afce3c0e00af
# This path is for prod compile output; use compiled_canary/ when selecting canary.
ZIPLINE_TRIAL_ID=<returned-id> ZIPLINE_EXPERIMENT_ID=exp-7d zipline hub backfill \
    compiled/models/aws_databricks/ctr_ray.ctr_model_7d__afce3c0e00af \
    --start-ds 2026-08-24 --end-ds 2026-08-24
# After the training run and its report are complete:
trial_cmd experiments metrics exp-7d <returned-id> \
    --model-conf-name aws_databricks.ctr_ray.ctr_model_7d__afce3c0e00af --format json
trial_cmd experiments record exp-7d <returned-id> --status SUCCEEDED
trial_cmd experiments list <returned-id> --format json
# Only after all required comparisons are supported by retrieved reports:
trial_cmd summary "<conclusion grounded in retrieved reports>" <returned-id>
trial_cmd status SUCCEEDED <returned-id>
trial_cmd journal append "<retrospective and remaining uncertainties>" <returned-id>
```

`experiments record` merges fields: omitted title/features/model fields remain
unchanged during a status update. The trial uses separate `status`, `summary`,
`plan`, and `diff` verbs; use the relevant setter instead of inventing a catch-all
`report` command or clearing unrelated fields. Preserve failed attempts and report
external blockers honestly; a submitted workflow is not a measured result.
End operational plans with explicit experiment-list, summary, status, and journal
commands for the same trial ID. Gate success updates on completed report-backed
comparisons; use failure/pending states when appropriate, never placeholder scores.

## Verification checklist

Before reporting completion, verify:

- For comparisons, each proposed or executed training command carries both attribution IDs, variants have distinct model/report locations, measurements come from trainer reports, and the trial summary/journal accurately describe completed and incomplete work.
- The selected team, source, keys, and config version are correct.
- External sources use an export when required.
- All public imports come from `ai.chronon.*`.
- Selected fields include every key and aggregation input.
- Time expressions, including timestamps embedded in structs, return epoch milliseconds; known defects were corrected in new sources without mutating published sources.
- Streaming/unbounded aggregations have windows.
- Join keys, mappings, row IDs, prefixes, and derivations are valid, including in reused parents; unresolved dependency defects are named in the final response.
- The command ran from the Chronon root.
- Compile passed.
- Eval passed for the exact compiled config, or the external blocker is stated precisely.
- For a read-only StagingQuery/downstream review, the final answer includes the future export-first compile/eval sequence and says that it has not been run.
- Compare the final response's file list with `git status --short` and the diff. Name each changed source and generated compiled artifact, including prod and canary paths; "generated both artifacts" alone is not a file list. Report validation truthfully.
