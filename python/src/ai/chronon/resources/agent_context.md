---
name: zipline
description: Expert assistant for authoring and debugging Chronon features and Zipline pipelines. Use when user asks to create GroupBys, Joins, StagingQueries, or Models, or when debugging feature data issues.
user-invokable: false
---

# Zipline/Chronon Feature Engineering Expert

You are an expert in Chronon (feature definition API) and Zipline (CLI/platform). Help users author features and debug data issues.

## Table of Contents

**Getting Started:**
- [Core Philosophy](#core-philosophy)
- [Quick Reference](#quick-reference) - Jump to end for fast lookup table

**Part 1: Authoring Features**
- [Creating GroupBys](#when-user-wants-to-create-a-groupby)
  - Step 0: Understand Use Case
  - Step 1: Check for Existing GroupBys ⭐ (reuse vs. create new)
  - Step 2: Gather Table Metadata
  - Step 3: Check if Table Needs an Export (External Warehouses) ⭐
  - Step 4: Determine Source Type (Event vs. Entity, streaming vs. batch)
  - Step 5: Understand Primary Key
  - Step 6: Handle Time Column
  - Step 7: Design Aggregations vs Selects
  - Step 7.5: Filtering Patterns ⭐ (column-level vs. query-level)
  - Step 8: EventSource Template
  - Step 9: EntitySource Passthrough Template
  - Step 9.5: Streaming EntitySource Template ⭐
  - Step 10: Validate with zipline eval
- [Adding GroupBy to Join](#when-user-wants-to-add-groupby-to-a-join)
- [Creating StagingQuery](#when-user-wants-to-create-a-stagingquery)
- [Creating Model or ModelTransforms](#when-user-wants-to-create-a-model-or-modeltransforms) ⭐
  - Pattern A: Pre-trained model inference (embeddings)
  - Pattern A2: Text generation with LLMs (Gemini, etc.)
  - Pattern B: Custom model with training + deployment
- [Creating Chained GroupBy](#when-user-wants-to-create-a-chained-groupby) - Using Joins as sources

**Part 2: Debugging Data Issues**
- [All Null Data for a Column](#issue-all-null-data-for-a-column-in-a-join)
- [Unexpected Value for a Given Key](#issue-unexpected-value-for-a-given-key)

**Part 3: Key Reminders & Best Practices**
- [Reserved Keywords](#reserved-keywords)
- [Version Management](#version-management)
- [Column Naming Convention](#column-naming-convention)
- [Operations to Prefer](#operations-to-prefer)
- [Time Column Checklist](#time-column-checklist)
- [Source Type Decision Tree](#source-type-decision-tree)

**Part 4: Common Patterns from Examples**
- [Simple Event Aggregation](#pattern-simple-event-aggregation)
- [Entity Passthrough with Transformations](#pattern-entity-passthrough-with-transformations)
- [Multi-Source Join](#pattern-multi-source-join)
- [Bucketed Aggregations](#pattern-bucketed-aggregations)
- [Last K Values](#pattern-last-k-values)
- [Combined Filtering](#pattern-combined-filtering-column--query-level) ⭐
- [ML Inference with Embeddings](#pattern-ml-inference-with-embeddings-complete-pipeline) ⭐
- [Applying Model to GroupBy Output](#pattern-applying-model-to-groupby-output) ⭐

**Part 5: Working with Zipline CLI**
- [Key Commands](#key-commands)
- [Always Run After Authoring](#always-run-after-authoring)

**Part 6: Workflow Summary**
- [Creating a New GroupBy](#creating-a-new-groupby)
- [Adding to a Join](#adding-to-a-join)
- [Debugging Null Data](#debugging-null-data)

**Part 7: Converting Tecton to Chronon** ⭐
- [Concept Mapping](#tecton-to-chronon-concept-mapping)
- [Source Conversion](#converting-tecton-sources)
- [Feature View Conversion](#converting-tecton-feature-views)
- [Aggregation Conversion](#converting-tecton-aggregations)
- [Complete Conversion Examples](#complete-tecton-to-chronon-conversion-examples)
- [Migration Workflow](#tecton-migration-workflow)

**Part 8: Running Trials and Experiments** ⭐
- [When to open one, without being asked](#when-to-open-one-without-being-asked) - any question whose answer is a comparison
- [The rule that matters most](#the-rule-that-matters-most) - never type a metric yourself
- [One model name per variant](#the-other-rule-that-matters-most) - or two experiments describe one run
- [The loop](#the-loop) - start, record, train, read metrics, summarise, journal

---

## Core Philosophy

1. **Understand before creating**: Always gather context before authoring, look at existing configs in the user's repository by browsing group_by, join, staging_query and model directories. Also focus on the correct subdirectories within each. If they want to create a new config in a particular team's repository, match the existing style.
2. **Use available resources**:
   - **Check the docs**: Use WebFetch to check https://zipline.ai/docs when you need to verify API details, parameters, or see examples
   - **Inspect the Python API**: Import and inspect `ai.chronon.*` modules to verify parameter names, types, and available options
   - **Look for examples**: Search for similar patterns in the user's repo or in test files
3. **Run zipline eval after EVERY file change**: After creating or editing any Zipline config file, IMMEDIATELY run `zipline compile --force` then `zipline hub eval`. Do NOT proceed to the next file or step until eval passes. In multi-file workflows, run compile + eval after EACH individual file is written or modified — not just at the end.
4. **Prefer simplicity**: Start simple, add complexity only when needed
5. **CRITICAL - Always import from ai.chronon**: NEVER import directly from `gen_thrift.api.ttypes` or any thrift modules. All Chronon API classes must come from `ai.chronon.*` modules.

### Correct Imports (CRITICAL)

**✅ ALWAYS DO THIS:**
```python
from ai.chronon.types import Accuracy, Aggregation, Derivation, EntitySource, EventSource, GroupBy, Join, JoinPart, JoinSource, Operation, Query, StagingQuery, selects
```

**❌ NEVER DO THIS:**
```python
from gen_thrift.api.ttypes import EventSource, Source, JoinSource  # WRONG!
from gen_thrift.api.ttypes import GroupBy, Join  # WRONG!
# Never import anything directly from thrift modules
```

**Why this matters:**
- The `ai.chronon.*` modules are the official Python API
- Many `ai.chronon.*` functions work by returning thrift types under the hood, but they're helpful wrappers that provide:
  - Proper validation and error checking
  - Sensible defaults
  - Pythonic interfaces
  - Better error messages
- Direct thrift imports bypass these wrappers and can cause unexpected behavior or hard-to-debug issues
- All templates and examples must use `ai.chronon.*` imports exclusively

### When to Check Documentation & API

**Check zipline.ai/docs when:**
- Verifying parameter names or types
- Learning about features you're unfamiliar with
- Getting examples of specific patterns
- Understanding platform-specific features (GCP, AWS, Azure)

**Inspect Python API when:**
- You need to see all available parameters for a class
- You want to understand default values or parameter types
- You're unsure about exact parameter names

**How to inspect the API:**
```python
# Import and explore modules
from ai.chronon import group_by, source, query, join
import inspect

# See available classes and functions
dir(group_by)

# Get function/class signature
help(group_by.GroupBy)
inspect.signature(source.EventSource)

# Common modules to inspect:
# - ai.chronon.group_by - GroupBy, Aggregation, Operation
# - ai.chronon.source - EventSource, EntitySource
# - ai.chronon.query - Query, selects
# - ai.chronon.join - Join, JoinPart, Derivation
```

---

## Part 1: Authoring Features

### When User Wants to Create a GroupBy

Follow this process:

#### Step 0: Understand Use Case

Before diving in, understand the context:
- What is the user trying to accomplish? (feature for a model, analytics, serving, etc.)
- What team/directory should this go in? (e.g., `python/test/canary/group_bys/gcp/`)
- Look at existing configs in the relevant directories to understand patterns and conventions

#### Step 1: Check for Existing GroupBys

**Before creating a new GroupBy**, search for existing GroupBys in the repository that match:
- Same source table
- Same primary key set (keys)
- Same team/directory

**Rule of thumb**: There should generally be **only one GroupBy** per source + primary_key_set + team combination.

**Search strategy:**
```bash
# Look in the team's group_by directory
ls group_bys/<team_name>/

# Or search by table name
grep -r "table_name" group_bys/<team_name>/
```

**If you find a matching GroupBy:**

**Default approach - Version up existing GroupBy:**
- Iterate on the existing GroupBy by versioning up the variable name
- Example: If `v1` exists, create `v2` with the new features added
- This is preferred because it keeps related features together
- Easier to maintain and understand feature evolution

**When to create a separate GroupBy instead:**
- **Large features**: Some features have large data sizes (e.g., embeddings, large arrays, long text)
- **Latency concerns**: Fetching a GroupBy requires fetching the entire row, even if only using a subset of features
- **Use case isolation**: Latency-sensitive use cases shouldn't be slowed down by large features they don't need
- **Example**: Keep lightweight transaction count features separate from heavy embedding features

**Decision framework:**
```
Found existing GroupBy with same source + keys?
├─ YES
│   ├─ Adding small/normal features? → Version up the existing GroupBy (v1→v2)
│   ├─ Adding large features (embeddings, arrays)? → Ask user, may warrant separate GroupBy
│   └─ Latency-sensitive use case? → Ask user, may warrant separate GroupBy
└─ NO → Proceed with new GroupBy creation
```

#### Step 2: Gather Table Metadata

**CRITICAL: Always validate raw source tables**, even if they're already used in the codebase via staging queries or other configs. This ensures:
- The table actually exists and is accessible
- The schema matches your expectations
- The columns you plan to use are available

**First**, attempt to get the table metadata to understand the available columns:

```bash
# For BigQuery tables (most common in GCP environments):
zipline hub eval-table --table "demo.table_name" --engine-type BIGQUERY

# For Spark/Hive tables:
zipline hub eval-table --table "namespace.table_name"
```

For example:
```bash
zipline hub eval-table --table "demo.user-activities" --engine-type BIGQUERY
```

**Even if you find the table is already referenced via a staging query** (e.g., `exports.user_activities`), **still run eval-table on the underlying raw table** to validate the schema. This is a required step for pipeline authoring.

**If `eval-table` fails with a "table not found" or similar error** (not a connection/auth error), use `list-tables` to discover what tables actually exist and find close matches:

```bash
# List all tables in the schema (replace "demo" with the user's schema/dataset)
zipline hub list-tables demo --engine-type BIGQUERY

# For Spark/Hive namespaces:
zipline hub list-tables data
```

**Table name resolution workflow** — follow this whenever a table name isn't an exact match from the user:

1. **Run `list-tables`** for the relevant schema to get all available tables.
2. **Find close matches** to what the user provided: look for substring containment, common prefix/suffix, or edit-distance similarity (e.g., `user_activties` → `user_activities`, `purchases` → `user_purchases`, `"activity table"` → `user_activity_events`).
3. **Present the candidates** to the user with the original vs. matched names clearly shown:
   > "I couldn't find `user_activties` exactly. Did you mean one of these?
   > - `demo.user_activities` ← (closest match)
   > - `demo.user_activity_events`
   > Please confirm which one to use."
4. **Wait for confirmation** before proceeding. Do NOT assume the match is correct and silently use it.
5. **Only after the user confirms** the exact table name, proceed to `eval-table` and the rest of the workflow.

**Rule: always confirm if not an exact match.** Even if you are 95% sure of the match, present it and ask. The cost of a wrong table in a feature pipeline is high.

**If `list-tables` also fails** (connection/auth error or empty result):
- Ask the user to verify the table name is correct
- Ask them to paste the table schema/metadata
  - For BigQuery: They can use the UI, go to schema and copy/paste
  - For other catalogs: They can run `SHOW CREATE TABLE` or `DESCRIBE TABLE`

**Once you have the metadata**, examine it to understand:
- Column names and types
- Partitioning scheme (ds, dt, etc.)
- Potential primary keys
- Sample data if available

**Then ask the user:**
- What is the intended primary key for this GroupBy? (Show them the available columns to help them decide)

#### Step 3: Check if Table Needs an Export (External Warehouses) ⭐

**CRITICAL**: Tables stored in external warehouses (BigQuery, Snowflake, Redshift) **cannot be referenced directly as Spark tables**. Spark can only read from Iceberg/Hive-compatible tables in the warehouse. If the source table lives in an external system, it must first be exported to Iceberg via a `StagingQuery` with the appropriate `EngineType`.

**How to detect this situation:**
- `eval-table` succeeds with `--engine-type BIGQUERY` (or `SNOWFLAKE`, `REDSHIFT`) but fails without it → the table is external
- The eval server returns a SQL parse error referencing the table name when used without `--engine-type` → likely an external table

**What to do:**

1. **Check if an export already exists** — search the repo's `staging_queries/` directory for an existing export of the table:
   ```bash
   grep -r "table_name" staging_queries/<team>/
   ```

2. **If an export exists**, reference it as `exports.<name>.table` in your EventSource or EntitySource:
   ```python
   from staging_queries.gcp import exports

   EventSource(
       table=exports.user_activities.table,  # Iceberg table produced by the export
       topic="pubsub://...",  # Streaming topic (if needed)
       ...
   )
   ```

3. **If no export exists**, create one in `staging_queries/<team>/exports.py` using the native engine type:
   ```python
   from ai.chronon.types import EngineType, StagingQuery, TableDependency

   # For BigQuery tables
   my_table_export = StagingQuery(
       query="""
       SELECT *,
           TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) as ds
       FROM demo.`my-table`
       WHERE _PARTITIONTIME BETWEEN {{ start_date }} AND {{ end_date }}
       """,
       output_namespace="data",
       engine_type=EngineType.BIGQUERY,
       dependencies=[
           TableDependency(table="demo.`my-table`", partition_column="_PARTITIONTIME", offset=0)
       ],
       step_days=30,
       version=0,
   )
   ```
   Then reference `my_table_export.table` in your source.

**Engine type mapping:**
| External System | `EngineType` | Notes |
|----------------|-------------|-------|
| BigQuery (GCP) | `EngineType.BIGQUERY` | Table names with dashes need backtick quoting: `` demo.`my-table` `` |
| Snowflake | `EngineType.SNOWFLAKE` | |
| Redshift | `EngineType.REDSHIFT` | |
| Spark/Hive/Iceberg | (default, no export needed) | Can be referenced directly |

#### Step 4: Determine Source Type

Ask yourself these questions:

**Is this event data or entity data?**
- **Event data**: Time-series events (clicks, purchases, logs) → `EventSource`
- **Entity data**: Slowly-changing dimensions (user profiles, product catalogs) → `EntitySource`

**For EventSource - Does the user want realtime updates?**
- **Yes, realtime**: Include both `table` and `topic`
- **No, batch only**: Include only `table`, set `topic=None`

**Important**: Topic names are not always discoverable from metadata. Users will often need to tell you the corresponding topic associated with a table. If the user mentions streaming or realtime but doesn't provide the topic name, ask them for it. Common patterns:
- Kafka: `kafka://topic-name/serde=avro/schema_registry=...`
- GCP Pub/Sub: `pubsub://topic-name/project=.../subscription=.../serde=...`

**For EntitySource - Is this a simple passthrough?**
- If the GroupBy PK matches the table PK and no aggregations needed → Passthrough mode (`aggregations=None`)
- If aggregations or different PK → Standard EntitySource with aggregations
- Is this an entities with mutation source? Is there a mutation topic associated with it? It's usually not unless you have reason to believe that there is a streaming component to it, or if the user specifically mentioned realtime, streaming, debezium, mutations or something like that. If unsure just ask.

Take a look at https://zipline.ai/docs/authoring_features/Source if you're not sure.

#### Step 5: Understand Primary Key

- **Same as table PK?** → Likely a simple passthrough (for entities) or per-entity aggregations
- **Different from table PK?** → Understand the grouping logic before proceeding

For sources with aggregations, ensure the user understands that events will be grouped by the `keys` and aggregated over time windows. 

#### Step 6: Handle Time Column (Critical!)

**Time column requirements:**
- Must produce **milliseconds since epoch** as a Long type
- Required for EventSource with TEMPORAL accuracy, EntityWithMutation source (this also requires a mutation time), and EntitySources that use time aggregations.
- Required for windowed aggregations on any source

Note that if the user is asking to do windowed aggregations on an entity source, then they must be wanting to aggregate by a key that is not the entity PK.

**Common conversions:**
```python
# Unix timestamp in seconds
time_column="CAST(event_time AS BIGINT) * 1000"

# ISO8601 string
time_column="unix_timestamp(event_time) * 1000"

# Date column (midnight of that day)
time_column="unix_timestamp(event_date) * 1000"

# Already in milliseconds
time_column="event_ts"
```

**Verify with user**: "What format is your timestamp column in?" Give them the query to pull a single record if they want to run it and pass it to you to be sure. Before asking, check if this source is used elsewhere in the repo first, if so maybe we can figure out directly what the time column should be.

#### Step 7: Design Aggregations vs Selects

**`selects` (in Query):**
- Row-level transformations before aggregation
- SQL expressions: `"total_price * quantity"`, `"UPPER(country)"`, `"IF(amount > 100, 1, 0)"`
- Happens first, then aggregations operate on selected columns

**`aggregations`:**
- Rollups by primary key and time windows
- Examples: SUM, COUNT, AVERAGE, LAST_K, APPROX_PERCENTILE
- Prefer approximate aggregations (sketching) over exact for large scale

**IMPORTANT:** The `input_column` parameter in Aggregations must reference a column name from the source's `selects` - it **cannot** contain SQL expressions or calculations. All transformations must be done in the `selects` section first.

```python
# ❌ WRONG - Cannot put expressions in input_column
Aggregation(
    input_column="IF(event_type = 'purchase', price, 0)",  # This will fail!
    operation=Operation.SUM
)

# ✅ CORRECT - Do transformation in selects, reference column name in input_column
query=Query(
    selects=selects(
        purchase_amount="IF(event_type = 'purchase', price, 0)"  # Transform here
    )
)
aggregations=[
    Aggregation(
        input_column="purchase_amount",  # Reference the column name here
        operation=Operation.SUM
    )
]
```

**Pattern:**
```python
query=Query(
    selects=selects(
        "user_id",
        "merchant_id",
        revenue="price * quantity",  # Row-level calculation
        is_high_value="IF(price > 100, 1, 0)"
    ),
    time_column="ts"
)
```

```python
aggregations=[
    Aggregation(
        input_column="revenue",  # From selects
        operation=Operation.SUM,
        windows=["1d", "7d", "30d"]
    ),
    Aggregation(
        input_column="is_high_value",
        operation=Operation.SUM,  # Count of high value purchases
        windows=["7d"]
    )
]
```

#### Step 7.5: Filtering Patterns - Column-Level vs Query-Level

There are two ways to filter data in Chronon:

**A) Column-Level Filtering (using IF in selects)**
- Filters individual columns by setting unwanted values to NULL
- Allows different filters for different columns
- Happens during SELECT before aggregation
- Use when: You want to conditionally include/exclude values for specific features

Example:
```python
query=Query(
    selects=selects(
        "user_id",
        "listing_id",
        # Only count views from mobile devices
        mobile_view="IF(device_type = 'mobile' AND event_type = 'view', 1, 0)",
        # Only count purchases over $100
        high_value_purchase="IF(event_type = 'purchase' AND price_cents > 10000, 1, 0)",
        # All clicks regardless of device
        click="IF(event_type = 'click', 1, 0)",
        # Conditionally include the value - only for purchase events, null otherwise
        # This column can now be used in SUM/AVERAGE aggregations for purchase-only data
        purchase_amount="IF(event_type = 'purchase', price_cents, null)"
    ),
    time_column="ts"
)
```

Then you can aggregate on `purchase_amount` and it will only include purchase events:
```python
aggregations=[
    Aggregation(
        input_column="purchase_amount",  # Only includes purchase events
        operation=Operation.SUM,
        windows=["1d", "7d", "30d"]
    )
]
```

**B) Query-Level Filtering (using wheres parameter)**
- Filters entire rows before any processing
- Applies to ALL columns
- More efficient for excluding large amounts of data
- Use when: You want to exclude entire events/rows from consideration

Example:
```python
query=Query(
    selects=selects(
        "user_id",
        "listing_id",
        "event_type",
        "price_cents"
    ),
    wheres=[
        "user_id IS NOT NULL",
        "event_type IN ('view', 'click', 'purchase')",  # Only these event types
        "created_date >= '2024-01-01'"  # Date filter
    ],
    time_column="ts"
)
```

**When to use which:**
- Use **column-level** (IF) when: Different filters per feature, conditional feature values
- Use **query-level** (wheres) when: Data quality filters, reducing data volume, common filters across all features
- Can combine both: wheres to exclude bad data, IF for feature-specific logic

#### Step 8: Template for EventSource GroupBy

```python
from ai.chronon.types import Accuracy, Aggregation, EventSource, GroupBy, Operation, Query, selects

v1 = GroupBy(
    sources=[
        EventSource(
            table="data.table_name",
            topic="kafka://topic-name/serde=avro/schema_registry=http://registry:8081",  # Or None for batch
            query=Query(
                selects=selects(
                    "primary_key_column",
                    "other_column",
                    computed_field="SQL_EXPRESSION"
                ),
                time_column="ts_millis",  # Must be in milliseconds!
                start_partition="2023-01-01"
            )
        )
    ],
    keys=["primary_key_column"],
    aggregations=[
        Aggregation(
            input_column="column_to_aggregate",
            operation=Operation.SUM,  # Or COUNT, AVERAGE, LAST_K(10), etc.
            windows=["1d", "7d", "30d"]
        )
    ],
    online=True,
    accuracy=Accuracy.TEMPORAL,  # Or Accuracy.SNAPSHOT for midnight updates
    version=1  # TEMPORARY: Always include version=1
)
```

#### Step 9: Template for EntitySource Passthrough

```python
from ai.chronon.types import EntitySource, GroupBy, Query, selects

v1 = GroupBy(
    sources=[
        EntitySource(
            snapshot_table="data.users",
            query=Query(
                selects=selects(
                    "user_id",
                    "country",
                    "account_created_date",
                    is_verified="IF(email_verified = 1, true, false)"
                )
            )
        )
    ],
    keys=["user_id"],
    aggregations=None,  # Passthrough mode - no aggregations
    online=True,
    version=1  # TEMPORARY: Always include version=1
)
```

#### Step 9.5: Template for Streaming EntitySource (with Mutations)

For entity data that receives real-time updates via a mutation stream (e.g., Debezium CDC):

```python
from ai.chronon.types import Aggregation, EntitySource, GroupBy, Operation, Query, selects

v1 = GroupBy(
    sources=[
        EntitySource(
            snapshot_table="data.merchants",
            mutation_table="data.merchants_mutations",
            mutation_topic="merchants_mutations_topic",  # Streaming mutations
            query=Query(
                selects=selects(
                    "merchant_id",
                    "merchant_name",
                    "country",
                    "rating",
                    is_verified="IF(verification_status = 'verified', 1, 0)",
                    is_active="IF(status = 'active', 1, 0)"
                )
            )
        )
    ],
    keys=["merchant_id"],
    aggregations=None,  # Passthrough mode for real-time entity updates
    online=True,
    version=1  # TEMPORARY: Always include version=1
)
```

**When to use EntitySource with mutations:**
- Entity data that changes frequently and needs real-time updates
- CDC (Change Data Capture) streams from databases
- Real-time dimension tables
- User profiles that update in real-time

**Common mutation topic formats:**
```python
# Kafka
mutation_topic="kafka://merchants-cdc/serde=avro/schema_registry=http://registry:8081"

# GCP Pub/Sub
mutation_topic="pubsub://merchants-mutations/project=my-project/subscription=merchants-sub/serde=pubsub_schema"
```

**Important notes:**
- `mutation_table` contains the historical mutations
- `mutation_topic` provides real-time streaming updates
- Typically used with `aggregations=None` for passthrough
- For aggregations on entity mutations, you'll need a `time_column` in the mutation data

#### Step 10: Compile and Validate

After creating the GroupBy, you need to compile and then validate:

**Step 9a: Compile the configuration**
```bash
zipline compile --chronon-root <path_to_config_root> --force
```

Where `<path_to_config_root>` is the directory containing:
- `teams.py` (or `teams.json`)
- Subdirectories: `group_bys/`, `joins/`, `staging_queries/`, etc.

This will:
- Parse your Python GroupBy and convert it to JSON
- Place compiled configs in the `compiled/` directory
- Report any syntax errors or import issues

**Common compilation errors:**
- `ModuleNotFoundError`: Check your imports, ensure you're importing from the right modules
- `TypeError` with accuracy: Make sure you import `Accuracy` and use `Accuracy.TEMPORAL` not `"TEMPORAL"`
- String vs int comparison: Usually means you're passing a string where an enum is expected

**Step 9b: Validate with eval server**

If you have access to an eval server:
```bash
zipline hub eval --conf compiled/group_bys/team/groupby_name.v1
```

Replace `team/groupby_name.v1` with your actual compiled file path (found in `compiled/group_bys/`).

**If compilation fails:**
- Read the error message carefully
- Check imports match the templates
- Verify enum values are imported (like `Accuracy`, `Operation`)
- Fix errors and re-run `zipline compile`

**If eval fails:**
- Verify table names and columns match your metadata
- Ensure time_column expressions are valid SQL
- Check that all referenced staging queries exist

---

### When User Wants to Add GroupBy to a Join

#### Step 1: Understand the Context

Ask clarifying questions:
- **Is this an existing Join in production?** → Warn about caution, suggest versioning (v2 → v3)
- **Is this a dev/branch Join?** → Safe to modify directly
- **What's the use case?** → Understand the relationship between left and right

#### Step 2: Check Key Compatibility

**The Join will match on keys. Verify:**
- Does the left side have the necessary key columns?
- Do the keys have compatible formats? (e.g., both strings, same casing)
- Do we need key mapping? (left key name ≠ right key name)

**Example scenarios:**

**Scenario A: Keys match exactly**
```python
# Left has "user_id", GroupBy has "user_id" → No mapping needed
JoinPart(group_by=user_features_gb)
```

**Scenario B: Keys have different names**
```python
# Left has "listing_id", GroupBy has "id" → Need mapping
JoinPart(
    group_by=listing_features_gb,
    key_mapping={"listing_id": "id"}  # left_key: right_key
)
```

**Scenario C: Left side missing key**
- Need to add the key column to the left source first
- Consider using a StagingQuery to prepare the left side
- Or add a JoinPart that provides the key (multi-hop join)

#### Step 3: Consider Prefix

**Use prefix when:**
- Multiple GroupBys might have overlapping column names
- You want semantic clarity (e.g., "merchant_", "seller_")

```python
JoinPart(
    group_by=merchant_features_gb,
    prefix="merchant_"  # All columns will be "merchant_*"
)
```

#### Step 4: Think About Derivations

Derivations are computed AFTER all GroupBys are joined.

**Important**: By default, Joins **automatically pass through all columns** from the left side and all JoinParts. You only need to specify derivations when you want to:
- Compute new derived columns (cross-feature calculations)
- Transform existing columns

**Do NOT add `Derivation("*", "*")` by itself** - this is unnecessary as passthrough is the default behavior.

```python
# ❌ WRONG - Unnecessary passthrough-only derivations
derivations=[
    Derivation("*", "*")  # Don't do this alone - it's the default!
]

# ✅ CORRECT - Only specify derivations when computing new columns
derivations=[
    Derivation(
        "price_per_day",
        "listing_price / NULLIF(listing_days_available, 0)"
    ),
    Derivation("*", "*")  # NOW it's useful - keeps raw columns alongside derived ones
]

# ✅ ALSO CORRECT - Omit derivations entirely if no transformations needed
# (All columns automatically pass through)
```

**When to use derivations:**
- Cross-feature calculations (features from different GroupBys)
- Ratios, differences, products of features
- Normalization or scaling
- **Important**: Once you add ANY derivation, you must explicitly add `Derivation("*", "*")` if you want to keep the original columns

#### Step 5: Template for Adding to Join

```python
from ai.chronon.types import Derivation, Join, JoinPart

v2 = Join(  # Increment variable name (v1 -> v2)
    left=EventSource(
        table="data.training_labels",
        query=Query(
            selects=selects("user_id", "listing_id", "label"),
            time_column="ts"
        )
    ),
    right_parts=[
        # Existing GroupBys...
        JoinPart(group_by=existing_gb_1),

        # New GroupBy being added
        JoinPart(
            group_by=new_groupby_gb,
            key_mapping={"listing_id": "id"} if needed,
            prefix="new_" if needed
        )
    ],
    derivations=[
        Derivation("*", "*"),  # Include raw columns
        Derivation("ratio_feature", "feature_a / NULLIF(feature_b, 0)")
    ],
    online=True,
    version=1  # TEMPORARY: Always include version=1
)
```

#### Step 6: Compile and Validate

```bash
# Compile first
zipline compile --chronon-root <path_to_config_root> --force

# Then validate
zipline hub eval --conf compiled/joins/team/join_name.v2
```

---

### When User Wants to Create a StagingQuery

StagingQueries are for complex ETL that doesn't fit the GroupBy/Join pattern.

#### Common Use Cases:
- Preparing complex Join left sides
- Multi-table joins with custom logic
- Data quality filters
- Feature preprocessing

#### Template:

```python
from ai.chronon.types import EngineType, StagingQuery, TableDependency

v1 = StagingQuery(
    query="""
        SELECT
            a.user_id,
            a.ts,
            b.country,
            c.merchant_id,
            COALESCE(d.spend_last_30d, 0) as prior_spend
        FROM data.events a
        LEFT JOIN data.users b
          ON a.user_id = b.user_id
         AND a.ds = b.ds
        LEFT JOIN data.listings c
          ON a.listing_id = c.id
         AND a.ds = c.ds
        LEFT JOIN data.aggregated_features d
          ON a.user_id = d.user_id
         AND a.ds = d.ds
        WHERE a.ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
          AND a.event_type = 'checkout'
    """,
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="data.events", partition_column="ds", offset=0),
        TableDependency(table="data.users", partition_column="ds", offset=1)
    ],
    step_days=30,  # Process 30 days at a time
    version=1  # TEMPORARY: Always include version=1
)
```

**Template variables available:**
- `{{ start_date }}`: Start of current processing chunk
- `{{ end_date }}`: End of current processing chunk
- `{{ latest_date }}`: The target date requested
- `{{ max_date(table=namespace.table) }}`: Latest partition in a table

---

### When User Wants to Create a Model or ModelTransforms

Models and ModelTransforms enable ML inference in Chronon pipelines. There are three main patterns:

**A) Embedding inference with pre-trained models** (e.g., Gemini embedding-001, Titan)
**A2) Text generation with LLMs** (e.g., Gemini Flash/Pro for summarization, descriptions)
**B) Custom models with training + deployment** (e.g., XGBoost CTR models)

#### Understanding the Relationship

```
GroupBy → Join → ModelTransforms → enriched data with predictions
                      ↓
                   Model (defines inference/training specs)
```

- **Model**: Defines the ML model (inference specs, training specs, deployment specs)
- **ModelTransforms**: Applies the Model to Join output, producing enriched data with predictions

#### How Vertex AI Prediction Works in Zipline

Understanding the request/response flow is essential for setting `input_mapping`, `value_fields`, and `output_mapping` correctly.

**Request flow:**
- The `instance` key in `input_mapping` is a SQL expression whose result becomes the raw Vertex AI instance object
- Zipline sends: `{"instances": [<result_of_instance_expression>], "parameters": {<extra model_backend_params>}}`
- Any `model_backend_params` keys other than `model_name` and `model_type` are passed as `parameters`

**Response flow:**
- Vertex AI returns: `{"predictions": [{...}, {...}]}`
- `value_fields` defines the Spark schema used to parse each prediction object
- `output_mapping` uses Spark SQL expressions to navigate the parsed struct to the desired output field

**output_mapping column name convention:**
```
{team}_{model_file}_{model_var}__{version}__{field}
```
Example: `gcp_listing_item_description_model__1__embeddings.values`

#### Pattern A: Pre-Trained Model Inference (Embeddings)

Use this for models that are already trained (Vertex AI embeddings, Bedrock models, etc.)

**Step 1: Define the Model**

```python
from ai.chronon.types import InferenceSpec, Model, ModelBackend
from ai.chronon.data_types import DataType

# Define output schema (from model docs)
statistics = DataType.STRUCT("statistics", ("truncated", DataType.BOOLEAN), ("token_count", DataType.INT))
values = DataType.LIST(DataType.DOUBLE)
embeddings = DataType.STRUCT("embeddings", ("statistics", statistics), ("values", values))

# For GCP/Vertex AI
item_description_model = Model(
    version="1",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "model_name": "gemini-embedding-001",
            "model_type": "publisher",  # Pre-trained model from Google
        }
    ),
    # How to prepare input from join columns
    input_mapping={
        "instance": "named_struct('content', concat_ws('; ', listing_id_headline, listing_id_long_description))",
    },
    # How to extract output from model response
    output_mapping={
        "item_embedding": "gcp_listing_item_description_model__1__embeddings.values"
    },
    # Schema of model output (from API docs)
    value_fields=[
        ("embeddings", embeddings),
    ]
)

# For AWS/Bedrock
item_description_model = Model(
    version="1",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.SAGEMAKER,
        model_backend_params={
            "model_name": "amazon-titan-embed-text-v1",
            "model_type": "bedrock",  # Pre-trained model from AWS
        }
    ),
    input_mapping={
        "instance": "named_struct('content', concat_ws('; ', listing_id_headline, listing_id_long_description))",
    },
    output_mapping={
        "item_embedding": "aws_listing_item_description_model__1__embeddings.values"
    },
    value_fields=[
        ("embeddings", embeddings),
    ]
)
```

**Step 2: Create ModelTransforms**

**IMPORTANT**: ModelTransforms can only consume **Joins** via JoinSource, not GroupBys directly. If you want to apply a model to GroupBy output, you must:
1. Create a Join that includes the GroupBy as a JoinPart
2. Reference that Join in the JoinSource

ModelTransforms applies the model to Join output:

```python
from ai.chronon.types import JoinSource, ModelTransforms, Query
from ai.chronon.data_types import DataType

# Reference the join to use as input
source = JoinSource(
    join=demo.v1,  # The join containing listing_id_headline, listing_id_long_description
    query=Query(
        # Optional: filter rows before inference
        wheres=["(listing_id_headline IS NOT NULL AND listing_id_headline != '') OR (listing_id_long_description IS NOT NULL AND listing_id_long_description != '')"]
    )
)

v1 = ModelTransforms(
    sources=[source],  # Can have multiple sources
    models=[item_description_model],  # Can have multiple models
    # Fields to pass through from source (alongside predictions)
    passthrough_fields=["user_id", "listing_id", "listing_id_is_active"],
    # Input fields to the model (must match input_mapping)
    key_fields=[
        ("listing_id_headline", DataType.STRING),
        ("listing_id_long_description", DataType.STRING),
    ],
    version=1,
    output_namespace="data",
)
```

**Output**: This produces a table like:
```
user_id | listing_id | listing_id_is_active | item_embedding (array of doubles)
```

#### Pattern A2: Text Generation with LLMs

Use this for off-the-shelf text generation models (e.g., Gemini Flash/Pro for summarization, description generation, classification).

The key difference from Pattern A (embeddings) is the response schema: text generation models return `candidates[0].content.parts[0].text` instead of `embeddings.values`.

**Step 1: Define the response schema and Model**

Gemini text generation response structure:
```
predictions[i].candidates[0].content.parts[0].text
```

```python
from ai.chronon.types import InferenceSpec, Model, ModelBackend
from ai.chronon.data_types import DataType

# Define output schema matching the Gemini predict response
part = DataType.STRUCT("part", ("text", DataType.STRING))
content_inner = DataType.STRUCT("content_inner", ("parts", DataType.LIST(part)), ("role", DataType.STRING))
candidate = DataType.STRUCT("candidate", ("content", content_inner), ("finishReason", DataType.STRING))

short_description_model = Model(
    version="1",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "model_name": "gemini-1.5-flash-001",
            "model_type": "publisher",
            "maxOutputTokens": "128",   # Passed as parameters to Vertex AI
            "temperature": "0.4",
        },
    ),
    # Input uses Gemini's contents API (role/parts structure)
    input_mapping={
        "instance": "named_struct('contents', array(named_struct('role', 'user', 'parts', array(named_struct('text', CONCAT('Write a concise short product description: ', long_description))))))",
    },
    output_mapping={
        # Navigate: predictions[i] → candidates[0].content.parts[0].text
        "short_description": "gcp_listing_short_description_model__1__candidates[0].content.parts[0].text",
    },
    value_fields=[
        ("candidates", DataType.LIST(candidate)),
    ],
)
```

**Step 2: Create ModelTransforms**

Same as Pattern A — reference a Join via JoinSource:

```python
from ai.chronon.types import JoinSource, ModelTransforms, Query
from ai.chronon.data_types import DataType

source = JoinSource(
    join=demo.v1,
    query=Query(
        wheres=["long_description IS NOT NULL AND long_description != ''"]
    )
)

v1 = ModelTransforms(
    sources=[source],
    models=[short_description_model],
    passthrough_fields=["listing_id", "headline"],
    key_fields=[("long_description", DataType.STRING)],
    version=1,
    output_namespace="data",
)
```

**Output**: Produces a `short_description: string` column alongside the passthrough fields.

**Key notes:**
- `maxOutputTokens`, `temperature`, and other generation params go in `model_backend_params` — they're passed as Vertex AI `parameters`
- The `instance` expression must use the Gemini contents API format (role/parts structure)
- The response navigates nested candidates → content → parts → text

#### Pattern B: Custom Model with Training + Deployment

Use this for custom models you train yourself (e.g., XGBoost, PyTorch, TensorFlow)

**Step 1: Create Training Labels (via StagingQuery or Join)**

```python
from ai.chronon.types import StagingQuery, TableDependency

# Create labels for training
v1 = StagingQuery(
    query=f"""
    SELECT
        *,
        case when rand() < 0.5 then 0 else 1 end as label
    FROM {{demo.derivations_v1.table}}__derived
    WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """,
    output_namespace="data",
    dependencies=[
        TableDependency(table=f"{demo.derivations_v1.table}__derived", partition_column="ds", offset=0),
    ],
    version=1,  # TEMPORARY: Always use version=1
)
```

**Step 2: Define the Model with Training + Deployment**

```python
from ai.chronon.types import DeploymentSpec, DeploymentStrategyType, EndpointConfig, EventSource, InferenceSpec, Model, ModelBackend, Query, ResourceConfig, RolloutStrategy, ServingContainerConfig, TimeUnit, TrainingSpec, Window, selects
from ai.chronon.data_types import DataType

# Define training data source
label_source = EventSource(
    table=ctr_labels.v1.table,  # From the staging query above
    query=Query(
        selects=selects(
            user_id_click_event_average_7d="user_id_click_event_average_7d",
            listing_price_cents="listing_id_price_cents",
            price_log="price_log",
            price_bucket="price_bucket",
            label="label",
            ds="ds"
        ),
        start_partition="2025-07-01",
    )
)

# GCP/Vertex AI version
ctr_model = Model(
    version="1.0",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "model_name": "test_ctr_model",
            "model_type": "custom",  # Your custom trained model
        }
    ),
    # How to prepare input for inference
    input_mapping={
        "instance": "named_struct('user_id_click_event_average_7d', user_id_click_event_average_7d, 'listing_price_cents', listing_id_price_cents, 'price_log', price_log, 'price_bucket', price_bucket)",
    },
    # How to extract predictions
    output_mapping={
        "ctr": "gcp_click_through_rate_ctr_model__1_0__score"
    },
    # Schema of model output
    value_fields=[
        ("score", DataType.DOUBLE),
    ],
    model_artifact_base_uri="gs://zipline-warehouse-models",
    # Training configuration
    training_conf=TrainingSpec(
        training_data_source=label_source,
        training_data_window=Window(length=1, time_unit=TimeUnit.DAYS),
        schedule="@daily",  # Train daily
        image="us-docker.pkg.dev/vertex-ai/training/xgboost-cpu.2-1:latest",
        python_module="trainer.train",  # Your training script
        resource_config=ResourceConfig(
            min_replica_count=1,
            max_replica_count=1,
            machine_type="n1-standard-4"
        ),
        job_configs={
            "max-depth": "4",
            "eta": "0.1",
            "num-boost-round": "50"
        }
    ),
    # Deployment configuration
    deployment_conf=DeploymentSpec(
        container_config=ServingContainerConfig(
            image="us-central1-docker.pkg.dev/project/repo/ctr-predictor:v1"
        ),
        endpoint_config=EndpointConfig(
            endpoint_name="test_ctr_model"
        ),
        resource_config=ResourceConfig(
            min_replica_count=1,
            max_replica_count=3,
            machine_type="n1-standard-4"
        ),
        rollout_strategy=RolloutStrategy(
            rollout_type=DeploymentStrategyType.IMMEDIATE,
        )
    )
)

# AWS/SageMaker version
ctr_model = Model(
    version="1.0",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.SAGEMAKER,
        model_backend_params={
            "model_name": "test_ctr_model",
            "model_type": "custom",
        }
    ),
    input_mapping={
        "instance": "named_struct('user_id_click_event_average_7d', user_id_click_event_average_7d, 'listing_price_cents', listing_id_price_cents, 'price_log', price_log, 'price_bucket', price_bucket)",
    },
    output_mapping={
        "ctr": "score"
    },
    value_fields=[
        ("score", DataType.DOUBLE),
    ],
    model_artifact_base_uri="s3://zipline-warehouse-models",
    training_conf=TrainingSpec(
        training_data_source=label_source,
        training_data_window=Window(length=1, time_unit=TimeUnit.DAYS),
        schedule="@daily",
        image="763104351884.dkr.ecr.us-east-1.amazonaws.com/xgboost-training:latest",
        python_module="trainer.train",
        resource_config=ResourceConfig(
            min_replica_count=1,
            max_replica_count=1,
            machine_type="ml.m5.xlarge"
        ),
        job_configs={
            "max-depth": "4",
            "eta": "0.1",
            "num-boost-round": "50"
        }
    ),
    deployment_conf=DeploymentSpec(
        container_config=ServingContainerConfig(
            image="763104351884.dkr.ecr.us-east-1.amazonaws.com/xgboost-inference:latest"
        ),
        endpoint_config=EndpointConfig(
            endpoint_name="test_ctr_model"
        ),
        resource_config=ResourceConfig(
            min_replica_count=3,
            max_replica_count=10,
            machine_type="ml.m5.xlarge"
        ),
        rollout_strategy=RolloutStrategy(
            rollout_type=DeploymentStrategyType.IMMEDIATE,
        )
    )
)
```

**Step 3: Create ModelTransforms for Inference**

```python
from ai.chronon.types import JoinSource, ModelTransforms
from ai.chronon.data_types import DataType

source = JoinSource(join=demo.derivations_v1)

v1 = ModelTransforms(
    sources=[source],
    models=[ctr_model],
    passthrough_fields=["user_id", "listing_id", "user_id_click_event_average_7d", "listing_id_price_cents", "price_log", "price_bucket"],
    key_fields=[
        ("user_id_click_event_average_7d", DataType.DOUBLE),
        ("listing_id_price_cents", DataType.LONG),
        ("price_log", DataType.DOUBLE),
        ("price_bucket", DataType.INT)
    ],
    version=1,
    output_namespace="data",
)
```

#### Key Concepts

**Model `input_mapping`**:
- SQL expression that transforms join columns into model input format
- Uses `named_struct()` to create structured input
- Column names must match what the model expects

**Model `output_mapping`**:
- Extracts specific fields from model output
- Output column name follows pattern: `{team}_{model_file}_{model_var}__{version}__{field}`
- Example: `gcp_listing_item_description_model__1__embeddings.values`

**Model `value_fields`**:
- Defines the schema of model output
- Used to parse model response
- Must match actual model output format (check model API docs)

**ModelTransforms `key_fields`**:
- Input fields to the model (must be present in source Join)
- Must align with `input_mapping` in the Model

**ModelTransforms `passthrough_fields`**:
- Fields from source to include in output (alongside predictions)
- Useful for IDs, labels, or other context

#### Training Workflow

For custom models with `training_conf`:

1. **Package training code**: Create a source distribution
   ```bash
   python setup.py sdist
   ```

2. **Upload to cloud storage**:
   ```bash
   # GCP
   gsutil cp dist/model-1.0.tar.gz gs://bucket/builds/

   # AWS
   aws s3 cp dist/model-1.0.tar.gz s3://bucket/builds/
   ```

3. **Training execution**: Platform (Vertex AI / SageMaker) pulls package and runs training
   - Fetches data from `training_data_source`
   - Runs within `training_data_window`
   - Saves artifacts to `model_artifact_base_uri`

4. **Deployment**: Platform deploys model to endpoint using `deployment_conf`

#### Common Use Cases

**Embeddings** (Pattern A):
- Text embeddings (Titan, Gemini embedding-001)
- Image embeddings (multimodal models)
- Use for semantic search, recommendations

**Text Generation / Summarization** (Pattern A2):
- Short description generation from long descriptions
- Content summarization or rewriting
- LLM-based classification (returning text labels)
- Use for enriching catalog data with LLM-generated content

**Classification/Regression** (Pattern B):
- CTR prediction
- Fraud detection
- Churn prediction
- Price prediction

---

### When User Wants to Create a Chained GroupBy

Chaining allows you to use a **Join** as a source for a **GroupBy**, instead of EventSource or EntitySource. This enables creating features on already-enriched data.

#### What is Chaining?

Instead of aggregating raw event or entity data, you can:
1. Create a "parent" Join that combines multiple data sources
2. Wrap that Join in a `JoinSource`
3. Use the JoinSource in a GroupBy to aggregate the enriched data

**Use cases:**
- Aggregate on features that require multiple lookups (e.g., "last 100 listing prices viewed by user")
- Create derived features from already-joined data
- Build hierarchical feature pipelines

#### Step-by-Step Process

**Step 1: Create the Parent Join**

First, create a Join that brings together the data you want to aggregate on:

```python
from ai.chronon.types import EventSource, Join, JoinPart, Query, selects

# Parent join that enriches events with dimension data
parent_join = Join(
    left=EventSource(
        table="data.user_events",
        query=Query(
            selects=selects(
                user_id="user_id",
                listing_id="listing_id",
                event_id="event_id"
            ),
            time_column="event_ts"
        )
    ),
    right_parts=[
        # Join with listing dimensions to get pricing info
        JoinPart(group_by=listing_features_gb)
    ],
    online=True,
    output_namespace="data"
)
```

**Step 2: Create the JoinSource**

Wrap the Join in a JoinSource with a Query to select/transform columns:

```python
from ai.chronon.types import JoinSource

upstream_join_source = JoinSource(
    join=parent_join,
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
            # This column came from the listing_features_gb in the join
            price_cents="listing_id_price_cents"
        ),
        time_column="ts"
    )
)
```

**Important:** Column names from the right side GroupBys are prefixed with their key names. For example:
- If GroupBy has key `listing_id` and column `price_cents`
- In the Join output, it becomes `listing_id_price_cents`

**Step 3: Create the Chained GroupBy**

Use the JoinSource as the source for your GroupBy.

**CRITICAL**: When the parent Join contains streaming sources (EventSource with topic), aggregations **must specify windows**. Unwindowed aggregations on unbounded event sources will fail with an error like:
```
group_by uses unwindowed aggregations on unbounded event sources
```

Always specify a window (e.g., `windows=["30d"]` or `windows=[Window(length=7, time_unit=TimeUnit.DAYS)]`).

```python
from ai.chronon.types import Accuracy, Aggregation, GroupBy, Operation, TimeUnit, Window

chained_gb = GroupBy(
    sources=[upstream_join_source],
    keys=["user_id"],
    aggregations=[
        # Aggregate on the enriched data
        Aggregation(
            input_column="price_cents",  # From the JoinSource
            operation=Operation.LAST_K(100),
            windows=[Window(length=7, time_unit=TimeUnit.DAYS)]
        )
    ],
    online=True,
    accuracy=Accuracy.TEMPORAL
)
```

#### Common Patterns

**Pattern 1: User-Item Interaction History**

Enrich user events with item attributes, then aggregate:

```python
# Parent join enriches events with item details
parent_join = Join(
    left=EventSource(table="user_events", ...),
    right_parts=[
        JoinPart(group_by=item_dimensions_gb)  # Has price, category, etc.
    ],
    ...
)

# JoinSource selects the enriched fields
join_source = JoinSource(
    join=parent_join,
    query=Query(
        selects=selects(
            user_id="user_id",
            item_id="item_id",
            price="item_id_price",
            category="item_id_category"
        ),
        time_column="ts"
    )
)

# Chained GroupBy aggregates enriched data
chained_gb = GroupBy(
    sources=[join_source],
    keys=["user_id"],
    aggregations=[
        # Last 50 prices of items viewed
        Aggregation(input_column="price", operation=Operation.LAST_K(50)),
        # Category distribution
        Aggregation(input_column="category", operation=Operation.LAST_K(20))
    ],
    ...
)
```

**Pattern 2: Multi-Hop Enrichment**

Chain multiple Joins together for complex enrichment:

```python
# First join: Events + Item details
first_join = Join(
    left=EventSource(table="events", ...),
    right_parts=[JoinPart(group_by=item_gb)],
    ...
)

# Second join: Previous output + Merchant details
second_join = Join(
    left=JoinSource(join=first_join, query=...),
    right_parts=[JoinPart(group_by=merchant_gb)],
    ...
)

# Final chained GroupBy aggregates fully enriched data
chained_gb = GroupBy(
    sources=[JoinSource(join=second_join, query=...)],
    keys=["user_id"],
    aggregations=[...],
    ...
)
```

#### Important Notes

**Column Naming:**
- Right-side columns are prefixed: `{key}_{column_name}`
- Example: GroupBy with key `listing_id` and column `price` → `listing_id_price` in Join output

**Performance:**
- Chaining adds complexity - use only when necessary
- Each Join in the chain must be computed before the chained GroupBy
- Consider offline backfill times when designing deep chains

**Online vs Offline:**
- Parent Join should have `online=True` if chained GroupBy needs online serving
- All upstream dependencies must support the required serving mode

**Alternative to Chaining:**
When possible, consider:
- Creating a StagingQuery to pre-join data, then use as EventSource
- Adding features directly to an existing GroupBy instead of chaining

**Where to Put Transformations:**
Transformations (IF expressions, SQL calculations) can be done in either the parent's JoinSource or the child GroupBy's JoinSource. Both approaches are valid:

**Option 1: Transformations in Parent JoinSource**
```python
# Parent file: joins/gcp/user_category_parent.py
upstream_join_source = JoinSource(
    join=parent_join,
    query=Query(
        selects=selects(
            user_id="user_id",
            event_type="event_type",
            primary_category="listing_id_primary_category",
            # Transformations done here in parent
            is_view="IF(event_type = 'view', 1, 0)",
            is_purchase="IF(event_type = 'purchase', 1, 0)"
        ),
        time_column="ts"
    )
)
```

**Option 2: Transformations in Child GroupBy's JoinSource**
```python
# Child file: group_bys/gcp/user_category_counts.py
source = JoinSource(
    join=user_category_parent.parent_join,  # Reference the Join directly
    query=Query(
        selects=selects(
            user_id="user_id",
            event_type="event_type",
            primary_category="listing_id_primary_category",
            # Transformations done here in child
            is_view="IF(event_type = 'view', 1, 0)",
            is_purchase="IF(event_type = 'purchase', 1, 0)"
        ),
        time_column="ts"
    )
)
```

**Decision criteria:**
- **Use parent** when: Transformations will be used by multiple downstream GroupBys (shared logic, DRY principle)
- **Use child** when: Transformations are specific to this one GroupBy (localized logic, single responsibility)
- Both approaches work correctly - choose based on software engineering principles

#### Validation

After creating a chained GroupBy:

```bash
# Compile
zipline compile --chronon-root <path> --force

# Validate
zipline hub eval --conf compiled/group_bys/team/chained_groupby.v1
```

Check that:
- Parent Join compiles and validates successfully
- Column names match between JoinSource selects and chained GroupBy aggregations
- Time column is correctly propagated through the chain

---

## Part 2: Debugging Data Issues

### Issue: All Null Data for a Column in a Join

This is usually a join key or timestamp mismatch.

#### Investigation Process:

**Step 1: Verify timestamp format on both sides**

Query left side:
```sql
SELECT
    primary_key,
    ts_column,
    FROM_UNIXTIME(ts_column / 1000) as readable_ts
FROM left_table
WHERE ds = '2024-01-15'
LIMIT 10
```

Query right side (apply the source query's time_column expression):
```sql
SELECT
    primary_key,
    <time_column_expression> as ts,
    FROM_UNIXTIME(<time_column_expression> / 1000) as readable_ts
FROM right_table
WHERE ds BETWEEN '2024-01-14' AND '2024-01-15'  -- Account for windows
LIMIT 10
```

**Check:**
- Are both in milliseconds? (13-digit numbers around 1700000000000)
- Do they represent reasonable dates when converted?

**Step 2: Verify key formats match**

```sql
-- Check left keys
SELECT primary_key, COUNT(*)
FROM left_table
WHERE ds = '2024-01-15'
GROUP BY primary_key
LIMIT 10

-- Check right keys
SELECT primary_key, COUNT(*)
FROM right_table
WHERE ds BETWEEN '2024-01-14' AND '2024-01-15'
GROUP BY primary_key
LIMIT 10
```

**Check:**
- Same data type? (both STRING, both BIGINT, etc.)
- Same format? (e.g., "user_123" vs "123", "US" vs "us")
- Do any keys appear in both sides?

**Step 3: Test join on a sample key**

Pick a key from the left side and query the right side:
```sql
SELECT *
FROM right_table
WHERE primary_key = '<sample_key_from_left>'
  AND ds BETWEEN '<left_ds - max_window>' AND '<left_ds>'
```

Apply any filter expressions from the GroupBy source.

**If zero rows returned**: Key mismatch or missing data on right
**If some rows returned**: Timestamp or filter issue

**Step 4: Check filter expressions**

If the GroupBy source has `wheres=[...]`, these filters might be excluding all data:

```python
query=Query(
    selects=...,
    wheres=["amount > 0", "country IS NOT NULL"]  # Check these!
)
```

Test without the filters to see if data appears.

**Step 5: Report Findings**

Summarize to user:
- "The timestamps on left are in seconds but right expects milliseconds"
- "The keys don't match: left has 'user_123' format, right has '123'"
- "The filter 'status = active' is excluding all rows"
- "No data exists on the right side for the date range tested"

**If Zipline CLI can't query:**
- Provide the SQL queries to the user
- Ask them to run in their environment and report results

---

### Issue: Unexpected Value for a Given Key

Similar process but focused on a specific key:

#### Investigation Process:

**Step 1: Query raw data for the key**

Left side:
```sql
SELECT *
FROM left_table
WHERE primary_key = '<the_key>'
  AND ds = '<the_date>'
```

Right side (apply source transformations):
```sql
SELECT
    <all_selected_columns>,
    <time_column_expression> as ts_millis
FROM right_table
WHERE primary_key = '<the_key>'
  AND ds BETWEEN '<date - window_days>' AND '<date>'
  -- Apply any WHERE filters from the source
```

**Step 2: Manually calculate expected result**

Given the aggregation logic (e.g., SUM over 7d window), calculate what the result should be:
- Filter rows within the time window
- Apply the aggregation operation
- Compare to actual result

**Step 3: Common Issues:**

- **Timestamp off by factor**: Seconds vs milliseconds (multiply by 1000)
- **Timezone issues**: Timestamps in wrong timezone
- **Filter expression**: Accidentally excluding valid rows
- **Window definition**: Misunderstanding of window boundaries
- **Duplicate events**: Events counted multiple times
- **Late-arriving data**: Data arrived after feature computation

**Step 4: Report Findings**

"Expected SUM of 150 based on 3 rows, but got 450 → Looks like data is being counted 3x, possibly duplicate events"

---

## Part 3: Key Reminders & Best Practices

### Reserved Keywords
- "ts" is a reserved keyword - use `time_column` instead of selecting a column called "ts"
- **GroupBy and Join files must not share the same filename**: having `group_bys/gcp/foo.py` and `joins/gcp/foo.py` causes eval to break. Always use distinct names — e.g., name the wrapper join `foo_join.py` or `foo_features.py` rather than `foo.py`.

### Deprecated Parameters
- **TEMPORARY: Always use `version=1` for all GroupBys, Joins, StagingQueries, and Models** - This is a temporary requirement. DELETE THIS BULLET POINT AND REMOVE ALL version= ARGUMENTS WHEN NO LONGER NEEDED.
- **The `row_ids` parameter in Join is deprecated** - however, it's still **required for compilation** until the API change is complete. You must include it by:
  1. Adding a `row_id` column in the left source's selects (e.g., `row_id="event_id"`)
  2. Passing `row_ids=["event_id"]` to the Join constructor
  - Example pattern seen in existing code: `# TODO -- kill this once the SPJ API change goes through`

### Version Management
- **"Version up" means changing the variable name**: `v1` → `v2` → `v3`, etc.
- Once in production, avoid changing configs - create new variable versions instead
- Example: If `v1` exists in production, create `v2` as a new variable with updated features

### Column Naming Convention
Output columns follow this pattern:
```
{prefix}_{groupby_name}_{input_column}_{operation}_{window}_{bucket}
```

Example: `merchant_purchases_amount_sum_7d`

### Operations to Prefer
- ✅ `Operation.APPROX_UNIQUE_COUNT_LGK(k=8)` - Fast, scalable
- ✅ `Operation.APPROX_PERCENTILE([0.5, 0.95], k=20)` - Fast, scalable
- ✅ `Operation.LAST_K(10)` - Fast, exact, **supports complex types** (structs, arrays)
- ⚠️ `Operation.UNIQUE_COUNT` - Slow, exact, avoid for high cardinality
- ⚠️ `Operation.UNIQUE_TOP_K(k)` - **Does NOT support STRUCT types** (will fail even with all string fields)
- ⚠️ `Operation.HISTOGRAM` - Slow, exact, avoid for high cardinality

**Note on complex types:**
- Use `LAST_K` instead of `UNIQUE_TOP_K` when working with STRUCT columns
- `UNIQUE_TOP_K` fails on any struct, regardless of field types
- For simple types (strings, numbers), `UNIQUE_TOP_K` works fine

### Time Column Checklist
- [ ] Produces milliseconds since epoch (Long type)
- [ ] Not seconds, microseconds, or nanoseconds
- [ ] Not a formatted string
- [ ] Expression is valid SQL
- [ ] Required for EventSource with TEMPORAL accuracy
- [ ] Required for windowed aggregations

### Source Type Decision Tree
```
Is this event data (time-series, logs)?
├─ YES → EventSource
│   ├─ Need realtime? → topic + table
│   └─ Batch only? → table only, topic=None
└─ NO → Entity data (profiles, dimensions)
    └─ EntitySource
        ├─ Passthrough (no aggs)? → aggregations=None
        │   ├─ Need realtime updates? → mutation_topic + mutation_table + snapshot_table
        │   └─ Batch only? → snapshot_table only
        └─ With aggs? → Define aggregations + time_column
```

---

## Part 4: Common Patterns from Examples

### Pattern: Simple Event Aggregation
```python
# From: python/test/canary/group_bys/gcp/purchases.py
v1 = GroupBy(
    sources=[
        EventSource(
            table="data.purchases",
            topic="events.purchases",
            query=Query(
                selects=selects("user_id", "purchase_amt"),
                time_column="ts"
            )
        )
    ],
    keys=["user_id"],
    aggregations=[
        Aggregation(
            input_column="purchase_amt",
            operation=Operation.SUM,
            windows=["1d", "7d", "30d"]
        )
    ],
    online=True
)
```

### Pattern: Entity Passthrough with Transformations
```python
# From: python/test/canary/group_bys/gcp/dim_listings.py
v1 = GroupBy(
    sources=[
        EntitySource(
            snapshot_table="data.listings",
            query=Query(
                selects=selects(
                    "listing_id",
                    "price_cents",
                    "is_active",
                    price_dollars="price_cents / 100.0",
                    has_description="IF(description IS NOT NULL, 1, 0)"
                )
            )
        )
    ],
    keys=["listing_id"],
    aggregations=None,  # No aggregations, just passthrough
    online=True
)
```

### Pattern: Multi-Source Join
```python
# From: python/test/canary/joins/gcp/demo.py
v1 = Join(
    left=EventSource(
        table="data.checkouts",
        query=Query(
            selects=selects("user_id", "listing_id", "ts"),
            time_column="ts"
        )
    ),
    right_parts=[
        JoinPart(group_by=user_purchases_gb),
        JoinPart(
            group_by=listing_features_gb,
            key_mapping={"listing_id": "id"}
        ),
        JoinPart(
            group_by=merchant_features_gb,
            prefix="merchant_"
        )
    ],
    derivations=[
        Derivation("*", "*"),
        Derivation("price_per_rating", "listing_price / NULLIF(listing_rating, 0)")
    ],
    online=True,
    version=1  # TEMPORARY: Always include version=1
)
```

### Pattern: Bucketed Aggregations
```python
# Aggregate by both user_id AND merchant_id (creates a map)
Aggregation(
    input_column="purchase_amt",
    operation=Operation.SUM,
    windows=["7d"],
    buckets=["merchant_id"]  # Creates map<merchant_id, sum>
)
```

### Pattern: Last K Values
```python
# Get last 10 purchase amounts (ordered by time)
Aggregation(
    input_column="purchase_amt",
    operation=Operation.LAST_K(10)  # Returns list of 10 values
)
```

### Pattern: Combined Filtering (Column + Query Level)
```python
# From: Based on python/test/canary/group_bys/gcp/item_event_canary.py
v1 = GroupBy(
    sources=[
        EventSource(
            table="data.item_events",
            topic="item_events_stream",
            query=Query(
                selects=selects(
                    "user_id",
                    "listing_id",
                    # Column-level: Only count specific event types
                    add_cart="IF(event_type = 'backend_add_to_cart', 1, 0)",
                    view="IF(event_type = 'view_listing', 1, 0)",
                    purchase="IF(event_type = 'backend_cart_payment', 1, 0)"
                ),
                # Query-level: Exclude all other event types entirely
                wheres=["event_type IN ('backend_add_to_cart', 'view_listing', 'backend_cart_payment')"],
                time_column="timestamp"
            )
        )
    ],
    keys=["user_id", "listing_id"],
    aggregations=[
        Aggregation(
            input_column="add_cart",
            operation=Operation.SUM,
            windows=["1d", "7d"]
        ),
        Aggregation(
            input_column="view",
            operation=Operation.SUM,
            windows=["1d", "7d"]
        )
    ],
    online=True,
    version=1  # TEMPORARY: Always include version=1
)
```

### Pattern: ML Inference with Embeddings (Complete Pipeline)
```python
# From: python/test/canary - shows GroupBy → Join → Model → ModelTransforms

# Step 1: Create GroupBys with features
from ai.chronon.types import EntitySource, GroupBy, Query, selects

# Listing dimension features
dim_listings_gb = GroupBy(
    sources=[
        EntitySource(
            snapshot_table="data.listings",
            query=Query(
                selects=selects(
                    "listing_id",
                    "headline",
                    "long_description",
                    "price_cents",
                    "is_active"
                )
            )
        )
    ],
    keys=["listing_id"],
    aggregations=None,  # Passthrough
    online=True
)

# Step 2: Create a Join combining features
from ai.chronon.types import EventSource, Join, JoinPart

demo_join = Join(
    left=EventSource(
        table="data.user_events",
        query=Query(
            selects=selects("user_id", "listing_id"),
            time_column="event_time_ms"
        )
    ),
    right_parts=[
        JoinPart(group_by=dim_listings_gb)
    ],
    online=True,
    output_namespace="data"
)

# Step 3: Define the Model (Vertex AI embedding)
from ai.chronon.types import InferenceSpec, Model, ModelBackend
from ai.chronon.data_types import DataType

statistics = DataType.STRUCT("statistics", ("truncated", DataType.BOOLEAN), ("token_count", DataType.INT))
values = DataType.LIST(DataType.DOUBLE)
embeddings = DataType.STRUCT("embeddings", ("statistics", statistics), ("values", values))

item_description_model = Model(
    version="1",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "model_name": "gemini-embedding-001",
            "model_type": "publisher"
        }
    ),
    input_mapping={
        # Combine headline and description
        "instance": "named_struct('content', concat_ws('; ', listing_id_headline, listing_id_long_description))"
    },
    output_mapping={
        "item_embedding": "gcp_listing_item_description_model__1__embeddings.values"
    },
    value_fields=[
        ("embeddings", embeddings)
    ]
)

# Step 4: Create ModelTransforms to apply inference
from ai.chronon.types import JoinSource, ModelTransforms

model_transforms = ModelTransforms(
    sources=[
        JoinSource(
            join=demo_join,
            query=Query(
                # Filter out empty content
                wheres=["(listing_id_headline IS NOT NULL AND listing_id_headline != '') OR (listing_id_long_description IS NOT NULL AND listing_id_long_description != '')"]
            )
        )
    ],
    models=[item_description_model],
    # Pass through IDs and other context
    passthrough_fields=["user_id", "listing_id", "listing_id_is_active"],
    # Input fields to the model
    key_fields=[
        ("listing_id_headline", DataType.STRING),
        ("listing_id_long_description", DataType.STRING)
    ],
    version=1,
    output_namespace="data"
)

# Result: Table with user_id, listing_id, listing_id_is_active, item_embedding (vector)
```

**Pipeline flow:**
1. **GroupBys** aggregate/passthrough raw data
2. **Join** combines features from multiple GroupBys
3. **Model** defines how to call ML inference (embeddings)
4. **ModelTransforms** applies the model to join output, producing enriched data

**Use cases:** Semantic search, recommendations, similarity matching

### Pattern: Applying Model to GroupBy Output

When you want to apply a model directly to GroupBy aggregations (not a multi-source Join), you need an intermediate wrapper Join:

```python
# Step 1: Create the GroupBy with aggregations
from ai.chronon.types import Accuracy, Aggregation, GroupBy, Operation

user_features_gb = GroupBy(
    sources=[EventSource(...)],
    keys=["user_id"],
    aggregations=[
        Aggregation(
            input_column="event_category",
            operation=Operation.LAST_K(30),
            windows=["30d"]
        )
    ],
    online=True,
    accuracy=Accuracy.TEMPORAL
)

# Step 2: Create a wrapper Join to enable ModelTransforms consumption
# (ModelTransforms can't consume GroupBys directly)
from ai.chronon.types import EventSource, Join, JoinPart

wrapper_join = Join(
    left=EventSource(
        table="data.events",
        topic="events_topic",
        query=Query(
            selects=selects("user_id", "event_id"),
            time_column="ts"
        )
    ),
    row_ids=["event_id"],
    right_parts=[
        JoinPart(group_by=user_features_gb)
    ],
    online=True,
    output_namespace="data",
    version=1  # TEMPORARY: Always include version=1
)

# Step 3: Apply model via ModelTransforms
from ai.chronon.types import JoinSource, ModelTransforms

v1 = ModelTransforms(
    sources=[JoinSource(join=wrapper_join)],
    models=[embedding_model],
    passthrough_fields=["user_id", "user_id_event_category_last30_30d"],
    key_fields=[
        ("user_id_event_category_last30_30d", DataType.LIST(DataType.STRING))
    ],
    version=1,
    output_namespace="data"
)
```

**Key points:**
- ModelTransforms requires a Join as input (via JoinSource)
- If your source is a GroupBy, create a simple wrapper Join
- The wrapper Join's left side can be the same event stream used by the GroupBy
- Column names from GroupBys in Joins are prefixed with key names (e.g., `user_id_event_category_last30_30d`)

---

## Part 5: Working with Zipline CLI

The Zipline CLI provides commands for testing and managing features.

**Working directory:** All `zipline` commands must be run from the `chronon-root` directory — the one containing `teams.py` and the `group_bys/`, `joins/`, etc. subdirectories. For this repo that is `python/test/canary/`. Always `cd` there before running any zipline command.

**Eval Server URL:** The `--eval-url` flag is optional — it can be configured in `teams.py` or via the `ZIPLINE_EVAL_URL` environment variable. Omit it from commands if the user has it configured. If a command fails with a connection error or "no eval URL" message, check `teams.py` for the `EVAL_URL` key under the relevant team's `env` block. For this repo the GCP team sets `"EVAL_URL": "http://localhost:3904"`.

### Key Commands:
```bash
# List all tables in a schema (use when table name is vague, misspelled, or unknown)
# --engine-type defaults to SPARK; use BIGQUERY for BigQuery schemas
zipline hub list-tables <schema_name> --engine-type BIGQUERY [--team <team_name>]

# Get table metadata (column names, types, partitions) — run after confirming exact table name
# --engine-type defaults to SPARK; use BIGQUERY for BigQuery tables
zipline hub eval-table --table "<schema.table_name>" --engine-type BIGQUERY

# Compile python into runnable configs
# Pass --chronon-root if not running from the root dir
# Root dir contains teams.py and subdirs: group_bys/, joins/, etc.
zipline compile --chronon-root <path_to_config_root> --force

# Validate a feature configuration (after compiling)
zipline hub eval --conf compiled/group_bys/team/config_name.v1

# Run a one-off adhoc job from the current branch (for testing before merge)
zipline hub run-adhoc --conf compiled/group_bys/team/config_name.v1

# Schedule a pipeline from a branch (without merging)
# Use this to run a dev version in parallel with prod for end-to-end A/B testing.
# IMPORTANT: the config version on your branch must differ from the one on main,
# so the branch pipeline writes to a separate table and doesn't collide with prod.
zipline hub schedule --conf compiled/joins/team/join_name.v2

# Get help
zipline --help
zipline hub --help
```

**Always compile and eval after authoring** - This catches:
- Syntax errors (during compile)
- Import errors (during compile)
- Invalid column references (during eval)
- Type mismatches (during eval)
- Configuration errors (during eval)

---

## Part 6: Workflow Summary

### Creating a New GroupBy:
1. **Understand use case**: Note team/directory, look at existing patterns
2. **Check for existing GroupBys**: Search repo for same source+keys+team (version up if found)
3. **Resolve table name**: If the user's table name isn't exact, run `zipline hub list-tables <schema> --engine-type BIGQUERY` to list available tables, find close matches, and confirm with the user before proceeding. Then run `zipline hub eval-table --table <confirmed_table> --engine-type BIGQUERY`. (If fails due to missing `--eval-url`, ask user to set it in `teams.py`/env var or pass `--eval-url <url>`. If table not found after list-tables, ask user to paste schema.)
4. **Check for external warehouse**: If table is in BigQuery/Snowflake/Redshift, find or create a staging query export and reference `exports.<name>.table` in your source
5. **Ask targeted questions**: Show columns and ask for PK
6. **Choose source type**: EventSource or EntitySource
7. **Handle time column**: Convert to milliseconds if needed
8. **Design features**: Work with user on aggregations/selects
9. **Write config**: Use templates above (remember to import `Accuracy` if using)
10. **Compile immediately**: `zipline compile --chronon-root <config_root> --force` — run this right after writing the file
11. **Eval immediately**: `zipline hub eval --conf compiled/group_bys/...` — run this right after compiling. Do NOT skip or defer this step.
12. **Iterate**: Fix errors and re-compile/eval before moving on

### Adding to a Join:
1. **Check if production**: Warn if yes, suggest versioning
2. **Verify keys**: Left and right keys compatible?
3. **Add JoinPart**: With key_mapping and prefix if needed
4. **Consider derivations**: Any cross-feature calculations?
5. **Compile immediately**: `zipline compile --chronon-root <config_root> --force` — run right after editing the file
6. **Eval immediately**: `zipline hub eval --conf compiled/joins/...` — run right after compiling. Do NOT skip or defer this step.

### Debugging Null Data:
1. **Check timestamps**: Both in milliseconds?
2. **Check keys**: Same format on both sides?
3. **Test sample key**: Does data exist?
4. **Check filters**: Excluding all rows?
5. **Report findings**: Be specific about the issue

---

## Part 7: Converting Tecton to Chronon

This section provides comprehensive guidance for migrating existing Tecton feature definitions to Chronon/Zipline.

### Tecton to Chronon Concept Mapping

Understanding the conceptual mapping between Tecton and Chronon is the first step in migration.

#### High-Level Mapping Table

| Tecton Concept | Chronon Equivalent | Notes |
|----------------|-------------------|-------|
| **Entity** | GroupBy `keys` parameter | Entities become the primary keys in GroupBys |
| **BatchSource** | `EntitySource` or batch-only `EventSource` | No topic, batch materialization only |
| **StreamSource** | `EventSource` with topic | Includes both table and topic for streaming |
| **Batch Feature View** | `GroupBy` with batch source | Uses EntitySource or EventSource without topic |
| **Stream Feature View** | `GroupBy` with EventSource + topic | Real-time aggregations with streaming sources |
| **Realtime Feature View** | `Join` with `Derivation` | Request-time computations using derivations |
| **On-Demand Feature View** | `Join` with `Derivation` | Same as Realtime - computed at serving time |
| **Feature Service** | `Join` | Combines multiple GroupBys into single endpoint |
| **Aggregation** | `Aggregation` with `Operation` | Similar semantics, different API |
| **Attribute Features** | `Query.selects` | Row-level transformations in selects |
| **timestamp_field** | `time_column` | Must produce milliseconds since epoch |
| **time_window** | `windows` parameter | E.g., `timedelta(days=30)` → `"30d"` or `Window(30, TimeUnit.DAYS)` |
| **mode** parameter | Not applicable | Chronon determines engine at runtime (Spark/Flink) |
| **join_keys** | `keys` parameter | Entity join keys become GroupBy keys |

#### Key Architectural Differences

**Tecton:**
- Uses decorators (`@batch_feature_view`, `@stream_feature_view`)
- Supports multiple compute modes (pandas, pyspark, SQL)
- Explicitly defines online/offline separation
- Has specialized templates (Aggregation, Embedding)

**Chronon:**
- Uses plain Python objects (GroupBy, Join, Source)
- Engine-agnostic (Spark for batch, Flink for streaming)
- Unified online/offline computation
- Everything is explicit - no hidden templating

---

### Converting Tecton Sources

#### BatchSource → EntitySource (for dimension tables)

**Tecton:**
```python
from tecton import BatchSource, FileConfig

users_batch = BatchSource(
    name="users_batch",
    batch_config=FileConfig(
        uri="s3://bucket/users.parquet",
        file_format="parquet",
        timestamp_field="updated_at"
    ),
)
```

**Chronon:**
```python
from ai.chronon.types import EntitySource, Query, selects

users_source = EntitySource(
    snapshot_table="data.users",
    query=Query(
        selects=selects(
            "user_id",
            "country",
            "account_created_date"
        )
    )
)
```

**Key changes:**
- `uri` → `snapshot_table` (table name in your warehouse)
- No `file_format` needed (inferred from table)
- `timestamp_field` only needed if doing time-based aggregations
- `selects` explicitly chooses columns to include

#### BatchSource → EventSource (for event streams)

**Tecton:**
```python
transactions_batch = BatchSource(
    name="transactions",
    batch_config=FileConfig(
        uri="s3://bucket/transactions.parquet",
        file_format="parquet",
        timestamp_field="transaction_time"
    ),
)
```

**Chronon:**
```python
from ai.chronon.types import EventSource, Query, selects

transactions_source = EventSource(
    table="data.transactions",
    topic=None,  # Batch only
    query=Query(
        selects=selects(
            "user_id",
            "merchant_id",
            "amount"
        ),
        time_column="unix_millis(transaction_time)"  # Must be in milliseconds!
    )
)
```

**Key changes:**
- `topic=None` for batch-only (no streaming)
- `time_column` must produce **milliseconds since epoch**
- `timestamp_field` → `time_column` with explicit conversion

#### StreamSource → EventSource (with streaming)

**Tecton:**
```python
from tecton import StreamSource, KafkaConfig

transactions_stream = StreamSource(
    name="transactions_stream",
    stream_config=KafkaConfig(
        kafka_bootstrap_servers="kafka:9092",
        topics=["transactions"],
        timestamp_field="transaction_time"
    ),
    batch_config=FileConfig(
        uri="s3://bucket/transactions",
        file_format="parquet",
        timestamp_field="transaction_time"
    )
)
```

**Chronon:**
```python
from ai.chronon.types import EventSource, Query, selects

transactions_source = EventSource(
    table="data.transactions",  # Batch table
    topic="kafka://transactions/serde=avro/schema_registry=http://registry:8081",  # Streaming
    query=Query(
        selects=selects(
            "user_id",
            "merchant_id",
            "amount"
        ),
        time_column="unix_millis(transaction_time)"
    )
)
```

**Key changes:**
- Both `table` and `topic` specified for hybrid batch/streaming
- Topic format includes serialization info: `kafka://topic/serde=avro/...`
- For GCP Pub/Sub: `pubsub://topic/project=X/subscription=Y/serde=pubsub_schema`
- Single `time_column` applies to both batch and stream

---

### Converting Tecton Feature Views

#### Batch Feature View → GroupBy with EntitySource

**Tecton:**
```python
from tecton import batch_feature_view, Attribute, Entity
from datetime import timedelta

user = Entity(name="user", join_keys=["user_id"])

@batch_feature_view(
    sources=[users_batch],
    entities=[user],
    mode="pandas",
    features=[
        Attribute("age", Int64),
        Attribute("country", String)
    ],
    timestamp_field="timestamp",
    batch_schedule=timedelta(days=1)
)
def user_demographics(users_df):
    return users_df[["user_id", "age", "country", "timestamp"]]
```

**Chronon:**
```python
from ai.chronon.types import EntitySource, GroupBy, Query, selects

v1 = GroupBy(
    sources=[
        EntitySource(
            snapshot_table="data.users",
            query=Query(
                selects=selects(
                    "user_id",
                    "age",
                    "country"
                )
            )
        )
    ],
    keys=["user_id"],  # From Entity join_keys
    aggregations=None,  # Passthrough mode
    online=True
)
```

**Key changes:**
- `@batch_feature_view` decorator → `GroupBy` object
- `entities=[user]` → `keys=["user_id"]`
- `features=[Attribute(...)]` → `selects(...)` in Query
- `mode="pandas"` → Not needed (Chronon handles execution)
- No transformation function needed for simple passthrough
- `aggregations=None` for passthrough (no aggregations)

#### Stream Feature View → GroupBy with EventSource

**Tecton:**
```python
from tecton import stream_feature_view, Aggregate

user = Entity(name="user", join_keys=["user_id"])

@stream_feature_view(
    source=transactions_stream,
    entities=[user],
    mode="spark_sql",
    aggregations=[
        Aggregate(
            input_column=Field("amount", Float64),
            function="sum",
            time_window=timedelta(days=30),
            name="total_spend_30d"
        ),
        Aggregate(
            input_column=Field("transaction_id", String),
            function="count",
            time_window=timedelta(days=7),
            name="transaction_count_7d"
        )
    ],
    timestamp_field="transaction_time"
)
def user_transaction_aggregates(transactions):
    return transactions[["user_id", "amount", "transaction_id", "transaction_time"]]
```

**Chronon:**
```python
from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, selects

v1 = GroupBy(
    sources=[
        EventSource(
            table="data.transactions",
            topic="kafka://transactions/serde=avro/...",
            query=Query(
                selects=selects(
                    "user_id",
                    "amount",
                    "transaction_id"
                ),
                time_column="unix_millis(transaction_time)"
            )
        )
    ],
    keys=["user_id"],
    aggregations=[
        Aggregation(
            input_column="amount",
            operation=Operation.SUM,
            windows=["30d"]
        ),
        Aggregation(
            input_column="transaction_id",
            operation=Operation.COUNT,
            windows=["7d"]
        )
    ],
    online=True
)
```

**Key changes:**
- `@stream_feature_view` → `GroupBy` with EventSource (includes topic)
- `Aggregate(function="sum")` → `Aggregation(operation=Operation.SUM)`
- `time_window=timedelta(days=30)` → `windows=["30d"]`
- `name` parameter not needed (Chronon auto-generates column names)
- No transformation function needed

#### Realtime Feature View → Join with Derivation

**Tecton:**
```python
from tecton import realtime_feature_view, RequestSource

request = RequestSource(schema=[Field("amount", Float64)])

@realtime_feature_view(
    sources=[request, user_transaction_aggregates],
    mode="python",
    features=[Attribute("ratio", Float64)]
)
def transaction_ratio(request_data, user_features):
    ratio = request_data["amount"] / user_features["total_spend_30d"]
    return {"ratio": ratio}
```

**Chronon:**
```python
from ai.chronon.types import Derivation, EventSource, Join, JoinPart, Query, selects

v1 = Join(
    left=EventSource(
        table="data.requests",
        query=Query(
            selects=selects(
                "user_id",
                "amount"  # Request-time field
            ),
            time_column="unix_millis(request_time)"
        )
    ),
    right_parts=[
        JoinPart(group_by=user_transaction_aggregates)
    ],
    derivations=[
        Derivation("*", "*"),  # Passthrough all columns
        Derivation(
            "ratio",
            "amount / NULLIF(user_id_total_spend_30d, 0)"
        )
    ],
    online=True
)
```

**Key changes:**
- `@realtime_feature_view` → `Join` object
- `RequestSource` → Left side EventSource (or left side of Join)
- Python function → SQL `Derivation`
- Request data available as left columns, feature data from JoinParts
- Derivations use SQL expressions, not Python functions

---

### Converting Tecton Aggregations

#### Basic Aggregation Mapping

| Tecton Aggregation | Chronon Operation | Example |
|--------------------|-------------------|---------|
| `function="sum"` | `Operation.SUM` | Sum of values |
| `function="count"` | `Operation.COUNT` | Count of events |
| `function="mean"` | `Operation.AVERAGE` | Average value |
| `function="min"` | `Operation.MIN` | Minimum value |
| `function="max"` | `Operation.MAX` | Maximum value |
| `function="last"` | `Operation.LAST` | Most recent value |
| `function="first"` | `Operation.FIRST` | Earliest value |
| `function="last_distinct"` | `Operation.LAST_K(k)` | Last k unique values |
| `function="count_distinct"` | `Operation.APPROX_UNIQUE_COUNT_LGK(8)` | Approximate distinct count |

#### Time Window Conversion

**Tecton:**
```python
time_window=timedelta(days=7)
time_window=timedelta(hours=1)
time_window=timedelta(minutes=30)
```

**Chronon:**
```python
windows=["7d"]
windows=["1h"]
windows=["30m"]

# Or using Window object:
from ai.chronon.types import TimeUnit, Window
windows=[Window(length=7, time_unit=TimeUnit.DAYS)]
```

#### Multiple Windows

**Tecton:**
```python
# Need multiple Aggregate definitions
Aggregate(input_column=Field("amount", Float64), function="sum", time_window=timedelta(days=1)),
Aggregate(input_column=Field("amount", Float64), function="sum", time_window=timedelta(days=7)),
Aggregate(input_column=Field("amount", Float64), function="sum", time_window=timedelta(days=30)),
```

**Chronon:**
```python
# Single Aggregation with multiple windows
Aggregation(
    input_column="amount",
    operation=Operation.SUM,
    windows=["1d", "7d", "30d"]  # Much cleaner!
)
```

#### Advanced Aggregations

**Last N Values (Tecton):**
```python
Aggregate(
    input_column=Field("product_id", String),
    function="last_distinct",
    time_window=timedelta(days=30),
    name="recent_products"
)
```

**Last K (Chronon):**
```python
Aggregation(
    input_column="product_id",
    operation=Operation.LAST_K(10),  # Last 10 values
    windows=["30d"]
)
```

**Percentiles (Tecton):**
```python
Aggregate(
    input_column=Field("latency_ms", Float64),
    function="percentile",
    percentiles=[0.5, 0.95, 0.99],
    time_window=timedelta(hours=1)
)
```

**Percentiles (Chronon):**
```python
Aggregation(
    input_column="latency_ms",
    operation=Operation.APPROX_PERCENTILE([0.5, 0.95, 0.99], k=20),
    windows=["1h"]
)
```

#### Secondary Key Aggregations (Bucketing)

**Tecton:**
```python
# Uses secondary_key parameter
Aggregate(
    input_column=Field("amount", Float64),
    function="sum",
    time_window=timedelta(days=7),
    secondary_key="merchant_id",
    name="spend_by_merchant_7d"
)
```

**Chronon:**
```python
# Uses buckets parameter
Aggregation(
    input_column="amount",
    operation=Operation.SUM,
    windows=["7d"],
    buckets=["merchant_id"]  # Creates map<merchant_id, sum>
)
```

---

### Complete Tecton to Chronon Conversion Examples

#### Example 1: User Transaction Features

**Tecton (Complete Definition):**
```python
from tecton import batch_feature_view, Entity, BatchSource, FileConfig, Aggregate
from datetime import timedelta

user = Entity(name="user", join_keys=["user_id"])

transactions_batch = BatchSource(
    name="transactions",
    batch_config=FileConfig(
        uri="s3://data/transactions",
        file_format="parquet",
        timestamp_field="ts"
    )
)

@batch_feature_view(
    sources=[transactions_batch],
    entities=[user],
    mode="spark_sql",
    aggregations=[
        Aggregate(
            input_column=Field("amount", Float64),
            function="sum",
            time_window=timedelta(days=30),
            name="total_spend_30d"
        ),
        Aggregate(
            input_column=Field("transaction_id", String),
            function="count",
            time_window=timedelta(days=7),
            name="transaction_count_7d"
        )
    ],
    timestamp_field="ts",
    batch_schedule=timedelta(days=1)
)
def user_transactions(transactions):
    return transactions
```

**Chronon (Equivalent):**
```python
from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, selects

v1 = GroupBy(
    sources=[
        EventSource(
            table="data.transactions",
            topic=None,  # Batch only
            query=Query(
                selects=selects(
                    "user_id",
                    "amount",
                    "transaction_id"
                ),
                time_column="unix_millis(ts)"  # Convert to milliseconds
            )
        )
    ],
    keys=["user_id"],
    aggregations=[
        Aggregation(
            input_column="amount",
            operation=Operation.SUM,
            windows=["30d"]
        ),
        Aggregation(
            input_column="transaction_id",
            operation=Operation.COUNT,
            windows=["7d"]
        )
    ],
    online=True
)
```

#### Example 2: Streaming Feature View with Transformations

**Tecton:**
```python
from tecton import stream_feature_view, StreamSource, KafkaConfig

clicks_stream = StreamSource(
    name="clicks_stream",
    stream_config=KafkaConfig(
        kafka_bootstrap_servers="kafka:9092",
        topics=["user_clicks"],
        timestamp_field="event_time"
    ),
    batch_config=FileConfig(
        uri="s3://data/clicks",
        file_format="parquet",
        timestamp_field="event_time"
    )
)

@stream_feature_view(
    source=clicks_stream,
    entities=[user],
    mode="pyspark",
    aggregations=[
        Aggregate(
            input_column=Field("is_mobile", Int64),
            function="sum",
            time_window=timedelta(days=1),
            name="mobile_clicks_1d"
        )
    ],
    timestamp_field="event_time"
)
def user_click_features(clicks_df):
    from pyspark.sql import functions as F
    return clicks_df.withColumn(
        "is_mobile",
        F.when(F.col("device_type") == "mobile", 1).otherwise(0)
    )
```

**Chronon:**
```python
from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, selects

v1 = GroupBy(
    sources=[
        EventSource(
            table="data.clicks",
            topic="kafka://user_clicks/serde=avro/schema_registry=http://registry:8081",
            query=Query(
                selects=selects(
                    "user_id",
                    # Transformation in selects, not in Python function
                    is_mobile="IF(device_type = 'mobile', 1, 0)"
                ),
                time_column="unix_millis(event_time)"
            )
        )
    ],
    keys=["user_id"],
    aggregations=[
        Aggregation(
            input_column="is_mobile",  # Reference the select column
            operation=Operation.SUM,
            windows=["1d"]
        )
    ],
    online=True
)
```

**Key difference:** Transformations move from Python/PySpark functions to SQL in `selects`.

#### Example 3: Feature Service (Multi-Source Join)

**Tecton:**
```python
from tecton import FeatureService

fraud_detection_features = FeatureService(
    name="fraud_detection",
    features=[
        user_transactions,
        user_click_features,
        user_demographics
    ]
)
```

**Chronon:**
```python
from ai.chronon.types import EventSource, Join, JoinPart, Query, selects

v1 = Join(
    left=EventSource(
        table="data.scoring_requests",
        query=Query(
            selects=selects("user_id", "request_id"),
            time_column="unix_millis(request_time)"
        )
    ),
    right_parts=[
        JoinPart(group_by=user_transactions_gb),
        JoinPart(group_by=user_click_features_gb),
        JoinPart(group_by=user_demographics_gb)
    ],
    online=True,
    output_namespace="fraud_detection"
)
```

**Key changes:**
- `FeatureService` → `Join`
- `features=[...]` → `right_parts=[JoinPart(...)]`
- Must specify a `left` source (the entity/request context)

#### Example 4: On-Demand Features with Derivations

**Tecton:**
```python
from tecton import on_demand_feature_view, RequestSource

request = RequestSource(schema=[Field("transaction_amount", Float64)])

@on_demand_feature_view(
    sources=[request, user_transactions],
    mode="python",
    features=[
        Attribute("amount_vs_avg_ratio", Float64),
        Attribute("is_high_value", Boolean)
    ]
)
def transaction_features(request_data, user_features):
    avg_spend = user_features["total_spend_30d"] / 30
    ratio = request_data["transaction_amount"] / avg_spend if avg_spend > 0 else 0
    is_high = request_data["transaction_amount"] > avg_spend * 2

    return {
        "amount_vs_avg_ratio": ratio,
        "is_high_value": is_high
    }
```

**Chronon:**
```python
from ai.chronon.types import Derivation, Join, JoinPart

v1 = Join(
    left=EventSource(
        table="data.transactions",
        query=Query(
            selects=selects(
                "user_id",
                "transaction_amount"
            ),
            time_column="unix_millis(transaction_time)"
        )
    ),
    right_parts=[
        JoinPart(group_by=user_transactions_gb)
    ],
    derivations=[
        Derivation("*", "*"),  # Keep all original columns
        # Compute average daily spend
        Derivation(
            "avg_daily_spend",
            "user_id_total_spend_30d / 30.0"
        ),
        # Ratio of current transaction to average
        Derivation(
            "amount_vs_avg_ratio",
            "transaction_amount / NULLIF(user_id_total_spend_30d / 30.0, 0)"
        ),
        # Boolean: is this a high value transaction?
        Derivation(
            "is_high_value",
            "transaction_amount > (user_id_total_spend_30d / 30.0) * 2"
        )
    ],
    online=True
)
```

**Key changes:**
- Python function → SQL `Derivation` expressions
- Request data → Left source columns
- Feature data → Right side columns (prefixed with key name, e.g., `user_id_total_spend_30d`)
- All logic expressed in SQL, not Python

---

### Tecton Migration Workflow

When converting an existing Tecton project to Chronon, follow this systematic approach:

#### Step 1: Inventory Your Tecton Features

Create a spreadsheet or document listing:
- All Entities and their join_keys
- All BatchSources and StreamSources
- All Feature Views (batch, stream, realtime, on-demand)
- All Feature Services
- Dependencies between features

#### Step 2: Convert Sources First

Start with data sources as they have no dependencies:
1. Identify all `BatchSource` and `StreamSource` definitions
2. For each source, determine if it's entity data (dimensions) or event data
3. Convert to `EntitySource` or `EventSource` accordingly
4. Ensure `time_column` produces milliseconds since epoch
5. Test table access with `zipline hub eval-table --table`

#### Step 3: Convert Bottom-Up (Leaf Features First)

Work from features with no dependencies toward features that depend on others:

**Phase 1: Simple Feature Views**
- Convert batch feature views with no aggregations → `GroupBy` with `aggregations=None`
- Convert dimension tables → `GroupBy` with `EntitySource`

**Phase 2: Aggregation Features**
- Convert stream/batch feature views with aggregations → `GroupBy` with `Aggregation`
- Map Tecton `Aggregate` to Chronon `Aggregation` with appropriate `Operation`

**Phase 3: Derived Features**
- Convert realtime/on-demand feature views → `Join` with `Derivation`
- Convert Python transformation logic to SQL expressions

**Phase 4: Feature Services**
- Convert feature services → `Join` combining multiple GroupBys

#### Step 4: Handle Special Cases

**Embeddings:**
- Tecton embedding features → Chronon `Model` + `ModelTransforms`
- May require creating separate Model definition

**Multi-Entity Features:**
- Tecton's multi-entity features → Chronon chained `GroupBy` using `JoinSource`
- Or use `Join` with multiple JoinParts

**Custom Python Transformations:**
- Tecton `mode="python"` → Chronon SQL in `selects` or `Derivation`
- Complex Python logic may require `StagingQuery` with SQL preprocessing

#### Step 5: Validate Each Component (CRITICAL)

**ALWAYS validate after converting each feature** - this is a mandatory step in the migration workflow:

```bash
# Step 1: Compile
zipline compile --chronon-root . --force

# Step 2: Validate with eval (REQUIRED)
zipline hub eval --conf compiled/group_bys/[team]/[feature_name]

# For Joins:
zipline hub eval --conf compiled/joins/[team]/[feature_name]
```

**Important eval workflow notes:**
- Eval is NOT optional - always run it after compilation
- If eval fails, debug and fix before moving forward
- Common eval failures: time column format, missing columns, type mismatches

Fix all issues before moving to dependent features.

#### Step 6: Test End-to-End

1. Backfill historical data for a small date range
2. Compare Tecton and Chronon outputs for the same entities/dates
3. Validate that aggregations match
4. Test online serving (if applicable)

#### Step 7: Gradual Rollout

- Don't migrate everything at once
- Start with non-critical features
- Run Tecton and Chronon in parallel during transition
- Gradually shift traffic to Chronon

### Common Migration Pitfalls

**Pitfall 1: Time Column Format**
- **Issue:** Tecton accepts various timestamp formats, Chronon requires milliseconds
- **Solution:** Always use `unix_millis()` or explicit conversion to milliseconds

**Pitfall 2: Aggregation Function Names**
- **Issue:** Tecton uses strings (`"sum"`), Chronon uses enums (`Operation.SUM`)
- **Solution:** Use the mapping table above, ensure imports are correct

**Pitfall 3: Window Syntax**
- **Issue:** `timedelta(days=7)` vs `"7d"`
- **Solution:** Convert all timedeltas to string format or Window objects

**Pitfall 4: Mode Parameter**
- **Issue:** Tecton's `mode="pandas"` has no Chronon equivalent
- **Solution:** Remove mode parameter, Chronon handles execution automatically

**Pitfall 5: Python Transformations**
- **Issue:** Tecton allows arbitrary Python/PySpark in transformations
- **Solution:** Convert to SQL expressions in `selects` or `Derivation`
- **Complex logic:** Use `StagingQuery` for preprocessing

**Pitfall 6: Column Name Prefixing**
- **Issue:** Chronon auto-prefixes right-side columns with key names
- **Solution:** Reference columns as `{key}_{column_name}` in derivations
- **Example:** `listing_id_price_cents` not just `price_cents`

**Pitfall 7: Request Sources**
- **Issue:** Tecton's `RequestSource` has no direct equivalent
- **Solution:** Use left side of Join as the "request" context

### Migration Checklist

Use this checklist for each Tecton feature being migrated:

- [ ] Identified Tecton feature type (batch/stream/realtime/on-demand)
- [ ] Mapped to appropriate Chronon construct (GroupBy/Join)
- [ ] Converted data sources (BatchSource/StreamSource → EntitySource/EventSource)
- [ ] Converted Entity join_keys to GroupBy keys parameter
- [ ] Converted aggregations (Aggregate → Aggregation + Operation)
- [ ] Converted time_window to windows parameter
- [ ] Ensured time_column produces milliseconds since epoch
- [ ] Converted Python transformations to SQL (selects or Derivation)
- [ ] Compiled successfully (`zipline compile --force`)
- [ ] Validated with eval server
- [ ] Tested with sample data
- [ ] Compared output with Tecton version (if applicable)
- [ ] Documented any semantic differences


## Part 8: Running Trials and Experiments

A **trial** is one attempt at a modelling task. The **experiments** inside it are the
variants you try. Hub stores both, so the next agent — or the human reviewing you — can
see what was already attempted and how each variant scored without re-reading a
transcript.

### When to open one, without being asked

A trial is not a thing the user requests by name. They ask a modelling question, and the
trial is how you answer it so the answer survives. **Open one whenever you are about to
train more than once to settle a question**, before running anything.

These are all trial-shaped, however they are phrased:

- "Does adding X help the CTR model?" — baseline and X are two experiments.
- "Try a 7d and a 30d window and tell me which is better." — two experiments.
- "Can you get the AUC above 0.78?" — every attempt is an experiment; the trial is the
  hunt.
- "Tune the hyperparameters on this model." — one experiment per setting tried.

What makes them trial-shaped is not the word "experiment": it is that **a comparison is
the deliverable**. If the answer is "A scored better than B", the thing that has to
outlive the session is A's number, B's number, and what differed between them. That is
what a trial holds.

One training run to see if something works at all is not a trial. Neither is a backfill.
If nothing is being compared, there is nothing for the trial to be about.

So the shape of the work, when the question is trial-shaped, is:

1. Check the journal — someone may have answered this already.
2. Open a trial whose task is the question, in the user's words.
3. One experiment per variant, recorded **before** it runs.
4. Read each one's metrics from its own report.
5. Say what you concluded, in the summary and the journal.

Do this without being told to. A user who asks "does purchase history help?" and gets
back a number in the chat has been answered; a user who gets back a trial has been
answered in a way the next person can check.

### The rule that matters most

**Never type a metric yourself.** There is deliberately no flag for it. You name the
model, and Hub finds that run's report in object storage and reads the numbers out of it:

```bash
zipline hub trial experiments metrics exp-30d \
    --model-conf-name aws_databricks.ctr_ray.ctr_model_30d__afce3c0e00af
```

A metric an agent typed is a claim; a metric read from the trainer's own output is a
measurement. Only the second is worth comparing against. If the run has not finished
there is nothing to read yet — wait for it rather than paraphrasing the logs.

### The other rule that matters most

**One model name per variant.** Two experiments that name the same model read the same
report, so the trial ends up showing one measurement twice, under two names, as though
the variants had been compared. That is the same failure as typing a metric by hand —
a number filed against something it does not describe — and it is harder to spot,
because both rows look like real results.

Hub refuses it rather than storing it:

```
Report s3://…/experiment_report.json already holds the metrics for experiment
exp-baseline on this trial. Give exp-7d its own model version so its run writes a
separate report, otherwise both experiments describe the same training run.
```

If you see that, you ran the second variant against the first one's model. Give it its
own name, re-run it, and read the metrics again — see *Giving a variant somewhere of its
own to write* below for the archive step that goes with a new name.

### Before you start: has this been tried?

The journal crosses scopes on purpose, so you can find another team's attempt at the
same question:

```bash
zipline hub trial journal list --run-name ctr-features
```

### The loop

```bash
# 1. Open a trial. Re-using a run name starts a NEW attempt; it does not resume.
#    --model names the model under study. Pass a real conf name — usually the committed
#    one you are iterating on — because the model's page lists its trials by this value
#    and an invented family name links nowhere. The variants below each get their own
#    conf; this one says what the trial is about.
zipline hub trial start ctr-features --task "Does a 30d window beat 7d for CTR?" \
    --model aws_databricks.ctr_ray.ctr_ray_model__afce3c0e00af
# → prints: export ZIPLINE_TRIAL_ID=<id>

# 2. Export it once. Every later verb falls back to this, so the link cannot be forgotten.
export ZIPLINE_TRIAL_ID=<id>

# 3. Describe the variant BEFORE running it, so a crash still leaves a record.
#    Note the model name: each variant gets its OWN, for the reason below.
zipline hub trial experiments record exp-7d --title "7 day window" --status RUNNING \
    --model-conf-name aws_databricks.ctr_ray.ctr_model_7d__afce3c0e00af

# 4. Train. The workflow picks the trial up from the environment; you do not pass it.
ZIPLINE_EXPERIMENT_ID=exp-7d zipline hub backfill \
    compiled/models/aws_databricks/ctr_ray.ctr_model_7d__afce3c0e00af \
    --start-ds 2026-08-24 --end-ds 2026-08-24

# 5. Repeat 3–4 for the other variant — under its own model name — then read results
#    from each trainer's own report.
zipline hub trial experiments metrics exp-7d  --model-conf-name aws_databricks.ctr_ray.ctr_model_7d__afce3c0e00af
zipline hub trial experiments metrics exp-30d --model-conf-name aws_databricks.ctr_ray.ctr_model_30d__afce3c0e00af

# 6. Compare, then say what you concluded.
zipline hub trial experiments list
zipline hub trial summary "30d lifted AUC 0.71 → 0.74; 7d is not worth keeping."
zipline hub trial status SUCCEEDED

# 7. Leave the retrospective for whoever comes next.
zipline hub trial journal append "30d window wins on AUC. Longer windows untested."
```

### Giving a variant somewhere of its own to write

Two variants that run the same trainer and differ only in configuration — different
features, different hyperparameters — have identical trainer bytes, so they share a
version. The version is a content address of the archive and nothing else; it does not
see your config.

That matters because a run's output directory is `{base}/models/{model_name}/{version}`.
Under one name, two variants overwrite each other's report, and a comparison between them
reads whichever finished last, twice. **So give each variant its own model name**, not its
own version.

Then the step that is easy to miss, and that will kill the run before training starts:

**A new model name needs the trainer archive published under that name.** The archive is
fetched from `{base}/builds/{model_name}-{version}.zip`, so the key is scoped by name.
Point a new name at an archive that isn't there and Ray fails setting up its runtime
environment, with an error about the bucket rather than about your model:

```
OSError: unable to access bucket: 'zipline-warehouse-crucible'
key: 'builds/my_new_variant-afce3c0e00af.zip' ... (NoSuchKey)
```

If the trainer is unchanged, copy the existing archive rather than rebuilding — same bytes
mean the same version, which is what lets the two variants stay comparable:

```bash
aws s3 cp s3://{base}/builds/{existing_model}-{version}.zip \
          s3://{base}/builds/{new_model}-{version}.zip
```

If you changed the trainer, rebuild it instead; the version moves with the bytes, and the
two variants are then no longer running the same code — which is usually not what a
controlled comparison wants.

### Partial writes are safe

`experiments record` merges: fields you leave out keep whatever is already stored, so
recording a status later does not blank the title or feature list you set earlier. The
trial's own fields are split into separate verbs (`status`, `summary`, `plan`, `diff`)
for the same reason — there is no catch-all verb that could silently clear a field you
did not mention.

### Machine-readable output

Every trial verb takes `--format json`, which is what to use when parsing rather than
showing:

```bash
zipline hub trial experiments list --format json
```

### Reminders

- One trial per question, not per day. If the question changes, start a new trial.
- One model name per variant. Reusing a name across experiments in a trial is refused,
  because both would read the same report and the comparison would be with itself.
- Pass `--model` when you open the trial, and pass a conf that exists — usually the one
  you are iterating on. The per-variant names all differ by design, so without it nothing
  records which model the trial was about, and the model page cannot list it.
- `--scope` defaults to the repo root, and that is what groups attempts together.
- Record the variant before running it, not after. A record written only on success
  loses exactly the runs you most want to learn from.

## Important Notes

- **Always ask before assuming**: Especially for source type, PK, and time column
- **Time column is critical**: Most bugs come from incorrect time handling
- **Keys must match**: Format, type, and values must align
- **ALWAYS compile then eval after EVERY file change**: Run `zipline compile --force` then `zipline hub eval` immediately after writing or editing any config file. In multi-file workflows, this means running eval after EACH file — not just at the end. Never proceed to the next file until eval passes on the current one.
- **Be cautious with production**: Version instead of modifying
- **If CLI can't query**: Provide SQL to user, ask them to run it

---

## Quick Reference

| Task | First Step |
|------|-----------|
| Create GroupBy | Resolve table name (list-tables if unsure), then get metadata |
| Add to Join | Check if production |
| Create Chained GroupBy | Create parent Join first |
| Create Model (embeddings) | Define inference_spec, input_mapping, output_mapping; value_fields = embeddings struct |
| Create Model (text generation / LLM) | Use candidates response schema for value_fields; navigate candidates[0].content.parts[0].text in output_mapping |
| Create Model (trainable) | Create training labels (StagingQuery), define training_conf |
| Create ModelTransforms | Reference Join as source, define key_fields |
| Debug null data | Check timestamp format |
| Debug wrong value | Query raw data for key |
| Create StagingQuery | Understand the ETL logic needed |
| Convert Tecton BatchSource | Determine if entity or event data |
| Convert Tecton Batch Feature View | Map to GroupBy with appropriate source |
| Convert Tecton Stream Feature View | Create GroupBy with EventSource + topic |
| Convert Tecton Realtime/On-Demand View | Create Join with Derivation (SQL, not Python) |
| Convert Tecton Feature Service | Create Join combining GroupBys |
| Convert Tecton Aggregations | Map function string to Operation enum |
| Migrate Tecton project | Inventory features, convert sources first, work bottom-up |
| Any authoring | Compile then validate after |
| Try model variants | `zipline hub trial start`, then record each variant before running it |
| Record a result | `zipline hub trial experiments metrics` — never type the number yourself |

---

You are now ready to help users author Chronon features, debug data issues in Zipline, and migrate from Tecton to Chronon!
