# Databricks Unity Catalog Integration Guide

This guide covers how to run Chronon staging query backfills that read from Databricks Unity Catalog (UC) Delta tables and write back as Iceberg tables, using EMR for Spark execution.

## Architecture Overview

Chronon uses a dual-catalog Spark configuration:
- **Read catalog** (`read_catalog`): Uses `UCSingleCatalog` to read Delta tables from Databricks UC
- **Write catalog** (`write_catalog`): Uses Iceberg REST catalog to write Iceberg tables back to Databricks UC

Authentication uses a Databricks service principal. The Chronon hub exchanges the service principal's client credentials for a short-lived OAuth token at each job submission.

## 1. Prerequisites

- Databricks workspace on AWS with Unity Catalog enabled
- Delta tables in UC that you want to backfill
- EMR cluster for Spark job execution
- Chronon hub deployed
- A Databricks service principal (created in the Databricks account console)

## 2. Databricks Setup

### 2a. Storage Credential & IAM Role

Create an IAM role that Databricks UC will use to access S3. The role needs two policy configurations:

**Permissions policy** — S3 access + self-assume:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::<YOUR_BUCKET>",
                "arn:aws:s3:::<YOUR_BUCKET>/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "arn:aws:iam::<YOUR_ACCOUNT_ID>:role/<THIS_ROLE_NAME>"
        }
    ]
}
```

> The `sts:AssumeRole` self-assume statement is **required**. UC uses it to generate scoped temporary credentials (credential vending). Without it, writes will fail with "no session policy allows s3:PutObject".

**Trust policy** — allow Databricks UC master role and self-assume:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::<YOUR_ACCOUNT_ID>:role/<THIS_ROLE_NAME>",
                    "arn:aws:iam::<YOUR_ACCOUNT_ID>:role/<UC_MASTER_ROLE>"
                ]
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<YOUR_UC_EXTERNAL_ID>"
                }
            }
        }
    ]
}
```

Create the storage credential in Databricks:

```sql
CREATE STORAGE CREDENTIAL <credential_name>
WITH (AWS_IAM_ROLE = 'arn:aws:iam::<YOUR_ACCOUNT_ID>:role/<THIS_ROLE_NAME>');
```

> Ref: [Create a storage credential and external location for S3](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual)

### 2b. External Location

Create an external location that covers the S3 path for your managed tables:

```sql
CREATE EXTERNAL LOCATION <location_name>
URL 's3://<YOUR_BUCKET>/<PATH_PREFIX>'
WITH (STORAGE CREDENTIAL <credential_name>);
```

> Ref: [Connect to cloud object storage using Unity Catalog](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/storage-credentials)

### 2c. Enable External Data Access on Metastore

This is **disabled by default** and must be enabled for credential vending to work:

1. In the Databricks workspace, go to **Catalog**
2. Click the **gear icon** and select **Metastore**
3. On the **Details** tab, toggle on **"External data access"**

> Without this, credential vending will silently produce credentials with empty session policies, causing S3 permission errors.
>
> Ref: [Enable external data access to Unity Catalog](https://docs.databricks.com/aws/en/external-access/admin)

### 2d. Service Principal Permissions

Grant the following permissions to your service principal. Use the service principal's **Application ID** (UUID), not the display name. Wrap it in backticks in SQL:

```sql
-- Schema access
GRANT USE CATALOG ON CATALOG <catalog_name> TO `<SP_APPLICATION_ID>`;
GRANT USE SCHEMA ON SCHEMA <catalog_name>.<schema_name> TO `<SP_APPLICATION_ID>`;

-- Credential vending (required for external engine access)
-- IMPORTANT: NOT included in ALL PRIVILEGES, must be granted explicitly by catalog owner
GRANT EXTERNAL USE SCHEMA ON SCHEMA <catalog_name>.<schema_name> TO `<SP_APPLICATION_ID>`;

-- External location access
GRANT EXTERNAL USE LOCATION ON EXTERNAL LOCATION <location_name> TO `<SP_APPLICATION_ID>`;
GRANT READ FILES ON EXTERNAL LOCATION <location_name> TO `<SP_APPLICATION_ID>`;
GRANT WRITE FILES ON EXTERNAL LOCATION <location_name> TO `<SP_APPLICATION_ID>`;

-- Read access on source tables
GRANT SELECT ON TABLE <catalog_name>.<schema_name>.<source_table> TO `<SP_APPLICATION_ID>`;
```

> **Key gotcha**: `EXTERNAL USE SCHEMA` is required for credential vending but is NOT included in `ALL PRIVILEGES` and is not granted by default even to schema owners or metastore admins. Only the parent catalog owner can grant it.
>
> Ref: [Unity Catalog credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)
> Ref: [Unity Catalog privileges and securable objects](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges)

### 2e. Schema Setup

Create a schema with a managed location for the output tables:

```sql
CREATE SCHEMA IF NOT EXISTS <catalog_name>.<schema_name>
MANAGED LOCATION 's3://<YOUR_BUCKET>/<PATH_PREFIX>/<schema_name>/';
```

Output Iceberg tables will be created automatically by the hub when staging queries run. The service principal's token is used for table creation, so the SP will be the table owner and will have `MODIFY` access automatically.

## 3. Chronon Configuration

### 3a. Team Configuration (teams.py)

Add a team with the dual-catalog configuration. Replace the placeholder values with your environment:

```python
from ai.chronon.repo.cluster import generate_emr_cluster_config
from ai.chronon.types import ClusterConfigProperties, ConfigProperties, EnvironmentVariables

my_team = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={
            "CLOUD_PROVIDER": "aws",
            "CUSTOMER_ID": "<YOUR_CUSTOMER_ID>",
            "VERSION": "latest",
            "AWS_REGION": "<YOUR_AWS_REGION>",
            "SPARK_CLUSTER_NAME": "<YOUR_EMR_CLUSTER>",
            "ARTIFACT_PREFIX": "s3://<YOUR_ARTIFACTS_BUCKET>",
            "WAREHOUSE_PREFIX": "s3://<YOUR_WAREHOUSE_BUCKET>",
            # DATABRICKS_HOST triggers OAuth token generation in the hub
            "DATABRICKS_HOST": "https://<YOUR_WORKSPACE>.cloud.databricks.com",
            "FRONTEND_URL": "<YOUR_FRONTEND_URL>",
            "HUB_URL": "<YOUR_HUB_URL>",
        },
    ),
    conf=ConfigProperties(
        common={
            # --- Spark Extensions ---
            # Order is required: Chronon data skipping, Delta, Chronon write fix, Iceberg
            "spark.sql.extensions": (
                "ai.chronon.spark.extensions.ChrononDeltaDataSkippingExtension,"
                "io.delta.sql.DeltaSparkSessionExtension,"
                "ai.chronon.spark.extensions.ChrononDeltaFixExtension,"
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.deletionVectors.enabled": "false",

            # --- Read Catalog (Delta via UCSingleCatalog) ---
            # Reads Delta tables from Databricks UC
            "spark.sql.catalog.read_catalog": "io.unitycatalog.spark.UCSingleCatalog",
            "spark.sql.catalog.read_catalog.uri": (
                "https://<YOUR_WORKSPACE>.cloud.databricks.com/api/2.1/unity-catalog"
            ),
            "spark.sql.catalog.read_catalog.token": "${DATABRICKS_OAUTH_TOKEN}",

            # --- Write Catalog (Iceberg via REST) ---
            # Writes Iceberg tables back to Databricks UC
            "spark.sql.catalog.write_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.write_catalog.type": "rest",
            "spark.sql.catalog.write_catalog.uri": (
                "https://<YOUR_WORKSPACE>.cloud.databricks.com"
                "/api/2.1/unity-catalog/iceberg-rest"
            ),
            "spark.sql.catalog.write_catalog.token": "${DATABRICKS_OAUTH_TOKEN}",
            "spark.sql.catalog.write_catalog.warehouse": "<YOUR_UC_CATALOG_NAME>",

            # --- Chronon Write Config ---
            "spark.chronon.partition.column": "ds",
            "spark.chronon.partition.format": "yyyy-MM-dd",

        },
    ),
    clusterConf=ClusterConfigProperties(
        common={
            "emr.config": generate_emr_cluster_config(
                instance_count=3,
                subnet_name="<YOUR_SUBNET>",
                security_group_name="<YOUR_SG>",
                instance_type="m5.xlarge",
                idle_timeout=300,
                release_label="emr-7.12.0"
            )
        }
    ),
)
```

**Key configuration notes:**
- `${DATABRICKS_OAUTH_TOKEN}` is a placeholder resolved by the hub at job submission time. Do not replace it with an actual token.
- The `read_catalog` and `write_catalog` names can be changed, but must match in both `teams.py` and your staging queries.
- `write_catalog.warehouse` should be set to the name of your Databricks UC catalog (e.g., `workspace`).

### 3b. Staging Query Configuration

Create a staging query that reads from the read catalog and writes through the write catalog:

```python
# staging_queries/<team_folder>/exports.py
from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency

my_table = StagingQuery(
    query="""
    SELECT
        *
    FROM read_catalog.<schema>.<table_name>
    WHERE
    ds BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    # output_namespace uses the WRITE catalog
    output_namespace="write_catalog.<schema>",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(
            table="read_catalog.<schema>.<table_name>",
            partition_column="ds",
            offset=0
        )
    ],
    version=0,
)
```

**Key points:**
- Source tables use the **read catalog** prefix: `read_catalog.<schema>.<table>`
- `output_namespace` uses the **write catalog** prefix: `write_catalog.<schema>`
- The output table name follows the convention: `<team>_<folder>_<variable>__<version>` (e.g., `my_team_exports_my_table__0`)

## 4. Hub Setup

The hub needs the service principal's client credentials as environment variables. It will exchange these for a short-lived OAuth token (1-hour validity) at each job submission.

Set these on the hub process (e.g., in docker-compose):

```yaml
# docker-compose environment section for the hub service
environment:
  DATABRICKS_CLIENT_ID: ${DATABRICKS_CLIENT_ID}
  DATABRICKS_CLIENT_SECRET: ${DATABRICKS_CLIENT_SECRET}
```

Then export them before starting the hub:

```bash
export DATABRICKS_CLIENT_ID=<your-service-principal-application-id>
export DATABRICKS_CLIENT_SECRET=<your-service-principal-secret>
```

The hub will automatically:
1. Detect `DATABRICKS_HOST` in the node's environment config
2. Read `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` from its own process environment
3. Call `POST https://<host>/oidc/v1/token` with client credentials grant
4. Inject the resulting `DATABRICKS_OAUTH_TOKEN` into the job's environment
5. Resolve `${DATABRICKS_OAUTH_TOKEN}` placeholders in the Spark configuration

## 5. Troubleshooting

### "no session policy allows s3:PutObject"

The S3 vended credentials from UC have an empty or read-only session policy. Check in order:

1. **External data access enabled?** — Metastore → Details → "External data access" toggle must be ON
2. **IAM self-assume?** — The storage credential's IAM role permissions policy must include `sts:AssumeRole` on itself
3. **EXTERNAL USE SCHEMA granted?** — Must be explicitly granted by the catalog owner. NOT included in `ALL PRIVILEGES`, not granted to schema owners or metastore admins by default
4. **EXTERNAL USE LOCATION granted?** — Also must be explicitly granted
5. **READ FILES + WRITE FILES on external location?** — Both needed

### "PERMISSION_DENIED: User does not have MODIFY on Table"

The service principal needs `MODIFY` permission on the output table. If the table was created by a different user, transfer ownership:

```sql
ALTER TABLE <catalog>.<schema>.<table> OWNER TO `<SP_APPLICATION_ID>`;
```

### "PRINCIPAL_DOES_NOT_EXIST" when granting permissions

Use the service principal's **Application ID** (UUID format), not the display name. Wrap in backticks:

```sql
-- Correct: use Application ID (UUID)
GRANT ... TO `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`;

-- Wrong: display name won't work
GRANT ... TO `my-service-principal`;
```

### Authentication errors

- Verify `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` are set as environment variables on the hub process
- Test the token endpoint manually:
  ```bash
  curl -X POST "https://<YOUR_WORKSPACE>.cloud.databricks.com/oidc/v1/token" \
    -u "<client_id>:<client_secret>" \
    -d "grant_type=client_credentials&scope=all-apis"
  ```
- Verify the service principal is added to the workspace

## References

- [Storage credential & external location setup (S3)](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual)
- [Storage credentials overview](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/storage-credentials)
- [Enable external data access](https://docs.databricks.com/aws/en/external-access/admin)
- [Credential vending for external systems](https://docs.databricks.com/aws/en/external-access/credential-vending)
- [Access tables from Iceberg clients](https://docs.databricks.com/aws/en/external-access/iceberg)
- [Unity Catalog privileges and securable objects](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges)
- [IAM role self-assume requirement](https://community.databricks.com/t5/product-platform-updates/update-your-uc-aws-iam-roles-to-include-self-assume-capabilities/ba-p/67347)
- [Service principal OAuth M2M](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m)
- [Manage service principals](https://docs.databricks.com/aws/en/admin/users-groups/manage-service-principals)
