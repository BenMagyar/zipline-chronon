# Manual spark submission of staging query for debugging.
set -eux
export OC_CREDENTIAL=$OC_CREDENTIAL
export OC_ROLE=$OC_ROLE
export OC_ACCOUNT=$OC_ACCOUNT
export SNOWFLAKE_PRIVATE_KEY="$(cat private_key.logs)"
spark-submit \
      --class ai.chronon.spark.Driver \
      --master "local[1]" \
      --driver-memory 1g \
      --executor-memory 1g \
      --conf spark.app.name=spark-azure_exports_dim_listings__0__staging-local-test \
      --conf spark.chronon.coalesce.factor=10 \
      --conf spark.chronon.partition.column=ds \
      --conf spark.chronon.partition.format=yyyy-MM-dd \
      --conf spark.default.parallelism=10 \
      --conf spark.executor.cores=1 \
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      --conf spark.kryo.registrator=ai.chronon.spark.submission.ChrononKryoRegistrator \
      --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.spark_catalog.credential=${OC_CREDENTIAL-client_id:client_secret} \
      --conf spark.sql.catalog.spark_catalog.header.X-Iceberg-Access-Delegation=vended-credentials \
      --conf spark.sql.catalog.spark_catalog.scope=PRINCIPAL_ROLE:${OC_ROLE-all} \
      --conf spark.sql.catalog.spark_catalog.type=rest \
      --conf spark.sql.catalog.spark_catalog.uri=https://${OC_ACCOUNT-account}.snowflakecomputing.com/polaris/api/catalog \
      --conf spark.sql.catalog.spark_catalog.warehouse=demo-v2 \
      --conf spark.sql.shuffle.partitions=10 \
      --conf spark.zipline.label.branch=main \
      --conf spark.zipline.label.job-type=spark \
      --conf spark.zipline.label.metadata-name=azure.exports.dim_listings__0__staging \
      --conf spark.zipline.label.zipline-version=latest \
      out/cloud_azure/assembly.dest/out.jar \
      staging-query-backfill \
      --conf-path=python/test/canary/compiled/staging_queries/azure/exports.dim_listings__0 \
      --start-partition=2026-01-01 \
      --end-date=2026-01-25 \
      --force-overwrite \
