package ai.chronon.spark.batch

import ai.chronon.api.secrets.SecretResolver
import ai.chronon.api.secrets.VaultSecretProvider
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JobSecretsSpec extends AnyFlatSpec with Matchers {

  private val SecretArn = "arn:aws:secretsmanager:us-west-2:1:secret:canary-zipline-databricks-client-secret"
  private val CredentialArn = "arn:aws:secretsmanager:us-west-2:1:secret:canary-zipline-databricks-credential"

  private val DatabricksConf = Map(
    "spark.sql.catalog.workspace.auth.oauth.clientId" -> "{DATABRICKS_CLIENT_ID}",
    "spark.sql.catalog.workspace.auth.oauth.clientSecret" -> "{DATABRICKS_CLIENT_SECRET}",
    "spark.sql.catalog.workspace_iceberg.credential" -> "{DATABRICKS_CREDENTIAL}",
    "spark.app.name" -> "unchanged"
  )

  private def refsFrom(values: Map[String, String]): Map[String, String] => Map[String, String] =
    env => SecretResolver.resolveVaultUris(env, uri => ((_: String) => values(uri)): VaultSecretProvider)

  "JobSecrets.resolve" should "fill placeholders from vault refs and plain env values" in {
    val env = Map(
      "DATABRICKS_CLIENT_ID" -> "sp-id",
      "DATABRICKS_CLIENT_SECRET_VAULT_URI" -> SecretArn,
      "DATABRICKS_CREDENTIAL_VAULT_URI" -> CredentialArn
    )
    val resolved =
      JobSecrets.resolve(env, DatabricksConf, refsFrom(Map(SecretArn -> "sp-secret", CredentialArn -> "sp-id:sp-secret")))

    resolved.sparkConfOverrides shouldBe Map(
      "spark.sql.catalog.workspace.auth.oauth.clientId" -> "sp-id",
      "spark.sql.catalog.workspace.auth.oauth.clientSecret" -> "sp-secret",
      "spark.sql.catalog.workspace_iceberg.credential" -> "sp-id:sp-secret"
    )
    resolved.secrets shouldBe Map("DATABRICKS_CLIENT_SECRET" -> "sp-secret", "DATABRICKS_CREDENTIAL" -> "sp-id:sp-secret")
  }

  it should "fail naming the placeholder when its configured ref cannot be fetched" in {
    val env = Map("DATABRICKS_CLIENT_ID" -> "sp-id", "DATABRICKS_CLIENT_SECRET_VAULT_URI" -> SecretArn)
    val conf = DatabricksConf - "spark.sql.catalog.workspace_iceberg.credential"
    val error = intercept[IllegalStateException] {
      JobSecrets.resolve(env, conf, refsFrom(Map.empty)) // provider throws -> resolver skips it
    }
    error.getMessage should include("{DATABRICKS_CLIENT_SECRET}")
    error.getMessage should include("spark.sql.catalog.workspace.auth.oauth.clientSecret")
  }

  it should "leave placeholders without a value or ref unfilled rather than fail" in {
    val conf = Map("spark.some.path" -> "{UNKNOWN_DIR}/x", "spark.catalog.x.credential" -> "{NO_REF}")
    JobSecrets.resolve(Map.empty, conf, identity).sparkConfOverrides shouldBe empty
  }

  it should "start a job that doesn't use secrets even when unrelated refs fail" in {
    // Shape of the canary `aws` team's StartJobRun: no secret placeholders in Spark conf, a RAY_
    // placeholder in env fan-out, and a ref no Spark property uses.
    val env = Map(
      "UNUSED_TOKEN_VAULT_URI" -> "arn:aws:secretsmanager:us-west-2:1:secret:missing",
      "AWS_REGION" -> "us-west-2"
    )
    val conf = Map(
      "spark.sql.catalog.spark_catalog" -> "org.apache.iceberg.spark.SparkSessionCatalog",
      "spark.sql.catalog.spark_catalog.catalog-impl" -> "org.apache.iceberg.aws.glue.GlueCatalog",
      "spark.hadoop.fs.s3a.aws.credentials.provider" -> "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
      "spark.emr-serverless.driverEnv.RAY_DATABRICKS_CLIENT_SECRET" -> "{DATABRICKS_CLIENT_SECRET}",
      "spark.executorEnv.RAY_DATABRICKS_CLIENT_SECRET" -> "{DATABRICKS_CLIENT_SECRET}"
    )

    val resolved = JobSecrets.resolve(env, conf, refsFrom(Map.empty))

    resolved.sparkConfOverrides shouldBe empty
    resolved.secrets shouldBe empty
  }

  it should "ignore env fan-out properties, which only carry env vars to processes" in {
    val env = Map("X_TOKEN" -> "value")
    val conf = Map(
      "spark.emr-serverless.driverEnv.RAY_X" -> "{X_TOKEN}",
      "spark.executorEnv.RAY_X" -> "{X_TOKEN}",
      "spark.kubernetes.driverEnv.RAY_X" -> "{X_TOKEN}",
      "spark.yarn.appMasterEnv.RAY_X" -> "{X_TOKEN}"
    )
    JobSecrets.resolve(env, conf, identity).sparkConfOverrides shouldBe empty
  }

  it should "prefer values already in env over refs" in {
    val env = Map("DATABRICKS_CLIENT_SECRET" -> "env-secret", "DATABRICKS_CLIENT_SECRET_VAULT_URI" -> SecretArn)
    val resolved = JobSecrets.resolve(env,
                                      Map("k" -> "{DATABRICKS_CLIENT_SECRET}"),
                                      refsFrom(Map(SecretArn -> "vault-secret")))
    resolved.sparkConfOverrides shouldBe Map("k" -> "env-secret")
  }

  it should "insert secret values containing $ and backslashes literally" in {
    val arn = "arn:aws:secretsmanager:us-west-2:1:secret:pw"
    val resolved = JobSecrets.resolve(Map("PW_VAULT_URI" -> arn), Map("k" -> "{PW}"), refsFrom(Map(arn -> """a$1\b""")))
    resolved.sparkConfOverrides shouldBe Map("k" -> """a$1\b""")
  }
}
