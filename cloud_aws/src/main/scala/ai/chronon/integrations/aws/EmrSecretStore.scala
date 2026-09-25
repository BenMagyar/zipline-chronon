package ai.chronon.integrations.aws

import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.{
  CreateSecretRequest,
  GetSecretValueRequest,
  PutResourcePolicyRequest,
  PutSecretValueRequest,
  ResourceNotFoundException,
  Tag
}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

/** Keeps submitter-injected secrets out of the EMR Serverless StartJobRun request.
  *
  * EMR Serverless echoes the full job configuration from GetJobRun, the console and
  * CloudTrail, so any credential the submitter expands into a Spark property is exposed
  * to anyone who can read job runs. EMR's escape hatch is the `EMR.secret@<name>`
  * annotation: the property carries a Secrets Manager reference and EMR resolves it on
  * the workers at job start.
  *
  * This store writes each protected value to a secret named by a hash of the property
  * key (stable across rotation) under the owning application, creates it on first use
  * and only rewrites it when the value changes.
  */
class EmrSecretStore(
    client: SecretsManagerClient,
    kmsKeyId: String,
    prefix: String,
    awsRegion: String,
    accountId: String
) {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  // secret name -> sha256 of the value last confirmed to be stored there
  private val known = new ConcurrentHashMap[String, String]()

  private[aws] def secretName(applicationId: String, propertyKey: String): String =
    s"$prefix/$applicationId/${EmrSecretStore.sha256Hex(propertyKey).take(24)}"

  private[aws] def applicationArn(applicationId: String): String =
    s"arn:aws:emr-serverless:$awsRegion:$accountId:/applications/$applicationId"

  /** Stores `value` for `propertyKey` under `applicationId` if it is not already there
    * and returns the `EMR.secret@` reference to put in the Spark property instead.
    */
  def reference(applicationId: String, propertyKey: String, value: String): String = {
    val name = secretName(applicationId, propertyKey)
    val digest = EmrSecretStore.sha256Hex(value)
    if (known.get(name) != digest) {
      ensureStored(name, applicationId, propertyKey, value, digest)
      known.put(name, digest)
    }
    s"${EmrSecretStore.ReferencePrefix}$name"
  }

  private def ensureStored(name: String,
                           applicationId: String,
                           propertyKey: String,
                           value: String,
                           digest: String): Unit = {
    val current =
      try Some(client.getSecretValue(GetSecretValueRequest.builder().secretId(name).build()).secretString())
      catch { case _: ResourceNotFoundException => None }

    current match {
      case Some(existing) if EmrSecretStore.sha256Hex(existing) == digest =>
        logger.info(s"EMR secret $name for '$propertyKey' is current")
      case Some(_) =>
        logger.info(s"EMR secret $name for '$propertyKey' changed; writing new value")
        client.putSecretValue(PutSecretValueRequest.builder().secretId(name).secretString(value).build())
      case None =>
        logger.info(s"Creating EMR secret $name for '$propertyKey'")
        client.createSecret(
          CreateSecretRequest
            .builder()
            .name(name)
            .kmsKeyId(kmsKeyId)
            .secretString(value)
            .description(s"Chronon EMR Serverless job property $propertyKey")
            .tags(Tag.builder().key("chronon:property").value(propertyKey).build(),
                  Tag.builder().key("chronon:application").value(applicationId).build())
            .build()
        )
        client.putResourcePolicy(
          PutResourcePolicyRequest
            .builder()
            .secretId(name)
            .resourcePolicy(EmrSecretStore.readPolicy(applicationArn(applicationId)))
            .blockPublicPolicy(true)
            .build()
        )
    }
  }
}

object EmrSecretStore {
  val ReferencePrefix = "EMR.secret@"
  val KmsKeyIdEnvVar = "EMR_SECRETS_KMS_KEY_ID"
  val PrefixEnvVar = "EMR_SECRETS_PREFIX"
  val DefaultPrefix = "chronon"

  private val AccountFromArn = """arn:aws:iam::(\d{12}):.*""".r

  def apply(kmsKeyId: String, prefix: String, awsRegion: String, executionRoleArn: String): EmrSecretStore = {
    val accountId = executionRoleArn match {
      case AccountFromArn(account) => account
      case other =>
        throw new IllegalArgumentException(s"Cannot derive AWS account id from execution role ARN '$other'")
    }
    new EmrSecretStore(SecretsManagerClient.create(), kmsKeyId, prefix, awsRegion, accountId)
  }

  def fromEnv(awsRegion: String, executionRoleArn: String): Option[EmrSecretStore] =
    sys.env.get(KmsKeyIdEnvVar).filter(_.nonEmpty).map { kmsKeyId =>
      apply(kmsKeyId, sys.env.getOrElse(PrefixEnvVar, DefaultPrefix), awsRegion, executionRoleArn)
    }

  private[aws] def readPolicy(applicationArn: String): String =
    s"""{
       |  "Version": "2012-10-17",
       |  "Statement": [{
       |    "Sid": "AllowEmrServerlessRead",
       |    "Effect": "Allow",
       |    "Principal": {"Service": "emr-serverless.amazonaws.com"},
       |    "Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
       |    "Resource": "*",
       |    "Condition": {"StringEquals": {"aws:SourceArn": "$applicationArn"}}
       |  }]
       |}""".stripMargin

  private[aws] def sha256Hex(s: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(s.getBytes(StandardCharsets.UTF_8))
      .map("%02x".format(_))
      .mkString
}
