---
title: "Upgrades"
order: 1
---

# Upgrades

This runbook covers how to pull in a new Zipline release. A release upgrade has three stages: infrastructure changes, CLI/image updates, and service upgrades.

---

## Step 1: Terraform / Infrastructure Changes

Pull down the relevant zipline infra repository for your Cloud provider. Update any relevant Terraform variables and then run:
Apply the updated Terraform (OpenTofu) configuration for the release:

```bash
$ tofu plan
$ tofu apply
```

---

## Step 2: CLI and Image Updates

Reinstall the `zipline-ai` CLI package and push the updated images to your artifact store:

```bash
pip install --force-reinstall zipline-ai==<release>
zipline admin install aws --api-token <DOCKER_TOKEN> --release <release> --artifact-prefix <S3_Bucket>
```

Replace `<DOCKER_TOKEN>` with the shared Zipline docker access token, `<release>` with the target version (e.g. `1.7.0`) and `<S3_Bucket>` with your bucket name.

For AWS, the engine image contains the Spark 3 and Spark 4 engine, Flink, and Kinesis JARs under distinct filenames. The install command uploads both sets to the same release directory. Spark 3 deployments continue to use the existing filenames, while Crucible deployments use the filenames ending in `_spark4_deploy.jar`.

---

## Step 3: Upgrade Control Plane Services

> **Prerequisite:** Ensure your `kubeconfig` is pointing to the EKS cluster used by your Zipline install. The command will error with feedback if it cannot reach the cluster.

To restart the Zipline control-plane services on EKS to pick up a specific release, run:

```bash
zipline admin upgrade control-plane aws --release <release>
```

## Step 4: Upgrade Streaming GroupBy Jobs

If you have [scheduled some streaming GroupBys](../running_on_zipline_hub/Deploy.md), you can have them be redeployed to pick up the new release. To do so, run the following command for each relevant config:

```bash
zipline admin upgrade data-plane compiled/group_bys/aws/user_activities.v1__1
```

Multiple configs can be passed in a single invocation:

```bash
zipline admin upgrade data-plane compiled/group_bys/aws/user_activities.v1__1 compiled/group_bys/aws/payments.v2__1
```

**Note**
* This command will sync the specified configs to Hub and trigger a restart of the streaming job(s) off the VERSION specified in the config.
* To trigger a deployment for a specific version (e.g. to roll back to a previous release), update the VERSION env variable in either teams.py or in the specific config(s) and then run the command.
