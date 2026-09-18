FROM scratch

ARG VERSION="dev"

# CI prepares build_output/engine/ with cloud-specific JARs:
#   GCP:   cloud_gcp_lib_deploy.jar, flink_assembly_deploy.jar, service_assembly_deploy.jar, connectors_pubsub_deploy.jar
#   AWS Spark 3: cloud_aws_lib_deploy.jar, flink_assembly_deploy.jar, service_assembly_deploy.jar, connectors_kinesis_deploy.jar
#   AWS Spark 4: cloud_aws_lib_spark4_deploy.jar, flink_assembly_spark4_deploy.jar, connectors_kinesis_spark4_deploy.jar
#   Azure: cloud_azure_lib_deploy.jar, flink_assembly_deploy.jar, service_assembly_deploy.jar
COPY build_output/engine/ /jars/
COPY build_output/engine_manifest.json /manifest.json
COPY scripts/ /scripts/
