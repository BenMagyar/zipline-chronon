FROM debian:trixie-slim

# Install Amazon Corretto 21 and curl for healthcheck
# We can't use newer JDKs as we need to have HADOOP-19212 fixed (and Spark pick it up) to not break
# Catalyst util
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y curl gnupg ca-certificates procps && \
    curl -fsSL https://apt.corretto.aws/corretto.key | gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" > /etc/apt/sources.list.d/corretto.list && \
    apt-get update && \
    apt-get install -y java-21-amazon-corretto-jdk && \
    apt-get remove --purge --autoremove -y gnupg && \
    apt-get install -y --only-upgrade libpcre2-8-0 liblz4-1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment and encoding
ENV JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto
ENV PATH=$PATH:$JAVA_HOME/bin
ENV LANG=C.UTF-8

# We expect jars to be copied to the build_output directory as docker can't read from bazel-bin as that's a symlink
# https://stackoverflow.com/questions/31881904/docker-follow-symlink-outside-context
ENV CLOUD_AWS_JAR_PATH=build_output/cloud_aws_lib_deploy.jar
ENV CLOUD_GCP_JAR_PATH=build_output/cloud_gcp_lib_deploy.jar
ENV CLOUD_AZURE_JAR_PATH=build_output/cloud_azure_lib_deploy.jar
ENV FETCHER_SVC_JAR_PATH=build_output/service_assembly_deploy.jar
ENV FETCHER_LAUNCH_SCRIPT=docker/fetcher/start.sh
ENV GCP_ONLINE_CLASS=ai.chronon.integrations.cloud_gcp.GcpApiImpl
ENV AWS_ONLINE_CLASS=ai.chronon.integrations.aws.AwsApiImpl
ENV AZURE_ONLINE_CLASS=ai.chronon.integrations.cloud_azure.AzureApiImpl

# Install Google Cloud Profiler
RUN mkdir -p /opt/cprof && \
  curl -s https://storage.googleapis.com/cloud-profiler/java/latest/profiler_java_agent.tar.gz \
  | tar xzv -C /opt/cprof


WORKDIR /srv/zipline

ENV CLOUD_AWS_JAR=${CLOUD_AWS_JAR:-"/srv/zipline/cloud_aws/cloud_aws.jar"}
ENV CLOUD_GCP_JAR=${CLOUD_GCP_JAR:-"/srv/zipline/cloud_gcp/cloud_gcp.jar"}
ENV CLOUD_AZURE_JAR=${CLOUD_AZURE_JAR:-"/srv/zipline/cloud_azure/cloud_azure.jar"}
ENV FETCHER_JAR=${FETCHER_JAR:-"/srv/zipline/fetcher/service.jar"}
ENV LOG_PATH=${LOG_PATH:-"/srv/zipline/fetcher/logs"}

COPY $CLOUD_AWS_JAR_PATH "$CLOUD_AWS_JAR"
COPY $CLOUD_GCP_JAR_PATH "$CLOUD_GCP_JAR"
COPY $CLOUD_AZURE_JAR_PATH "$CLOUD_AZURE_JAR"
COPY $FETCHER_SVC_JAR_PATH "$FETCHER_JAR"
COPY $FETCHER_LAUNCH_SCRIPT /srv/zipline/fetcher/start.sh

ENV FETCHER_PORT=9000

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:$FETCHER_PORT/ping || exit 1

RUN mkdir -p $LOG_PATH && \
    chmod 755 $LOG_PATH

CMD /srv/zipline/fetcher/start.sh
