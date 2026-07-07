FROM maven:3.9-eclipse-temurin-11 AS azure-plugin-builder

# Build flink-azure-fs-hadoop with Hadoop 3.4.1 for AKS WorkloadIdentity support.
# The official plugin bundles Hadoop 3.3.4 (fs.hadoopshaded.version in the
# flink-filesystems parent pom), which lacks WorkloadIdentityTokenProvider
# (HADOOP-18610, added in 3.4.1). FLINK-36694 is still open as of Flink 1.20.3.
ARG FLINK_VERSION=1.20.3
RUN git clone --depth 1 --branch release-${FLINK_VERSION} \
      https://github.com/apache/flink.git /tmp/flink-source && \
    cd /tmp/flink-source && \
    mvn package \
      -pl flink-filesystems/flink-azure-fs-hadoop \
      -am \
      -DskipTests \
      -Dfs.hadoopshaded.version=3.4.1 \
      -Dcheckstyle.skip=true \
      -Denforcer.skip=true \
      -Dspotless.check.skip=true \
      -Dmaven.javadoc.skip=true \
      -Drat.skip=true \
      -Dremote-resources.skip=true \
      -T 4

FROM flink:1.20.3

# Flink filesystem plugins — each must be in its own subdirectory under /opt/flink/plugins/
# See: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/filesystems/plugins/
ARG FLINK_VERSION=1.20.3

USER root

# flink:1.20.3 ships Temurin 11 JRE which lacks jdk.compiler — avro-fastserde
# generates Java at runtime and needs ToolProvider.getSystemJavaCompiler().
# openjdk-17-jdk-headless includes jdk.compiler, matching what the EMR Corretto
# 17 image provides. We override JAVA_HOME so Flink picks up the JDK instead of
# the bundled Temurin JRE at /opt/java/openjdk.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sfn "$(readlink -f /usr/bin/javac | sed 's|/bin/javac||')" /usr/lib/jvm/java-17

ENV JAVA_HOME=/usr/lib/jvm/java-17

# S3 (flink-s3-fs-hadoop — uses Hadoop S3A, supports IRSA/WebIdentity)
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}/flink-s3-fs-hadoop-${FLINK_VERSION}.jar" \
      -o /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-${FLINK_VERSION}.jar

# GCS (flink-gs-fs-hadoop — uses GCS Hadoop connector, supports workload identity)
RUN mkdir -p /opt/flink/plugins/gs-fs-hadoop && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/flink/flink-gs-fs-hadoop/${FLINK_VERSION}/flink-gs-fs-hadoop-${FLINK_VERSION}.jar" \
      -o /opt/flink/plugins/gs-fs-hadoop/flink-gs-fs-hadoop-${FLINK_VERSION}.jar

# Azure (custom build with Hadoop 3.4.1 — see builder stage above)
# hadoop-azure 3.4.1 classes reference unshaded com.fasterxml.jackson.* — add
# jackson jars so the plugin classloader resolves them.
ARG JACKSON_VERSION=2.15.2
RUN mkdir -p /opt/flink/plugins/azure-fs-hadoop && \
    curl -fsSL "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/${JACKSON_VERSION}/jackson-core-${JACKSON_VERSION}.jar" \
      -o /opt/flink/plugins/azure-fs-hadoop/jackson-core-${JACKSON_VERSION}.jar && \
    curl -fsSL "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/${JACKSON_VERSION}/jackson-databind-${JACKSON_VERSION}.jar" \
      -o /opt/flink/plugins/azure-fs-hadoop/jackson-databind-${JACKSON_VERSION}.jar && \
    curl -fsSL "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/${JACKSON_VERSION}/jackson-annotations-${JACKSON_VERSION}.jar" \
      -o /opt/flink/plugins/azure-fs-hadoop/jackson-annotations-${JACKSON_VERSION}.jar
COPY --from=azure-plugin-builder \
     /tmp/flink-source/flink-filesystems/flink-azure-fs-hadoop/target/flink-azure-fs-hadoop-${FLINK_VERSION}.jar \
     /opt/flink/plugins/azure-fs-hadoop/flink-azure-fs-hadoop-${FLINK_VERSION}.jar

# Verify all plugins
RUN ls -la /opt/flink/plugins/*/

USER flink
