FROM apache/spark:4.1.3-java17

ARG HADOOP_VERSION=3.4.2
ARG AWS_SDK_V1_BUNDLE_VERSION=1.12.681
ARG AWS_SDK_V2_BUNDLE_VERSION=2.35.5
ARG GCS_CONNECTOR_VERSION=hadoop3-2.2.28
ARG SPARK_BQ_CONNECTOR_VERSION=0.44.2-preview

USER root

# Overlay Hadoop 3.4.2 cloud connectors + matching client jars. 3.4.x is the
# minimum for Azure WorkloadIdentityTokenProvider (HADOOP-18610) and for the
# Configuration.getEnumSet S3AFileSystem.initialize relies on.
RUN cd /opt/spark/jars && \
    curl -fsSLO "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" && \
    curl -fsSLO "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_BUNDLE_VERSION}.jar" && \
    curl -fsSLO "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_BUNDLE_VERSION}/bundle-${AWS_SDK_V2_BUNDLE_VERSION}.jar" && \
    curl -fsSLO "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/${GCS_CONNECTOR_VERSION}/gcs-connector-${GCS_CONNECTOR_VERSION}-shaded.jar" && \
    curl -fsSLO "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/${HADOOP_VERSION}/hadoop-azure-${HADOOP_VERSION}.jar" && \
    curl -fsSLO "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar" && \
    rm -f hadoop-client-api-*.jar hadoop-client-runtime-*.jar && \
    curl -fsSLO "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar" && \
    curl -fsSLO "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar" && \
    curl -fsSLO "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-4.1-bigquery/${SPARK_BQ_CONNECTOR_VERSION}/spark-4.1-bigquery-${SPARK_BQ_CONNECTOR_VERSION}.jar"

# Snowflake JDBC writes its config under $HOME by default; the spark user has
# a read-only $HOME on EKS pods.
RUN echo '{"common":{"log_level":"INFO","log_path":"/tmp"}}' > /etc/sf_client_config.json
ENV SF_CLIENT_CONFIG_FILE=/etc/sf_client_config.json

# Guava 33 — Chronon needs Preconditions.checkArgument(boolean, String, char, Object)
# (>= 20); Hive 2.3.x needs Guava collections. 33 satisfies both.
RUN rm -f /opt/spark/jars/guava-14.0.1.jar && \
    curl -fsSLO "https://repo1.maven.org/maven2/com/google/guava/guava/33.4.8-jre/guava-33.4.8-jre.jar" && \
    mv guava-33.4.8-jre.jar /opt/spark/jars/ && \
    curl -fsSLO "https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.2/failureaccess-1.0.2.jar" && \
    mv failureaccess-1.0.2.jar /opt/spark/jars/

# Gson 2.11 — Chronon BQ client uses JsonParser.parseString() (added in 2.8.6).
RUN rm -f /opt/spark/jars/gson-2.2.4.jar && \
    curl -fsSLO "https://repo1.maven.org/maven2/com/google/code/gson/gson/2.11.0/gson-2.11.0.jar" && \
    mv gson-2.11.0.jar /opt/spark/jars/

# Unity Catalog credential vending: hadoop-aws resolves
# fs.s3a.aws.credentials.provider with its own classloader (S3AUtils.java:641),
# so AwsVendedTokenProvider must live on the system classpath (/opt/spark/jars),
# not inside the chronon uber jar. Mirrors chronon PR #1898.
# UC 0.5.1 publishes Spark 4.x/Scala 2.13 artifacts; no Zipline rebuild needed.
ARG UC_VERSION=0.5.1
RUN cd /opt/spark/jars && \
    curl -fsSLO "https://repo1.maven.org/maven2/io/unitycatalog/unitycatalog-hadoop/${UC_VERSION}/unitycatalog-hadoop-${UC_VERSION}.jar" && \
    curl -fsSLO "https://repo1.maven.org/maven2/io/unitycatalog/unitycatalog-client/${UC_VERSION}/unitycatalog-client-${UC_VERSION}.jar"

RUN mkdir -p /tmp/spark-events && chmod 777 /tmp/spark-events

# UC + Iceberg metadata commonly come back as s3:// (not s3a://) — Hadoop 3.x
# doesn't register fs.s3.impl by default. Pin the FS schemes for all three
# clouds so the same image serves any deployment.
COPY spark-defaults.conf /tmp/spark-defaults.conf
RUN mkdir -p /opt/spark/conf && \
    cat /tmp/spark-defaults.conf >> /opt/spark/conf/spark-defaults.conf && \
    rm /tmp/spark-defaults.conf

USER spark
