# ─────────────────────────────────────────────────────────────────────────────
# Custom Spark Submit Image
# Extends bitnamilegacy/spark:3.5 and pre-bundles the hadoop-aws JARs
# needed to read/write S3 via the s3a:// protocol.
#
# By pre-bundling JARs here, we avoid --packages at job submit time,
# which sidesteps the Hadoop UserGroupInformation/UnixPrincipal issue
# that occurs when the container runs as an anonymous UID.
# ─────────────────────────────────────────────────────────────────────────────
FROM bitnamilegacy/spark:3.5

USER root

# Fix 1: Add a /etc/passwd entry for UID 1001 (Bitnami's default non-root UID).
# Without this, Hadoop's UnixLoginModule calls getpwuid(1001), gets null,
# and throws "invalid null input: name" from UnixPrincipal.
RUN echo "spark:x:1001:1001:Spark User:/tmp:/bin/bash" >> /etc/passwd && \
    echo "spark:x:1001:" >> /etc/group

# Fix 2: Tell Hadoop to use "spark" as the user name directly.
# Belt-and-suspenders with the /etc/passwd entry above.
ENV HADOOP_USER_NAME=spark
ENV SPARK_USER=spark
ENV HOME=/tmp

# Fix 3: Write spark.jars.ivy into spark-defaults.conf so Ivy always uses
# /tmp/.ivy2 regardless of what HOME evaluates to at runtime.
RUN echo "spark.jars.ivy /tmp/.ivy2" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    mkdir -p /tmp/.ivy2 && chmod 777 /tmp/.ivy2

# Fix 4: Configure Hadoop to use simple (non-Kerberos) authentication.
# Prevents any JAAS/Kerberos login attempt that would also fail.
RUN mkdir -p /opt/bitnami/spark/conf && \
    printf '<configuration>\n  <property>\n    <name>hadoop.security.authentication</name>\n    <value>simple</value>\n  </property>\n</configuration>\n' \
    > /opt/bitnami/spark/conf/core-site.xml

# Download hadoop-aws and its AWS SDK dependency directly into Spark's jars dir.
# These versions are tested compatible with Spark 3.5 + Hadoop 3.3.x
RUN curl -fSL \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fSL \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
    -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Switch back to the bitnami non-root user (now has a proper passwd entry)
USER 1001
