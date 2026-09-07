FROM flink:1.18-scala_2.12-java17

USER root

RUN apt-get update && apt-get install -y \
    curl \
    default-jdk \
    python3 python3-pip python3-venv && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Ensure JAVA_HOME points correctly
ENV JAVA_HOME=/usr/lib/jvm/default-java

# ─── Copy Kafka connector JARs (already in repo) ───────────────────────────
COPY jars/*.jar /opt/flink/lib/

# ─── Install PyFlink ────────────────────────────────────────────────────────
RUN pip3 install --upgrade pip && \
    pip3 install apache-flink==1.18.0 redis datasketch --timeout=200

# ─── Copy PyFlink job scripts ───────────────────────────────────────────────
COPY src/ /opt/flink/usrlib/src/

# ─── Copy Flink configuration ────────────────────────────────────────────────
COPY config.yaml /opt/flink/conf/config.yaml

RUN chown -R flink:flink /opt/flink/usrlib/ && \
    chown flink:flink /opt/flink/conf/config.yaml

RUN $JAVA_HOME/bin/java -version

USER flink
ENTRYPOINT ["/docker-entrypoint.sh"]
