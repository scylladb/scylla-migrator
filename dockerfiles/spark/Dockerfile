FROM alpine:3.20

ENV SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3 \
    SCALA_VERSION=2.13 \
    SPARK_HOME="/spark"

RUN set -ex; \
    apk add --no-cache openjdk11-jre bash rsync procps openssh coreutils; \
    wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz; \
    tar --directory / -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz; \
    mv /spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION} ${SPARK_HOME}; \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz

ENV PATH="${SPARK_HOME}/sbin:${SPARK_HOME}/bin:${PATH}"

WORKDIR ${SPARK_HOME}

COPY entrypoint.sh .
ENTRYPOINT ["/spark/entrypoint.sh"]
