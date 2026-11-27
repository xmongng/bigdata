
FROM eclipse-temurin:11-jre-jammy

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip curl ca-certificates tini bash && \
    rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install --no-cache-dir boto3==1.34.162
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV AWS_REGION=us-east-1

COPY spark-3.5.7-bin-hadoop3.tgz /tmp/spark.tgz

RUN mkdir -p "${SPARK_HOME}" && \
    tar -xzf /tmp/spark.tgz -C "${SPARK_HOME}" --strip-components=1 && \
    rm /tmp/spark.tgz

WORKDIR /app

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["bash"]
