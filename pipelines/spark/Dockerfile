ARG SPARK_BASE_IMAGE=vn-news-spark:3.5.7
FROM ${SPARK_BASE_IMAGE}
WORKDIR /app
ENV PYTHONPATH=/app PYSPARK_PYTHON=python3 AWS_REGION=us-east-1
COPY src/ /app/src/
COPY run.sh /app/run.sh
CMD ["bash", "/app/run.sh"]
