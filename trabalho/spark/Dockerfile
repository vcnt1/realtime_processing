FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_HOSTNAME=localhost \
    PYSPARK_PYTHON=python3

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    boto3==1.35.89 \
    confluent-kafka==2.6.1 \
    orjson==3.10.12 \
    pyspark==3.5.5

CMD ["spark-submit", "--version"]