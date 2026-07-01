# Airflow image with Docker CLI + Python deps so the DAG can spawn the Spark job
# as a sibling container via DockerOperator.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io curl && \
    rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt