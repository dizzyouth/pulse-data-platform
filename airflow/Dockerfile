FROM apache/airflow:2.11.2-python3.12

USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/pulse-requirements.txt
RUN pip install --no-cache-dir -r /tmp/pulse-requirements.txt
