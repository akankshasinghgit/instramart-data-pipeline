FROM apache/airflow:2.9.3

USER root

# Install Java (needed for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Install PySpark and other Python dependencies
RUN pip install --no-cache-dir --default-timeout=120 --retries 5 pyspark==4.1.1 pyarrow duckdb "pandas>=2.2.0"
