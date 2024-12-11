FROM apache/airflow:2.7.1-python3.9

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        procps \
        openjdk-11-jdk \
        vim && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

RUN mkdir -p /tmp/spark-temp && chmod -R 777 /tmp/spark-temp

USER airflow

RUN pip install apache-airflow==2.7.2 apache-airflow-providers-apache-spark==4.0.1 pyspark==3.5.0 \
    openlineage-airflow==1.24.1 streamlit==1.26.0 requests==2.31.0 duckdb==0.9.1 pyarrow==14.0.0