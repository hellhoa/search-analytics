FROM --platform=linux/amd64 apache/airflow:2.7.1
USER root

# Install OpenJDK and other dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow
# Install additional providers
RUN pip install apache-airflow-providers-apache-spark==4.1.5 \
    && pip install apache-airflow-providers-mysql==5.5.2
