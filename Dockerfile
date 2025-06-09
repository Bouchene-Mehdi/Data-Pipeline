FROM apache/airflow:2.7.3

USER root
# Install OpenJDK
RUN apt-get update && apt-get install -y openjdk-11-jre && apt-get clean

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to airflow user and install Python dependencies
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
