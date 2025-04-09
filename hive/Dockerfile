FROM openjdk:8-jdk-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV HADOOP_VERSION=3.3.4
ENV HIVE_VERSION=3.1.3
ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$HADOOP_HOME/bin:$HIVE_HOME/bin

# Install Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz \
    && mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# Install Hive
RUN wget https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
    && tar -xzf apache-hive-${HIVE_VERSION}-bin.tar.gz \
    && mv apache-hive-${HIVE_VERSION}-bin ${HIVE_HOME} \
    && rm apache-hive-${HIVE_VERSION}-bin.tar.gz

# Install MySQL connector for Hive metastore
RUN wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.33.tar.gz \
    && tar -xzf mysql-connector-java-8.0.33.tar.gz \
    && cp mysql-connector-java-8.0.33/mysql-connector-java-8.0.33.jar ${HIVE_HOME}/lib/ \
    && rm -rf mysql-connector-java-8.0.33*

# Copy configuration files
COPY config/hive-site.xml ${HIVE_HOME}/conf/
COPY config/core-site.xml ${HADOOP_HOME}/etc/hadoop/
COPY config/hdfs-site.xml ${HADOOP_HOME}/etc/hadoop/

# Create necessary directories
RUN mkdir -p ${HIVE_HOME}/logs /tmp/hive

# Copy sample queries
COPY queries/ /opt/hive/queries/

# Set working directory
WORKDIR /opt/hive

# Initialize schema
RUN ${HIVE_HOME}/bin/schematool -dbType derby -initSchema

# Expose ports
EXPOSE 10000 10002

# Default command
CMD ["hive", "--service", "hiveserver2"]