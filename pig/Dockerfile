FROM openjdk:8-jdk-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV HADOOP_VERSION=3.3.4
ENV PIG_VERSION=0.17.0
ENV HADOOP_HOME=/opt/hadoop
ENV PIG_HOME=/opt/pig
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PIG_HOME/bin

# Install Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz \
    && mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# Install Pig
RUN wget https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz \
    && tar -xzf pig-${PIG_VERSION}.tar.gz \
    && mv pig-${PIG_VERSION} ${PIG_HOME} \
    && rm pig-${PIG_VERSION}.tar.gz

# Set up Hadoop configuration
RUN mkdir -p ${HADOOP_HOME}/logs

# Copy configuration files
COPY config/ ${HADOOP_HOME}/etc/hadoop/

# Set up working directory
WORKDIR /app

# Copy Pig scripts and data
COPY scripts/ /app/scripts/
COPY data/ /app/data/

# Create necessary directories
RUN mkdir -p /app/output /app/logs

# Set permissions
RUN chmod +x /app/scripts/*.pig

# Default command
CMD ["pig", "-x", "local", "/app/scripts/sample_analysis.pig"]