FROM openjdk:8-jdk-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    ssh \
    rsync \
    python3 \
    python3-pip \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV HADOOP_VERSION=3.3.4
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Install Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz \
    && mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# Create hadoop user
RUN useradd -m -s /bin/bash hadoop \
    && chown -R hadoop:hadoop ${HADOOP_HOME}

# Set up SSH for hadoop user
RUN mkdir -p /home/hadoop/.ssh \
    && ssh-keygen -t rsa -P '' -f /home/hadoop/.ssh/id_rsa \
    && cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys \
    && chmod 600 /home/hadoop/.ssh/authorized_keys \
    && chown -R hadoop:hadoop /home/hadoop/.ssh

# Create data directories
RUN mkdir -p /opt/hadoop/data/namenode \
    /opt/hadoop/data/datanode \
    /opt/hadoop/logs \
    && chown -R hadoop:hadoop /opt/hadoop/data /opt/hadoop/logs

# Copy configuration files
COPY config/ ${HADOOP_CONF_DIR}/
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Format namenode
USER hadoop
RUN ${HADOOP_HOME}/bin/hdfs namenode -format -force

USER root

# Expose ports
EXPOSE 9000 9870 8088 19888

# Default command
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]