FROM openjdk:8-jdk-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip \
    wget \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV STORM_VERSION=2.4.0
ENV STORM_HOME=/opt/storm
ENV PATH=$PATH:$STORM_HOME/bin

# Install Storm
RUN wget https://archive.apache.org/dist/storm/apache-storm-${STORM_VERSION}/apache-storm-${STORM_VERSION}.tar.gz \
    && tar -xzf apache-storm-${STORM_VERSION}.tar.gz \
    && mv apache-storm-${STORM_VERSION} ${STORM_HOME} \
    && rm apache-storm-${STORM_VERSION}.tar.gz

# Install Python dependencies
RUN pip3 install streamparse kafka-python faker requests

# Create necessary directories
RUN mkdir -p /opt/storm/logs /opt/storm/data

# Copy configuration files
COPY config/storm.yaml ${STORM_HOME}/conf/storm.yaml
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Copy Storm topologies
COPY topologies/ /opt/storm/topologies/

# Set working directory
WORKDIR /opt/storm

# Expose ports
EXPOSE 8080 6700 6701 6702 6703

# Set permissions
RUN chown -R root:root /opt/storm

# Default command
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]