FROM flink:1.18.0-scala_2.12-java11

# Install Python for PyFlink
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install PyFlink and other dependencies
RUN pip3 install apache-flink==1.18.0 \
    kafka-python==2.0.2 \
    pandas==1.5.3 \
    numpy==1.24.3 \
    faker==20.1.0

# Copy application files
COPY app/ /opt/flink/app/

# Set working directory
WORKDIR /opt/flink

# Create necessary directories
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints

# Set permissions
RUN chown -R flink:flink /opt/flink/app /opt/flink/checkpoints /opt/flink/savepoints