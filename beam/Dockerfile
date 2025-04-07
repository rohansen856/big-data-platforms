FROM python:3.9-slim

# Install Java for Beam
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create working directory
WORKDIR /app

# Copy application files
COPY app/ /app/

# Set default command
CMD ["python", "batch_pipeline.py"]