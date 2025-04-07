#!/bin/bash

echo "Testing Beam Docker build..."

# Build the image
docker build -t beam-test .

if [ $? -eq 0 ]; then
    echo "✅ Build successful!"
    
    # Test running the simple batch script
    echo "Testing simple batch script..."
    docker run --rm -v $(pwd)/app:/app -v $(pwd)/../shared-data:/shared-data beam-test python simple_batch.py
    
    if [ $? -eq 0 ]; then
        echo "✅ Simple batch script executed successfully!"
    else
        echo "❌ Simple batch script failed"
    fi
else
    echo "❌ Build failed"
fi