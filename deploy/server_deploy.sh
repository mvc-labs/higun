#!/bin/bash

# Exit on error
set -e

echo "Starting deployment of FT-UTXO indexer..."

# Check parameters
if [ $# -ne 1 ]; then
    echo "Usage: $0 <zip file path>"
    echo "Example: $0 utxo-indexer.zip"
    exit 1
fi

ZIP_FILE=$1

# Check if zip file exists
if [ ! -f "$ZIP_FILE" ]; then
    echo "Error: File not found $ZIP_FILE"
    exit 1
fi

# Extract file
echo "Extracting file..."
unzip -q "$ZIP_FILE"

# Enter project directory
echo "Entering project directory..."
cd utxo-indexer

# Stop old containers
echo "Stopping old containers..."
docker-compose -f deploy/docker-compose.ft.yml down || true

# Build new image
echo "Building new image..."
docker-compose -f deploy/docker-compose.ft.yml build --no-cache

# Start service
echo "Starting service..."
docker-compose -f deploy/docker-compose.ft.yml up -d

# Check service status
echo "Checking service status..."
docker-compose -f deploy/docker-compose.ft.yml ps

echo "Deployment completed!"
echo "You can view logs with the following command:"
echo "docker-compose -f deploy/docker-compose.ft.yml logs -f"

# Clean up zip file
echo "Cleaning up zip file..."
cd ..
rm -f "$ZIP_FILE" 