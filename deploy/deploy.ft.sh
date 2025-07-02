#!/bin/bash

# Exit on error
set -e

echo "Starting deployment of FT-UTXO indexer..."

# Stop and remove old containers (if exist)
docker-compose -f docker-compose.ft.yml down || true

# Build new image
echo "Building new image..."
docker-compose -f docker-compose.ft.yml build

# Start service
echo "Starting service..."
docker-compose -f docker-compose.ft.yml up -d

# Check service status
echo "Checking service status..."
docker-compose -f docker-compose.ft.yml ps

echo "Deployment completed!"
echo "You can view logs with the following command:"
echo "docker-compose -f docker-compose.ft.yml logs -f" 