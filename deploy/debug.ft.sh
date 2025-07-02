#!/bin/bash

# Exit on error
set -e

echo "Starting local debug of FT-UTXO indexer..."

# Run with local config file
echo "Starting service with local config file..."
go run ../apps/ft-main/main.go --config config_mvc_ft_local.yaml

echo "Service stopped" 