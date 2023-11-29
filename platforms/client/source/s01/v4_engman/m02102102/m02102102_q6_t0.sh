#!/bin/bash
# Ensure pip is installed
which pip || {
  echo "pip not found, installing..."
  sudo apt-get update
  sudo apt-get install -y python-pip
}

# Install Python dependencies
pip install pandas redis direct_redis
