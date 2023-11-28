#!/bin/bash

# Ensure that pip is installed
if ! command -v pip &>/dev/null; then
    echo "pip could not be found. Please install pip before running this script."
    exit 1
fi

# Install dependencies with pip
pip install pandas redis direct_redis
