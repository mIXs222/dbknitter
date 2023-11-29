#!/bin/bash
set -e

# Ensure pip is available
if ! command -v pip &>/dev/null; then
    echo "Installing pip..."
    sudo apt update
    sudo apt install -y python3-pip
fi

# Install pymysql
pip install pymysql -q
