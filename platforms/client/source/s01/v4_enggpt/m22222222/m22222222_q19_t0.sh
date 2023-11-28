#!/bin/bash
set -euo pipefail

# Install pip if not already installed
if ! command -v pip &>/dev/null; then
    echo "Installing pip..."
    sudo apt-get update && sudo apt-get install -y python3-pip
fi

# Upgrading pip
pip3 install --upgrade pip

# Install Python packages
pip3 install pandas redis direct_redis

# Give notice that installation is complete
echo "All dependencies have been installed."
