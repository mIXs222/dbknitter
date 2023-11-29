#!/bin/bash

# Updating package information
echo "Updating package information..."
sudo apt-get update

# Installing Python 3 pip if not already installed
if ! command -v pip3 &>/dev/null; then
    echo "Installing pip for Python 3..."
    sudo apt-get install python3-pip -y
else
    echo "pip for Python 3 is already installed."
fi

# Installing necessary Python libraries
echo "Installing necessary Python libraries..."
pip3 install pymysql pymongo pandas redis direct-redis

echo "All dependencies have been installed."
