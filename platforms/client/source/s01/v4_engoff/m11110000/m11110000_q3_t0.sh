#!/bin/bash
set -e

# Update package lists
echo "Updating package lists..."
sudo apt-get update

# Install Python3 and pip if they are not installed
echo "Installing Python3 and pip..."
sudo apt-get install -y python3 python3-pip

# Install the required Python library 'pymysql'
echo "Installing pymysql..."
pip3 install pymysql

echo "All dependencies are installed."
