#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip if they are not installed
apt-get install -y python3 python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install required Python libraries
pip3 install pymysql pandas direct-redis

echo "Required dependencies are installed successfully."
