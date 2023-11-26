#!/bin/bash

# Note: The user should grant execute permission to this script before running.
# chmod +x install_dependencies.sh

# Update package list and install pip if not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install the required Python packages using pip
pip3 install pymysql pandas direct-redis
