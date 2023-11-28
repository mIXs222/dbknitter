#!/bin/bash
# File: install_dependencies.sh

# Update package list and Install the required system packages
sudo apt-get update
sudo apt-get install -y python3-pip

# Install necessary Python libraries
pip3 install pandas pymysql direct-redis
