#!/bin/bash

# Update and Install Python 3 and pip
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install necessary Python packages
pip3 install pandas
pip3 install direct_redis
