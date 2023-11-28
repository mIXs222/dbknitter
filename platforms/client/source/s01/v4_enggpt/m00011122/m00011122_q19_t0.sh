#!/bin/bash
# File: install_dependencies.sh

# Update and install pip if it's not present
sudo apt update
sudo apt install -y python3-pip

# Install the pymysql and pandas (for DataFrame manipulation)
pip3 install pymysql pandas

# Install direct_redis (additional package for handling the redis data fetch)
pip3 install direct_redis
