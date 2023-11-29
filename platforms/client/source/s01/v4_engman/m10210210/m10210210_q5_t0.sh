#!/bin/bash
# file: setup_dependencies.sh

# Update and install system dependencies
sudo apt update
sudo apt install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pymongo pandas direct-redis
