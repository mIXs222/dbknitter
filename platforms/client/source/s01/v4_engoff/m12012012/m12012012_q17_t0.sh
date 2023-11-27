#!/bin/bash

# Update and install system dependencies (if needed)
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql pandas direct-redis
