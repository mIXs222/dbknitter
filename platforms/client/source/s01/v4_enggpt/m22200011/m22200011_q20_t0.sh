#!/bin/bash

# Update and install system dependencies
sudo apt-get update
sudo apt-get install -y python3-pip python3-dev build-essential

# Upgrade pip and install Python dependencies
pip3 install --upgrade pip
pip3 install pymysql pymongo pandas direct_redis
