#!/bin/bash

# Update and install system dependencies
sudo apt-get update
sudo apt-get install -y python3-pip python3-dev libmysqlclient-dev

# Install Python dependencies
pip3 install pymysql pandas redis direct-redis
