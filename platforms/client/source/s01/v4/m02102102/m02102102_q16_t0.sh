#!/bin/bash

# Update package list and install Python pip
apt-get update
apt-get install -y python3-pip

# Install required Python libraries
pip3 install PyMySQL pymongo pandas direct_redis
