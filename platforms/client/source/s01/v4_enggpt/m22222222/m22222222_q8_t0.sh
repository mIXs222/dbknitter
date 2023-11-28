#!/bin/bash

# Update the system
sudo apt-get update -y

# Install Python 3 and PIP (if not already installed)
sudo apt-get install python3 python3-pip -y

# Install Python libraries
pip3 install pandas redis

# Install direct_redis (assuming it's available in PIP or replace with correct source)
pip3 install direct_redis
