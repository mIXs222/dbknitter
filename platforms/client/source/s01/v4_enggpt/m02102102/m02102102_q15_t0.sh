#!/bin/bash

# Ensure pip, a Python package installer, is available
sudo apt-get update && sudo apt-get install -y python3-pip

# Install Python libraries
pip3 install pymysql pandas direct-redis
