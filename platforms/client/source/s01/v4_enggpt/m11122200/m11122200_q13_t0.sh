#!/bin/bash

# Ensure pip is installed
sudo apt update
sudo apt install -y python3-pip

# Install required Python packages
pip3 install pymysql
pip3 install pandas
pip install direct-redis
