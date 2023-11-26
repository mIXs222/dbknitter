#!/bin/bash

# Ensure pip is installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python package dependencies
pip3 install pymysql
pip3 install direct-redis
pip3 install pandas
