# Bash script to install dependencies required to run the Python code
#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3, pip and necessary system libraries
sudo apt-get install -y python3 python3-pip python3-dev build-essential

# Install pymysql
pip3 install pymysql

# Install pymongo
pip3 install pymongo

# Install direct_redis
pip3 install direct_redis

# Install pandas
pip3 install pandas
