#!/bin/bash

# script.sh

# Update package lists
apt-get update

# Install Python3, pip and MySQL server (since pymysql is used)
apt-get install -y python3 python3-pip mysql-server

# Install the required Python libraries
pip3 install pandas pymysql redis direct-redis

# If direct_redis is not available through pip, it needs to be installed through another method, e.g., from source
