#!/bin/bash

# Update system package index (may require sudo)
apt-get update

# Install Python3 and Pip (may require sudo)
apt-get install python3 python3-pip -y

# Install PyMySQL
pip3 install pymysql

# Installation of Redis and dependency for direct_redis
apt-get install gcc python3-dev -y # Redis dependencies
pip3 install redis direct_redis

# Install pandas
pip3 install pandas
