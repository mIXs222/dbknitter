#!/bin/bash

# Script to install all dependencies required for the Python code execution

# Update the repositories and install Python3 and pip if they are not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the pymysql, pymongo and pandas Python libraries
pip3 install pymysql pymongo pandas

# Install Redis and the direct_redis library for Redis-Pandas DataFrame integration
sudo apt-get install -y redis-server
pip3 install direct_redis
