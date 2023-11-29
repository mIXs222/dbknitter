#!/bin/bash

# Update package lists
apt-get update -y

# Install python3 and pip
apt-get install python3-pip -y

# Install Python libraries
pip3 install pymysql pymongo pandas direct_redis
