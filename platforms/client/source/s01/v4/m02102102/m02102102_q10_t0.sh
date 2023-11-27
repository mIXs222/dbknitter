#!/bin/bash

# Update package lists
apt-get update

# Install Python and Pip if they are not installed
apt-get install -y python3
apt-get install -y python3-pip

# Install MySQL, MongoDB, and Redis dependencies for Python
pip3 install pymysql pymongo pandas direct_redis
