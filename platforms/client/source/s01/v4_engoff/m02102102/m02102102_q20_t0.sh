#!/bin/bash

# Update package repository and upgrade packages
apt-get update
apt-get -y upgrade

# Install Python and pip
apt-get install -y python3
apt-get install -y python3-pip

# Install MySQL, MongoDB, and Redis clients for Python
pip3 install pymysql pymongo pandas direct-redis
