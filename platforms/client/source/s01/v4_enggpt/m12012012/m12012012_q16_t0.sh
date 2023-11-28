#!/bin/bash

# Update package information
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pandas pymysql pymongo redis direct-redis
