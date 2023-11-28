#!/bin/bash

# Ensure system packages are up to date
apt-get update

# Install Python and Pip
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas pymysql direct_redis
