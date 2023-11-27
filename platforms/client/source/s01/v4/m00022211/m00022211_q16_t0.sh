#!/bin/bash

# Update package list and upgrade current packages
apt-get update
apt-get upgrade -y

# Install Python and pip (if not already installed)
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pandas direct_redis
