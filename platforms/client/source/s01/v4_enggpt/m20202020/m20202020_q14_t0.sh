#!/bin/bash

# Update package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install Python package dependencies
pip3 install pymysql pandas direct_redis
