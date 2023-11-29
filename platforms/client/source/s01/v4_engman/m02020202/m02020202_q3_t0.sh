#!/bin/bash

# Update package lists
apt-get update

# Install pip
apt-get install -y python3-pip

# Install Python package dependencies with pip
pip3 install pymysql pandas direct-redis
