#!/bin/bash

# Install Python dependencies

# Update the package list
apt-get update

# Install pip, Python's package installer
apt-get install -y python3-pip

# Install Python packages
pip3 install pymysql pandas direct-redis
