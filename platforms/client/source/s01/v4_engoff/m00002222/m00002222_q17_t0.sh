#!/bin/bash

# Update the package list
apt-get update

# Install Python and Pip (if not already installed)
apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pandas direct-redis
