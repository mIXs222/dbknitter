#!/bin/bash

# Update package lists
apt-get update

# Install Python and Pip if not already installed
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pandas "direct_redis[hiredis]"
