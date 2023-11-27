#!/bin/bash

# Update the package list
apt-get update

# Install Python and Pip if they're not installed
apt-get install -y python3
apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymysql pandas direct_redis
