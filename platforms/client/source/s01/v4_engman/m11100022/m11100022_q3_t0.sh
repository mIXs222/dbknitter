#!/bin/bash

# Update the package list
apt-get update

# Install Python 3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pandas pymysql redis direct_redis
