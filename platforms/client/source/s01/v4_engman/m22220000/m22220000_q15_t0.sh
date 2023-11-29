#!/bin/bash
# Install the necessary libraries for Python script

# Update and upgrade the system
apt-get update && apt-get upgrade -y

# Install Python3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pandas direct_redis
