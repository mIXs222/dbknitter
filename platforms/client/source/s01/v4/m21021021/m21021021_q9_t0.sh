#!/bin/bash

# Update package list
apt-get update

# Install Python and pip if they're not available
apt-get install -y python3
apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct-redis
