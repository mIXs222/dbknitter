#!/bin/bash

# Update the package list
apt-get update

# Install Python and pip if they are not installed
apt-get install -y python3
apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo pandas direct-redis
