#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip if they are not installed
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo pandas sqlalchemy direct-redis
