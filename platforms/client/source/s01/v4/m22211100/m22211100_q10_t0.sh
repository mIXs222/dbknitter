#!/bin/bash

# Update and install pip if not installed
apt-get update -y
apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas direct_redis
