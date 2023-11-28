#!/bin/bash

# Update and install Python pip
apt-get update
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql pymongo pandas direct_redis sqlalchemy
