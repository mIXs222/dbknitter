#!/bin/bash

# Update package manager and install pip
apt-get update
apt-get install -y python3-pip

# Install required packages
pip3 install pymysql pymongo pandas direct-redis
