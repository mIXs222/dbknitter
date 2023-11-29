#!/bin/bash

# Update package list and install pip
apt-get update
apt-get install -y python3-pip

# Install Python package dependencies
pip3 install pymysql pandas direct-redis
