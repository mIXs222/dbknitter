#!/bin/bash

# Update package manager and install Python pip
apt-get update
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pandas pymysql direct-redis
