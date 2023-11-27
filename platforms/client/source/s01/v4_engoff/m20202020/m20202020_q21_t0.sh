#!/bin/bash

# Update package list and upgrade existing packages
sudo apt-get update
sudo apt-get -y upgrade

# Install python3 and pip if not available
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pandas, pymysql, and redis-py
pip3 install pandas pymysql redis

# Install direct_redis (assuming it's available on PyPi)
pip3 install direct_redis
