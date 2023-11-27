#!/bin/bash

# Update package list and install python3-pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymysql, pandas and direct_redis using pip
pip3 install pymysql pandas direct_redis
