#!/bin/bash

# Update package list
sudo apt-get update

# Install pip and Python dev tools
sudo apt-get install -y python3-pip python3-dev

# Install pymysql, pymongo, pandas and direct-redis via pip
pip3 install pymysql pymongo pandas direct-redis
