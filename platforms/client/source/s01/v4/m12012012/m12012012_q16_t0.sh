#!/bin/bash

# Update package list
sudo apt-get update

# Install python3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install pymysql, pymongo, pandas and direct_redis
pip3 install pymysql pymongo pandas direct_redis
