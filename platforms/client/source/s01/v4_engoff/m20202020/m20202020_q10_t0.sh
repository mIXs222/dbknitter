#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python if not installed
sudo apt-get install -y python3

# Install pip if not installed
sudo apt-get install -y python3-pip

# Install Python MySQL client
pip3 install pymysql

# Install Pandas library
pip3 install pandas

# Install direct_redis library
pip3 install direct_redis
