#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 pip if not present
sudo apt-get install python3-pip -y

# Install Python MySQL client
pip3 install pymysql

# Install pandas for data manipulation
pip3 install pandas

# Install direct_redis
pip3 install direct-redis
