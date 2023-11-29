#!/bin/bash

# Update package list
apt-get update

# Install pip if it's not installed
apt-get install -y python3-pip

# Install pymysql to connect to MySQL
pip3 install pymysql

# Install direct_redis to connect to Redis
pip3 install direct-redis

# Install pandas (already required by direct_redis, just to make sure)
pip3 install pandas
