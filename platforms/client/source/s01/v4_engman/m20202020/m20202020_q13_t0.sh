#!/bin/bash

# Update package lists
apt-get update

# Install pip if not installed
apt-get install -y python3-pip

# Install Python MySQL client
pip3 install pymysql

# Install Pandas
pip3 install pandas

# Install direct_redis
pip3 install direct_redis
