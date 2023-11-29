#!/bin/bash

# Update package lists
apt-get update

# Install pip if not installed 
apt-get install -y python3-pip

# Install Python MySQL client
pip3 install pymysql

# Install Python Redis client
pip3 install direct-redis

# Install pandas
pip3 install pandas
