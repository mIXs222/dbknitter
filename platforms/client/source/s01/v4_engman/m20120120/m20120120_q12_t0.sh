#!/bin/bash

# Update package list
apt-get update

# Install pip if not installed
apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql

# Install direct_redis (assuming the package exists, as per the given instructions)
pip3 install direct_redis

# Install pandas
pip3 install pandas
