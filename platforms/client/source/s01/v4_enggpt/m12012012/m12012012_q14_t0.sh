#!/bin/bash

# Update package list
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql

# Install pandas
pip3 install pandas

# Install direct_redis and Redis
pip3 install direct_redis redis
