#!/bin/bash

# Bash script to install Python dependencies

# Update the package list
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pandas pymysql pymongo direct_redis
