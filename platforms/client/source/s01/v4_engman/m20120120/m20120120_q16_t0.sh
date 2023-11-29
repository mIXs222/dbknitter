#!/bin/bash

# Update package list and upgrade packages
sudo apt-get update -y
sudo apt-get upgrade -y

# Install pip if it's not installed
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas direct_redis
