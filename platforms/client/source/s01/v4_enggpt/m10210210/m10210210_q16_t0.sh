#!/bin/bash

# Update package lists
sudo apt update

# Install Python pip if not already installed
sudo apt install python3-pip -y

# Install required Python libraries
pip3 install pymysql pymongo pandas direct_redis
