#!/bin/bash

# Update package list and install Python3 and pip if they are not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymysql pandas direct_redis
