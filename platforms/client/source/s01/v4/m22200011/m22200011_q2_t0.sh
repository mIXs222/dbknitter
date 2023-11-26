#!/bin/bash

# Update package list and install Python and Pip if they are not installed
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas pymysql direct_redis
