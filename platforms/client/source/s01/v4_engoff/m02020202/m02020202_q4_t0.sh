#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and pip if not installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pandas pymysql direct_redis
