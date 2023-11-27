#!/bin/bash

# Update package list
sudo apt-get update

# Install Python pip if not already present
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql
pip3 install pandas
pip3 install direct_redis
