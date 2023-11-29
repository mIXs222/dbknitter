#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip if it's not installed
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct-redis
