#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python3 and Pip if not installed
sudo apt-get install -y python3 python3-pip

# Install the required python packages
pip3 install pandas pymysql pymongo direct-redis
