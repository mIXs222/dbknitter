#!/bin/bash

# Update package list and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install necessary Python packages
pip3 install pymysql pymongo pandas
pip3 install direct-redis
