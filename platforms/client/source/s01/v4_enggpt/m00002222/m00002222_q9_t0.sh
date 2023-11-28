#!/bin/bash

# Update package list and install python3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pandas pymysql direct-redis
