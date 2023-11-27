#!/bin/bash

# Update package list and install pip if it's not installed
sudo apt update
sudo apt install -y python3-pip

# Install the required Python libraries
pip3 install pymysql pandas direct_redis
