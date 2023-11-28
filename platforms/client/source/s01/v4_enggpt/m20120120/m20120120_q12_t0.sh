#!/bin/bash

# Update package information
sudo apt update

# Install Python3 and pip if they are not installed
sudo apt install -y python3 python3-pip

# Install required python packages
pip3 install pymysql pandas direct_redis
