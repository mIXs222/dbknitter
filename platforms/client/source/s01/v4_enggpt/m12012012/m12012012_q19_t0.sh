#!/bin/bash

# Update repositories and install Python 3 and pip if they are not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pandas redis direct-redis
