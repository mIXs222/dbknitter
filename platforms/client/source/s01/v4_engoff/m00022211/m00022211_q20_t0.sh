#!/bin/bash

# Install Python and Pip if not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymongo pymysql pandas direct_redis
