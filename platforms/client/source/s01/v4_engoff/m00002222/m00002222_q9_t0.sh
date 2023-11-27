#!/bin/bash

# Update repositories and install Python 3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python modules
pip3 install pymysql pandas direct_redis
