#!/bin/bash

# Updating repositories and installing Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pandas direct_redis
