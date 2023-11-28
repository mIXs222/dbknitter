#!/bin/bash

# Update repositories and upgrade the system
sudo apt-get update
sudo apt-get upgrade -y

# Install pip for Python3 and required packages
sudo apt-get install -y python3-pip
pip3 install pymongo pandas redis direct_redis
