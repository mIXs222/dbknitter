#!/bin/bash

# Update package list and Upgrade system
sudo apt-get update -y && sudo apt-get upgrade -y

# Install pip for Python 3
sudo apt-get install python3-pip -y

# Install required Python packages
pip3 install pymongo pandas direct_redis
