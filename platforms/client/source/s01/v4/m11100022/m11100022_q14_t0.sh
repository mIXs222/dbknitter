#!/bin/bash

# Updating packages and installing Python3 and pip3
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the Python dependencies
pip3 install pymongo pandas direct-redis
