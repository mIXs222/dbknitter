#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python3 and pip if not installed
sudo apt-get install -y python3 python3-pip

# Install MongoDB Python Driver (pymongo)
pip3 install pymongo

# Install Pandas: Python Data Analysis Library
pip3 install pandas

# Install Redis Python client
pip3 install direct-redis
