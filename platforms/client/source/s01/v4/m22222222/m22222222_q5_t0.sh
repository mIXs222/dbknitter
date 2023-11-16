#!/bin/bash

# Install Python 3 and pip (if not already installed)
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Redis and required Python library for Pandas DataFrame
pip3 install pandas
pip3 install direct_redis
