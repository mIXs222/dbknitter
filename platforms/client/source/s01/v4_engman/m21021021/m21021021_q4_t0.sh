#!/bin/bash

# Update package list
sudo apt-get update

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install pymongo
pip3 install pymongo

# Install pandas
pip3 install pandas

# Install custom direct_redis library (assuming it can be pip-installed, otherwise you need to provide the correct way to install it)
pip3 install direct_redis
