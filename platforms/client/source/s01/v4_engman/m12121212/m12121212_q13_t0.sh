#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and Pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install the pymongo library
pip3 install pymongo

# Install the redis library
pip3 install redis

# Install pandas
pip3 install pandas
