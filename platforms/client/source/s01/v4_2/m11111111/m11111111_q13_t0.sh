#!/bin/bash

# Update package lists
sudo apt-get update

# Install python3-pip and python3
sudo apt-get install -y python3 python3-pip

# Install pandas library
pip3 install pandas

# Install pymongo library
pip3 install pymongo
