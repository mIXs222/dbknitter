#!/bin/bash

# Update package list
apt-get update

# Install Python3, pip and MongoDB driver for Python
apt-get install -y python3 python3-pip
pip3 install pymongo
