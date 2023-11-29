#!/bin/bash

# Update package list and install Python3 and pip
apt-get update
apt-get install -y python3 python3-pip

# Install pymongo for MongoDB access
pip3 install pymongo
