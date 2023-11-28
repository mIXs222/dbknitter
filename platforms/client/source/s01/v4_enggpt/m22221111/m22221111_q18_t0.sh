#!/bin/bash

# Update package list
apt-get update

# Install Python 3 and pip
apt-get install -y python3 python3-pip

# Install pymongo package
pip3 install pymongo
