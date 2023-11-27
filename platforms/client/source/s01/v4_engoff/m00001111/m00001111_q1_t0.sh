#!/bin/bash

# Update package list
apt-get update

# Install pip for Python3 if not already installed
apt-get install -y python3-pip

# Install pymongo package
pip3 install pymongo
