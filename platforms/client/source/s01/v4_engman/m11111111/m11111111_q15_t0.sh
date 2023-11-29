#!/bin/bash

# Update package list
apt-get update

# Install pip and Python MongoDB driver (pymongo)
apt-get install -y python3-pip
pip3 install pymongo
