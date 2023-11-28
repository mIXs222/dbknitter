#!/bin/bash

# Update package list
apt-get update

# Install Python3 and pip if not present
apt-get install -y python3
apt-get install -y python3-pip

# Install pymongo
pip3 install pymongo
