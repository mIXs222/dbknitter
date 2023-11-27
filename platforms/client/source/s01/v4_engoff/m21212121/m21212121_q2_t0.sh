#!/bin/bash

# Update package list
apt-get update

# Install Python and Pip if they are not installed
apt-get install -y python3 python3-pip

# Install required Python modules
pip3 install pymongo pandas direct_redis
