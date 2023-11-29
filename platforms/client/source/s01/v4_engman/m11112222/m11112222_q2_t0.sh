#!/bin/bash

# Update repository and package list
sudo apt-get update

# Install Python pip if not installed
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymongo pandas redis direct-redis

# Install msgpack for pandas if not already installed (for read_msgpack functionality)
pip3 install msgpack-python
