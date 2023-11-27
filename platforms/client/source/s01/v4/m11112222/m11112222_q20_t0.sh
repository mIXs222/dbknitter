#!/bin/bash

# Update the repository and install Python pip package installer
apt-get update
apt-get install -y python3-pip

# Install necessary Python libraries
pip3 install pymongo pandas redis direct-redis
