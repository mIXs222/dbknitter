#!/bin/bash

# Update and upgrade the package list
apt-get update && apt-get upgrade -y

# Install Python and pip
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo direct_redis pandas
