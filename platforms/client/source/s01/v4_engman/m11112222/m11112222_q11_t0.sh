#!/bin/bash

# Update the package list
apt-get update

# Install pip and Python development headers
apt-get install -y python3-pip python3-dev build-essential

# Install the required Python packages
pip3 install pymongo pandas direct-redis
