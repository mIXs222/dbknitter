#!/bin/bash

# Update the package list
apt-get update

# Install Python and Pip if they're not installed
apt-get install -y python3 pip

# Install the necessary Python packages
pip install pymongo pandas direct_redis
