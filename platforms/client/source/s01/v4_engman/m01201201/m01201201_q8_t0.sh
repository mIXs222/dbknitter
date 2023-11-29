#!/bin/bash

# Update and install pip and Python headers
sudo apt-get update
sudo apt-get install -y python3-pip python3-dev

# Install required Python packages
pip3 install pymysql pymongo pandas

# Since direct_redis is a fictitious module for this example, we assume it is available somehow.
# Usually, it could be installed using a package manager like pip if it existed.
#pip3 install direct_redis
