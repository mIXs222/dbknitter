#!/bin/bash

# Update the package lists
apt-get update

# Install Python and Pip (if not already installed)
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo
