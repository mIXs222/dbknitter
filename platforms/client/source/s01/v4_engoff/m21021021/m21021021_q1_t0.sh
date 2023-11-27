#!/bin/bash

# Update package list and install Python 3 if it's not available
if ! which python3 > /dev/null; then
   sudo apt-get update
   sudo apt-get install -y python3
fi

# Install pip for Python 3 if it's not available
if ! which pip3 > /dev/null; then
   sudo apt-get update
   sudo apt-get install -y python3-pip
fi

# Install csv module if it is not installed
pip3 show csv || sudo pip3 install csv

# Install pymongo for Mongo DB connection
pip3 show pymongo || sudo pip3 install pymongo
