#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymongo
pip3 install pymongo

# Install direct_redis, which likely requires installing the git 
# because it seems to be a custom package not available in PyPi
# This is just a placeholder line assuming direct_redis is available in a git repository
# Substitute <git_repo_url> with the actual URL of the git repository
# git clone <git_repo_url>
# cd direct_redis
# python3 setup.py install
# cd ..

# Install pandas
pip3 install pandas
