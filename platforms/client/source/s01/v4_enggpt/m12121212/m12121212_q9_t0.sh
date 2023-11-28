#!/bin/bash

# Ensure Python and pip are installed

# Update system package list
sudo apt-get update

# Install Python, both Python 3 and pip
sudo apt-get install -y python3 python3-pip

# Install necessary Python packages
pip3 install pymongo redis pandas direct-redis

# Note that additional steps might be needed if Python package dependencies are not met.
# If there are any permission issues while installing the packages, you might want to use a virtual environment or add `--user` to `pip3` command.
