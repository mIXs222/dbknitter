#!/bin/bash

# Update the list of available packages and their versions
apt-get update

# Install python3 and pip3 if not already installed
apt-get install -y python3 python3-pip

# Install the necessary Python packages
pip3 install pymongo direct_redis pandas
