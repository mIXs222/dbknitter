#!/bin/bash

# Update the package list
apt-get update

# Install Python 3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install the necessary Python packages
pip3 install pymongo direct_redis pandas
