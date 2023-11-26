#!/bin/bash

# Update package list and upgrade existing packages
apt-get update -y && apt-get upgrade -y

# Install pip for Python 3
apt-get install python3-pip -y

# Install pymongo using pip
pip3 install pymongo
