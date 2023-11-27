#!/bin/bash

# install_dependencies.sh
# Update the package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install the pymongo package
pip3 install pymongo

# Install pandas package
pip3 install pandas

# Install redis package
pip3 install redis
