#!/bin/bash
# install_dependencies.sh

# Update package list and upgrade existing packages
apt-get update && apt-get upgrade -y

# Install pip and Python dev packages
apt-get install python3-pip python3-dev -y

# Install pymongo package
pip3 install pymongo

# Install direct_redis package
pip3 install direct_redis

# Install pandas package
pip3 install pandas
