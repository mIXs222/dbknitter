#!/bin/bash

# Update package list
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install MongoDB driver
pip3 install pymongo

# Install pandas
pip3 install pandas

# Here you would install the direct_redis package, however,
# direct_redis is not a standard package and cannot be located
# in public repositories, hence we assume it is provided otherwise.
# You should replace the line below with the correct installation
# method for direct_redis.

# pip3 install direct_redis

# Install CSV module (part of Python's standard library, so no action needed)
