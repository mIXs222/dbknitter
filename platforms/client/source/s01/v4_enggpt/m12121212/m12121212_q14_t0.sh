#!/bin/bash

# update package list
sudo apt-get update

# install pip for python3 if not already installed
sudo apt-get install -y python3-pip

# install MongoDB driver for python
pip3 install pymongo

# install Pandas library for python
pip3 install pandas

# install Redis library for python
# Assuming 'direct_redis' is a placeholder, otherwise install correct library
pip3 install redis
