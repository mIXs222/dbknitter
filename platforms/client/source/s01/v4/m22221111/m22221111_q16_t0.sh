#!/bin/bash

# Update system package index
sudo apt-get update

# Install the dependencies
sudo apt-get install -y python3-pip
pip3 install pandas
pip3 install pymongo
pip3 install redis
pip3 install direct-redis
