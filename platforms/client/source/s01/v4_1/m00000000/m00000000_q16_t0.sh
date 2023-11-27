#!/bin/bash

# update package lists
apt-get update -y

# Get python package manager - pip
apt-get install -y python3-pip

# Install Python libraries 
pip3 install pandas
pip3 install PyMySQL
pip3 install pymongo
pip3 install direct-redis
