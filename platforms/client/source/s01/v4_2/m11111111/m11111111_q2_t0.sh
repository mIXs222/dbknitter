#! /bin/bash

# Update packages
apt-get update -y

# Upgrade packages
apt-get upgrade -y

# Install python3-pip
apt-get install -y python3-pip

# Install pymongo
pip3 install pymongo

# Install pandas
pip3 install pandas
