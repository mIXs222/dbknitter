#!/bin/bash

# Update the package list
sudo apt-get update

# Upgrade packages
sudo apt-get upgrade -y

# Install pip and MongoDB driver (pymongo)
sudo apt-get install -y python3-pip
pip3 install pymongo
