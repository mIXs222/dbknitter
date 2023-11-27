#!/bin/bash

# Update package list and upgrade system packages
sudo apt-get update && sudo apt-get upgrade -y

# Install pip and MongoDB driver (pymongo)
sudo apt-get install -y python3-pip
pip3 install pymongo
