#!/bin/bash

# Install Python and Pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo redis pandas
