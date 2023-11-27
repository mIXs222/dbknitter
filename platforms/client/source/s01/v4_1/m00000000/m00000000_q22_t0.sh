#!/bin/bash

# Update the package lists 
sudo apt-get update

# Install Python and PIP
sudo apt-get install -y python3.8
sudo apt-get install -y python3-pip

# Install the python libraries
pip3 install pandas
pip3 install PyMySQL
pip3 install pymongo
