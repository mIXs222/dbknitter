#!/bin/bash

# install_dependencies.sh

# Update the package index
sudo apt-get update

# Install pip for Python 3 if not already installed
sudo apt-get install -y python3-pip

# Install PyMySQL
pip3 install PyMySQL

# Install PyMongo
pip3 install pymongo
