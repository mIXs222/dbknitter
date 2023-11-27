#!/bin/bash

# Update the packages
sudo apt update && sudo apt upgrade -y

# Install Python and Pip
sudo apt install -y python3 pip

# Install Python MySQL connector and PyMongo
pip install mysql-connector-python pymongo
