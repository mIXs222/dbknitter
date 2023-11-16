#!/bin/bash

#Update packages
sudo apt-get update

# Install Python and pip
sudo apt install -y python3.8 python3-pip

# Install necessary Python packages
pip3 install pymongo
pip3 install dnspython
