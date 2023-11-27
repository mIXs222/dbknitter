#!/bin/sh

# Update the packages.
sudo apt-get update

# Install pip for python3 if not yet installed.
sudo apt-get install -y python3-pip

# Install pymongo library.
pip3 install pymongo

# Install csv module.
pip3 install python-csv
