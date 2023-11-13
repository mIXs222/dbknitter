#!/bin/bash

echo "Updating packages"
sudo apt-get update -y

echo "Installing pip for Python"
sudo apt-get install -y python3-pip

echo "Installing required Python packages"
pip3 install pymongo
pip3 install pandas
pip3 install numpy
pip3 install sqlalchemy

echo "Dependencies installation completed"
