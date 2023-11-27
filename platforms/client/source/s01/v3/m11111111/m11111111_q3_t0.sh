#!/bin/bash
echo "Installing Python3 and Pip3"
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip
echo "Installing Python dependencies"
pip3 install pymongo
pip3 install mysql-connector-python
echo "Done installing dependencies"
