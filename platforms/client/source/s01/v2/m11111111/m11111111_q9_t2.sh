#!/bin/bash
echo "Updating packages"
sudo apt-get update

echo "Installing Python3"
sudo apt-get install -y python3

echo "Installing pip for Python3"
sudo apt-get install -y python3-pip

echo "Installing pymongo library"
pip3 install pymongo
