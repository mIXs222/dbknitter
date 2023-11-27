#!/bin/bash

echo "Updating package lists for repositories and PPAs"
sudo apt-get update

echo "Install Python3 pip"
sudo apt-get install -y python3-pip

echo "Install MongoDB"
sudo apt-get install -y mongodb

echo "Install pymogo"
pip3 install pymongo

echo "Install numpy"
pip3 install numpy
