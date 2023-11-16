#!/bin/bash
echo "Updating Repositories"
sudo apt-get update 
echo "Installing Python3 Pip"
sudo apt-get install -y python3-pip 
echo "Installing Pandas"
pip3 install pandas 
echo "Installing Pymongo"
pip3 install pymongo 
