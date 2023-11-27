# install_dependencies.sh
#!/bin/bash

# Update the package list
sudo apt update

# Install Python if it's not already installed
sudo apt install -y python3

# Install pip for Python3 if it's not already installed
sudo apt install -y python3-pip

# Install pymysql and pymongo via pip
pip3 install pymysql pymongo
