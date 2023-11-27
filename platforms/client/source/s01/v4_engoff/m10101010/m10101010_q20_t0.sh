#!/bin/bash

# Update repositories and upgrade packages
sudo apt-get update
sudo apt-get upgrade -y

# Install Python 3 and Pip
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo
