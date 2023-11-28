#!/bin/bash

# install_dependencies.sh

# Update repositories and install Python3 and pip3
sudo apt-get update
sudo apt-get install python3 python3-pip -y

# Install the required Python libraries
pip3 install pymysql pymongo
