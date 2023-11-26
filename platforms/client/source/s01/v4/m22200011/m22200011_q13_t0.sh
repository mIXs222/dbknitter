#!/bin/bash
# install_dependencies.sh

# Update and install Python and pip if not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the Python libraries for MySQL and MongoDB
pip3 install pymysql pymongo
