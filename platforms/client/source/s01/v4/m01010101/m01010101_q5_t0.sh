#!/bin/bash
# install_dependencies.sh

# Update and install system-wide dependencies
sudo apt-get update
sudo apt-get install -y python3 python3-pip libmysqlclient-dev

# Install Python dependencies using pip
pip3 install pymongo pymysql
