#!/bin/bash

# install_dependencies.sh

# Update the package list
apt-get update

# Install pip if it's not already installed
apt-get install -y python3-pip

# Install PyMySQL and pymongo
pip3 install pymysql pymongo
