#!/bin/bash

# install_dependencies.sh
# Script to install dependencies for running the Python code

# Update package list
apt-get update

# Install Python3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install python mysql client
pip3 install pymysql

# Install python mongodb client
pip3 install pymongo
