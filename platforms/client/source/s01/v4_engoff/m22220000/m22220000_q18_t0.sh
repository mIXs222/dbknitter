#!/bin/bash
# Filename: install_dependencies.sh

# Updating the package index and installing python3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql Python package
pip3 install pymysql
