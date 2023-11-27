#!/bin/bash

# install_dependencies.sh

# Update package lists
sudo apt-get update

# Install Python if it's not already installed
sudo apt-get install -y python3

# Install pip for Python package management, if it's not already installed
sudo apt-get install -y python3-pip

# Install the pymysql package
pip3 install pymysql
