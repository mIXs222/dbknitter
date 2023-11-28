#!/bin/bash

# install_dependencies.sh

# Update package list and upgrade existing packages
sudo apt-get update
sudo apt-get -y upgrade

# Install Python3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql
