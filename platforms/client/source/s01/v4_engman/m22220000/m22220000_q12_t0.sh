#!/bin/bash

# Update the package list and upgrade existing packages
sudo apt-get update
sudo apt-get -y upgrade

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql
