#!/bin/bash

# Update package list and upgrade packages
apt-get update
apt-get -y upgrade

# Install pip for Python 3
apt-get install -y python3-pip

# Install Python MySQL library (pymysql)
pip3 install pymysql

# Install csv library is part of the Python standard library, so no need to install.
