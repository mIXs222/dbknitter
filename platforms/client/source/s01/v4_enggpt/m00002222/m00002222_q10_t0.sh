#!/bin/bash

# Update the package index
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install the pymysql package
pip3 install pymysql

# Install pandas package
pip3 install pandas

# Install direct_redis package
pip3 install direct_redis
