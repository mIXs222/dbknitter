#!/bin/bash

# Update the package index
apt-get update

# Install Python 3 and pip
apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql
