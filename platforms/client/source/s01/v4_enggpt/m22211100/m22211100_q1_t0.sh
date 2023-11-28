#!/bin/bash

# Update repositories
apt-get update

# Install Python if not already installed
apt-get install -y python3

# Install pip for Python3 if not already installed
apt-get install -y python3-pip

# Install pymysql using pip
pip3 install pymysql
