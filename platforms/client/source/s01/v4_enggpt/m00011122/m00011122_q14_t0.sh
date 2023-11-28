#!/bin/bash

# Update package lists
apt-get update

# Install pip for Python 3 if not already installed
apt-get install -y python3-pip

# Install the required Python Libraries
pip3 install pymysql pandas
