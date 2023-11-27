#!/bin/bash

# Update package list
apt-get update

# Install Python and pip (If needed)
apt-get install -y python3
apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql
