#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install pymysql via pip
pip3 install pymysql
