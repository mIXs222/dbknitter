#!/bin/bash

# Update package list
apt-get update

# Install Python 3 and pip (if not already installed)
apt-get install python3 python3-pip -y

# Install pymysql
pip3 install pymysql
