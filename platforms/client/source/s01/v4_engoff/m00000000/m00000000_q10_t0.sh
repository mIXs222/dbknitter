#!/bin/bash

# Update package lists
apt-get update

# Upgrade packages
apt-get upgrade -y

# Install Python and pip if not installed
apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql

# Install csv for Python if needed (it's part of the Python standard library, so installation is typically unnecessary)
