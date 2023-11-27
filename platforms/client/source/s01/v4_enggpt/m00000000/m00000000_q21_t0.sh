#!/bin/bash

# Update the package list
apt-get update

# Install Python and pip (if they aren't already installed)
apt-get install python3 python3-pip -y

# Install the PyMySQL library
pip3 install pymysql
