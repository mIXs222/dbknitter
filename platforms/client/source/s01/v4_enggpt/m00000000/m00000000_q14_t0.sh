#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python 3 and pip if not already present
sudo apt-get install -y python3 python3-pip

# Install the 'pymysql' library using pip
pip3 install pymysql
