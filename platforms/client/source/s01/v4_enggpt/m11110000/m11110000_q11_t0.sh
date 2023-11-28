#!/bin/bash
# Install the required dependencies for the python script.

# Update the package index
apt-get update -y

# Install Python and Pip if not installed
apt-get install python3 -y
apt-get install python3-pip -y

# Install Python libraries
pip3 install pymysql pymongo
