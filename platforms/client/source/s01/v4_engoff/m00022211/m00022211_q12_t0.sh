#!/bin/bash

# Ensure the script is executed with superuser privileges
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

# Update the package list
apt-get update

# Install Python 3 and PIP if they are not installed
apt-get install python3 python3-pip -y

# Install the required Python packages using pip
pip3 install pymongo
