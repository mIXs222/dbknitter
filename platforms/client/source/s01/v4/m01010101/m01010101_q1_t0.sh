#!/bin/bash

# Ensure the script is run with sudo privileges
if [ "$EUID" -ne 0 ]
  then echo "Please run as root"
  exit
fi

# Update package lists
apt-get update

# Upgrade packages
apt-get upgrade -y

# Install pip for Python3
apt-get install python3-pip -y

# Install pymongo for connecting to MongoDB
pip3 install pymongo
