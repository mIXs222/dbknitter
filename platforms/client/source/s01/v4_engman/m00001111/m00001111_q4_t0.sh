#!/bin/bash

# Ensure script is run as root
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit
fi

# Update system package list
apt-get update

# Install Python and pip if they are not already installed
apt-get install -y python3 python3-pip

# Install the required Python package using pip
pip3 install pymongo
