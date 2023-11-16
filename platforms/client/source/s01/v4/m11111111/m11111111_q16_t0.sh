#!/bin/bash
# install.sh

# Update repositories and install pip if it is not installed
apt-get update
apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo pandas
