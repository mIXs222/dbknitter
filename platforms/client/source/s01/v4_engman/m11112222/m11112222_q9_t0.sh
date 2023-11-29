#!/bin/bash
# save this as install_dependencies.sh

# Update packages and install Python and Pip if they're not already installed
sudo apt update
sudo apt install -y python3 python3-pip

# Install required Python libraries
pip3 install pymongo pandas redis
