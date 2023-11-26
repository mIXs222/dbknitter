#!/bin/bash
# install_dependencies.sh

# Update package lists
sudo apt-get update

# Install Python and pip if necessary
sudo apt-get install python3 python3-pip -y

# Install required Python packages
pip3 install pymongo pandas redis direct-redis
