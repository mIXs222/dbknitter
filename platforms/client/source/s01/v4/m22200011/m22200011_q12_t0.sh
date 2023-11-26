# install.sh

#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python3 and pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install the pymongo Python package
pip3 install pymongo
