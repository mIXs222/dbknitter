# install_dependencies.sh

#!/bin/bash

# Update package list and install pip for Python3
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymongo for interacting with MongoDB
pip3 install pymongo
