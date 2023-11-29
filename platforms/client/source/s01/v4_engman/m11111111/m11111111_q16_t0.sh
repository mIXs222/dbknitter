# install_dependencies.sh

#!/bin/bash

# Update package index
sudo apt-get update

# Install pip if not already installed
sudo apt-get install -y python3-pip

# Install pymongo
pip3 install pymongo
