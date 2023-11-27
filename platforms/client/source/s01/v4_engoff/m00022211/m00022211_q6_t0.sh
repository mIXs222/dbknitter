# install_dependencies.sh

#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python and pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install the required Python packages using pip
pip3 install pymongo
