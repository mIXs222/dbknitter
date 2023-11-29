# install_dependencies.sh

#!/bin/bash

# Update package list
apt update

# Install pip for Python 3
apt install -y python3-pip

# Install Python MongoDB driver
pip3 install pymongo
