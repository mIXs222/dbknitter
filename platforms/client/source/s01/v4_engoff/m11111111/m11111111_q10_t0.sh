# install_dependencies.sh

#!/bin/bash

# Update and install Python3 and pip3
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymongo Python package
pip3 install pymongo
