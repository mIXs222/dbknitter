# install_dependencies.sh
#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python3 and pip (Python package installer)
sudo apt-get install -y python3 python3-pip

# Install pymongo using pip
pip3 install pymongo
