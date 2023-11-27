# install_dependencies.sh
#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and PIP
sudo apt-get install -y python3 python3-pip

# Install pymongo using pip
pip3 install pymongo
