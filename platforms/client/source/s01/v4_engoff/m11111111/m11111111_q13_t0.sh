# install_dependencies.sh
#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python3 and pip3
sudo apt-get install -y python3 python3-pip

# Install pymongo
pip3 install pymongo
