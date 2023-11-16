# install_dependencies.sh
#!/bin/bash

# Update package list
sudo apt-get update

# Upgrade existing packages
sudo apt-get upgrade -y

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install pymongo library
pip3 install pymongo
