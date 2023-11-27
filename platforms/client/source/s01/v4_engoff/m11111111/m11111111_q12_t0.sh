# install_dependencies.sh
#!/bin/bash

# Updating repository and installing python3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Installing the pymongo package
pip3 install pymongo
