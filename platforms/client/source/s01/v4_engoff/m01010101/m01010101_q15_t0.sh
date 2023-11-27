#!/bin/bash
# install_dependencies.sh

# It is assumed that Python3 and pip are already installed
# Update package lists
sudo apt update

# Install MongoDB driver for Python (pymongo)
pip install pymongo
