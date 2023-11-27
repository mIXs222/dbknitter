# install_dependencies.sh

#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install necessary Python packages
pip3 install pymysql pandas

# Assuming direct_redis is a custom library for which there's no direct PyPI package, so you might need to install it from the source or your own repository.
# Replace the following line with the actual command to install `direct_redis` if it has different installation instructions.
pip3 install direct_redis 
