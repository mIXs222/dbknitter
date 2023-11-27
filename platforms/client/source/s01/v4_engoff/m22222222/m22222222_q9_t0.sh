# install_dependencies.sh

#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip and Python development packages, if they aren't already installed
sudo apt-get install -y python3-pip python3-dev

# Install Redis
sudo apt-get install -y redis-server

# Install pandas and direct_redis through pip
pip3 install pandas direct_redis
