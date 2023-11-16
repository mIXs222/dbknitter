# install_dependencies.sh
#!/bin/bash

# Update package list
sudo apt-get update

# Install pip for Python package management
sudo apt-get install -y python3-pip

# Install Python Redis client and Pandas library using pip
pip3 install direct-redis pandas
