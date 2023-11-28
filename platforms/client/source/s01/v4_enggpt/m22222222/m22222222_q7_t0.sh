# install_dependencies.sh
#!/bin/bash

# Update package list
sudo apt-get update

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install Redis client for Python
pip3 install direct-redis pandas
