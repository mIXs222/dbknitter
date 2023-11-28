# install_dependencies.sh
#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python 3 and pip, if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymongo pandas direct_redis
