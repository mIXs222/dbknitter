# install_dependencies.sh
#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python 3 if not already installed (optional)
# sudo apt-get install -y python3

# Install pip for Python 3 if not already installed
sudo apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymongo pandas direct_redis
