# filename: install_dependencies.sh

#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install Pandas and direct_redis packages using pip
pip3 install pandas direct-redis
