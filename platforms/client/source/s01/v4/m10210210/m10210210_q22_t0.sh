# Bash script (install_dependencies.sh)

#!/bin/bash

# Update package list
sudo apt-get update

# Install pip if not already installed for Python 3
sudo apt-get install -y python3-pip

# Install MongoDB driver (pymongo)
pip3 install pymongo

# Install Redis driver and direct_redis library for Python 3
pip3 install redis direct_redis

# Install pandas library for data manipulation
pip3 install pandas
