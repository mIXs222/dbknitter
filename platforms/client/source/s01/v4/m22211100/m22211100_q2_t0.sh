# install_dependencies.sh

#!/bin/bash

# Update package list
sudo apt-get update

# Install pip for Python if not already installed
sudo apt-get install -y python3-pip

# Install MongoDB driver and Redis driver for Python
pip3 install pymongo
pip3 install direct_redis
pip3 install pandas
