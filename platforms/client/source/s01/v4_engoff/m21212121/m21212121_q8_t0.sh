# install_dependencies.sh

#!/bin/bash

# update package index
sudo apt-get update

# install Python and pip
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# install necessary python packages
pip3 install pymongo pandas redis

# Note: direct_redis is not a standard python package and cannot be installed via pip.
# You may need to include the direct_redis module in your project or install it from a provided source.
