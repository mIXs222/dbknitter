# install_dependencies.sh

#!/bin/bash

# Update package list
apt-get update

# Install Python and pip if not already installed
apt-get install -y python3 python3-pip

# Install Python MongoDB driver (pymongo)
pip3 install pymongo

# Install Python Redis driver (hiredis required by direct_redis)
pip3 install hiredis direct_redis

# Install pandas library
pip3 install pandas
