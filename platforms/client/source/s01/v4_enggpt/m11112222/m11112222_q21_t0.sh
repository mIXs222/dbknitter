# install_dependencies.sh

#!/bin/bash

# Update package lists
apt-get update

# Install MongoDB dependencies
apt-get install -y mongodb-clients

# Install Redis dependencies
apt-get install -y redis-tools

# Install Python and pip for Python packages
apt-get install -y python3 python3-pip

# Install Python packages
pip3 install pymongo pandas redis direct_redis
