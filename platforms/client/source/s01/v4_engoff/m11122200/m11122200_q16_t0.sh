# File: install_dependencies.sh

#!/bin/bash

# Update repositories
sudo apt-get update

# Install Python3 if not installed
sudo apt-get install python3

# Install MongoDB Python Driver - pymongo
python3 -m pip install --upgrade pip
python3 -m pip install pymongo

# Install Redis Python Client and Pandas
python3 -m pip install redis pandas

# Note: Since the 'direct_redis.DirectRedis' client is not a standard library and its source is not provided,
# I have used the standard 'redis.StrictRedis' client instead. If 'direct_redis' were a custom or third-party package,
# you would need to include additional installation steps or provide the package with your deployment.
