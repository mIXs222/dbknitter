# install_dependencies.sh

#!/bin/bash

# Update Package Lists
sudo apt-get update

# Install MongoDB Python Driver (pymongo)
sudo pip3 install pymongo

# Install Pandas for data manipulation
sudo pip3 install pandas

# Install Redis Python Client
# Assuming direct_redis is a specialized library, otherwise this will not work
# You may need to replace it with the appropriate package or method to install
sudo pip3 install git+https://github.com/???/direct_redis.git
