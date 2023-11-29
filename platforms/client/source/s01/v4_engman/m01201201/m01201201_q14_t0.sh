# install_dependencies.sh

#!/bin/bash

# Update and install Python and pip
apt-get update -y
apt-get install -y python3 python3-pip

# Install Python MongoDB client library
pip3 install pymongo

# Install Pandas
pip3 install pandas

# Install DirectRedis
pip3 install direct_redis
