# install_dependencies.sh
#!/bin/bash

# Update the package index
sudo apt update

# Install Python 3 and Pip
sudo apt install -y python3 python3-pip

# Install MongoDB driver 'pymongo'
pip3 install pymongo

# Install Redis client 'direct_redis'
pip3 install direct_redis

# Install 'pandas'
pip3 install pandas
