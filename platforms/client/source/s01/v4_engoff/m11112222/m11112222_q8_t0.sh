# install_dependencies.sh

#!/bin/bash

# Update repositories and install Python and pip
sudo apt update
sudo apt install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas redis

# Install direct_redis via git (assuming it's a custom or third-party package not available in PyPI)
# Replace `git_repository_url` with the actual URL of the direct_redis repository
# git clone git_repository_url
# cd direct_redis
# python3 setup.py install
# cd ..

# If direct_redis is actually available through pip, uncomment the following line:
# pip3 install direct_redis
