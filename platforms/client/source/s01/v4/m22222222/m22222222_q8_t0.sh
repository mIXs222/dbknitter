# install_dependencies.sh

#!/bin/bash
# Update and install system packages required for Python3 and pip
apt-get update
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pandas redis direct_redis
