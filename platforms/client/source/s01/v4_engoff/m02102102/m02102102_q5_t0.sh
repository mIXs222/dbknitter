# setup.sh

#!/bin/bash

# Update system packages
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install the required libraries
pip3 install pymysql pymongo pandas direct_redis

# Note: Depending on the environment, you may need to use sudo or adjust the package manager commands (e.g., using yum instead of apt-get)
