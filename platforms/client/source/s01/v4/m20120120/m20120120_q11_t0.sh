# install_dependencies.sh

#!/bin/bash

# Update package lists
apt-get update

# Install Python 3 pip (if not installed)
apt-get install -y python3-pip

# Install the necessary Python libraries
pip3 install pymysql pandas redis direct_redis
