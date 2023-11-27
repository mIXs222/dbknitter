# install_dependencies.sh

#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and pip (if needed)
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install the pymysql library
pip3 install pymysql
