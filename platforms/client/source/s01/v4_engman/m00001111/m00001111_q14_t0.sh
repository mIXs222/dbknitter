# install_dependencies.sh
#!/bin/bash

# Update the package lists
sudo apt-get update

# Install Python3 and PIP if they are not installed
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymysql pymongo
