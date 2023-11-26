# install_dependencies.sh

#!/bin/bash

# Install Python and pip if they are not already installed
sudo apt update
sudo apt install -y python3 python3-pip

# Install pymysql and pymongo Python packages
pip3 install pymysql pymongo
