# install.sh

#!/bin/bash
sudo apt-get update

# Install Python3 & PIP if they are not installed
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo
