# install_dependencies.sh

#!/bin/bash

# Update package list and upgrade
sudo apt-get update && sudo apt-get upgrade -y

# Install Python3 and PIP
sudo apt-get install python3 python3-pip -y

# Install Python libraries
pip3 install pymysql pymongo
