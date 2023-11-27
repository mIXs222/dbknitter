# install_dependencies.sh

#!/bin/bash

# Update the package list and install the Python 3 pip package
sudo apt-get update
sudo apt-get install python3-pip -y

# Install the pymysql library using pip
pip3 install pymysql
