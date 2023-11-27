# install_dependencies.sh

#!/bin/bash

# Update and install python3 and pip if not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql
