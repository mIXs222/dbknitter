# install_dependencies.sh
#!/bin/bash

# Update the package manager
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install the pymysql package
pip3 install pymysql
