# install_dependencies.sh
#!/bin/bash

# Update the package index
apt-get update

# Install python3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install pymysql using pip
pip3 install pymysql
