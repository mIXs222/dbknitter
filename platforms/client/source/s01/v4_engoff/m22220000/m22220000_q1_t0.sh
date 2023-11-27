# install_dependencies.sh
#!/bin/bash

# Update lists of packages
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql
