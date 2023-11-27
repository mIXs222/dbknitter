# install_dependencies.sh
#!/bin/bash

# Update the package manager
apt-get update

# Install Python and pip (if not already installed)
apt-get install -y python3 python3-pip

# Install pymysql using pip
pip3 install pymysql
