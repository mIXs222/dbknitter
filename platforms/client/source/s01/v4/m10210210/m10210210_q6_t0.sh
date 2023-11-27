# file: install_dependencies.sh
#!/bin/bash
# Update and Install Python3 and pip
apt-get update
apt-get install -y python3 python3-pip

# Install pymysql Python library
pip3 install pymysql
