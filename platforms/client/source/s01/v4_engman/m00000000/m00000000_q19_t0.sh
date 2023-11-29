# install_dependencies.sh
#!/bin/bash

# Update package list
apt-get update

# Install python3 and pip if not already installed
apt-get install python3 python3-pip -y

# Install pymysql using pip
pip3 install pymysql
