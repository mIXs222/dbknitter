#!/bin/bash
# install_dependencies.sh

# Install Python and pip if they are not already installed
command -v python3 >/dev/null 2>&1 || {
    echo "Installing Python3..."
    sudo apt-get update
    sudo apt-get install python3 -y
}

command -v pip3 >/dev/null 2>&1 || {
    echo "Installing pip3..."
    sudo apt-get install python3-pip -y
}

# Install required Python libraries
echo "Installing the required Python libraries..."
pip3 install pandas pymysql direct-redis

echo "Dependencies have been installed."
