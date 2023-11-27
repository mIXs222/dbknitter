#!/bin/bash
set -e

# Update and install Python3 and pip, if not already present
if ! command -v python3 &> /dev/null; then
    sudo apt-get update
    sudo apt-get install -y python3
fi

if ! command -v pip3 &> /dev/null; then
    sudo apt-get install -y python3-pip
fi

# Install pymysql library
pip3 install pymysql
