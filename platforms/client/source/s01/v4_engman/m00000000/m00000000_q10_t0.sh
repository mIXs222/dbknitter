#!/bin/bash

# Update system's package index.
sudo apt-get update -y

# Install Python if it is not installed.
command -v python3 &>/dev/null || {
    sudo apt-get install python3 -y
}

# Install pip if it is not installed.
command -v pip3 &>/dev/null || {
    sudo apt-get install python3-pip -y
}

# Install pymysql using pip.
pip3 install pymysql
