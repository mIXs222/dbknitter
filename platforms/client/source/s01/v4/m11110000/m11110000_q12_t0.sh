#!/bin/bash

# Install Python and pip if not already installed (assumes Debian-based system)
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql using pip
pip3 install pymysql
