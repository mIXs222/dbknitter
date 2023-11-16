#!/bin/bash
set -e

# Update package list and upgrade all packages
sudo apt-get update
sudo apt-get -y upgrade

# Install pip if it's not already installed
sudo apt-get -y install python3-pip

# Install pymysql using pip
pip3 install pymysql
