#!/bin/bash

# Update package list and upgrade pre-installed packages
sudo apt-get update
sudo apt-get -y upgrade

# Install pip for python package management
sudo apt-get install -y python3-pip

# Install python packages required for the script to run
pip3 install pymongo pandas
