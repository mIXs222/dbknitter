#!/bin/bash

# Update the system
sudo apt-get update

# Python3 Installation
sudo apt-get install python3.8 -y

# Pip Installation
sudo apt-get install python3-pip -y

# Install redis and pandas library
pip3 install redis pandas
