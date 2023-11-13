#!/bin/bash

# Install python and pip
sudo apt update
sudo apt install python3.8 python3-pip

# Install the required python packages
pip3 install pymongo pandas
