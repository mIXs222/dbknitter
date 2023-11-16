#!/bin/bash

# Install Python3, Pip
sudo apt-get update
sudo apt-get upgrade
sudo apt-get -y install python3
sudo apt-get -y install python3-pip

# Install Redis, Pandas
pip install redis pandas
