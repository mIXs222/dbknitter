#!/bin/bash

# install_dependencies.sh

# Updating the package list
apt-get update

# Installing python3 and pip3
apt-get install -y python3
apt-get install -y python3-pip

# Installing PyMySQL
pip3 install PyMySQL
