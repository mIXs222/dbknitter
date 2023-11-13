#!/bin/bash

# Update package lists
sudo apt-get update -y

# Install pip if not already installed
which pip > /dev/null || { echo installing pip; sudo apt-get install -y python3-pip; }

# List of python packages
python_packages=(
    "mysql-connector-python"
    "pandas"
)

# Install python packages
for package in "${python_packages[@]}"; do
    pip3 install "$package"
done
